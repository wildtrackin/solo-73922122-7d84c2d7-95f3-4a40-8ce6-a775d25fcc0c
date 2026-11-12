"""Modbus TCP 采集客户端。

设计要点
========
* 进程内所有调用共用一条 TCP 连接（懒连接）。
* 支持流水线（pipelining）：多笔读取可同时在途，发送下一笔不必等待上一笔应答，
  仅用一把短临界区锁串行化“写 socket + drain”动作，不限制在途请求数量。
* 严格校验 MBAP 头与响应内容，按事务标识符（TID）匹配请求，天然支持乱序响应；
  TID 仅在请求完成/失败后才回收复用，迟到且找不到归属 TID 的报文一律丢弃。
* 每笔请求的超时窗口从调用 ``writer.write`` 写出请求那一刻起算，统一覆盖
  「drain 发送缓冲」与「等待应答」两个阶段；即使 ``drain()`` 长时间阻塞也会
  按时失败，不会把发送锁和其他回路的采样一起挂死。
* 发送/应答超时或发送期间被取消，都会关闭**所属代（generation）**的连接：
  在途读取随之失败、发送锁得以释放，下一次调用自动重连。
* 旧连接的 drain 迟到完成或迟到报错只作用于它自己那一代连接：代际不符的
  拆除动作一律忽略，既不会关闭新连接，也不会清掉新连接上的待响应请求。
* 发送期间（持锁、drain 中）被取消时，必定清掉自身待响应项、释放发送锁并
  关闭所属连接，不遗留待响应项、不遗留被占住的发送锁。
* 连接断开后下一次调用自动重连；每代连接带 generation 编号，旧连接的读取循环
  即使收到残余报文也无法触碰新连接的待响应表，实现旧任务与新连接完全隔离。
* 不缓存任何读数，每次调用都发起真实的寄存器读取。
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
import struct
import time
from dataclasses import dataclass


class ModbusErrorCode(str, enum.Enum):
    """采集失败的错误类别。"""

    CONNECT_FAILED = "connect_failed"
    TIMEOUT = "timeout"
    CONNECTION_LOST = "connection_lost"
    ILLEGAL_ADDRESS = "illegal_address"
    EXCEPTION_RESPONSE = "exception_response"
    PROTOCOL_ERROR = "protocol_error"


class ModbusError(Exception):
    """所有采集失败统一抛出的异常。"""

    def __init__(self, code: ModbusErrorCode, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


# ---- Modbus 协议常量 -------------------------------------------------------

_READ_HOLDING_REGISTERS = 0x03
_EXCEPTION_BIT = 0x80
_MBAP_SIZE = 7
# 正常响应正文（MBAP 之后）：功能码 1B + 字节计数 1B + 单寄存器值 2B = 4B
_RESPONSE_BODY_SIZE = 4
_MIN_FRAME_SIZE = _MBAP_SIZE + 2
_TID_MASK = 0xFFFF
_MAX_REGISTER_ADDRESS = 0xFFFF


@dataclass(slots=True)
class _Pending:
    """一笔在途请求。"""

    tid: int
    generation: int
    future: asyncio.Future[int]
    sent_at: float


def _consume_future_exception(future: asyncio.Future) -> None:
    """消费 future 上无人 await 的异常，避免事件循环的未检索异常告警。"""
    if future.done() and not future.cancelled():
        with contextlib.suppress(Exception):
            future.exception()


# 持有后台回收任务的强引用，防止被垃圾回收中途消失。
_BACKGROUND_TASKS: set[asyncio.Task] = set()


class ModbusTcpClient:
    """单连接、支持并发在途读取的 Modbus TCP 主站。"""

    def __init__(
        self,
        host: str,
        port: int,
        unit_id: int = 1,
        timeout: float = 2.0,
    ) -> None:
        if not 0 <= unit_id <= 255:
            raise ValueError("unit_id 必须在 0~255 之间")
        self._host = host
        self._port = port
        self._unit_id = unit_id
        self._timeout = timeout

        # 连接状态（仅在事件循环线程内访问，无需额外加锁）。
        self._writer: asyncio.StreamWriter | None = None
        self._reader_task: asyncio.Task | None = None
        self._generation = 0
        # TID -> 在途请求。读取循环与发送路径都会访问，
        # 配合 _write_lock 在短临界区内修改。
        self._pending: dict[int, _Pending] = {}
        self._next_tid = 1
        # 仅串行化 socket 写入与连接建立，不要求“收到应答才能发下一笔”。
        self._write_lock = asyncio.Lock()
        self._connect_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # 对外 API
    # ------------------------------------------------------------------
    async def read_holding_register(self, address: int) -> tuple[int, float]:
        """读取单个保持寄存器，返回 ``(原始有符号16位值, 接收时间戳)``。

        任何失败都抛出 :class:`ModbusError`，不缓存、不重试。
        """
        if not 0 <= address <= _MAX_REGISTER_ADDRESS:
            raise ModbusError(
                ModbusErrorCode.ILLEGAL_ADDRESS,
                f"采集失败：寄存器地址 {address} 超出 0~65535 范围",
            )

        await self._ensure_connected()

        loop = asyncio.get_running_loop()

        async with self._write_lock:
            # 持锁期间再次确认连接仍然有效（可能刚刚被其他任务拆除）。
            if self._writer is None or self._writer.is_closing():
                raise ModbusError(
                    ModbusErrorCode.CONNECTION_LOST,
                    "采集失败：连接已断开，正在等待自动重连",
                )

            # TID 分配、待响应登记与请求构造都放在锁内：保证并发请求拿到互不
            # 相同的 TID。若在锁外分配，多笔并发请求会撞到同一个 TID，在拆除/
            # 重连路径上互相串台（owner_tid 误匹配、待响应项被错误清理）。
            tid = self._alloc_tid()
            future: asyncio.Future[int] = loop.create_future()
            # 本笔请求所属的连接代。后续任何清理都必须核对代际，
            # 防止旧连接的迟到事件误关新连接、误清新请求。
            generation = self._generation
            pending = _Pending(
                tid=tid,
                generation=generation,
                future=future,
                sent_at=loop.time(),
            )
            request = self._build_request(tid, address)
            self._pending[tid] = pending

            # 超时窗口从“写出请求”这一刻起算，覆盖 drain 与等待应答全程；
            # drain 若长期阻塞也会在同一截止时刻失败，不会占住发送锁挂死他人。
            deadline = loop.time() + self._timeout
            self._writer.write(request)
            try:
                # drain 可能因对端不接收而长期阻塞，必须受同一超时约束。
                await asyncio.wait_for(
                    self._writer.drain(),
                    timeout=max(0.0, deadline - loop.time()),
                )
            except asyncio.CancelledError:
                # 必须先于 TimeoutError 捕获：超时与任务取消同时发生时，
                # wait_for 取消 drain 的瞬间可能把 CancelledError 抛给本任务，
                # 此时按“发送期间取消”处理，而不是误判成超时/连接断开。
                # 关闭所属代连接（代际核对，避免误伤新连接），随后原样向上抛，
                # 不遗留待响应项、不占住发送锁。
                await self._tear_down(
                    ModbusErrorCode.CONNECTION_LOST,
                    "采集失败：发送请求期间调用被取消，连接已关闭等待重连",
                    generation=generation,
                    owner_tid=tid,
                )
                raise
            except asyncio.TimeoutError:
                # 发送（drain）阶段超时：按超时失败并拆除所属代连接，
                # 释放发送锁，使在途读取失败、后续调用自动重连。
                await self._tear_down(
                    ModbusErrorCode.TIMEOUT,
                    f"采集失败：事务 {tid} 发送 {self._timeout:g} 秒仍未排空"
                    "（drain 阻塞，控制器接收过慢）",
                    generation=generation,
                    owner_tid=tid,
                )
                raise ModbusError(
                    ModbusErrorCode.TIMEOUT,
                    f"采集失败：寄存器 {address} 读取超时（{self._timeout:g}s 未响应）",
                ) from None
            except (ConnectionError, RuntimeError, OSError) as exc:
                # drain 迟到报错同样只作用于所属代连接；若该代已被拆除
                # （_tear_down 内部代际不符直接忽略），不得波及新连接。
                await self._tear_down(
                    ModbusErrorCode.CONNECTION_LOST,
                    f"采集失败：发送请求时连接断开（{exc.__class__.__name__}）",
                    generation=generation,
                    owner_tid=tid,
                )
                raise ModbusError(
                    ModbusErrorCode.CONNECTION_LOST,
                    f"采集失败：发送请求时连接断开（{exc.__class__.__name__}）",
                ) from exc

        try:
            # 等待应答使用同一个截止时刻：总预算（write 起 2s）不随阶段重置。
            raw_value = await asyncio.wait_for(
                future, timeout=max(0.0, deadline - loop.time())
            )
        except asyncio.TimeoutError:
            # 2 秒未响应：本笔失败，拆除所属代连接，其余在途读取一并失败。
            await self._tear_down(
                ModbusErrorCode.TIMEOUT,
                f"采集失败：事务 {tid} 发出后 {self._timeout:g} 秒未收到响应",
                generation=generation,
                owner_tid=tid,
            )
            raise ModbusError(
                ModbusErrorCode.TIMEOUT,
                f"采集失败：寄存器 {address} 读取超时（{self._timeout:g}s 未响应）",
            ) from None
        except asyncio.CancelledError:
            # 调用方在等待应答期间取消：仅清理自身在途条目，不影响其他请求，
            # 同样核对代际，绝不触碰新连接的待响应表。
            if self._generation == generation:
                self._pending.pop(tid, None)
                self._release_tid(tid)
            raise
        else:
            self._release_tid(tid)
            return raw_value, time.time()

    async def aclose(self) -> None:
        """关闭连接并令全部在途读取失败（应用停机时调用）。"""
        await self._tear_down(
            ModbusErrorCode.CONNECTION_LOST, "采集失败：采集客户端正在关闭"
        )

    # ------------------------------------------------------------------
    # 连接管理
    # ------------------------------------------------------------------
    async def _ensure_connected(self) -> None:
        if self._writer is not None and not self._writer.is_closing():
            return
        async with self._connect_lock:
            if self._writer is not None and not self._writer.is_closing():
                return
            # 防御性清理上一代残余状态。
            await self._close_writer()
            self._pending.clear()
            self._generation += 1
            generation = self._generation
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self._host, self._port),
                    timeout=self._timeout,
                )
            except (OSError, asyncio.TimeoutError) as exc:
                self._writer = None
                raise ModbusError(
                    ModbusErrorCode.CONNECT_FAILED,
                    f"采集失败：无法连接控制器 {self._host}:{self._port}"
                    f"（{exc.__class__.__name__}）",
                ) from exc
            self._writer = writer
            self._reader_task = asyncio.create_task(
                self._reader_loop(reader, generation),
                name=f"modbus-reader-gen{generation}",
            )

    async def _tear_down(
        self,
        code: ModbusErrorCode,
        message: str,
        generation: int | None = None,
        owner_tid: int | None = None,
    ) -> None:
        """拆除当前连接，并让其上全部在途读取失败。

        ``generation`` 给定时仅当它仍是当前连接代才执行拆除。旧连接的 drain
        迟到完成/报错或旧任务异常会带着旧代际调用本方法，此时直接忽略，
        保证新连接与新连接上的待响应请求不被触碰。``generation=None`` 表示
        不做代际核对（应用停机等无条件拆除场景）。

        ``owner_tid`` 是触发本次拆除的那笔请求自身的 TID：它的 future 不在此
        置失败，由所属任务自行抛出准确错误（超时/连接断开/取消），避免被这里
        统一设置的异常经 ``wait_for`` 反向覆盖成别的错误码。
        """
        if generation is not None and generation != self._generation:
            return

        reader_task = self._reader_task
        writer = self._writer
        pending_items = list(self._pending.items())
        self._pending.clear()
        self._writer = None
        self._reader_task = None

        for tid, item in pending_items:
            self._release_tid(tid)
            if tid == owner_tid:
                # 触发拆除的任务自己负责报错，跳过它的 future。
                continue
            if not item.future.done():
                item.future.set_exception(ModbusError(code, message))
                # 防御性消费：确保任何无人 await 的异常都不会触发事件循环的
                # “exception was never retrieved”告警；对正常 await 的 future 无副作用。
                item.future.add_done_callback(_consume_future_exception)

        # 显式关闭本代 socket，不能依赖 _close_writer（self._writer 已置空）。
        if writer is not None:
            with contextlib.suppress(Exception):
                writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
        if reader_task is not None:
            # 取消读取循环，但绝不在触发拆除的任务里直接 await 它：await 一个被
            # cancel 的任务会把 CancelledError 重新投递进当前任务（3.11 uncancel
            # 计数），把本应抛出的准确错误（如 TIMEOUT）顶掉。改为后台任务吸收
            # 其结束：读取循环退出路径已带代际核对，旧 reader 不会触碰新连接，
            # socket 关闭也会让其 readexactly 尽快返回。
            reader_task.cancel()
            self._drain_reader_task(reader_task)

    def _drain_reader_task(self, reader_task: asyncio.Task) -> None:
        """在后台等待被取消的读取循环结束，避免任务悬挂与未检索异常。

        与触发拆除的调用解耦：不把 CancelledError 投递回业务任务。
        """
        async def _absorb() -> None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await reader_task

        task = asyncio.create_task(_absorb(), name="modbus-reader-reap")
        _BACKGROUND_TASKS.add(task)
        task.add_done_callback(_BACKGROUND_TASKS.discard)

    async def _close_writer(self) -> None:
        writer = self._writer
        if writer is not None:
            with contextlib.suppress(Exception):
                writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    # ------------------------------------------------------------------
    # 接收循环：分片重组、粘包拆分、乱序匹配
    # ------------------------------------------------------------------
    async def _reader_loop(
        self, reader: asyncio.StreamReader, generation: int
    ) -> None:
        try:
            while True:
                mbap = await reader.readexactly(_MBAP_SIZE)
                tid, protocol_id, length, unit_id = struct.unpack(">HHHB", mbap)
                if length < 2:
                    raise ModbusError(
                        ModbusErrorCode.PROTOCOL_ERROR,
                        f"采集失败：MBAP 长度字段非法（length={length}）",
                    )
                # 限制单帧最大尺寸，防止异常 length 导致异常内存占用。
                if length > 256:
                    raise ModbusError(
                        ModbusErrorCode.PROTOCOL_ERROR,
                        f"采集失败：MBAP 长度字段超出上限（length={length}）",
                    )
                body = await reader.readexactly(length - 1)
                frame_generation = generation
                self._handle_frame(
                    tid, protocol_id, unit_id, body, frame_generation
                )
        except asyncio.IncompleteReadError:
            # 对端关闭 / 复位：连接断开。
            await self._on_connection_lost(
                generation, "采集失败：控制器关闭了连接（断线）"
            )
        except asyncio.CancelledError:
            raise
        except ModbusError as exc:
            await self._on_connection_lost(generation, exc.message)
        except (OSError, RuntimeError) as exc:
            await self._on_connection_lost(
                generation,
                f"采集失败：连接读取错误（{exc.__class__.__name__}）",
            )

    def _handle_frame(
        self,
        tid: int,
        protocol_id: int,
        unit_id: int,
        body: bytes,
        generation: int,
    ) -> None:
        # 旧连接（上一代）的残余报文：直接丢弃，绝不触碰新连接的待响应表。
        if generation != self._generation or self._reader_task is None:
            return

        pending = self._pending.get(tid)
        if pending is None:
            # 迟到响应（原请求已超时失败、TID 已回收）：不得用于后续采样，丢弃。
            return

        # ---- MBAP / 响应内容逐字段核对 ----
        if protocol_id != 0:
            self._fail_and_drop(
                pending,
                ModbusErrorCode.PROTOCOL_ERROR,
                f"采集失败：协议标识符应为 0，实际为 {protocol_id}",
            )
            return
        if unit_id != self._unit_id:
            self._fail_and_drop(
                pending,
                ModbusErrorCode.PROTOCOL_ERROR,
                f"采集失败：单元标识符应为 {self._unit_id}，实际为 {unit_id}",
            )
            return
        function_code = body[0]
        if function_code & _EXCEPTION_BIT:
            # Modbus 异常响应：功能码 | 0x80，后跟 1 字节异常码，正文共 2 字节。
            if len(body) != 2:
                self._fail_and_drop(
                    pending,
                    ModbusErrorCode.PROTOCOL_ERROR,
                    f"采集失败：异常响应正文长度应为 2，实际为 {len(body)}",
                )
                return
            exception_code = body[1]
            self._fail_and_drop(
                pending,
                ModbusErrorCode.EXCEPTION_RESPONSE,
                f"采集失败：控制器返回异常响应（功能码 0x{function_code:02X}，"
                f"异常码 0x{exception_code:02X}）",
            )
            return
        if function_code != _READ_HOLDING_REGISTERS:
            self._fail_and_drop(
                pending,
                ModbusErrorCode.PROTOCOL_ERROR,
                f"采集失败：功能码应为 0x03，实际为 0x{function_code:02X}",
            )
            return
        if len(body) != _RESPONSE_BODY_SIZE:
            self._fail_and_drop(
                pending,
                ModbusErrorCode.PROTOCOL_ERROR,
                f"采集失败：响应正文长度应为 {_RESPONSE_BODY_SIZE}，实际为 {len(body)}",
            )
            return

        byte_count = body[1]
        if byte_count != 2:
            self._fail_and_drop(
                pending,
                ModbusErrorCode.PROTOCOL_ERROR,
                f"采集失败：字节计数应为 2，实际为 {byte_count}",
            )
            return

        (raw_unsigned,) = struct.unpack(">H", body[2:4])
        # 有符号 16 位整数转换。
        raw_value = raw_unsigned - 0x10000 if raw_unsigned >= 0x8000 else raw_unsigned
        self._pending.pop(tid, None)
        if not pending.future.done():
            pending.future.set_result(raw_value)

    def _fail_and_drop(
        self,
        pending: _Pending,
        code: ModbusErrorCode,
        message: str,
    ) -> None:
        self._pending.pop(pending.tid, None)
        self._release_tid(pending.tid)
        if not pending.future.done():
            pending.future.set_exception(ModbusError(code, message))

    async def _on_connection_lost(self, generation: int, message: str) -> None:
        # 旧连接的读取循环报告的事件不得影响新连接。
        if generation != self._generation:
            return
        await self._tear_down(ModbusErrorCode.CONNECTION_LOST, message)

    # ------------------------------------------------------------------
    # 帧组装与 TID 管理
    # ------------------------------------------------------------------
    def _build_request(self, tid: int, address: int) -> bytes:
        # MBAP: TID(2) 协议ID(2)=0 长度(2) 单元号(1)；PDU: 功能码(1) 地址(2) 数量(2)=1
        pdu = struct.pack(">BHH", _READ_HOLDING_REGISTERS, address, 1)
        return struct.pack(">HHHB", tid, 0, len(pdu) + 1, self._unit_id) + pdu

    def _alloc_tid(self) -> int:
        # 跳过仍在途的 TID，避免复用造成串值。
        for _ in range(_TID_MASK):
            tid = self._next_tid
            self._next_tid = 1 if self._next_tid >= _TID_MASK else self._next_tid + 1
            if tid not in self._pending:
                return tid
        raise ModbusError(
            ModbusErrorCode.PROTOCOL_ERROR, "采集失败：在途事务过多，无可用 TID"
        )

    def _release_tid(self, tid: int) -> None:
        # TID 编号空间循环使用；当前模型下分配时即检查在途表，此处保留扩展点。
        _ = tid
