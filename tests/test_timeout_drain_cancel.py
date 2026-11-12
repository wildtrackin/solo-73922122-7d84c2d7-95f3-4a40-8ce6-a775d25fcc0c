"""超时窗口 / drain 阻塞 / 发送取消 / 旧发送任务迟到 的回归测试。

这些场景无法用真实回环 socket 稳定复现（drain 是否阻塞取决于内核缓冲），
因此用可控的假 writer/reader 精确驱动：

* :class:`_BlockingWriter` —— drain 可永久阻塞、可被解除、可让迟到的 drain
  返回成功或抛出连接错误，并记录 close 调用，用于断言“关的是哪一代连接”。
* :class:`_ScriptedReader` —— 按队列吐出固定字节，模拟读取循环收到的报文。
"""

from __future__ import annotations

import asyncio
import contextlib
import struct

import pytest

from app.modbus_client import ModbusError, ModbusErrorCode, ModbusTcpClient
from simulator import app as simulator

DEFAULT_VALUES = [-180, -175, 50, 235, -200, 300, -45, 105]


class _BlockingWriter:
    """可控 drain 的 StreamWriter 替身。"""

    def __init__(self) -> None:
        # 默认放行；block_drain() 后 drain 会挂起，直到 release_drain()/close()。
        self._drain_blocked = asyncio.Event()
        self._drain_blocked.set()
        # 每次 drain 调用独立的“已开始”闸门，避免上一笔的残留状态误判。
        self._drain_started: asyncio.Event | None = None
        self._drain_error: Exception | None = None
        self._closed = False
        self._closing = False
        self.written: list[bytes] = []
        self.close_count = 0
        # 单调递增的 drain 完成计数，配合事件实现“等够 N 次 drain 完成”。
        self.drains_completed = 0
        self._idle = asyncio.Event()
        self._idle.set()

    # --- 测试驱动钩子 ---
    def block_drain(self) -> None:
        self._idle.clear()
        self._drain_blocked.clear()

    def release_drain(self, error: Exception | None = None) -> None:
        """解除 drain 阻塞；error 非空时让正在等待的 drain 以该错误结束。"""
        self._drain_error = error
        self._drain_blocked.set()

    async def wait_drain_started(self) -> None:
        """等待下一次 drain 真正进入（每次调用拿到的都是新闸门）。"""
        self._drain_started = asyncio.Event()
        await self._drain_started.wait()

    async def wait_drains_done(self, count: int = 1) -> None:
        """等待累计 ``count`` 次 drain 真正返回，避免基于 sleep 的竞态。"""
        while self.drains_completed < count:
            await self._idle.wait()
            self._idle.clear()
        self._idle.set()

    # --- StreamWriter 接口 ---
    def write(self, data: bytes) -> None:
        self.written.append(data)

    async def drain(self) -> None:
        if self._drain_started is not None:
            self._drain_started.set()
        self._idle.clear()
        try:
            # 先让出一次控制权，模拟真实 drain 的挂起点；
            # 随后若闸门关闭则持续阻塞，直到被 release/close 唤醒。
            await asyncio.sleep(0)
            await self._drain_blocked.wait()
            if self._drain_error is not None:
                raise self._drain_error
        finally:
            self.drains_completed += 1
            self._idle.set()

    def close(self) -> None:
        self.close_count += 1
        self._closing = True
        self._closed = True
        # 关闭 socket 必须能让卡住的 drain 解脱，避免发送任务永久挂起。
        self._drain_blocked.set()

    def is_closing(self) -> bool:
        return self._closing

    async def wait_closed(self) -> None:
        self._closed = True


class _ScriptedReader:
    """按预设字节队列顺序满足 readexactly 的 StreamReader 替身。

    无数据时阻塞在事件上（模拟真实 StreamReader 等待内核数据），
    绝不能用 ``sleep(0)`` 忙等，否则会饿死同循环内的其他任务。
    """

    def __init__(self, chunks: list[bytes] | None = None) -> None:
        self._buffer = b"".join(chunks or [])
        self._available = asyncio.Event()
        if self._buffer:
            self._available.set()

    def feed(self, data: bytes) -> None:
        self._buffer += data
        self._available.set()

    async def readexactly(self, n: int) -> bytes:
        # 标准事件等待模式：clear 后先复查，避免 clear/feed 竞争导致永久等待。
        while len(self._buffer) < n:
            self._available.clear()
            if len(self._buffer) >= n:
                break
            await self._available.wait()
        chunk, self._buffer = self._buffer[:n], self._buffer[n:]
        if not self._buffer:
            self._available.clear()
        return chunk


def _install_fake_connection(
    client: ModbusTcpClient,
    writer: _BlockingWriter,
    reader: _ScriptedReader,
) -> asyncio.Task:
    """绕过真实建连，直接把假连接装成客户端的当前代。

    与真实 ``_ensure_connected`` 的建连收尾保持一致：清空在途表、复位 TID
    分配器，保证不同请求拿到不同 TID（否则 owner_tid 会误匹配到别的请求）。
    """
    client._pending.clear()
    client._next_tid = 1
    client._generation += 1
    generation = client._generation
    client._writer = writer
    task = asyncio.create_task(
        client._reader_loop(reader, generation),
        name=f"fake-reader-gen{generation}",
    )
    client._reader_task = task
    return task


async def _stop_reader(client: ModbusTcpClient) -> None:
    task = client._reader_task
    if task is not None:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


def _response_frame(tid: int, value: int, unit_id: int = 1) -> bytes:
    pdu = struct.pack(">BBH", 0x03, 2, value & 0xFFFF)
    return struct.pack(">HHHB", tid, 0, len(pdu) + 1, unit_id) + pdu


# ---------------------------------------------------------------------------
# 1) 可控 drain 阻塞：超时从 write 起算，drain 阻塞也必须按时失败
# ---------------------------------------------------------------------------

async def test_blocked_drain_times_out_from_write_deadline():
    client = ModbusTcpClient("127.0.0.1", 1, timeout=0.2)
    writer = _BlockingWriter()
    writer.block_drain()  # drain 永久阻塞
    reader = _ScriptedReader()
    _install_fake_connection(client, writer, reader)

    t0 = asyncio.get_running_loop().time()
    with pytest.raises(ModbusError) as exc:
        await client.read_holding_register(0)
    elapsed = asyncio.get_running_loop().time() - t0

    assert exc.value.code == ModbusErrorCode.TIMEOUT
    # 必须在超时窗口附近返回（而不是无限挂起），留少量调度余量。
    assert 0.18 <= elapsed < 0.6, f"drain 阻塞未按时超时，实际 {elapsed:.2f}s"

    # 超时即关闭所属连接、清空在途表、释放发送锁。
    assert client._writer is None
    assert client._pending == {}
    assert not client._write_lock.locked()
    assert writer.is_closing()
    await _stop_reader(client)


async def test_blocked_drain_does_not_block_other_circuits_after_recovery(
    simulator_server, modbus_port
):
    """drain 超时拆连后，下一笔调用自动重连且其他回路不再被挂住。"""
    client = ModbusTcpClient("127.0.0.1", modbus_port, timeout=0.05)

    # 先用假连接制造一次 drain 阻塞超时。
    writer = _BlockingWriter()
    writer.block_drain()
    _install_fake_connection(client, writer, _ScriptedReader())
    with pytest.raises(ModbusError) as exc:
        await client.read_holding_register(0)
    assert exc.value.code == ModbusErrorCode.TIMEOUT
    assert client._writer is None
    assert not client._write_lock.locked()

    # 后续调用自动重连到真实模拟器并成功（流水线能力不退化）。
    raws = await asyncio.gather(
        *(client.read_holding_register(addr) for addr in range(8))
    )
    assert [raw for raw, _ in raws] == DEFAULT_VALUES
    assert not client._write_lock.locked()
    assert client._pending == {}
    await client.aclose()


async def test_drain_timeout_also_covers_reply_wait_with_shared_budget(
    simulator_server, modbus_port
):
    """write 之后即便 drain 先耗掉一部分预算，等待应答也只享用剩余预算。"""
    simulator.state.delay_seconds = 0.3
    client = ModbusTcpClient("127.0.0.1", modbus_port, timeout=0.05)
    try:
        t0 = asyncio.get_running_loop().time()
        with pytest.raises(ModbusError) as exc:
            await client.read_holding_register(0)
        elapsed = asyncio.get_running_loop().time() - t0
        assert exc.value.code == ModbusErrorCode.TIMEOUT
        # 总预算不随阶段重置：约 0.05s 即失败，而不是 drain+应答各 0.05s（~0.1s）。
        assert elapsed < 0.15, f"预算被阶段重置，实际 {elapsed:.2f}s"
        assert client._writer is None
    finally:
        simulator.state.delay_seconds = 0.0
        await client.aclose()


# ---------------------------------------------------------------------------
# 2) 发送期间取消：不得遗留待响应项或发送锁
# ---------------------------------------------------------------------------

async def test_cancel_during_blocked_drain_cleans_pending_and_lock():
    client = ModbusTcpClient("127.0.0.1", 1, timeout=5.0)
    writer = _BlockingWriter()
    writer.block_drain()
    _install_fake_connection(client, writer, _ScriptedReader())

    task = asyncio.create_task(client.read_holding_register(0))
    await writer.wait_drain_started()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # 取消后无待响应项残留、发送锁已释放、所属连接已关闭。
    assert client._pending == {}
    assert not client._write_lock.locked()
    assert client._writer is None
    assert writer.is_closing()
    await _stop_reader(client)


async def test_recover_after_cancel_during_send(simulator_server, modbus_port):
    """发送期取消并拆连后，下一笔调用能够自动重连成功。"""
    client = ModbusTcpClient("127.0.0.1", modbus_port, timeout=5.0)

    writer = _BlockingWriter()
    writer.block_drain()
    _install_fake_connection(client, writer, _ScriptedReader())
    task = asyncio.create_task(client.read_holding_register(0))
    await writer.wait_drain_started()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert not client._write_lock.locked()
    raw, _ = await client.read_holding_register(1)
    assert raw == DEFAULT_VALUES[1]
    await client.aclose()


async def test_cancel_during_send_while_other_inflight_remains_clean():
    """A 在 drain 中被取消拆连时，同代其余在途请求一并失败且无残留。"""
    client = ModbusTcpClient("127.0.0.1", 1, timeout=5.0)
    writer = _BlockingWriter()
    reader = _ScriptedReader()
    _install_fake_connection(client, writer, reader)

    # B 先正常发出并进入等待应答（pending 留在表里）。
    async def send_b() -> tuple[int, float]:
        return await client.read_holding_register(1)

    task_b = asyncio.create_task(send_b())
    await writer.wait_drains_done(1)  # 等 B 的 write+drain 真正完成
    # drain 完成后再让客户端任务推进到“等待应答”，确保待响应项已登记。
    for _ in range(100):
        if client._pending:
            break
        await asyncio.sleep(0)
    assert 1 in {p.tid for p in client._pending.values()}

    # A 的 drain 被卡住。
    writer.block_drain()
    task_a = asyncio.create_task(client.read_holding_register(0))
    await writer.wait_drain_started()
    task_a.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task_a

    # 连接被拆除：B 也随连接失败，待响应表清空、锁释放。
    with pytest.raises(ModbusError) as exc_b:
        await task_b
    assert exc_b.value.code == ModbusErrorCode.CONNECTION_LOST
    assert client._pending == {}
    assert not client._write_lock.locked()
    assert client._writer is None
    await _stop_reader(client)


# ---------------------------------------------------------------------------
# 3) 旧连接的 drain 迟到完成 / 迟到报错：不得关闭新连接或清掉新请求
# ---------------------------------------------------------------------------

async def test_old_connection_late_drain_error_does_not_touch_new_generation():
    client = ModbusTcpClient("127.0.0.1", 1, timeout=0.15)

    # ---- 第一代：假连接，drain 永久阻塞 ----
    old_writer = _BlockingWriter()
    old_writer.block_drain()
    old_reader = _ScriptedReader()
    _install_fake_connection(client, old_writer, old_reader)
    old_generation = client._generation

    old_task = asyncio.create_task(client.read_holding_register(0))
    await old_writer.wait_drain_started()

    # 等待它自己的 drain 超时触发拆连（旧代被拆除，连接关闭、锁释放）。
    with pytest.raises(ModbusError) as exc:
        await old_task
    assert exc.value.code == ModbusErrorCode.TIMEOUT
    assert client._generation == old_generation
    assert client._writer is None

    # ---- 第二代：新假连接，并放一笔在途请求 ----
    new_writer = _BlockingWriter()
    new_reader = _ScriptedReader()
    _install_fake_connection(client, new_writer, new_reader)
    new_generation = client._generation
    assert new_generation == old_generation + 1

    async def new_read() -> tuple[int, float]:
        return await client.read_holding_register(2)

    new_task = asyncio.create_task(new_read())
    # 等新请求完成 write+drain 并进入等待应答。
    await new_writer.wait_drains_done(1)
    for _ in range(100):
        if client._pending:
            break
        await asyncio.sleep(0)
    assert client._writer is new_writer
    assert len(client._pending) == 1

    # 模拟旧连接 drain“迟到报错”：旧代任务带着旧代际走错误清理路径。
    # 代际不符的 _tear_down 必须是空操作。
    await client._tear_down(
        ModbusErrorCode.CONNECTION_LOST,
        "采集失败：旧连接 drain 迟到报错",
        generation=old_generation,
    )

    # 新连接与新请求完好无损。
    assert client._writer is new_writer
    assert client._generation == new_generation
    assert len(client._pending) == 1
    assert not new_writer.is_closing()

    # 新连接收到正常应答后成功返回。
    tid_of_new = next(iter(client._pending))
    new_reader.feed(_response_frame(tid_of_new, DEFAULT_VALUES[2]))
    raw, _ = await new_task
    assert raw == DEFAULT_VALUES[2]

    await client.aclose()


async def test_old_connection_late_drain_success_does_not_touch_new_generation():
    """旧 drain 迟到“成功完成”同样只能影响旧代，不能关闭新连接。"""
    client = ModbusTcpClient("127.0.0.1", 1, timeout=0.15)

    old_writer = _BlockingWriter()
    old_writer.block_drain()
    _install_fake_connection(client, old_writer, _ScriptedReader())
    old_generation = client._generation

    old_task = asyncio.create_task(client.read_holding_register(0))
    await old_writer.wait_drain_started()
    with pytest.raises(ModbusError):
        await old_task  # drain 超时，旧代被拆除

    # 建立新一代连接并产生一笔在途请求。
    new_writer = _BlockingWriter()
    new_reader = _ScriptedReader()
    _install_fake_connection(client, new_writer, new_reader)
    new_generation = client._generation

    new_task = asyncio.create_task(client.read_holding_register(3))
    await new_writer.wait_drains_done(1)
    for _ in range(100):
        if client._pending:
            break
        await asyncio.sleep(0)
    assert client._writer is new_writer
    assert len(client._pending) == 1

    # 旧连接的 drain 此刻才“迟到成功”，随后其读取循环报告断线：
    # 旧代事件必须被代际隔离忽略。
    old_writer.release_drain()  # drain 正常返回（无异常）
    await client._on_connection_lost(
        old_generation, "采集失败：旧连接读取循环迟到的断线事件"
    )

    assert client._writer is new_writer
    assert client._generation == new_generation
    assert len(client._pending) == 1
    assert not new_writer.is_closing()

    tid_of_new = next(iter(client._pending))
    new_reader.feed(_response_frame(tid_of_new, DEFAULT_VALUES[3]))
    raw, _ = await new_task
    assert raw == DEFAULT_VALUES[3]

    await client.aclose()


async def test_timeout_teardown_is_generation_scoped_under_concurrent_send():
    """并发场景：一笔 drain 超时拆连时，只关当前代，不影响随后重连的新请求。"""
    client = ModbusTcpClient("127.0.0.1", 1, timeout=0.15)

    writer = _BlockingWriter()
    reader = _ScriptedReader()
    _install_fake_connection(client, writer, reader)
    gen1 = client._generation

    # 先放一笔正常在途请求 B。
    task_b = asyncio.create_task(client.read_holding_register(1))
    await writer.wait_drains_done(1)  # B 的 write+drain 完成后进入等待应答
    for _ in range(100):
        if client._pending:
            break
        await asyncio.sleep(0)
    assert len(client._pending) == 1

    # A 卡在 drain，持锁直到超时。
    writer.block_drain()
    task_a = asyncio.create_task(client.read_holding_register(0))
    await writer.wait_drain_started()

    with pytest.raises(ModbusError) as exc_a:
        await task_a
    assert exc_a.value.code == ModbusErrorCode.TIMEOUT

    # 同一代的 B 随连接失败。
    with pytest.raises(ModbusError) as exc_b:
        await task_b
    assert exc_b.value.code in {
        ModbusErrorCode.TIMEOUT,
        ModbusErrorCode.CONNECTION_LOST,
    }
    assert client._writer is None
    assert client._pending == {}
    assert not client._write_lock.locked()
    assert writer.is_closing()
    assert client._generation == gen1  # 尚未重连，代际不变

    await _stop_reader(client)
