"""本地 Modbus TCP 模拟控制器。

同一进程内提供：
* 端口 5020：Modbus TCP 从站（功能码 03，unit=1，保持寄存器地址 0~7）
* 端口 8081：控制 API，可在运行时切换
  - 倒序回复（reverse）：把同一连接上同时到达的一批请求按相反顺序应答
  - 回复延迟（delay_seconds）：每笔应答前固定等待，可触发采集端 2 秒超时
  - 断线（disconnect_mode=next/all）
  - 寄存器数值、异常响应注入

仅用于本地演示与测试，不做任何生产用途的健壮性保证。
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import struct
from dataclasses import dataclass, field

from fastapi import FastAPI
from pydantic import BaseModel, Field

# ---- 可调状态 ---------------------------------------------------------------

DEFAULT_REGISTERS = [-180, -175, 50, 235, -200, 300, -45, 105]


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    try:
        return float(_env(name, str(default)))
    except ValueError:
        return default


@dataclass
class SimulatorState:
    registers: list[int]
    reply_mode: str = "normal"          # normal | reverse
    delay_seconds: float = 0.0
    disconnect_mode: str = "none"       # none | next | all
    # 地址 -> [异常码, 剩余次数]
    exceptions: dict[int, list[int]] = field(default_factory=dict)
    connections: int = 0
    request_count: int = 0

    def snapshot(self) -> dict:
        return {
            "registers": list(self.registers),
            "reply_mode": self.reply_mode,
            "delay_seconds": self.delay_seconds,
            "disconnect_mode": self.disconnect_mode,
            "exceptions": {
                str(addr): {"code": v[0], "remaining": v[1]}
                for addr, v in self.exceptions.items()
            },
            "active_connections": self.connections,
            "total_requests": self.request_count,
        }


def _initial_registers() -> list[int]:
    raw = os.environ.get("REGISTER_VALUES")
    if not raw:
        return list(DEFAULT_REGISTERS)
    values = []
    for part in raw.split(","):
        part = part.strip()
        if part:
            values.append(int(part))
    return values if values else list(DEFAULT_REGISTERS)


state = SimulatorState(
    registers=_initial_registers(),
    reply_mode=_env("REPLY_MODE", "normal"),
    delay_seconds=_env_float("DELAY_SECONDS", 0.0),
    disconnect_mode=_env("DISCONNECT_MODE", "none"),
)
BURST_WINDOW = _env_float("REVERSE_BURST_WINDOW", 0.2)
MODBUS_PORT = int(_env("MODBUS_PORT", "5020"))
CONTROL_PORT = int(_env("CONTROL_PORT", "8081"))

# ---- Modbus 协议 ------------------------------------------------------------

_MBAP = 7
_FUNC_READ = 0x03
_FUNC_READ_EXCEPTION = 0x83


def _parse_frame(
    mbap: bytes, body: bytes
) -> tuple[int, int, int, int, int, int]:
    tid, protocol_id, length, unit_id = struct.unpack(">HHHB", mbap)
    function_code, address, quantity = struct.unpack(">BHH", body)
    return tid, protocol_id, unit_id, function_code, address, quantity


def _build_response(tid: int, unit_id: int, value: int) -> bytes:
    unsigned = value & 0xFFFF
    pdu = struct.pack(">BBH", _FUNC_READ, 2, unsigned)
    return struct.pack(">HHHB", tid, 0, len(pdu) + 1, unit_id) + pdu


def _build_exception(tid: int, unit_id: int, exception_code: int) -> bytes:
    pdu = struct.pack(">BB", _FUNC_READ_EXCEPTION, exception_code)
    return struct.pack(">HHHB", tid, 0, len(pdu) + 1, unit_id) + pdu


async def _read_frame(reader: asyncio.StreamReader) -> tuple[bytes, bytes] | None:
    """读取一帧；对端关闭时返回 None。自动处理分片与粘包。"""
    try:
        mbap = await reader.readexactly(_MBAP)
    except asyncio.IncompleteReadError:
        return None
    tid, protocol_id, length, unit_id = struct.unpack(">HHHB", mbap)
    try:
        body = await reader.readexactly(length - 1)
    except asyncio.IncompleteReadError:
        return None
    return mbap, body


def _process(frame: tuple[bytes, bytes]) -> bytes | None:
    """根据当前状态生成应答字节；返回 None 表示不应答（用于断线场景）。"""
    mbap, body = frame
    tid, protocol_id, unit_id, function_code, address, quantity = _parse_frame(mbap, body)
    state.request_count += 1

    if protocol_id != 0:
        return _build_exception(tid, unit_id, 0x01)
    if function_code != _FUNC_READ:
        return _build_exception(tid, unit_id, 0x01)
    if quantity != 1:
        return _build_exception(tid, unit_id, 0x03)
    if not 0 <= address < len(state.registers):
        return _build_exception(tid, unit_id, 0x02)

    injected = state.exceptions.get(address)
    if injected is not None and injected[1] > 0:
        code = injected[0]
        injected[1] -= 1
        if injected[1] == 0:
            state.exceptions.pop(address, None)
        return _build_exception(tid, unit_id, code)

    return _build_response(tid, unit_id, state.registers[address])


async def _reply_one(writer: asyncio.StreamWriter, frame: tuple[bytes, bytes]) -> None:
    """单帧处理与应答；每帧独立任务，使并发在途请求可并行延迟、并行回复。"""
    response = _process(frame)
    if response is None:
        return
    if state.delay_seconds > 0:
        await asyncio.sleep(state.delay_seconds)
    # 多任务写同一 socket 由事件循环原子地完成单次 write 调用。
    writer.write(response)
    await writer.drain()


async def _handle_connection(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    state.connections += 1
    peer = writer.get_extra_info("peername")
    reply_tasks: set[asyncio.Task] = set()
    try:
        if state.disconnect_mode == "all":
            # 立即断开每一条新连接。
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            return

        while True:
            frame = await _read_frame(reader)
            if frame is None:
                break

            if state.disconnect_mode == "next":
                # 下一笔请求到达后直接断线且不应答，然后恢复模式。
                state.disconnect_mode = "none"
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
                return

            if state.reply_mode == "reverse":
                # 收集同一连接上同时到达的一批请求，再倒序应答。
                burst = [frame]
                while True:
                    try:
                        more = await asyncio.wait_for(
                            _read_frame(reader), timeout=BURST_WINDOW
                        )
                    except asyncio.TimeoutError:
                        break
                    if more is None:
                        break
                    burst.append(more)
                for one_frame in reversed(burst):
                    response = _process(one_frame)
                    if response is not None:
                        if state.delay_seconds > 0:
                            await asyncio.sleep(state.delay_seconds)
                        writer.write(response)
                        await writer.drain()
            else:
                # normal 模式：每帧一个应答任务，多笔读取可同时在途、并行回复，
                # 回复顺序不做保证（用于验证主站按 TID 匹配）。
                task = asyncio.create_task(_reply_one(writer, frame))
                reply_tasks.add(task)
                task.add_done_callback(reply_tasks.discard)
    except (ConnectionError, BrokenPipeError, OSError):
        pass
    finally:
        # 等待已派发的应答任务结束后再关闭连接。
        if reply_tasks:
            await asyncio.gather(*reply_tasks, return_exceptions=True)
        state.connections -= 1
        with contextlib.suppress(Exception):
            writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        print(f"[simulator] connection closed: {peer}", flush=True)


# ---- 控制 API ---------------------------------------------------------------

class ModeBody(BaseModel):
    reply_mode: str | None = Field(None, pattern="^(normal|reverse)$")
    delay_seconds: float | None = Field(None, ge=0, le=60)
    disconnect_mode: str | None = Field(None, pattern="^(none|next|all)$")


class RegistersBody(BaseModel):
    values: list[int] = Field(..., min_length=1)


class DisconnectBody(BaseModel):
    mode: str = Field(..., pattern="^(none|next|all)$")


class ExceptionBody(BaseModel):
    address: int = Field(..., ge=0, le=7)
    code: int = Field(1, ge=1, le=4)
    count: int = Field(1, ge=1, le=100)


control_app = FastAPI(title="模拟控制器控制 API", version="1.0.0")


@control_app.get("/state", summary="查看模拟器当前状态")
async def get_state() -> dict:
    return state.snapshot()


@control_app.post("/mode", summary="切换倒序回复 / 延迟 / 断线模式")
async def set_mode(body: ModeBody) -> dict:
    if body.reply_mode is not None:
        state.reply_mode = body.reply_mode
    if body.delay_seconds is not None:
        state.delay_seconds = body.delay_seconds
    if body.disconnect_mode is not None:
        state.disconnect_mode = body.disconnect_mode
    return state.snapshot()


@control_app.post("/registers", summary="批量设置保持寄存器值（有符号 16 位整数）")
async def set_registers(body: RegistersBody) -> dict:
    for value in body.values:
        if not -32768 <= value <= 32767:
            return {"error": "值必须为有符号 16 位整数（-32768~32767）"}
    if len(body.values) > len(state.registers):
        return {"error": f"最多 {len(state.registers)} 个寄存器"}
    for index, value in enumerate(body.values):
        state.registers[index] = value
    return state.snapshot()


@control_app.post("/disconnect", summary="触发断线：next=下一笔请求后断线，all=新连接立即断线")
async def trigger_disconnect(body: DisconnectBody) -> dict:
    state.disconnect_mode = body.mode
    return state.snapshot()


@control_app.post("/exception", summary="对指定地址注入 N 次 Modbus 异常响应")
async def inject_exception(body: ExceptionBody) -> dict:
    state.exceptions[body.address] = [body.code, body.count]
    return state.snapshot()


@control_app.post("/reset", summary="恢复正常模式并清空异常注入")
async def reset() -> dict:
    state.reply_mode = "normal"
    state.delay_seconds = 0.0
    state.disconnect_mode = "none"
    state.exceptions.clear()
    state.registers = list(DEFAULT_REGISTERS)
    return state.snapshot()


# ---- 启动 -------------------------------------------------------------------

def reset_state_for_tests() -> None:
    """供测试使用：恢复初始寄存器与全部默认模式。"""
    state.registers = list(DEFAULT_REGISTERS)
    state.reply_mode = "normal"
    state.delay_seconds = 0.0
    state.disconnect_mode = "none"
    state.exceptions.clear()
    state.request_count = 0


async def main() -> None:
    modbus_server = await asyncio.start_server(
        _handle_connection, "0.0.0.0", MODBUS_PORT
    )
    print(
        f"[simulator] Modbus TCP listening on :{MODBUS_PORT}, "
        f"control API on :{CONTROL_PORT}",
        flush=True,
    )
    print(f"[simulator] initial state: {state.snapshot()}", flush=True)

    import uvicorn

    control_server = uvicorn.Config(
        control_app, host="0.0.0.0", port=CONTROL_PORT, log_level="warning"
    )
    control = uvicorn.Server(control_server)

    async with modbus_server:
        await asyncio.gather(modbus_server.serve_forever(), control.serve())


if __name__ == "__main__":
    asyncio.run(main())
