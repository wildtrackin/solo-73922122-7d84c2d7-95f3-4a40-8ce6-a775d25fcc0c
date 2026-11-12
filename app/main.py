"""冷库温度采集 API（单进程 FastAPI 应用）。

端点
----
* ``GET  /``                演示页面（并发采集 + 故障注入）
* ``GET  /health``          健康检查
* ``GET  /read/{circuit}``  读取单个回路当前温度
* ``GET  /read/all``        并发读取全部 8 个回路
* ``POST /read/batch``      并发读取指定回路列表

每次请求都发起一次真实 Modbus 读取，服务端不缓存任何读数。
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field

from .config import Settings, get_settings
from .modbus_client import ModbusError, ModbusErrorCode, ModbusTcpClient

settings: Settings = get_settings()
client = ModbusTcpClient(
    host=settings.modbus_host,
    port=settings.modbus_port,
    unit_id=settings.modbus_unit_id,
    timeout=settings.modbus_timeout,
)

_HTTP_STATUS_BY_CODE = {
    ModbusErrorCode.TIMEOUT: 504,
    ModbusErrorCode.CONNECT_FAILED: 502,
    ModbusErrorCode.CONNECTION_LOST: 502,
    ModbusErrorCode.ILLEGAL_ADDRESS: 400,
    ModbusErrorCode.EXCEPTION_RESPONSE: 502,
    ModbusErrorCode.PROTOCOL_ERROR: 502,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _to_celsius(raw: int) -> float:
    """有符号 16 位整数除以 10，得到摄氏温度。"""
    return round(raw / 10.0, 2)


class Reading(BaseModel):
    """单个回路的一次温度采集结果。"""

    circuit: int = Field(..., description="回路编号（对应 Modbus 保持寄存器地址）")
    address: int = Field(..., description="读取的保持寄存器地址（0~7）")
    temperature_c: float = Field(..., description="摄氏温度，寄存器有符号整数 ÷ 10")
    raw_register: int = Field(..., description="寄存器原始有符号 16 位整数值")
    received_at: str = Field(..., description="采集应答接收时间（UTC，ISO 8601）")


class ErrorBody(BaseModel):
    error: str = Field(..., description="错误类别")
    message: str = Field(..., description="中文错误说明，均以“采集失败”开头")


class BatchRequest(BaseModel):
    circuits: list[int] = Field(
        ...,
        min_length=1,
        description="要并发采集的回路编号列表（即寄存器地址，0~7）",
    )


class BatchItem(BaseModel):
    circuit: int
    ok: bool = Field(..., description="该回路是否采集成功")
    data: Reading | None = Field(None, description="成功时的温度读数")
    error: str | None = Field(None, description="失败时的错误类别")
    message: str | None = Field(None, description="失败时的中文说明")


class BatchResponse(BaseModel):
    results: list[BatchItem]
    success_count: int
    failure_count: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await client.aclose()


app = FastAPI(
    title="冷库温度采集 API",
    description=(
        "单进程 Modbus TCP 采集服务。所有请求共用一条 TCP 连接，支持多笔读取"
        "同时在途（流水线）、乱序响应匹配、2 秒超时与断线自动重连，读数不缓存。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


def _validate_circuit(circuit: int) -> None:
    if not 0 <= circuit < settings.register_count:
        raise HTTPException(
            status_code=400,
            detail={
                "error": ModbusErrorCode.ILLEGAL_ADDRESS.value,
                "message": (
                    f"采集失败：回路编号必须在 0~{settings.register_count - 1} 之间，"
                    f"收到 {circuit}"
                ),
            },
        )


async def _collect_one(circuit: int) -> BatchItem:
    """采集单个回路，异常转换为批量结果中的失败项（不抛异常）。"""
    try:
        raw, _ = await client.read_holding_register(circuit)
    except ModbusError as exc:
        return BatchItem(
            circuit=circuit, ok=False, data=None,
            error=exc.code.value, message=exc.message,
        )
    reading = Reading(
        circuit=circuit,
        address=circuit,
        temperature_c=_to_celsius(raw),
        raw_register=raw,
        received_at=_now_iso(),
    )
    return BatchItem(circuit=circuit, ok=True, data=reading)


@app.get(
    "/read/all",
    response_model=BatchResponse,
    summary="并发读取全部回路（0~7）当前温度",
)
async def read_all() -> BatchResponse:
    """8 笔读取通过 ``asyncio.gather`` 同时发出，可同时在途。"""
    items = await asyncio.gather(
        *(_collect_one(i) for i in range(settings.register_count))
    )
    return _build_batch(items)


@app.post(
    "/read/batch",
    response_model=BatchResponse,
    summary="并发读取指定回路列表",
)
async def read_batch(body: BatchRequest) -> BatchResponse:
    for circuit in body.circuits:
        _validate_circuit(circuit)
    items = await asyncio.gather(*(_collect_one(c) for c in body.circuits))
    return _build_batch(items)


@app.get(
    "/read/{circuit}",
    response_model=Reading,
    summary="读取单个回路当前温度",
    responses={502: {"model": ErrorBody}, 504: {"model": ErrorBody}},
)
async def read_circuit(circuit: int) -> Reading:
    """对 unit=1 的保持寄存器 ``circuit`` 执行功能码 03 单寄存器读取。"""
    _validate_circuit(circuit)
    try:
        raw, _ = await client.read_holding_register(circuit)
    except ModbusError as exc:
        raise HTTPException(
            status_code=_HTTP_STATUS_BY_CODE[exc.code],
            detail={"error": exc.code.value, "message": exc.message},
        ) from exc
    return Reading(
        circuit=circuit,
        address=circuit,
        temperature_c=_to_celsius(raw),
        raw_register=raw,
        received_at=_now_iso(),
    )


def _build_batch(items: tuple[BatchItem, ...] | list[BatchItem]) -> BatchResponse:
    success = sum(1 for item in items if item.ok)
    return BatchResponse(
        results=list(items),
        success_count=success,
        failure_count=len(items) - success,
    )


@app.get("/health", summary="健康检查")
async def health() -> dict:
    return {"status": "ok", "controller": f"{settings.modbus_host}:{settings.modbus_port}"}


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    index_path = Path(__file__).parent / "static" / "index.html"
    return FileResponse(index_path)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    detail = exc.detail
    if isinstance(detail, dict) and "message" in detail:
        return JSONResponse(status_code=exc.status_code, content=detail)
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": "http_error", "message": str(detail)},
    )
