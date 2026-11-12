"""测试夹具：在同一事件循环内启动模拟控制器与采集客户端。"""

from __future__ import annotations

import asyncio
import contextlib
import socket

import pytest

from app.modbus_client import ModbusTcpClient
from simulator import app as simulator


@pytest.fixture
async def simulator_server() -> int:
    """启动模拟器并返回其实际监听端口（绑定端口 0，杜绝端口竞争）。"""
    simulator.reset_state_for_tests()
    server = await asyncio.start_server(
        simulator._handle_connection, "127.0.0.1", 0
    )
    port = server.sockets[0].getsockname()[1]
    task = asyncio.create_task(server.serve_forever())
    try:
        yield port
    finally:
        task.cancel()
        server.close()
        await server.wait_closed()
        with contextlib.suppress(asyncio.CancelledError):
            await task


@pytest.fixture
def modbus_port(simulator_server: int) -> int:
    return simulator_server


@pytest.fixture
def unused_port() -> int:
    """获取一个当前空闲端口（尽力而为，用于连接失败类测试）。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
async def client(modbus_port: int) -> ModbusTcpClient:
    c = ModbusTcpClient(
        host="127.0.0.1", port=modbus_port, unit_id=1, timeout=2.0
    )
    try:
        yield c
    finally:
        await c.aclose()
