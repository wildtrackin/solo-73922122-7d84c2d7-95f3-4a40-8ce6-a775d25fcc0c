"""FastAPI 采集接口测试（ASGI 进程内调用，无需真实端口）。"""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from app import main as web
from simulator import app as simulator


@pytest.fixture
async def api_client(simulator_server, modbus_port):
    # 用测试端口替换模块级单例客户端。
    original = web.client
    from app.modbus_client import ModbusTcpClient

    web.client = ModbusTcpClient(
        host="127.0.0.1", port=modbus_port, unit_id=1, timeout=2.0
    )
    transport = ASGITransport(app=web.app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    await web.client.aclose()
    web.client = original


async def test_health(api_client: AsyncClient):
    r = await api_client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


async def test_read_single_returns_celsius(api_client: AsyncClient):
    r = await api_client.get("/read/0")
    assert r.status_code == 200
    body = r.json()
    assert body["circuit"] == 0
    assert body["address"] == 0
    assert body["raw_register"] == -180
    assert body["temperature_c"] == -18.0
    assert "received_at" in body and body["received_at"].endswith("+00:00")


async def test_negative_register_scaling(api_client: AsyncClient):
    # 寄存器 4 = -200 → -20.0 ℃；寄存器 6 = -45 → -4.5 ℃
    r = await api_client.get("/read/4")
    assert r.json()["temperature_c"] == -20.0
    r = await api_client.get("/read/6")
    assert r.json()["temperature_c"] == -4.5


async def test_invalid_circuit_returns_400(api_client: AsyncClient):
    r = await api_client.get("/read/99")
    assert r.status_code == 400
    body = r.json()
    assert body["error"] == "illegal_address"
    assert body["message"].startswith("采集失败")


async def test_read_all_concurrent(api_client: AsyncClient):
    r = await api_client.get("/read/all")
    assert r.status_code == 200
    body = r.json()
    assert body["success_count"] == 8
    assert body["failure_count"] == 0
    assert [item["data"]["temperature_c"] for item in body["results"]] == [
        -18.0, -17.5, 5.0, 23.5, -20.0, 30.0, -4.5, 10.5
    ]


async def test_batch_endpoint(api_client: AsyncClient):
    r = await api_client.post("/read/batch", json={"circuits": [0, 3, 7]})
    assert r.status_code == 200
    body = r.json()
    assert body["success_count"] == 3
    assert [item["circuit"] for item in body["results"]] == [0, 3, 7]


async def test_exception_response_mapped_to_502(api_client: AsyncClient):
    simulator.state.exceptions[1] = [0x02, 1]
    r = await api_client.get("/read/1")
    assert r.status_code == 502
    body = r.json()
    assert body["error"] == "exception_response"
    assert "采集失败" in body["message"]


async def test_timeout_mapped_to_504(simulator_server, modbus_port):
    from app.modbus_client import ModbusTcpClient

    original = web.client
    web.client = ModbusTcpClient(
        host="127.0.0.1", port=modbus_port, unit_id=1, timeout=0.05
    )
    simulator.state.delay_seconds = 0.3
    transport = ASGITransport(app=web.app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            r = await ac.get("/read/0")
            assert r.status_code == 504
            assert r.json()["error"] == "timeout"
    finally:
        await web.client.aclose()
        web.client = original
        simulator.state.delay_seconds = 0.0


async def test_disconnect_then_reconnect_via_api(api_client: AsyncClient):
    simulator.state.disconnect_mode = "next"
    r1 = await api_client.get("/read/2")
    assert r1.status_code == 502
    assert r1.json()["error"] == "connection_lost"

    # 自动重连：下一笔成功。
    r2 = await api_client.get("/read/2")
    assert r2.status_code == 200
    assert r2.json()["temperature_c"] == 5.0


async def test_batch_partial_failure_structure(api_client: AsyncClient):
    simulator.state.exceptions[3] = [0x03, 1]
    r = await api_client.post("/read/batch", json={"circuits": [2, 3, 4]})
    body = r.json()
    assert body["success_count"] == 2
    assert body["failure_count"] == 1
    failed = next(i for i in body["results"] if not i["ok"])
    assert failed["circuit"] == 3
    assert failed["error"] == "exception_response"


async def test_no_caching_each_call_hits_controller(api_client: AsyncClient):
    r1 = await api_client.get("/read/5")
    assert r1.json()["raw_register"] == 300

    simulator.state.registers[5] = 315
    r2 = await api_client.get("/read/5")
    assert r2.json()["raw_register"] == 315
    assert r2.json()["temperature_c"] == 31.5
