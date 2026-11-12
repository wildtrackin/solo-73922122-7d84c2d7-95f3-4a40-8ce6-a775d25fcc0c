"""ModbusTcpClient 协议与并发行为测试。"""

from __future__ import annotations

import asyncio
import struct

import pytest

from app.modbus_client import ModbusError, ModbusErrorCode, ModbusTcpClient
from simulator import app as simulator

DEFAULT_VALUES = [-180, -175, 50, 235, -200, 300, -45, 105]


async def test_single_read_returns_signed_scaled_value(client: ModbusTcpClient):
    raw, _ = await client.read_holding_register(0)
    assert raw == -180  # 有符号 16 位

    raw, _ = await client.read_holding_register(3)
    assert raw == 235


async def test_all_eight_addresses(client: ModbusTcpClient):
    values = await asyncio.gather(
        *(client.read_holding_register(addr) for addr in range(8))
    )
    assert [raw for raw, _ in values] == DEFAULT_VALUES


async def test_invalid_address_rejected(client: ModbusTcpClient):
    with pytest.raises(ModbusError) as exc:
        await client.read_holding_register(65536)
    assert exc.value.code == ModbusErrorCode.ILLEGAL_ADDRESS


async def test_concurrent_pipelined_reads_share_one_connection(
    client: ModbusTcpClient,
):
    # 每笔应答在模拟器侧耗时 0.1s；串行需要 ~0.8s，流水线应在 ~0.2s 内完成。
    simulator.state.delay_seconds = 0.1
    try:
        t0 = asyncio.get_running_loop().time()
        results = await asyncio.gather(
            *(client.read_holding_register(addr) for addr in range(8))
        )
        elapsed = asyncio.get_running_loop().time() - t0
    finally:
        simulator.state.delay_seconds = 0.0

    assert [raw for raw, _ in results] == DEFAULT_VALUES
    assert elapsed < 0.55, f"读取未流水线化，耗时 {elapsed:.2f}s"
    # 全部请求始终共用同一条连接、同一个 generation。
    assert client._generation == 1
    assert len(client._pending) == 0


async def test_all_calls_share_one_tcp_connection(client: ModbusTcpClient):
    """多笔调用（含并发在途）在模拟器侧只建立一条 TCP 连接。"""
    # 等待前序测试可能残留的关闭事件结算，取稳定基线。
    for _ in range(20):
        baseline = simulator.state.connections
        await asyncio.sleep(0.02)
        if simulator.state.connections == baseline:
            break

    await client.read_holding_register(0)
    assert simulator.state.connections == baseline + 1

    simulator.state.delay_seconds = 0.15
    try:
        await asyncio.gather(
            *(client.read_holding_register(addr) for addr in range(8))
        )
        # 8 笔并发在途期间连接数不增加。
        assert simulator.state.connections == baseline + 1
    finally:
        simulator.state.delay_seconds = 0.0

    await client.read_holding_register(7)
    assert simulator.state.connections == baseline + 1


async def test_out_of_order_reverse_replies_are_matched_by_tid(
    client: ModbusTcpClient,
):
    simulator.state.reply_mode = "reverse"
    try:
        results = await asyncio.gather(
            *(client.read_holding_register(addr) for addr in range(8))
        )
    finally:
        simulator.state.reply_mode = "normal"

    # 即使应答顺序与发送顺序相反，也必须按 TID 正确匹配，不能串值。
    assert [raw for raw, _ in results] == DEFAULT_VALUES


async def test_timeout_fails_and_tears_down_connection(
    simulator_server, modbus_port
):
    simulator.state.delay_seconds = 0.3
    short_client = ModbusTcpClient(
        "127.0.0.1", modbus_port, unit_id=1, timeout=0.05
    )
    with pytest.raises(ModbusError) as exc:
        await short_client.read_holding_register(0)
    assert exc.value.code == ModbusErrorCode.TIMEOUT
    assert "采集失败" in exc.value.message
    # 超时后连接已被拆除。
    assert short_client._writer is None
    assert short_client._pending == {}
    await short_client.aclose()


async def test_late_response_is_not_reused_by_later_sample(
    simulator_server, modbus_port
):
    # 第一笔请求超时，模拟器的迟到应答在旧连接上到达后必须被丢弃。
    simulator.state.delay_seconds = 0.3
    slow_client = ModbusTcpClient(
        "127.0.0.1", modbus_port, unit_id=1, timeout=0.05
    )
    with pytest.raises(ModbusError):
        await slow_client.read_holding_register(0)
    await slow_client.aclose()

    # 等待迟到响应在旧 socket 上消亡。
    await asyncio.sleep(0.4)
    simulator.state.delay_seconds = 0.0

    fresh = ModbusTcpClient("127.0.0.1", modbus_port, unit_id=1, timeout=2.0)
    raw, _ = await fresh.read_holding_register(0)
    assert raw == DEFAULT_VALUES[0]
    await fresh.aclose()


async def test_exception_response_is_collection_failure(client: ModbusTcpClient):
    simulator.state.exceptions[2] = [0x02, 1]
    with pytest.raises(ModbusError) as exc:
        await client.read_holding_register(2)
    assert exc.value.code == ModbusErrorCode.EXCEPTION_RESPONSE
    assert "异常响应" in exc.value.message
    # 异常响应后连接保持可用。
    raw, _ = await client.read_holding_register(2)
    assert raw == DEFAULT_VALUES[2]


async def test_disconnect_fails_all_inflight_then_auto_reconnects(
    client: ModbusTcpClient,
):
    # 先建立首条连接，确定初始 generation 基线。
    raw0, _ = await client.read_holding_register(0)
    assert raw0 == DEFAULT_VALUES[0]
    gen_before = client._generation

    simulator.state.disconnect_mode = "next"
    with pytest.raises(ModbusError) as exc:
        await client.read_holding_register(1)
    assert exc.value.code == ModbusErrorCode.CONNECTION_LOST

    # 后续调用自动重连并成功，generation 恰好递增一代。
    raw, _ = await client.read_holding_register(1)
    assert raw == DEFAULT_VALUES[1]
    assert client._generation == gen_before + 1


async def test_all_inflight_fail_together_on_disconnect(
    client: ModbusTcpClient,
):
    simulator.state.disconnect_mode = "next"
    tasks = [
        asyncio.create_task(client.read_holding_register(addr))
        for addr in range(4)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(isinstance(r, ModbusError) for r in results)
    assert {r.code for r in results} == {ModbusErrorCode.CONNECTION_LOST}

    # 自动重连后全部恢复。
    recovered = await asyncio.gather(
        *(client.read_holding_register(addr) for addr in range(4))
    )
    assert [raw for raw, _ in recovered] == DEFAULT_VALUES[:4]


async def test_old_connection_cannot_interfere_with_new_generation(
    simulator_server, modbus_port
):
    c = ModbusTcpClient("127.0.0.1", modbus_port, timeout=2.0)
    await c.read_holding_register(0)
    old_generation = c._generation
    old_task = c._reader_task
    old_writer = c._writer

    await c._tear_down(
        ModbusErrorCode.CONNECTION_LOST, "采集失败：测试主动拆除"
    )
    assert old_task is not None and old_task.done()
    # 旧 socket 必须真正关闭，防止 fd 泄漏与旧连接残活。
    assert old_writer is not None and old_writer.is_closing()

    await c.read_holding_register(0)
    assert c._generation == old_generation + 1
    assert c._reader_task is not old_task
    await c.aclose()


async def test_teardown_closes_socket_each_generation(
    simulator_server, modbus_port
):
    """连续断线重连多代，每代 socket 均被关闭，无连接累积。"""
    c = ModbusTcpClient("127.0.0.1", modbus_port, timeout=2.0)
    writers = []
    for _ in range(3):
        await c.read_holding_register(0)
        writers.append(c._writer)
        await c._tear_down(
            ModbusErrorCode.TIMEOUT, "采集失败：模拟超时拆除"
        )
    assert len({w for w in writers}) == 3
    assert all(w.is_closing() for w in writers)
    await c.aclose()


async def test_fragmented_and_coalesced_frames(unused_port):
    """底层 TCP 服务器验证分片（一字节一字节发）与粘包（两帧合并发）。"""

    port = unused_port

    def frame(tid: int, value: int) -> bytes:
        pdu = struct.pack(">BBH", 0x03, 2, value & 0xFFFF)
        return struct.pack(">HHHB", tid, 0, len(pdu) + 1, 1) + pdu

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            # 第一帧：逐字节发送，验证分片重组。
            mbap = await reader.readexactly(7)
            tid = struct.unpack(">H", mbap[:2])[0]
            length = struct.unpack(">H", mbap[4:6])[0]
            body = await reader.readexactly(length - 1)
            _ = body
            response = frame(tid, 111)
            for byte in response:
                writer.write(bytes([byte]))
                await writer.drain()

            # 连续读取两帧后合并为一次 write，验证粘包拆分。
            frames = []
            for _ in range(2):
                head = await reader.readexactly(7)
                t = struct.unpack(">H", head[:2])[0]
                ln = struct.unpack(">H", head[4:6])[0]
                rest = await reader.readexactly(ln - 1)
                frames.append(frame(t, 222 if t == 2 else 333))
                _ = head, rest
            writer.write(b"".join(frames))
            await writer.drain()
            await asyncio.sleep(0.3)
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", port)
    async with server:
        c = ModbusTcpClient("127.0.0.1", port, timeout=2.0)
        raw1, _ = await c.read_holding_register(0)
        assert raw1 == 111
        (raw2, _), (raw3, _) = await asyncio.gather(
            c.read_holding_register(1), c.read_holding_register(2)
        )
        assert (raw2, raw3) == (222, 333)
        await c.aclose()


async def test_mbap_mismatch_rejected(unused_port):
    """protocol id / unit id 错误的响应必须报采集失败。"""

    port = unused_port

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        mbap = await reader.readexactly(7)
        tid = struct.unpack(">H", mbap[:2])[0]
        length = struct.unpack(">H", mbap[4:6])[0]
        await reader.readexactly(length - 1)
        pdu = struct.pack(">BBH", 0x03, 2, 10)
        # 协议标识符故意写成 1。
        writer.write(struct.pack(">HHHB", tid, 1, len(pdu) + 1, 1) + pdu)
        await writer.drain()
        await asyncio.sleep(0.3)
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", port)
    async with server:
        c = ModbusTcpClient("127.0.0.1", port, timeout=1.0)
        with pytest.raises(ModbusError) as exc:
            await c.read_holding_register(0)
        assert exc.value.code == ModbusErrorCode.PROTOCOL_ERROR
        await c.aclose()


async def test_connect_failure_reported(modbus_port, unused_port):
    c = ModbusTcpClient("127.0.0.1", unused_port, timeout=1.0)
    with pytest.raises(ModbusError) as exc:
        await c.read_holding_register(0)
    assert exc.value.code == ModbusErrorCode.CONNECT_FAILED
    await c.aclose()
