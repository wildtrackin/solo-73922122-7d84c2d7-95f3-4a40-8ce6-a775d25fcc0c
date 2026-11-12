#!/usr/bin/env python3
"""并发采集调用示例：对 8 个回路同时发起读取，演示同连接流水线在途。

用法（容器内或本地均可）：
    python scripts/demo_concurrent.py http://127.0.0.1:8000
"""

from __future__ import annotations

import asyncio
import sys
import time

import httpx


async def main(base_url: str) -> None:
    async with httpx.AsyncClient(base_url=base_url, timeout=10.0) as http:
        t0 = time.perf_counter()
        # 8 个请求同时发出；服务端在同一条 Modbus TCP 连接上并发在途发送，
        # 不等待上一笔应答即可发送下一笔。
        tasks = [http.get(f"/read/{circuit}") for circuit in range(8)]
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        elapsed = (time.perf_counter() - t0) * 1000

        print(f"8 回路并发采集完成，总耗时 {elapsed:.0f} ms\n")
        print(f"{'回路':<4}{'温度(℃)':<10}{'原始值':<8}{'状态':<6}接收时间(UTC)")
        for circuit, resp in enumerate(responses):
            if isinstance(resp, Exception):
                print(f"{circuit:<4}{'-':<10}{'-':<8}失败   {resp}")
                continue
            if resp.status_code != 200:
                body = resp.json()
                print(f"{circuit:<4}{'-':<10}{'-':<8}失败   {body.get('message')}")
                continue
            d = resp.json()
            print(
                f"{d['circuit']:<4}{d['temperature_c']:<10}"
                f"{d['raw_register']:<8}成功   {d['received_at']}"
            )


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
    asyncio.run(main(url))
