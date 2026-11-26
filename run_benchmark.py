#!/usr/bin/env python3
"""简单基准：测量内存存储与 TCP 往返的吞吐（纯标准库）。"""

import socket
import time

from mini_redis.server import RedisServer
from mini_redis.storage import Database

STORAGE_OPS = 50_000
PING_REQUESTS = 5_000


def bench_storage() -> None:
    db = Database()
    start = time.perf_counter()
    for i in range(STORAGE_OPS):
        db.set(f"k{i}".encode(), b"v")
    for i in range(STORAGE_OPS):
        db.get(f"k{i}".encode())
    elapsed = time.perf_counter() - start
    print(f"storage SET+GET: {2 * STORAGE_OPS / elapsed:,.0f} ops/s ({elapsed:.2f}s)")


def bench_network() -> None:
    server = RedisServer("localhost", 0)
    server.start()
    try:
        payload = b"*1\r\n$4\r\nPING\r\n"
        expected = b"+PONG\r\n"
        with socket.create_connection(("localhost", server.port), timeout=10) as sock:
            start = time.perf_counter()
            for _ in range(PING_REQUESTS):
                sock.sendall(payload)
                buf = b""
                while len(buf) < len(expected):
                    chunk = sock.recv(64)
                    if not chunk:
                        raise RuntimeError("connection closed unexpectedly")
                    buf += chunk
            elapsed = time.perf_counter() - start
        print(f"TCP PING: {PING_REQUESTS / elapsed:,.0f} req/s ({elapsed:.2f}s)")
    finally:
        server.stop()


if __name__ == "__main__":
    bench_storage()
    bench_network()
