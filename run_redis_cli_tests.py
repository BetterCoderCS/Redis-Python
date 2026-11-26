#!/usr/bin/env python3
"""用 redis-cli 对 mini-redis 做端到端命令测试。

需要先安装 Redis,并确保 redis-cli 在 PATH 中。脚本会在随机端口启动本项目服务器，
逐条执行 README 中的示例命令并断言 redis-cli 的 --raw 输出。
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import threading
import time

from mini_redis.server import RedisServer

CLI = shutil.which("redis-cli")


def run_cli(port: int, *args: str, timeout: float = 5.0) -> str:
    """执行一次 redis-cli,返回去掉行尾空白的原始输出。"""
    result = subprocess.run(
        [str(CLI), "--raw", "-p", str(port), *args],
        capture_output=True,
        text=True,
        encoding="utf-8",  # 避免 Windows 本地代码页把中文响应当成 GBK 解码
        timeout=timeout,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(f"redis-cli {args} failed: {result.stderr.strip()}")
    return result.stdout.replace("\r\n", "\n").rstrip("\n")


def main() -> int:
    if CLI is None:
        print(
            "redis-cli not found. Install Redis and add redis-cli to PATH.",
            file=sys.stderr,
        )
        return 1

    server = RedisServer("localhost", 0)
    server.start()
    failures: list[str] = []

    def check(name: str, args: tuple[str, ...], expected: str) -> None:
        try:
            actual = run_cli(server.port, *args)
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{name}: {exc}")
            print(f"FAIL {name}: {exc}")
            return
        if actual != expected:
            failures.append(f"{name}: expected {expected!r}, got {actual!r}")
            print(f"FAIL {name}: expected {expected!r}, got {actual!r}")
        else:
            print(f"PASS {name}")

    try:
        check("PING", ("PING",), "PONG")
        check("PING lowercase", ("ping",), "PONG")
        check("ECHO", ("ECHO", "hey"), "hey")
        check("ECHO UTF-8", ("ECHO", "你好"), "你好")
        check("SET", ("SET", "foo", "bar"), "OK")
        check("GET", ("GET", "foo"), "bar")
        check("GET missing", ("GET", "missing"), "")
        check("SET PX", ("SET", "pxkey", "pxval", "PX", "5000"), "OK")
        check("GET after PX set", ("GET", "pxkey"), "pxval")
        check("RPUSH", ("RPUSH", "tasks", "a", "b", "c"), "3")
        check("LRANGE", ("LRANGE", "tasks", "0", "-1"), "a\nb\nc")
        check("LLEN", ("LLEN", "tasks"), "3")
        check("LPOP single", ("LPOP", "tasks"), "a")
        check("LPOP count", ("LPOP", "tasks", "2"), "b\nc")

        def push_later() -> None:
            time.sleep(0.15)
            try:
                run_cli(server.port, "RPUSH", "queue", "x")
            except Exception as exc:  # noqa: BLE001
                failures.append(f"BLPOP pusher: {exc}")

        threading.Thread(target=push_later, daemon=True).start()
        check("BLPOP", ("BLPOP", "queue", "1"), "queue\nx")
    finally:
        server.stop()

    if failures:
        print(f"\n{len(failures)} check(s) failed.")
        return 1
    print("\nAll redis-cli checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
