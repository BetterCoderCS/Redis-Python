"""命令分派层：把解析后的命令映射到存储操作，并编码为 RESP 响应。"""

from __future__ import annotations

from collections.abc import Callable

from .resp import (
    encode_array,
    encode_bulk_string,
    encode_error,
    encode_integer,
    encode_simple_string,
)
from .storage import Database

Handler = Callable[[Database, list[bytes]], bytes]

# 命令名不区分大小写：解析时统一 upper()
COMMANDS: dict[bytes, Handler] = {}


def command(name: str) -> Callable[[Handler], Handler]:
    """注册命令处理器的装饰器。"""

    def decorator(func: Handler) -> Handler:
        COMMANDS[name.upper().encode("ascii")] = func
        return func

    return decorator


def execute(db: Database, parts: list[bytes]) -> bytes:
    """执行一条命令，返回编码好的 RESP 响应。"""
    if not parts:
        return encode_error("ERR empty command")

    name = parts[0].upper()
    handler = COMMANDS.get(name)
    if handler is None:
        shown = parts[0].decode("utf-8", errors="replace")
        return encode_error(f"ERR unknown command '{shown}'")
    return handler(db, parts)


def _wrong_args(name: str) -> bytes:
    return encode_error(f"ERR wrong number of arguments for '{name.lower()}' command")


def _parse_int(value: bytes) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None


@command("PING")
def _ping(db: Database, parts: list[bytes]) -> bytes:
    return encode_simple_string("PONG")


@command("ECHO")
def _echo(db: Database, parts: list[bytes]) -> bytes:
    if len(parts) != 2:
        return _wrong_args("echo")
    return encode_bulk_string(parts[1])


@command("SET")
def _set(db: Database, parts: list[bytes]) -> bytes:
    if len(parts) < 3:
        return _wrong_args("set")
    key, value = parts[1], parts[2]
    ttl_ms: int | None = None

    # 可选参数：EX <秒> / PX <毫秒>，选项名不区分大小写
    if len(parts) > 3:
        if len(parts) != 5:
            return _wrong_args("set")
        option = parts[3].upper()
        amount = _parse_int(parts[4])
        if amount is None or amount < 0:
            return encode_error("ERR value is not an integer or out of range")
        if option == b"PX":
            ttl_ms = amount
        elif option == b"EX":
            ttl_ms = amount * 1000
        else:
            return encode_error(f"ERR syntax error")

    db.set(key, value, ttl_ms)
    return encode_simple_string("OK")


@command("GET")
def _get(db: Database, parts: list[bytes]) -> bytes:
    if len(parts) != 2:
        return _wrong_args("get")
    return encode_bulk_string(db.get(parts[1]))


@command("RPUSH")
def _rpush(db: Database, parts: list[bytes]) -> bytes:
    if len(parts) < 3:
        return _wrong_args("rpush")
    return encode_integer(db.rpush(parts[1], parts[2:]))


@command("LPUSH")
def _lpush(db: Database, parts: list[bytes]) -> bytes:
    if len(parts) < 3:
        return _wrong_args("lpush")
    return encode_integer(db.lpush(parts[1], parts[2:]))


@command("LRANGE")
def _lrange(db: Database, parts: list[bytes]) -> bytes:
    if len(parts) != 4:
        return _wrong_args("lrange")
    start, stop = _parse_int(parts[2]), _parse_int(parts[3])
    if start is None or stop is None:
        return encode_error("ERR value is not an integer or out of range")
    return encode_array(db.lrange(parts[1], start, stop))


@command("LLEN")
def _llen(db: Database, parts: list[bytes]) -> bytes:
    if len(parts) != 2:
        return _wrong_args("llen")
    return encode_integer(db.llen(parts[1]))


@command("LPOP")
def _lpop(db: Database, parts: list[bytes]) -> bytes:
    if len(parts) not in (2, 3):
        return _wrong_args("lpop")
    if len(parts) == 2:
        value = db.lpop(parts[1])
        return encode_bulk_string(value if isinstance(value, bytes) else None)
    count = _parse_int(parts[2])
    if count is None:
        return encode_error("ERR value is not an integer or out of range")
    if count <= 0:
        return encode_array([])
    values = db.lpop(parts[1], count)
    return encode_array(values if isinstance(values, list) else [])


@command("BLPOP")
def _blpop(db: Database, parts: list[bytes]) -> bytes:
    if len(parts) != 3:
        return _wrong_args("blpop")
    try:
        timeout = float(parts[2])
    except ValueError:
        return encode_error("ERR timeout is not a float or out of range")
    if timeout < 0:
        timeout = 0.0

    key = parts[1]
    waiter, value = db.blpop_register(key)
    if waiter is not None:
        # 在锁外阻塞，不占用全局锁；timeout=0 表示无限期等待
        value = db.blpop_wait(key, waiter, timeout)
    if value is None:
        return encode_array(None)
    return encode_array([key, value])
