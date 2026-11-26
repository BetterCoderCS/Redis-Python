"""RESP 协议层：负责解析请求与编码响应。

RESP (REdis Serialization Protocol) 是 Redis 客户端与服务器之间的二进制协议。
本模块只处理协议本身，不涉及网络 I/O 或数据存储。
"""

from __future__ import annotations

CRLF = b"\r\n"


class RespProtocolError(ValueError):
    """数据已足够但仍不符合 RESP 语法；继续等待不会变成合法命令。"""


# 响应类型前缀
SIMPLE_STRING = b"+"
ERROR = b"-"
INTEGER = b":"
BULK_STRING = b"$"
ARRAY = b"*"


def parse_resp(data: bytes) -> tuple[list[bytes] | None, int]:
    """从 data 开头解析一个 RESP 数组命令。

    命令形如 ``*2\\r\\n$4\\r\\nECHO\\r\\n$3\\r\\nhey\\r\\n``，
    解析结果为 ``[b"ECHO", b"hey"]``。

    Returns:
        ``(parts, consumed)``:
            - parts: 命令及参数列表；数据不完整时为 None。
            - consumed: 消耗的字节数；数据不完整时为 0，调用方应缓冲后重试。

    Raises:
        RespProtocolError: 数据已足够但仍不符合 RESP 语法；
            调用方应回复协议错误并关闭连接，而不是继续缓冲。

    注意:
        TCP 分包会造成「数据不完整」，这是常态，不抛异常；
        「语法非法」则继续等待也不会变合法，必须显式区分。
    """
    if not data:
        return None, 0
    if data[:1] != ARRAY:
        raise RespProtocolError("command must start with a RESP array")

    # 解析数组长度：*<count>\r\n
    end = data.find(CRLF)
    if end == -1:
        return None, 0
    try:
        count = int(data[1:end])
    except ValueError as exc:
        raise RespProtocolError("invalid array length") from exc
    if count < 0:
        raise RespProtocolError("negative array length")

    pos = end + 2
    parts: list[bytes] = []
    for _ in range(count):
        # 每个元素都应为 bulk string：$<length>\r\n<payload>\r\n
        if pos >= len(data):
            return None, 0
        if data[pos : pos + 1] != BULK_STRING:
            raise RespProtocolError("array element must be a bulk string")
        end = data.find(CRLF, pos)
        if end == -1:
            return None, 0
        try:
            length = int(data[pos + 1 : end])
        except ValueError as exc:
            raise RespProtocolError("invalid bulk string length") from exc
        if length < 0:
            raise RespProtocolError("negative bulk string length")

        pos = end + 2
        payload_end = pos + length
        # 数据区 + 尾部 CRLF 都到齐才能解析
        if len(data) < payload_end + 2:
            return None, 0
        if data[payload_end : payload_end + 2] != CRLF:
            raise RespProtocolError("bulk string must end with CRLF")
        parts.append(data[pos:payload_end])
        pos = payload_end + 2

    return parts, pos


def encode_simple_string(value: str | bytes) -> bytes:
    """编码简单字符串：+OK\\r\\n。"""
    if isinstance(value, str):
        value = value.encode("utf-8")
    return SIMPLE_STRING + value + CRLF


def encode_error(message: str | bytes) -> bytes:
    """编码错误响应：-ERR ...\\r\\n。"""
    if isinstance(message, str):
        message = message.encode("utf-8")
    return ERROR + message + CRLF


def encode_integer(value: int) -> bytes:
    """编码整数：:5\\r\\n。"""
    return INTEGER + str(value).encode("ascii") + CRLF


def encode_bulk_string(value: bytes | str | None) -> bytes:
    """编码 bulk string；value 为 None 时返回空 bulk string ``$-1\\r\\n``。"""
    if value is None:
        return BULK_STRING + b"-1" + CRLF
    if isinstance(value, str):
        value = value.encode("utf-8")
    return BULK_STRING + str(len(value)).encode("ascii") + CRLF + value + CRLF


def encode_array(items: list[bytes] | None) -> bytes:
    """编码 bulk string 数组;items 为 None 时返回 null 数组 ``*-1\\r\\n``。"""
    if items is None:
        return ARRAY + b"-1" + CRLF
    out = ARRAY + str(len(items)).encode("ascii") + CRLF
    for item in items:
        out += encode_bulk_string(item)
    return out
