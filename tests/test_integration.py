"""端到端集成测试：启动真实 TCP 服务器，用原生 socket 客户端验证。

覆盖:单连接多命令、多客户端并发、TCP 分包、跨连接 BLPOP 唤醒与公平性。
"""

import socket
import time
import unittest
from typing import ClassVar

from mini_redis.server import RedisServer

RECV_SIZE = 4096


class Client:
    """极简 RESP 客户端，用于集成测试。"""

    def __init__(self, port: int) -> None:
        self.sock = socket.create_connection(("localhost", port), timeout=3)
        self.sock.settimeout(3)
        self.buffer = b""

    def send(self, payload: bytes) -> None:
        self.sock.sendall(payload)

    def recv(self, expected: bytes) -> bytes:
        """读满 expected 长度的字节再返回（TCP 不保证一次 recv 收全）。"""
        while len(self.buffer) < len(expected):
            chunk = self.sock.recv(RECV_SIZE)
            if not chunk:
                raise ConnectionError("server closed connection")
            self.buffer += chunk
        data, self.buffer = self.buffer[: len(expected)], self.buffer[len(expected) :]
        return data

    def expect(self, expected: bytes) -> None:
        actual = self.recv(expected)
        if actual != expected:
            raise AssertionError(f"expected {expected!r}, got {actual!r}")

    def close(self) -> None:
        self.sock.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()


def blpop(key: bytes, timeout: bytes) -> bytes:
    """构造 BLPOP key timeout 的 RESP 字节串。"""
    return (
        b"*3\r\n$5\r\nBLPOP\r\n$"
        + str(len(key)).encode()
        + b"\r\n"
        + key
        + b"\r\n$"
        + str(len(timeout)).encode()
        + b"\r\n"
        + timeout
        + b"\r\n"
    )


def rpush(key: bytes, *values: bytes) -> bytes:
    """构造 RPUSH key v... 的 RESP 字节串。"""
    out = b"*" + str(2 + len(values)).encode() + b"\r\n$5\r\nRPUSH\r\n"
    out += b"$" + str(len(key)).encode() + b"\r\n" + key + b"\r\n"
    for value in values:
        out += b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"
    return out


PING = b"*1\r\n$4\r\nPING\r\n"
SET_FOO = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
GET_FOO = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"


class TestIntegration(unittest.TestCase):
    # 在 setUpClass 中赋值，属于类级共享夹具，而非每个测试实例的字段
    server: ClassVar[RedisServer]

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = RedisServer("localhost", 0)  # 端口 0 = 系统分配
        cls.server.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    def test_ping(self) -> None:
        with Client(self.server.port) as c:
            c.send(PING)
            c.expect(b"+PONG\r\n")

    def test_utf8_payload_round_trip(self) -> None:
        """中文按 UTF-8 字节原样往返：长度前缀按字节数算，响应不被改写。"""
        value = "你好".encode("utf-8")
        self.assertEqual(len(value), 6)
        echo = (
            b"*2\r\n$4\r\nECHO\r\n$"
            + str(len(value)).encode()
            + b"\r\n"
            + value
            + b"\r\n"
        )
        expected = b"$6\r\n" + value + b"\r\n"
        with Client(self.server.port) as c:
            c.send(echo)
            c.expect(expected)

    def test_protocol_error_returns_error_and_closes_connection(self) -> None:
        with Client(self.server.port) as c:
            c.send(b"BAD\r\n")
            c.expect(b"-ERR Protocol error\r\n")
            self.assertEqual(c.sock.recv(1), b"")

    def test_multiple_commands_on_one_connection(self) -> None:
        with Client(self.server.port) as c:
            c.send(SET_FOO + GET_FOO + PING)
            # 三条响应分批到达，按长度逐段读取
            c.expect(b"+OK\r\n")
            c.expect(b"$3\r\nbar\r\n")
            c.expect(b"+PONG\r\n")

    def test_split_packet(self) -> None:
        """把一条命令拆成多次 send,服务器应缓冲后正确解析。"""
        with Client(self.server.port) as c:
            c.send(b"*3\r\n$3\r\nSE")
            c.send(b"T\r\n$3\r\nfo")
            c.send(b"o\r\n$3\r\nbar\r\n")
            c.expect(b"+OK\r\n")

    def test_concurrent_clients(self) -> None:
        """两个连接同时 PING,各自收到 PONG。"""
        with Client(self.server.port) as c1, Client(self.server.port) as c2:
            c1.send(PING)
            c2.send(PING)
            c1.expect(b"+PONG\r\n")
            c2.expect(b"+PONG\r\n")

    def test_cross_connection_set_get(self) -> None:
        with Client(self.server.port) as writer, Client(self.server.port) as reader:
            writer.send(b"*3\r\n$3\r\nSET\r\n$5\r\nshare\r\n$5\r\nvalue\r\n")
            writer.expect(b"+OK\r\n")
            reader.send(b"*2\r\n$3\r\nGET\r\n$5\r\nshare\r\n")
            reader.expect(b"$5\r\nvalue\r\n")

    def test_blpop_woken_by_push_from_another_connection(self) -> None:
        with Client(self.server.port) as blocker, Client(self.server.port) as pusher:
            blocker.send(blpop(b"lst", b"0"))
            time.sleep(0.05)  # 确保 BLPOP 已进入阻塞状态
            pusher.send(rpush(b"lst", b"foo"))
            pusher.expect(b":1\r\n")
            blocker.expect(b"*2\r\n$3\r\nlst\r\n$3\r\nfoo\r\n")

    def test_blpop_timeout(self) -> None:
        with Client(self.server.port) as c:
            c.send(blpop(b"empty_k", b"0.1"))
            c.expect(b"*-1\r\n")

    def test_list_commands(self) -> None:
        with Client(self.server.port) as c:
            c.send(rpush(b"l", b"a", b"b", b"c"))
            c.expect(b":3\r\n")
            c.send(b"*4\r\n$6\r\nLRANGE\r\n$1\r\nl\r\n$1\r\n0\r\n$2\r\n-1\r\n")
            c.expect(b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n")
            c.send(b"*2\r\n$4\r\nLLEN\r\n$1\r\nl\r\n")
            c.expect(b":3\r\n")

    def test_blpop_fifo_across_connections(self) -> None:
        """先阻塞的客户端先拿到元素。"""
        with (
            Client(self.server.port) as c1,
            Client(self.server.port) as c2,
            Client(self.server.port) as pusher,
        ):
            c1.send(blpop(b"fifolst", b"0"))
            time.sleep(0.05)
            c2.send(blpop(b"fifolst", b"0"))
            time.sleep(0.05)

            for _ in range(2):
                pusher.send(rpush(b"fifolst", b"value"))
                pusher.expect(b":1\r\n")

            expected = b"*2\r\n$7\r\nfifolst\r\n$5\r\nvalue\r\n"
            c1.expect(expected)
            c2.expect(expected)


if __name__ == "__main__":
    unittest.main()
