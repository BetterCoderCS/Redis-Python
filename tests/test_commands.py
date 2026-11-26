"""命令层单元测试：逐条验证命令的 RESP 字节输出。"""

import unittest

from mini_redis.commands import execute
from mini_redis.storage import Database, now_ms


class TestCommands(unittest.TestCase):
    # 类体初始化仅用于满足类型检查；setUp 会为每个用例重新创建数据库
    db: Database = Database()

    def setUp(self) -> None:
        self.db = Database()

    def run_cmd(self, *args: bytes) -> bytes:
        return execute(self.db, list(args))

    def test_ping(self) -> None:
        self.assertEqual(self.run_cmd(b"PING"), b"+PONG\r\n")
        self.assertEqual(self.run_cmd(b"ping"), b"+PONG\r\n")  # 大小写不敏感

    def test_echo(self) -> None:
        self.assertEqual(self.run_cmd(b"ECHO", b"hey"), b"$3\r\nhey\r\n")
        self.assertEqual(self.run_cmd(b"EcHo", b"hi"), b"$2\r\nhi\r\n")

    def test_set_and_get(self) -> None:
        self.assertEqual(self.run_cmd(b"SET", b"foo", b"bar"), b"+OK\r\n")
        self.assertEqual(self.run_cmd(b"GET", b"foo"), b"$3\r\nbar\r\n")
        self.assertEqual(self.run_cmd(b"get", b"missing"), b"$-1\r\n")

    def test_set_px_expiry(self) -> None:
        self.assertEqual(self.run_cmd(b"SET", b"k", b"v", b"px", b"30"), b"+OK\r\n")
        self.assertEqual(self.run_cmd(b"GET", b"k"), b"$1\r\nv\r\n")
        # 直接用时钟验证过期，避免测试 sleep
        self.db.set(b"k2", b"v2", ttl_ms=-1)
        self.assertEqual(self.run_cmd(b"GET", b"k2"), b"$-1\r\n")

    def test_set_ex_expiry(self) -> None:
        self.assertEqual(self.run_cmd(b"SET", b"k", b"v", b"EX", b"1"), b"+OK\r\n")
        self.assertEqual(self.run_cmd(b"GET", b"k"), b"$1\r\nv\r\n")

    def test_set_invalid_ttl(self) -> None:
        response = self.run_cmd(b"SET", b"k", b"v", b"PX", b"abc")
        self.assertTrue(response.startswith(b"-ERR "))

    def test_rpush_lpush_lrange(self) -> None:
        self.assertEqual(self.run_cmd(b"RPUSH", b"l", b"a", b"b"), b":2\r\n")
        self.assertEqual(self.run_cmd(b"LPUSH", b"l", b"c"), b":3\r\n")
        self.assertEqual(
            self.run_cmd(b"LRANGE", b"l", b"0", b"-1"),
            b"*3\r\n$1\r\nc\r\n$1\r\na\r\n$1\r\nb\r\n",
        )

    def test_lrange_missing_key(self) -> None:
        self.assertEqual(self.run_cmd(b"LRANGE", b"missing", b"0", b"2"), b"*0\r\n")

    def test_llen(self) -> None:
        self.assertEqual(self.run_cmd(b"LLEN", b"l"), b":0\r\n")
        self.run_cmd(b"RPUSH", b"l", b"a", b"b", b"c")
        self.assertEqual(self.run_cmd(b"LLEN", b"l"), b":3\r\n")

    def test_lpop_single(self) -> None:
        self.run_cmd(b"RPUSH", b"l", b"one", b"two")
        self.assertEqual(self.run_cmd(b"LPOP", b"l"), b"$3\r\none\r\n")
        self.assertEqual(
            self.run_cmd(b"LRANGE", b"l", b"0", b"-1"), b"*1\r\n$3\r\ntwo\r\n"
        )
        self.assertEqual(self.run_cmd(b"LPOP", b"missing"), b"$-1\r\n")

    def test_lpop_multiple(self) -> None:
        self.run_cmd(b"RPUSH", b"l", b"one", b"two", b"three")
        self.assertEqual(
            self.run_cmd(b"LPOP", b"l", b"2"),
            b"*2\r\n$3\r\none\r\n$3\r\ntwo\r\n",
        )
        self.assertEqual(self.run_cmd(b"LPOP", b"l", b"10"), b"*1\r\n$5\r\nthree\r\n")

    def test_blpop_immediate(self) -> None:
        self.run_cmd(b"RPUSH", b"l", b"foo")
        self.assertEqual(
            self.run_cmd(b"BLPOP", b"l", b"0"),
            b"*2\r\n$1\r\nl\r\n$3\r\nfoo\r\n",
        )

    def test_blpop_timeout(self) -> None:
        response = self.run_cmd(b"BLPOP", b"l", b"0.01")
        self.assertEqual(response, b"*-1\r\n")

    def test_error_responses(self) -> None:
        cases = [
            ([], b"-ERR empty command\r\n"),
            ([b"NOPE"], b"-ERR unknown command 'NOPE'\r\n"),
            ([b"PING", b"extra"], b"+PONG\r\n"),  # PING 忽略多余参数
            ([b"GET"], b"-ERR wrong number of arguments for 'get' command\r\n"),
            (
                [b"LPOP", b"l", b"abc"],
                b"-ERR value is not an integer or out of range\r\n",
            ),
        ]
        for parts, expected in cases:
            with self.subTest(parts=parts):
                self.assertEqual(execute(self.db, parts), expected)

    def test_now_ms_returns_int(self) -> None:
        self.assertIsInstance(now_ms(), int)


if __name__ == "__main__":
    unittest.main()
