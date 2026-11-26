"""RESP 协议层单元测试：解析分包、多命令，以及各类响应编码。"""

import unittest

from mini_redis.resp import (
    RespProtocolError,
    encode_array,
    encode_bulk_string,
    encode_error,
    encode_integer,
    encode_simple_string,
    parse_resp,
)


class TestParseResp(unittest.TestCase):
    def test_parse_single_command(self) -> None:
        data = b"*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n"
        self.assertEqual(parse_resp(data), ([b"ECHO", b"hey"], len(data)))

    def test_parse_command_with_empty_argument(self) -> None:
        data = b"*2\r\n$3\r\nSET\r\n$0\r\n\r\n"
        self.assertEqual(parse_resp(data), ([b"SET", b""], len(data)))

    def test_incomplete_data_returns_zero_consumed(self) -> None:
        # 缺少最后一个 CRLF，或 payload 长度还不够：都只是半包
        for data in (
            b"",
            b"*2\r\n$4\r\nECHO\r\n$3\r\nhey",
            b"*1\r\n$3\r\nab\r\n",
        ):
            with self.subTest(data=data):
                self.assertEqual(parse_resp(data), (None, 0))

    def test_parse_two_commands_and_report_consumed(self) -> None:
        first = b"*1\r\n$4\r\nPING\r\n"
        second = b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\nb\r\n"
        parts, consumed = parse_resp(first + second)
        self.assertEqual(parts, [b"PING"])
        self.assertEqual(consumed, len(first))
        # 用消耗字节数切掉第一条后，能解析出第二条
        self.assertEqual(parse_resp((first + second)[consumed:]), ([b"SET", b"a", b"b"], len(second)))

    def test_reassemble_split_packet(self) -> None:
        """模拟 TCP 分包：分两次喂入，拼起来后应能解析。"""
        full = b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n"
        for split_at in range(1, len(full)):
            with self.subTest(split_at=split_at):
                self.assertEqual(parse_resp(full[:split_at]), (None, 0))
                self.assertEqual(parse_resp(full), ([b"ECHO", b"hello"], len(full)))

    def test_invalid_inputs_raise_protocol_error(self) -> None:
        for data in (
            b"PING\r\n",
            b"*abc\r\n",
            b"*-1\r\n",
            b"*1\r\n+OK\r\n",
            b"*1\r\n$3\r\nabcXX",
        ):
            with self.subTest(data=data):
                with self.assertRaises(RespProtocolError):
                    parse_resp(data)


class TestEncode(unittest.TestCase):
    def test_simple_string(self) -> None:
        self.assertEqual(encode_simple_string("OK"), b"+OK\r\n")
        self.assertEqual(encode_simple_string(b"PONG"), b"+PONG\r\n")

    def test_error(self) -> None:
        self.assertEqual(encode_error("ERR bad"), b"-ERR bad\r\n")

    def test_integer(self) -> None:
        self.assertEqual(encode_integer(0), b":0\r\n")
        self.assertEqual(encode_integer(-3), b":-3\r\n")

    def test_bulk_string(self) -> None:
        self.assertEqual(encode_bulk_string(b"hey"), b"$3\r\nhey\r\n")
        self.assertEqual(encode_bulk_string(None), b"$-1\r\n")
        self.assertEqual(encode_bulk_string("中文"), b"$6\r\n\xe4\xb8\xad\xe6\x96\x87\r\n")

    def test_array(self) -> None:
        self.assertEqual(encode_array([b"a", b"bb"]), b"*2\r\n$1\r\na\r\n$2\r\nbb\r\n")
        self.assertEqual(encode_array([]), b"*0\r\n")
        self.assertEqual(encode_array(None), b"*-1\r\n")


if __name__ == "__main__":
    unittest.main()
