"""存储层单元测试：字符串过期、列表语义、BLPOP 阻塞与公平性。"""

import threading
import time
import unittest
from collections.abc import Callable

from mini_redis.storage import Database, normalize_index


class FakeClock:
    """可控时钟，用于精确测试过期逻辑。"""

    def __init__(self, start: int = 1000) -> None:
        self.now = start

    def __call__(self) -> int:
        return self.now

    def advance(self, ms: int) -> None:
        self.now += ms


class TimeoutFirstEvent:
    """模拟超时先返回，但在 wait 期间唤醒方已经写入 value 的竞态。"""

    def __init__(self, on_wait: Callable[[], None]) -> None:
        self._on_wait = on_wait
        self._set = False

    def wait(self, timeout: float | None = None) -> bool:
        self._on_wait()
        return False

    def set(self) -> None:
        self._set = True

    def is_set(self) -> bool:
        return self._set


class TestNormalizeIndex(unittest.TestCase):
    def test_negative_and_out_of_range(self) -> None:
        self.assertEqual(normalize_index(-1, 5), 4)
        self.assertEqual(normalize_index(-2, 5), 3)
        self.assertEqual(normalize_index(-6, 5), 0)
        self.assertEqual(normalize_index(2, 5), 2)


class TestStringStorage(unittest.TestCase):
    # 类体初始化仅用于满足类型检查；setUp 会为每个用例创建独立的时钟和数据库
    clock: FakeClock = FakeClock()
    db: Database = Database(clock=FakeClock())

    def setUp(self) -> None:
        self.clock = FakeClock()
        self.db = Database(clock=self.clock)

    def test_get_missing_key(self) -> None:
        self.assertIsNone(self.db.get(b"missing"))

    def test_set_and_get(self) -> None:
        self.db.set(b"foo", b"bar")
        self.assertEqual(self.db.get(b"foo"), b"bar")

    def test_ttl_expires(self) -> None:
        self.db.set(b"foo", b"bar", ttl_ms=100)
        self.clock.advance(99)
        self.assertEqual(self.db.get(b"foo"), b"bar")
        self.clock.advance(1)
        self.assertIsNone(self.db.get(b"foo"))

    def test_overwrite_clears_old_expiry(self) -> None:
        self.db.set(b"foo", b"old", ttl_ms=10)
        self.db.set(b"foo", b"new")  # 无过期时间
        self.clock.advance(1000)
        self.assertEqual(self.db.get(b"foo"), b"new")


class TestListStorage(unittest.TestCase):
    # 类体初始化仅用于满足类型检查；setUp 会为每个用例创建新数据库
    db: Database = Database()

    def setUp(self) -> None:
        self.db = Database()

    def test_rpush_and_lpush(self) -> None:
        self.assertEqual(self.db.rpush(b"l", [b"a", b"b"]), 2)
        self.assertEqual(self.db.lpush(b"l", [b"c", b"d"]), 4)
        self.assertEqual(self.db.lrange(b"l", 0, -1), [b"d", b"c", b"a", b"b"])

    def test_lrange_boundaries(self) -> None:
        self.db.rpush(b"l", [b"a", b"b", b"c", b"d", b"e"])
        cases = [
            ((0, 1), [b"a", b"b"]),
            ((2, 4), [b"c", b"d", b"e"]),
            ((-2, -1), [b"d", b"e"]),
            ((0, -3), [b"a", b"b", b"c"]),
            ((2, -1), [b"c", b"d", b"e"]),
            ((-6, -1), [b"a", b"b", b"c", b"d", b"e"]),
            ((10, 20), []),
            ((3, 1), []),
        ]
        for (start, stop), expected in cases:
            with self.subTest(start=start, stop=stop):
                self.assertEqual(self.db.lrange(b"l", start, stop), expected)

    def test_lrange_missing_key(self) -> None:
        self.assertEqual(self.db.lrange(b"missing", 0, -1), [])

    def test_llen(self) -> None:
        self.assertEqual(self.db.llen(b"missing"), 0)
        self.db.rpush(b"l", [b"a", b"b"])
        self.assertEqual(self.db.llen(b"l"), 2)

    def test_lpop_single_and_count(self) -> None:
        self.db.rpush(b"l", [b"a", b"b", b"c"])
        self.assertEqual(self.db.lpop(b"l"), b"a")
        self.assertEqual(self.db.lpop(b"l", 2), [b"b", b"c"])
        self.assertIsNone(self.db.lpop(b"l"))
        self.assertEqual(self.db.lpop(b"l", 3), [])


class TestBlockingList(unittest.TestCase):
    def test_rpush_wakes_blocked_client(self) -> None:
        db = Database()
        waiter, value = db.blpop_register(b"l")
        # 列表为空时注册等待者，返回值不可能是 None；收窄类型以替代 type: ignore
        assert waiter is not None
        self.assertIsNone(value)

        def push() -> None:
            time.sleep(0.02)
            db.rpush(b"l", [b"foo"])

        threading.Thread(target=push).start()
        self.assertEqual(db.blpop_wait(b"l", waiter, 1.0), b"foo")

    def test_immediate_pop_when_list_has_elements(self) -> None:
        db = Database()
        db.rpush(b"l", [b"ready"])
        waiter, value = db.blpop_register(b"l")
        self.assertIsNone(waiter)
        self.assertEqual(value, b"ready")

    def test_timeout_returns_none_and_cleans_up(self) -> None:
        db = Database()
        waiter, _ = db.blpop_register(b"l")
        # 空列表场景必然拿到等待者，显式收窄类型后再传给 blpop_wait
        assert waiter is not None
        self.assertIsNone(db.blpop_wait(b"l", waiter, 0.05))
        # 超时后等待者应被清理，后续 RPUSH 不应再尝试唤醒它
        self.assertEqual(db.rpush(b"l", [b"late"]), 1)
        self.assertEqual(db.lpop(b"l"), b"late")

    def test_timeout_racing_with_push_returns_reserved_value(self) -> None:
        db = Database()
        waiter, _ = db.blpop_register(b"l")
        assert waiter is not None

        def push_during_wait() -> None:
            db.rpush(b"l", [b"raced"])

        # 超时先返回 False，但唤醒方已经把元素留给了这个等待者
        waiter.event = TimeoutFirstEvent(push_during_wait)  # type: ignore[assignment]
        self.assertEqual(db.blpop_wait(b"l", waiter, 0.01), b"raced")
        self.assertIsNone(db.lpop(b"l"))

    def test_cancelled_waiter_does_not_consume_value(self) -> None:
        db = Database()
        waiter, _ = db.blpop_register(b"l")
        assert waiter is not None
        waiter.cancelled = True
        self.assertEqual(db.rpush(b"l", [b"kept"]), 1)
        self.assertEqual(db.lpop(b"l"), b"kept")

    def test_fifo_fairness(self) -> None:
        """等待最久的客户端先拿到元素。"""
        db = Database()
        waiter1, _ = db.blpop_register(b"l")
        waiter2, _ = db.blpop_register(b"l")
        # 两次注册都在空列表上完成，收窄类型后才能访问 value / event
        assert waiter1 is not None
        assert waiter2 is not None

        db.rpush(b"l", [b"first"])
        db.rpush(b"l", [b"second"])

        self.assertEqual(waiter1.value, b"first")
        self.assertEqual(waiter2.value, b"second")
        self.assertTrue(waiter1.event.is_set())
        self.assertTrue(waiter2.event.is_set())

    def test_lpush_also_wakes_waiter(self) -> None:
        db = Database()
        waiter, _ = db.blpop_register(b"l")
        # 空列表场景必然拿到等待者，收窄类型后再等待唤醒
        assert waiter is not None
        threading.Timer(0.02, db.lpush, args=(b"l", [b"x"])).start()
        self.assertEqual(db.blpop_wait(b"l", waiter, 1.0), b"x")


if __name__ == "__main__":
    unittest.main()
