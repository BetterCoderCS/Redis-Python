"""存储层：字符串键值、列表、过期时间与阻塞等待队列。

所有公开方法都是线程安全的，内部用一把互斥锁保护共享状态。
时钟通过构造函数注入，便于单元测试精确控制过期行为。
"""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field


def now_ms() -> int:
    """返回当前的毫秒时间戳。"""
    return int(time.time() * 1000)


def normalize_index(index: int, length: int) -> int:
    """把负索引转为正索引：-1 是最后一个，越界的负索引按 0 处理。"""
    if index < 0:
        index = length + index
        if index < 0:
            index = 0
    return index


@dataclass
class StringEntry:
    """一条字符串记录，expiry_ms 为 None 表示永不过期。"""

    value: bytes
    expiry_ms: int | None = None


@dataclass
class Waiter:
    """一个 BLPOP 等待者：event 用于阻塞/唤醒，value 用于传递弹出的元素。"""

    event: threading.Event = field(default_factory=threading.Event)
    value: bytes | None = None
    cancelled: bool = False


class Database:
    """线程安全的内存数据库。"""

    def __init__(self, clock: Callable[[], int] = now_ms) -> None:
        self._strings: dict[bytes, StringEntry] = {}
        self._lists: dict[bytes, list[bytes]] = {}
        self._blocked: dict[bytes, deque[Waiter]] = {}
        self._lock = threading.Lock()
        self._clock = clock

    # ---------------- 字符串 ----------------

    def set(self, key: bytes, value: bytes, ttl_ms: int | None = None) -> None:
        """设置键值:ttl_ms 为 None 表示永不过期，否则换算为绝对过期时间戳。"""
        expiry = None if ttl_ms is None else self._clock() + ttl_ms
        with self._lock:
            self._strings[key] = StringEntry(value, expiry)

    def get(self, key: bytes) -> bytes | None:
        """读取键值,惰性过期：发现已过期时删除并返回 None。"""
        with self._lock:
            entry = self._strings.get(key)
            if entry is None:
                return None
            if entry.expiry_ms is not None and self._clock() >= entry.expiry_ms:
                del self._strings[key]
                return None
            return entry.value

    # ---------------- 列表 ----------------

    def rpush(self, key: bytes, values: list[bytes]) -> int:
        """尾部追加元素，返回新长度；如有阻塞等待者则唤醒一个。"""
        with self._lock:
            items = self._lists.setdefault(key, [])
            items.extend(values)
            length = len(items)
            self._wake_blocked_locked(key)
            return length

    def lpush(self, key: bytes, values: list[bytes]) -> int:
        """头部逐个插入元素，返回新长度；如有阻塞等待者则唤醒一个。"""
        with self._lock:
            items = self._lists.setdefault(key, [])
            for value in values:
                items.insert(0, value)
            length = len(items)
            self._wake_blocked_locked(key)
            return length

    def lrange(self, key: bytes, start: int, stop: int) -> list[bytes]:
        """返回 [start, stop] 闭区间元素，支持负索引与越界。"""
        with self._lock:
            items = self._lists.get(key, [])
            length = len(items)
            start = normalize_index(start, length)
            stop = normalize_index(stop, length)
            # 切片天然处理 start > stop、越界等情况
            return list(items[start : stop + 1])

    def llen(self, key: bytes) -> int:
        """返回列表长度；列表不存在时返回 0。"""
        with self._lock:
            return len(self._lists.get(key, []))

    def lpop(self, key: bytes, count: int | None = None) -> bytes | list[bytes] | None:
        """从头部弹出元素。

        count 为 None 时返回单个元素（空列表返回 None);
        count 非 None 时返回元素列表（空列表返回 []）。
        """
        with self._lock:
            items = self._lists.get(key)
            if not items:
                return [] if count is not None else None
            if count is None:
                return items.pop(0)
            popped = items[:count]
            del items[:count]
            return popped

    # ---------------- 阻塞操作 ----------------

    def blpop_register(self, key: bytes) -> tuple[Waiter | None, bytes | None]:
        """登记 BLPOP 等待者。

        Returns:
            ``(waiter, value)``:
                - 列表已有元素：``(None, value)``，调用方可立即返回。
                - 列表为空：``(waiter, None)``，调用方需随后调用 blpop_wait。

        两个操作在锁内原子完成，避免「检查列表」与「登记等待」之间被 RPUSH 抢先。
        """
        with self._lock:
            items = self._lists.get(key)
            if items:
                return None, items.pop(0)
            waiter = Waiter()
            self._blocked.setdefault(key, deque()).append(waiter)
            return waiter, None

    def blpop_wait(self, key: bytes, waiter: Waiter, timeout_s: float) -> bytes | None:
        """等待唤醒。timeout_s <= 0 表示无限期等待；超时返回 None。

        超时与 RPUSH 唤醒可能同时发生：只要元素已经为此等待者弹出，
        即使 Event.wait 因超时返回 False，也必须把元素交还给它，避免丢数据。
        """
        woken = waiter.event.wait(timeout_s if timeout_s > 0 else None)
        with self._lock:
            if waiter.value is not None:
                # 唤醒方已在锁内为该等待者保留了元素
                return waiter.value
            if not woken:
                # 超时未被唤醒：标记为已取消，并从队列移除自己
                waiter.cancelled = True
                waiters = self._blocked.get(key)
                if waiters is not None and waiter in waiters:
                    waiters.remove(waiter)
                    if not waiters:
                        del self._blocked[key]
            return None

    def _wake_blocked_locked(self, key: bytes) -> None:
        """唤醒等待该 key 的最早有效客户端（调用方必须已持有 _lock）。"""
        waiters = self._blocked.get(key)
        items = self._lists.get(key)
        if not waiters or not items:
            return
        while waiters and items:
            waiter = waiters.popleft()
            if waiter.cancelled:
                # 超时清理还没拿到锁，跳过它，不能消费列表元素
                continue
            waiter.value = items.pop(0)
            waiter.event.set()
        if not waiters:
            del self._blocked[key]
