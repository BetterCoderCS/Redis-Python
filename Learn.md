# Learn.md：mini-redis 实现原理梳理

本文记录当前项目「为什么这么写」，而不是逐行翻译代码。配合 `README.md` 的命令列表和 `python run_benchmark.py` 的数据一起阅读效果更好。

---

## 1. 整体分层

```
客户端 (redis-cli)
    │  RESP 字节流
    ▼
server.py        TCP 监听、每连接一线程、缓冲与收发
    │  bytes buffer
    ▼
resp.py          RESP 编解码：parse_resp / encode_*
    │  list[bytes]
    ▼
commands.py      命令注册表与处理器：execute(db, parts)
    │  方法调用
    ▼
storage.py       Database：字符串 / 列表 / 阻塞队列 + 一把锁
```

每一层只依赖下一层：

- `resp.py` 不碰网络和存储；
- `commands.py` 不碰 socket，只接收解析好的 `list[bytes]`；
- `storage.py` 不关心 RESP，只提供线程安全的内存操作；
- `server.py` 负责把三者串起来并管理连接生命周期。

这种分层让单元测试可以逐层打：协议测试不需要起服务器，命令测试不需要真实 socket，存储测试不需要网络。

---

## 2. RESP 协议层：`mini_redis/resp.py`

### 2.1 请求与响应的形状

请求统一是 **RESP 数组**，数组里每个元素都是 **bulk string**：

```
*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
```

解析后得到 `[b"ECHO", b"hey"]`。bulk string 用长度前缀，所以 payload 里可以包含任意字节（包括 `\r\n`），是二进制安全的。

响应则按命令返回四种类型：

- simple string：`+OK\r\n`
- error：`-ERR ...\r\n`
- integer：`:3\r\n`
- bulk string：`$3\r\nbar\r\n`，不存在时 `$-1\r\n`
- array：`*2\r\n$1\r\na\r\n$1\r\nb\r\n`，null 数组 `*-1\r\n`

对应函数分别是 `encode_simple_string`、`encode_error`、`encode_integer`、`encode_bulk_string`、`encode_array`。

### 2.2 `parse_resp` 的返回值设计

`parse_resp(data) -> tuple[list[bytes] | None, int]` 有两个输出：

- `parts`：解析出的命令；数据不完整时为 `None`。
- `consumed`：本次消耗了多少字节；数据不完整时为 `0`。

关键点是**消费长度**而不是只返回命令。TCP 可能一次 `recv` 收到多条命令，也可能只收到半条：

- 收到多条：调用方按 `consumed` 切掉已处理部分，继续循环解析剩余 buffer；
- 收到半条：`consumed == 0`，调用方保留 buffer，等下一次 `recv` 追加后重试。

### 2.3 半包 vs 语法非法

这是协议层最容易写错的地方。两者表现都很像「暂时解析不出来」，但处理方式完全相反：

| 情况 | 例子 | 含义 | 处理 |
|---|---|---|---|
| 半包 | `*2\r\n$4\r\nECHO\r\n$3\r\nhey` | 字节还不够，再等等会合法 | 返回 `(None, 0)`，继续缓冲 |
| 语法非法 | `PING\r\n`、`*abc\r\n`、`*1\r\n+OK\r\n` | 字节已经够了，但永远不合法 | 抛 `RespProtocolError` |

如果混为一谈，客户端发来非法数据后，buffer 会一直保留这些字节，后续所有合法命令都解析不出来，连接实际作废但服务器毫不知情。现在 `server.py` 捕获 `RespProtocolError` 后会返回 `-ERR Protocol error` 并关闭连接。

判断规则：CRLF 还没到、payload 还没收齐 → 半包；前缀不对、长度不是整数、长度是负数、元素不是 `$`、尾部不是 CRLF → 语法非法。

---

## 3. 网络层：`mini_redis/server.py`

### 3.1 监听与生命周期

`create_server_socket()` 优先用 `socket.create_server(..., reuse_port=True)`，Windows/Cygwin 不支持时回退到普通创建。端口传 `0` 时由系统分配，实际端口从 `getsockname()[1]` 读出，集成测试就靠这个避免端口冲突。

`RedisServer` 提供两种运行方式：

- `start()`：后台 accept 线程 + `_ready` Event，非阻塞返回，适合测试；
- `serve_forever()`：当前线程直接跑 accept 循环，适合命令行入口。

`stop()` 关闭监听 socket，并对已记录的客户端 socket 做 `shutdown` + `close`，让阻塞在 `recv` 的连接线程尽快退出。

### 3.2 每连接一线程

`_accept_loop()` 每接受一个连接就起一个 daemon 线程，交给 `ClientConnection.run()`。连接线程的逻辑是：

1. `recv(4096)` 追加到本地 `buffer`；
2. 只要 buffer 非空就调用 `parse_resp`；
3. `consumed == 0` → 跳出内层循环，等下一次 `recv`；
4. 否则切掉 `consumed` 字节，调用 `execute(db, parts)` 得到 RESP 响应；
5. `sendall(response)`；若协议非法则发错误响应并 `return`（连接关闭时 `with self._sock` 自动清理）。

共享的 `Database` 只有一个实例，被所有连接线程共享，因此线程安全完全由 `storage.py` 的锁保证。

**取舍**：线程模型直观、调试容易，但每个连接一个线程，不适合 C10K 级别并发。README 里也明确把它列为与生产级 Redis 的差距。

---

## 4. 命令层：`mini_redis/commands.py`

### 4.1 装饰器注册表

```python
COMMANDS: dict[bytes, Handler] = {}

def command(name: str):
    def decorator(func):
        COMMANDS[name.upper().encode("ascii")] = func
        return func
    return decorator
```

每个处理器用 `@command("GET")` 注册；`execute()` 把命令名 `upper()` 后查表。因此命令名和 SET 的 `EX`/`PX` 选项都天然大小写不敏感。

新增命令只需要写一个处理器并加装饰器，不需要改 `execute()`。这也是后续跟随 Build Your Own Redis 加命令时最省事的地方。

### 4.2 处理器的共同套路

1. 先校验参数个数，不对就返回 `_wrong_args(...)`；
2. 用 `_parse_int` / `float()` 解析数值参数，失败返回规范错误；
3. 调用 `db` 的方法；
4. 用 `resp.py` 的编码函数把结果变回 RESP 字节。

当前命令按语义分三组：

- 字符串：`SET key value [EX|PX n]`（TTL 存成绝对毫秒时间戳）、`GET key`（惰性过期）；
- 列表：`RPUSH`、`LPUSH`、`LRANGE`（闭区间 + 负索引）、`LLEN`、`LPOP [count]`；
- 阻塞列表：`BLPOP key timeout`，`0` 表示无限等待，超时返回 null 数组。

`LPOP` 有一个类型上的细节：`Database.lpop()` 同时服务「不带 count」和「带 count」两种调用，静态返回类型是 `bytes | list[bytes] | None`。因为处理器在调用时已经确定了参数形态，所以用 `isinstance(...)` 在运行期收窄后再交给 `encode_bulk_string` / `encode_array`。

---

## 5. 存储层：`mini_redis/storage.py`

### 5.1 数据结构

```python
class Database:
    _strings: dict[bytes, StringEntry]      # 字符串 + 过期时间
    _lists:   dict[bytes, list[bytes]]      # 列表
    _blocked: dict[bytes, deque[Waiter]]    # 每个 key 的 BLPOP 等待队列
    _lock:    threading.Lock                # 保护以上所有状态
    _clock:   Callable[[], int]             # 可注入的毫秒时钟
```

`StringEntry(value, expiry_ms)` 中 `expiry_ms=None` 表示永不过期；`Set` 时换算成 `_clock() + ttl_ms` 的绝对时间戳。

### 5.2 惰性过期与时钟注入

`GET` 时才检查 `expiry_ms`：

- 当前时间 `>=` 过期时间 → 删掉该键，返回 `None`；
- 否则返回值。

好处是实现极简；代价是过期键如果一直没被访问就一直占内存。项目明确把它列为「与官方 Redis 的差距」之一。

时钟通过构造函数注入（`Database(clock=FakeClock())`），测试可以精确推进时间，不需要 `time.sleep()`。这是整个项目里最值得借鉴的可测试性设计。

### 5.3 列表操作与索引归一化

`normalize_index(index, length)` 把负索引换算成正索引，越界的负索引钳到 `0`。`LRANGE` 直接切片 `items[start:stop+1]`，Python 切片天然处理 `start > stop` 和越界。

`RPUSH`/`LPUSH` 在修改列表后都会调用 `_wake_blocked_locked(key)`，尝试唤醒一个等待者。

### 5.4 BLPOP：注册、等待、唤醒

这是项目里并发最复杂的部分，拆成三步：

**① `blpop_register(key)` —— 原子地「立即取」或「登记等待」**

在锁内检查列表：

- 有元素：直接 `pop(0)`，返回 `(None, value)`，调用方无需等待；
- 空列表：创建 `Waiter`，放进 `_blocked[key]` 的 `deque` 尾部，返回 `(waiter, None)`。

「检查」和「登记」在同一把锁内完成，避免这中间被 `RPUSH` 抢先导致元素没人接。

**② `blpop_wait(key, waiter, timeout)` —— 锁外等待**

```python
woken = waiter.event.wait(timeout_s if timeout_s > 0 else None)
```

`Event.wait()` 在**锁外**调用，这一点很关键：如果拿着全局锁去等，其他客户端的 `GET`/`RPUSH` 全部会被卡住。

**③ `_wake_blocked_locked(key)` —— 锁内 FIFO 唤醒**

由 `RPUSH`/`LPUSH` 在持锁状态下调用：

- 从 `deque` 头部 `popleft()`，保证「等得最久的先拿到」；
- 从列表头弹出一个元素，写到 `waiter.value`；
- `waiter.event.set()` 唤醒等待线程。

**④ 超时与唤醒的竞态修复**

最隐蔽的 bug：等待线程超时返回 `False` 后，还没拿到锁去清理自己；这时 `RPUSH` 抢到锁，把该等待者从队列里弹出、取走元素、设置 event。等等待线程终于拿到锁时，它发现队列里已经没有自己，直接返回 `None`——元素被消费了却没人收到。

现在用两个字段解决：

- `waiter.cancelled`：超时清理时标记；唤醒方跳过已取消的等待者，绝不为其消费元素；
- `waiter.value`：`blpop_wait` 在锁内先检查它。只要值已被写入，即使 `Event.wait()` 因超时返回 `False`，也把值交还给调用方。

`tests/test_storage.py` 里的 `TimeoutFirstEvent` 用一个假的 event 把「超时先返回、唤醒随后发生」这个交错变成确定性测试，不依赖 sleep 碰运气。

### 5.5 锁的边界总结

- 所有共享状态的读写都在 `with self._lock` 内；
- `Event.wait()` 和 `sendall()` 这类可能阻塞的 I/O 都在锁外；
- 唤醒和超时清理都要进锁，靠 `cancelled` / `value` 协调先后顺序。

---

## 6. 测试与质量保障

| 文件 | 层次 | 覆盖重点 |
|---|---|---|
| `tests/test_resp.py` | 协议 | 单命令、多命令、任意位置拆包、空参数、半包、语法非法 |
| `tests/test_storage.py` | 存储 | TTL 惰性过期、列表语义、BLPOP 唤醒/超时/FIFO、超时竞态 |
| `tests/test_commands.py` | 命令 | 每条命令的精确 RESP 字节输出和错误响应 |
| `tests/test_integration.py` | 端到端 | 真实 TCP、并发连接、跨连接 BLPOP、协议错误断连 |
| `run_redis_cli_tests.py` | 外部工具 | 调用系统 `redis-cli --raw` 验证 README 示例命令的输出 |

设计上的几个点：

- 时钟注入让过期测试完全确定；
- `blpop_register` 返回 `Waiter | None`，测试里用 `assert waiter is not None` 做真实收窄，而不是靠 `# type: ignore`；
- `TestIntegration.server` 标注为 `ClassVar[RedisServer]`，因为它由 `setUpClass` 在类上赋值，所有测试共享同一个服务器；
- CI 矩阵覆盖 Python 3.10–3.13，先 `mypy .` 再 `pytest -v`；本地 `pyright .` 也为 0 error / 0 warning。

`run_benchmark.py` 提供可复现的吞吐测量；本地实测：内存 SET+GET 1,370,715 ops/s（0.07s），TCP PING 11,516 req/s（0.43s）（Python 3.13，单次结果，仅作量级参考）。

---

## 7. 已知边界与后续方向

当前实现**没有**：

- 持久化（RDB / AOF）和主动过期删除；
- 事务、发布订阅、主从复制、集群；
- 完整命令集（`DEL`、`EXISTS`、`TTL`、`INCR`、`RPOP`、`BRPOP` 等）；
- 事件循环 / IO 多路复用，目前是每连接一线程。

跟随 Build Your Own Redis 继续做时，建议保持现有分层：

1. 新命令：在 `commands.py` 加 `@command` 处理器，参数校验返回规范错误；
2. 新数据结构/语义：在 `storage.py` 加方法，共享状态一律进锁；
3. 新协议类型：在 `resp.py` 加解析/编码，并补半包与非法输入两类测试；
4. 每完成一个阶段就更新 `README.md`、补测试、单独提一个 commit，让仓库历史能体现学习路径。

尤其要注意：真实 Redis 的很多「简单命令」会引入新的并发交互（比如 `BRPOP` 和 `BLPOP` 共享同一套 blocked 队列语义），复用 `Waiter` 模型时能省掉大量重复代码。
