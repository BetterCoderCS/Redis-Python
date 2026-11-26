# mini-redis

> 一个用纯 Python 标准库从零实现的 Redis 兼容服务器 —— 包含 RESP 协议、并发连接、过期键、列表与阻塞命令。

<!-- 上传 GitHub 后把 OWNER 替换为你的用户名即可启用徽章 -->
[![CI](https://github.com/OWNER/mini-redis/actions/workflows/ci.yml/badge.svg)](https://github.com/OWNER/mini-redis/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Dependencies](https://img.shields.io/badge/dependencies-none-green)

这是 [CodeCrafters "Build Your Own Redis"](https://app.codecrafters.io/courses/redis) 挑战的重构版实现。原始版本是单文件练习代码；本项目将其拆分为分层架构、补全类型注解与错误处理，并配上完整的单元 / 集成测试与 CI。

---

## 快速开始

```bash
# 启动服务器（默认 localhost:6379）
python run_server.py

# 或指定地址端口
python run_server.py 127.0.0.1 6380

# 另一种启动方式
python -m mini_redis
```

另开一个终端，用 `redis-cli` 连接：

```bash
redis-cli -p 6379
```

```
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET foo bar PX 5000
OK
127.0.0.1:6379> GET foo
"bar"
127.0.0.1:6379> RPUSH tasks a b c
(integer) 3
127.0.0.1:6379> LRANGE tasks 0 -1
1) "a"
2) "b"
3) "c"
127.0.0.1:6379> LPOP tasks 2
1) "a"
2) "b"
```

服务器按字节原样存取，UTF-8 等非 ASCII 内容不会被改写。如果 `redis-cli` 把中文显示为 `"\xe4\xbd\xa0\xe5\xa5\xbd"`，那是 CLI 在 TTY 下对非打印字节做的转义显示，不是服务器返回错了。用 `--raw` 即可直接看到中文：

```bat
redis-cli --raw echo "你好"
```

输出：

```
你好
```

交互模式同样在启动时加 `--raw`：

```bat
redis-cli --raw -p 6379
```

> `--raw` 关闭 CLI 的格式化转义。如果终端代码页不是 UTF-8（可用 `chcp` 查看），
> 还需先执行 `chcp 65001`，让 cmd 按 UTF-8 解码收到的字节。

## 运行测试

```bash
# 零依赖，使用标准库 unittest
python run_tests.py

# 如果装了 pytest(CI使用此方式)
python -m pytest -v

# 类型检查（CI 也会执行）
python -m mypy .

# 吞吐基准（约 1 秒）
python run_benchmark.py

# 用 redis-cli 做端到端验证（需先安装 Redis 并将 redis-cli 加入 PATH）
python run_redis_cli_tests.py
```

测试共 53 个，分四层：

| 文件 | 覆盖内容 |
|------|----------|
| `tests/test_resp.py` | RESP 解析（分包、多命令、非法输入）与全部编码函数 |
| `tests/test_storage.py` | 字符串过期、列表语义、BLPOP 唤醒 / 超时 / FIFO 公平性 |
| `tests/test_commands.py` | 每条命令的精确 RESP 字节输出、错误响应 |
| `tests/test_integration.py` | 真实 TCP 连接：并发客户端、跨连接 SET/GET、BLPOP 唤醒 |

`run_redis_cli_tests.py` 是供 CodeCrafters 风格验证使用的外部脚本：它在随机端口启动服务器，调用系统里的 `redis-cli --raw` 逐条执行 README 示例命令并断言输出。脚本在找不到 `redis-cli` 时会直接报错退出，不会影响零依赖的 `run_tests.py`。

---

## 已实现的命令

| 命令 | 说明 | 关键细节 |
|------|------|----------|
| `PING` | 返回 `+PONG` | — |
| `ECHO msg` | 回显参数 | 返回 bulk string |
| `SET key value [EX/PX n]` | 设置键值 | `EX` 秒 / `PX` 毫秒，选项大小写不敏感 |
| `GET key` | 读取键值 | 惰性过期，不存在返回 `$-1` |
| `RPUSH key v...` | 尾部追加 | 支持一次多个元素，返回新长度 |
| `LPUSH key v...` | 头部插入 | 逐个头插，返回新长度 |
| `LRANGE key start stop` | 区间查询 | 闭区间 + 负索引，越界自动收敛 |
| `LLEN key` | 列表长度 | 不存在的列表返回 `:0` |
| `LPOP key [count]` | 头部弹出 | 不带 count 返回 bulk string；带 count 返回数组 |
| `BLPOP key timeout` | 阻塞弹出 | `0` 无限等待；浮点秒数超时；FIFO 公平唤醒 |

命令名与选项名均不区分大小写。

---

## 架构设计

```
              ┌──────────────────────────────────────────┐
  客户端 ───► │  server.py    TCP 监听 / 每连接一线程      │
 (redis-cli)  │  ClientConnection: 缓冲 + 解析循环        │
              └───────────────┬──────────────────────────┘
                              │  原始字节
                              ▼
              ┌──────────────────────────────────────────┐
              │  resp.py      RESP 编解码                 │
              │  parse_resp() → (parts, consumed)         │
              └───────────────┬──────────────────────────┘
                              │  list[bytes]
                              ▼
              ┌──────────────────────────────────────────┐
              │  commands.py  命令分派（装饰器注册表）     │
              │  execute(db, parts) → RESP bytes          │
              └───────────────┬──────────────────────────┘
                              │  方法调用
                              ▼
              ┌──────────────────────────────────────────┐
              │  storage.py   Database（线程安全）        │
              │  _strings / _lists / _blocked + Lock      │
              └──────────────────────────────────────────┘
```

### 关键设计决策

1. **TCP 分包处理**：`parse_resp` 返回 `(parts, consumed)`，数据不完整时返回 `(None, 0)`。连接层用 `buffer` 累积字节，直到解析出完整命令。一次 `recv` 含多条命令时会循环解析、逐条响应。

2. **并发模型**：每连接一个守护线程，主线程只负责 `accept`。共享数据用一把 `threading.Lock` 保护。优先启用 `SO_REUSEPORT`，Windows/Cygwin 不支持时自动回退。

3. **BLPOP 公平性**：`blocked[key]` 是 `deque` 等待队列，`RPUSH`/`LPUSH` 在锁内 `popleft()` 唤醒最早等待者。等待线程在**锁外** `Event.wait(timeout)`，不阻塞其他客户端。超时后自我清理，避免队列泄漏。

4. **惰性过期**：`SET` 存绝对毫秒时间戳，`GET` 时比对当前时间，过期即删除。时钟通过构造函数注入，测试无需 `sleep`。

5. **类型注解**：全部公开 API 带类型标注，`mypy`/pyright 可检查。

6. **错误处理**：非法参数返回规范的 `-ERR ...\r\n`，而非静默忽略。

7. **协议错误显式处理**：`parse_resp` 区分「半包」与「语法非法」。半包继续缓冲；语法非法返回 `-ERR Protocol error` 并关闭连接，避免缓冲区无限增长。

8. **BLPOP 竞态防护**：超时清理与唤醒都在锁内核对 `Waiter.cancelled` / `value`。即使超时先返回，已为该等待者弹出的元素也会被交还，不会丢数据。

### 性能基准

在 Python 3.13 本地环境实测（`python run_benchmark.py`，单次结果，仅供参考）：

| 场景 | 吞吐 | 耗时 |
|------|------:|-----:|
| 内存 SET + GET | 1,370,715 ops/s | 0.07s |
| TCP PING 往返 | 11,516 req/s | 0.43s |

每连接一线程模型并不适合高并发压测，这部分数据主要用来观察实现开销。

---

## 项目结构

```
redis-python/
├── mini_redis/
│   ├── __init__.py      # 包入口
│   ├── __main__.py      # python -m mini_redis
│   ├── resp.py          # RESP 协议编解码
│   ├── storage.py       # Database：字符串 / 列表 / 阻塞队列
│   ├── commands.py      # 命令注册表与处理器
│   └── server.py        # TCP 服务器与连接管理
├── tests/               # 单元 + 集成测试
├── .github/workflows/ci.yml
├── pyproject.toml
├── run_server.py
├── run_tests.py
├── run_benchmark.py
├── run_redis_cli_tests.py
└── LICENSE
```

---

## 与官方 Redis 的差距

本项目是学习Build Your Own Redis实现，未包含：

- 事务（MULTI/EXEC）、发布订阅、主从复制、集群
- 持久化（RDB / AOF）
- 主动过期删除（目前仅惰性删除）
- 完整命令集（如 `DEL`、`EXISTS`、`TTL`、`INCR`、`RPOP`、`BRPOP` 等）
- 高并发场景下的事件循环模型（目前是每连接一线程）

这些是后续可扩展的方向。

---

## License

[MIT](LICENSE)
