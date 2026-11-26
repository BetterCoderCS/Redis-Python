"""网络层:TCP 服务器 + 每连接一线程的并发模型。

职责划分：
    - RedisServer 负责监听端口、accept 连接、管理生命周期；
    - ClientConnection 负责单个连接的缓冲、解析与响应循环。
协议解析交给 resp.py,命令执行交给 commands.py,本模块不做业务判断。
"""

from __future__ import annotations

import socket
import threading

from .commands import execute
from .resp import RespProtocolError, encode_error, parse_resp
from .storage import Database

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 6379
RECV_SIZE = 4096
ACCEPT_TIMEOUT = 0.2  # 让 stop() 能及时退出 accept 循环


def create_server_socket(host: str, port: int) -> socket.socket:
    """创建监听 socket；Windows/Cygwin 不支持 SO_REUSEPORT 时自动回退。"""
    try:
        return socket.create_server((host, port), reuse_port=True)
    except ValueError:
        # 平台不支持 SO_REUSEPORT，退化为普通创建方式
        return socket.create_server((host, port))


class ClientConnection:
    """一个客户端连接的完整生命周期。"""

    def __init__(self, sock: socket.socket, db: Database) -> None:
        self._sock = sock
        self._db = db

    def run(self) -> None:
        """持续接收数据并逐条响应，直到客户端断开。"""
        with self._sock:
            buffer = b""
            while True:
                try:
                    data = self._sock.recv(RECV_SIZE)
                except OSError:
                    return
                if not data:
                    return
                buffer += data

                # 一个 recv 可能包含多条命令，也可能只有半条命令
                while buffer:
                    try:
                        parts, consumed = parse_resp(buffer)
                    except RespProtocolError:
                        # 语法非法：回复错误后关闭连接，避免缓冲区无限增长
                        try:
                            self._sock.sendall(encode_error("ERR Protocol error"))
                        except OSError:
                            pass
                        return
                    if consumed == 0:
                        break  # 数据不完整，等下一次 recv
                    buffer = buffer[consumed:]
                    response = execute(self._db, parts or [])
                    try:
                        self._sock.sendall(response)
                    except OSError:
                        return


class RedisServer:
    """可从任意线程启动/停止的 Redis 兼容服务器。

    port=0 时由系统分配空闲端口，可通过 .port 读取实际端口，便于集成测试。
    """

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        db: Database | None = None,
    ) -> None:
        self._sock = create_server_socket(host, port)
        self._sock.settimeout(ACCEPT_TIMEOUT)
        self.host = host
        self.port = self._sock.getsockname()[1]
        self._db = db if db is not None else Database()
        self._running = False
        self._accept_thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._clients: set[socket.socket] = set()
        self._clients_lock = threading.Lock()

    def start(self) -> None:
        """非阻塞启动：在后台线程运行 accept 循环，返回时服务器已就绪。"""
        if self._running:
            self._ready.wait()
            return
        self._running = True
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()
        self._ready.wait()  # 等待 accept 循环真正开始

    def serve_forever(self) -> None:
        """阻塞运行，供命令行入口使用。"""
        self._running = True
        self._ready.set()
        self._accept_loop()

    def stop(self) -> None:
        """停止接受新连接，并关闭所有活跃客户端连接。"""
        self._running = False
        try:
            self._sock.close()
        except OSError:
            pass
        with self._clients_lock:
            clients = list(self._clients)
            self._clients.clear()
        for sock in clients:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass

    def _accept_loop(self) -> None:
        self._ready.set()  # 通知 start()：循环已开始，可以接受连接
        while self._running:
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return  # socket 已关闭
            with self._clients_lock:
                self._clients.add(conn)
            thread = threading.Thread(
                target=self._handle, args=(conn,), daemon=True
            )
            thread.start()

    def _handle(self, conn: socket.socket) -> None:
        try:
            ClientConnection(conn, self._db).run()
        finally:
            with self._clients_lock:
                self._clients.discard(conn)
