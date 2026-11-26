"""命令行入口:python -m mini_redis"""

from __future__ import annotations

from .server import DEFAULT_HOST, DEFAULT_PORT, RedisServer


def main() -> None:
    server = RedisServer(DEFAULT_HOST, DEFAULT_PORT)
    print(f"mini-redis listening on {server.host}:{server.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down...")
    finally:
        server.stop()


if __name__ == "__main__":
    main()
