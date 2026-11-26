#!/usr/bin/env python3
"""本地启动脚本：python run_server.py [host] [port]"""

import sys

from mini_redis.__main__ import main

if __name__ == "__main__":
    if len(sys.argv) > 2:
        from mini_redis.server import RedisServer

        host, port = sys.argv[1], int(sys.argv[2])
        server = RedisServer(host, port)
        print(f"mini-redis listening on {server.host}:{server.port}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.stop()
    else:
        main()
