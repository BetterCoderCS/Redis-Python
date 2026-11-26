#!/usr/bin/env python3
"""一键运行全部测试：python run_tests.py

使用标准库 unittest，零第三方依赖；CI 中同样可用 pytest 运行同一套测试。
"""

import sys
import unittest


def main() -> int:
    suite = unittest.defaultTestLoader.discover("tests", pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    sys.exit(main())
