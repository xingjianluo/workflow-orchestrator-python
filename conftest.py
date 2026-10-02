"""仓库根占位配置：保证以 pytest 控制台脚本方式调用（`uv run pytest`，
sys.path 不自动包含当前目录）时也能导入应用包；同时固定 rootdir/测试发现。

直接使用 `uv run python -m pytest` 时该文件同样无害。
"""

import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
