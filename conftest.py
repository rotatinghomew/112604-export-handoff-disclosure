"""确保 pytest 从仓库根目录收集时也能导入根目录的标准库服务模块。"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
