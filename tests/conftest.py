import sys
from pathlib import Path

# 将仓库根目录加入导入路径，便于直接运行 pytest。
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
