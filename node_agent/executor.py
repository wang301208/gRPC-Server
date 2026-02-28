from __future__ import annotations

# 兼容旧导入路径：ExecutorEngine 等价于 SubprocessExecutor。
from node_agent.executors import SubprocessExecutor


ExecutorEngine = SubprocessExecutor
