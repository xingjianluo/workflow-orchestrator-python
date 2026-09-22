"""本地可运行的异步工作流编排服务。

模块划分:
    models     状态模型 / 领域对象 / 错误码
    store      SQLite 持久化（作业、步骤、事件）
    executors  可注册的步骤执行器
    scheduler  单作业运行时：调度、并发槽、重试退避、取消传播
    service    应用服务：提交（幂等）、查询、取消、重启恢复
    api        FastAPI 路由
"""

__all__ = ["__version__"]
__version__ = "0.1.0"
