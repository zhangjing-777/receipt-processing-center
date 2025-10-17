import logging
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from core.config import settings

logger = logging.getLogger(__name__)

# 创建异步引擎 (优化参数)
engine = create_async_engine(
    settings.database_url,
    pool_size=100,              # 增加连接池大小以支持更高并发
    max_overflow=50,            # 超出后最多再创建
    pool_pre_ping=True,         # 自动检测失效连接
    pool_recycle=3600,          # 1小时回收连接
    pool_timeout=30,            # 获取连接超时时间
    echo=False,
    connect_args={
        "server_settings": {
            "application_name": "receipt_processing_center",
            "jit": "off"        # 关闭 JIT 以避免某些性能问题
        },
        "command_timeout": 60,  # 命令超时 60 秒
        "timeout": 30           # 连接超时 30 秒
    }
)

# 创建异步 Session 工厂
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False
)

Base = declarative_base()


async def get_db():
    """
    获取数据库会话（依赖注入）
    
    使用示例:
    ```python
    async with AsyncSessionLocal() as session:
        result = await session.execute(query)
        await session.commit()
    ```
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db():
    """初始化数据库（可选）"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def close_db():
    """关闭数据库连接池"""
    await engine.dispose()
    logger.info("✅ Database connection pool closed")
