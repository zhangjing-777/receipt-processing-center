import redis.asyncio as redis
from core.config import settings


# ✅ 创建全局异步 Redis 连接池
redis_client = redis.from_url(
    f"redis://{settings.redis_host}:{settings.redis_port}/{settings.redis_db}",
    password=settings.redis_password or None,
    encoding="utf-8",
    decode_responses=True,  # 自动把字节 -> 字符串
    socket_connect_timeout=3,  # 连接超时 3 秒
    socket_timeout=5           # 请求超时 5 秒
)
