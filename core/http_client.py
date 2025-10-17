import httpx
import logging
from typing import Optional
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class AsyncHTTPClient:
    """全局异步 HTTP 客户端单例"""
    
    _instance: Optional[httpx.AsyncClient] = None
    
    @classmethod
    def get_client(cls) -> httpx.AsyncClient:
        """获取全局客户端实例"""
        if cls._instance is None:
            cls._instance = httpx.AsyncClient(
                timeout=httpx.Timeout(60.0, connect=10.0),
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
                follow_redirects=True
            )
            logger.info("✅ Global async HTTP client initialized")
        return cls._instance
    
    @classmethod
    async def close(cls):
        """关闭全局客户端"""
        if cls._instance is not None:
            await cls._instance.aclose()
            cls._instance = None
            logger.info("🧹 Global async HTTP client closed")


@asynccontextmanager
async def get_http_client():
    """上下文管理器,用于需要独立客户端的场景"""
    client = httpx.AsyncClient(
        timeout=httpx.Timeout(60.0, connect=10.0),
        limits=httpx.Limits(max_keepalive_connections=10, max_connections=50)
    )
    try:
        yield client
    finally:
        await client.aclose()