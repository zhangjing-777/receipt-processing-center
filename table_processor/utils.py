import logging
import asyncio
import uuid
from asyncpg.pgproto.pgproto import UUID as AsyncpgUUID
from core.config import settings
from core.redis_client import redis_client
from core.encryption import decrypt_data
from core.supabase_storage import get_async_storage_client

logger = logging.getLogger(__name__)

# 信号量控制并发数
sem = asyncio.Semaphore(20)

CACHE_TTL = 82800     # 23小时
SUPABASE_TTL = 86400  # 24小时


async def get_signed_url(file_url: str) -> str:
    """
    异步获取签名 URL (带 Redis 缓存)
    
    Args:
        file_url: 文件存储路径
        
    Returns:
        签名 URL
    """
    key = f"signed:{file_url}"
    
    # 尝试从 Redis 获取缓存
    cached_url = await redis_client.get(key)
    
    if cached_url:
        logger.info(f"Redis cache hit for {file_url}")
        return cached_url
    
    logger.info(f"Redis miss, generating new signed URL for {file_url}")
    
    storage_client = get_async_storage_client()
    
    try:
        signed_url = await storage_client.create_signed_url(
            file_url, expires_in=SUPABASE_TTL
        )
        
        if signed_url:
            # 写入 Redis 缓存
            await redis_client.setex(key, CACHE_TTL, signed_url)
            return signed_url
        else:
            logger.warning(f"Failed to generate signed URL for {file_url}")
            return file_url
            
    except Exception as e:
        logger.warning(f"Failed to generate signed URL: {e}")
        return file_url

    
async def process_record(record_dict: dict, table_name: str = None, file_url: str = None) -> dict:
    """
    并行解密和签名 URL 生成 (完全异步)
    
    Args:
        record_dict: 数据库记录字典
        table_name: 表名 (用于解密)
        file_url: 需要生成签名 URL 的字段名
        
    Returns:
        处理后的记录字典
    """
    async with sem:
        logger.debug(f"Processing record from table: {table_name}")
        
        # 如果 record_dict 里包着 ORM 实例
        if len(record_dict) == 1 and isinstance(list(record_dict.values())[0], object):
            model_obj = list(record_dict.values())[0]
            # 展开成 {column_name: value}
            record_dict = {
                c.name: getattr(model_obj, c.name)
                for c in model_obj.__table__.columns
            }

        # 转换特殊类型 (UUID, datetime)
        record_dict = {
            k: (
                str(v)
                if isinstance(v, (uuid.UUID, AsyncpgUUID))
                else v.isoformat() if hasattr(v, "isoformat")
                else v
            )
            for k, v in record_dict.items()
        }
        
        # 解密 (在线程池中执行,因为解密是 CPU 密集型)
        if table_name:
            loop = asyncio.get_running_loop()
            record_dict = await loop.run_in_executor(
                None,  # 使用默认线程池
                decrypt_data,
                table_name,
                record_dict
            )

        # 异步生成签名 URL
        if file_url and record_dict.get(file_url):
            record_dict[file_url] = await get_signed_url(record_dict[file_url])
            
        return record_dict
