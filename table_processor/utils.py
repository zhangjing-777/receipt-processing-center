import logging
import asyncio
import uuid
from supabase import create_client, Client
from asyncpg.pgproto.pgproto import UUID as AsyncpgUUID
from concurrent.futures import ThreadPoolExecutor
from core.config import settings
from core.redis_client import redis_client
from core.encryption import decrypt_data


logger = logging.getLogger(__name__)

sem = asyncio.Semaphore(20)
executor = ThreadPoolExecutor(max_workers=8)
supabase: Client = create_client(settings.supabase_url, settings.supabase_service_role_key)

CACHE_TTL = 82800     # 23小时
SUPABASE_TTL = 86400  # 24小时

async def get_signed_url(file_url: str):
    key = f"signed:{file_url}"
    cached_url = await redis_client.get(key)
    
    if cached_url:
        logger.info(f"Redis cache hit for {file_url}")
        return cached_url
    
    logger.info(f"Redis miss, generating new signed URL for {file_url}")
    try:
        signed = supabase.storage.from_(settings.supabase_bucket).create_signed_url(
            file_url, expires_in=SUPABASE_TTL
        )
        signed_url = signed.get("signedURL", file_url)
        await redis_client.setex(key, CACHE_TTL, signed_url)
        return signed_url
    except Exception as e:
        logger.warning(f"Failed to generate signed URL: {e}")
        return file_url

    
# 并行解密和签名
async def process_record(record_dict: dict, table_name: str = None, file_url: str = None):
    async with sem:
        logger.info(f"The record_dict is {record_dict}")
        # 如果 record_dict 里包着 ORM 实例，比如 {'SesEmlInfoEN': <model>}
        if len(record_dict) == 1 and isinstance(list(record_dict.values())[0], object):
            model_obj = list(record_dict.values())[0]
            # 展开成 {column_name: value}
            record_dict = {
                c.name: getattr(model_obj, c.name)
                for c in model_obj.__table__.columns
            }

        record_dict = {
            k: (
                str(v)
                if isinstance(v, (uuid.UUID, AsyncpgUUID))
                else v.isoformat() if hasattr(v, "isoformat")
                else v
            )
            for k, v in record_dict.items()
        }
        # 解密
        if table_name:
            loop = asyncio.get_running_loop()
            record_dict = await loop.run_in_executor(executor, decrypt_data, table_name, record_dict)

        # 生成签名URL（I/O操作）
        if file_url and record_dict.get(file_url):
            record_dict[file_url] = await get_signed_url(record_dict[file_url])
        return record_dict
