import os
import logging
import asyncio
import aiohttp
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict
from fastapi import UploadFile
from supabase import create_client, Client
from core.utils import make_safe_storage_path


load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL") or ""
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")

logger = logging.getLogger(__name__)

def upload_files_to_supabase(user_id: str, files: List[UploadFile], type:str = "save") -> Dict[str, str]:
    """ 批量上传文件到Supabase私有存储 """
    # 创建Supabase客户端
    
    result = {}
    
    for file in files:
        try:
            # 读取文件内容
            file_content = file.file.read()

            # 生成安全路径
            safe_filename = make_safe_storage_path(file.filename)
            date_url = datetime.utcnow().date().isoformat()
            timestamp = datetime.utcnow().isoformat()
            storage_path = f"{type}/{user_id}/{date_url}/{timestamp}_{safe_filename}"
            
            # 上传文件到Supabase存储
            supabase.storage.from_(SUPABASE_BUCKET).upload(
                path=storage_path,
                file=file_content,
                file_options={"content-type": file.content_type}
            )
      
            result[file.filename] = storage_path
            
            # 重置文件指针
            file.file.seek(0)
            
        except Exception as e:
            logger.info(f"上传文件 {file.filename} 失败: {str(e)}")
            result[file.filename] = ""
    logger.info(f"Files upload process completed. Successfully uploaded {len(result)}/{len(files)} files")
    return result


async def upload_file_async(session, user_id: str, file: UploadFile, type: str) -> Dict[str, str]:
    try:
        file_content = await file.read()
        safe_filename = make_safe_storage_path(file.filename)
        date_url = datetime.utcnow().date().isoformat()
        timestamp = datetime.utcnow().isoformat()
        storage_path = f"{type}/{user_id}/{date_url}/{timestamp}_{safe_filename}"
        upload_url = f"{SUPABASE_URL}/storage/v1/object/{SUPABASE_BUCKET}/{storage_path}"

        headers = {
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": file.content_type or "application/octet-stream"
        }

        async with session.post(upload_url, data=file_content, headers=headers) as resp:
            if resp.status in (200, 201):
                logger.info(f"✅ Uploaded {file.filename} to {storage_path}")
                return {file.filename: storage_path}
            else:
                text = await resp.text()
                logger.warning(f"❌ Upload failed for {file.filename}: {resp.status} - {text}")
                return {file.filename: ""}
    except Exception as e:
        logger.exception(f"Exception uploading {file.filename}: {e}")
        return {file.filename: ""}


async def upload_files_to_supabase_async(user_id: str, files: List[UploadFile], type: str = "save") -> Dict[str, str]:
    """并发上传文件到 Supabase"""
    async with aiohttp.ClientSession() as session:
        tasks = [upload_file_async(session, user_id, f, type) for f in files]
        results = await asyncio.gather(*tasks)
        merged = {k: v for d in results for k, v in d.items()}
        logger.info(f"异步上传完成，共 {len(merged)} 个文件")
        return merged
    

async def smart_upload_files(user_id: str, files: List[UploadFile], type: str = "save") -> Dict[str, str]:
    """
    智能选择上传方式：
    - 文件数 ≤ 2：使用同步上传（低开销）
    - 文件数 > 2：使用异步并发上传
    """
    if len(files) <= 2:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, upload_files_to_supabase, user_id, files, type)
    else:
        result = await upload_files_to_supabase_async(user_id, files, type)
    return result