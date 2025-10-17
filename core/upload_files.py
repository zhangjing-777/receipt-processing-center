import logging
import asyncio
from datetime import datetime
from typing import List, Dict
from fastapi import UploadFile
from core.utils import make_safe_storage_path
from core.supabase_storage import get_async_storage_client

logger = logging.getLogger(__name__)


async def upload_single_file(
    user_id: str,
    file: UploadFile,
    file_type: str = "save"
) -> tuple[str, str]:
    """
    异步上传单个文件
    
    Args:
        user_id: 用户 ID
        file: 上传的文件对象
        file_type: 文件类型分类 (save/summary 等)
        
    Returns:
        (filename, storage_path) 或 (filename, "")
    """
    storage_client = get_async_storage_client()
    
    try:
        # 读取文件内容
        file_content = await file.read()
        
        # 生成安全路径
        safe_filename = make_safe_storage_path(file.filename)
        date_url = datetime.utcnow().date().isoformat()
        timestamp = datetime.utcnow().isoformat()
        storage_path = f"{file_type}/{user_id}/{date_url}/{timestamp}_{safe_filename}"
        
        # 异步上传
        result = await storage_client.upload(
            path=storage_path,
            file_data=file_content,
            content_type=file.content_type or "application/octet-stream"
        )
        
        if result["success"]:
            logger.info(f"✅ Uploaded {file.filename} to {storage_path}")
            # 重置文件指针
            await file.seek(0)
            return (file.filename, storage_path)
        else:
            logger.warning(f"❌ Upload failed for {file.filename}: {result.get('error')}")
            return (file.filename, "")
            
    except Exception as e:
        logger.exception(f"Exception uploading {file.filename}: {e}")
        return (file.filename, "")


async def upload_files_to_supabase_async(
    user_id: str,
    files: List[UploadFile],
    file_type: str = "save"
) -> Dict[str, str]:
    """
    异步批量上传文件到 Supabase Storage
    
    Args:
        user_id: 用户 ID
        files: 文件列表
        file_type: 文件类型分类
        
    Returns:
        {filename: storage_path} 字典
    """
    logger.info(f"Starting async upload for {len(files)} file(s)")
    
    # 并发上传所有文件
    tasks = [upload_single_file(user_id, file, file_type) for file in files]
    results = await asyncio.gather(*tasks)
    
    # 组装结果字典
    result_dict = {filename: path for filename, path in results}
    
    success_count = sum(1 for path in result_dict.values() if path)
    logger.info(f"Upload completed: {success_count}/{len(files)} successful")
    
    return result_dict


async def smart_upload_files(
    user_id: str,
    files: List[UploadFile],
    file_type: str = "save"
) -> Dict[str, str]:
    """
    智能上传 (已全部异步化,无需区分)
    
    Args:
        user_id: 用户 ID
        files: 文件列表
        file_type: 文件类型分类
        
    Returns:
        {filename: storage_path} 字典
    """
    return await upload_files_to_supabase_async(user_id, files, file_type)
