import logging
import asyncio
from PIL import Image
import pillow_heif
from io import BytesIO
from datetime import datetime
from typing import List, Dict
from fastapi import UploadFile
from core.utils import make_safe_storage_path
from core.supabase_storage import get_async_storage_client

logger = logging.getLogger(__name__)

# 注册 HEIF 格式支持
pillow_heif.register_heif_opener()

async def convert_heic_to_png(file_content: bytes) -> tuple[bytes, str]:
    """
    异步转换 HEIC 到 PNG
    
    Args:
        file_content: HEIC 文件内容
        
    Returns:
        (png_content, "image/png")
    """
    try:
        # 在线程池中执行转换（CPU 密集型操作）
        loop = asyncio.get_running_loop()
        png_content = await loop.run_in_executor(
            None,
            _convert_heic_sync,
            file_content
        )
        return png_content, "image/png"
    except Exception as e:
        logger.exception(f"Failed to convert HEIC to PNG: {e}")
        raise

def _convert_heic_sync(file_content: bytes) -> bytes:
    """同步转换函数"""
    heif_image = Image.open(BytesIO(file_content))
    png_buffer = BytesIO()
    heif_image.save(png_buffer, format='PNG')
    return png_buffer.getvalue()

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
        
        # 检查是否为 HEIC 格式
        original_filename = file.filename
        content_type = file.content_type or "application/octet-stream"
        
        if original_filename.lower().endswith(('.heic', '.heif')):
            logger.info(f"Converting HEIC file: {original_filename}")
            file_content, content_type = await convert_heic_to_png(file_content)
            # 修改文件名后缀
            safe_filename = make_safe_storage_path(original_filename.rsplit('.', 1)[0] + '.png')
        else:
            safe_filename = make_safe_storage_path(original_filename)
        
        # 生成安全路径
        date_url = datetime.utcnow().date().isoformat()
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        storage_path = f"{file_type}/{user_id}/{date_url}/{timestamp}_{safe_filename}"
        
        # 异步上传
        result = await storage_client.upload(
            path=storage_path,
            file_data=file_content,
            content_type=content_type
        )
        
        if result["success"]:
            logger.info(f"✅ Uploaded {original_filename} to {storage_path}")
            # 重置文件指针
            await file.seek(0)
            return (original_filename, storage_path)
        else:
            error_msg = result.get('error', 'Unknown error')
            logger.error(f"❌ Upload failed for {original_filename}: {error_msg}")
            logger.error(f"Full result: {result}")
            raise Exception(f"Upload failed for {original_filename}: {error_msg}")
            
    except Exception as e:
        logger.exception(f"Exception uploading {file.filename}: {e}")
        raise Exception(f"Upload failed for {original_filename}: {error_msg}")

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
