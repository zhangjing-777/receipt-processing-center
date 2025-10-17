import base64
import logging
import asyncio
from datetime import datetime
from typing import List, Dict
from core.config import settings
from core.utils import make_safe_storage_path
from core.supabase_storage import get_async_storage_client

logger = logging.getLogger(__name__)


async def upload_single_attachment(
    attachment: Dict,
    user_id: str,
    bucket: str = settings.supabase_bucket
) -> tuple[str, str]:
    """
    异步上传单个附件
    
    Args:
        attachment: 附件字典 {filename, content_type, binary}
        user_id: 用户 ID
        bucket: 存储桶名称
        
    Returns:
        (filename, storage_path) 或 (filename, "")
    """
    storage_client = get_async_storage_client()
    
    try:
        filename = attachment["filename"]
        logger.info(f"Processing attachment: {filename}")
        
        safe_filename = make_safe_storage_path(filename)
        logger.info(f"Safe filename generated: {safe_filename}")
        
        binary = attachment["binary"]
        if isinstance(binary, bytes):
            binary_data = base64.b64decode(binary)
            logger.info(f"Decoded base64 binary data, size: {len(binary_data)} bytes")
        else:
            binary_data = binary  # 已经是 bytes
            logger.info(f"Binary data already in bytes format, size: {len(binary_data)} bytes")

        date_url = datetime.utcnow().date().isoformat()
        timestamp = datetime.utcnow().isoformat()
        storage_path = f"save/{user_id}/{date_url}/{timestamp}_{safe_filename}"
        logger.info(f"Generated storage path: {storage_path}")

        logger.info(f"Uploading {filename} to storage at {storage_path}")
        result = await storage_client.upload(
            path=storage_path,
            file_data=binary_data,
            content_type=attachment.get("content_type", "application/octet-stream")
        )

        if result["success"]:
            logger.info(f"✅ Upload successful: {storage_path}")
            return (filename, storage_path)
        else:
            logger.warning(f"❌ Upload failed for {filename}: {result.get('error')}")
            return (filename, "")
    
    except Exception as e:
        logger.exception(f"Failed to upload attachment {attachment.get('filename', 'unknown')}: {str(e)}")
        return (attachment.get('filename', 'unknown'), "")


async def upload_attachments_to_storage(
    attachments: List[Dict],
    user_id: str,
    bucket: str = settings.supabase_bucket
) -> Dict[str, str]:
    """
    异步批量上传附件到存储
    
    Args:
        attachments: 附件列表
        user_id: 用户 ID
        bucket: 存储桶名称
        
    Returns:
        {filename: storage_path} 字典
    """
    logger.info(f"Starting attachment upload process for {len(attachments)} attachments to bucket: {bucket}")
    
    # 并发上传所有附件
    tasks = [
        upload_single_attachment(att, user_id, bucket)
        for att in attachments
    ]
    
    results = await asyncio.gather(*tasks)
    
    # 组装结果字典
    records = {filename: path for filename, path in results}
    
    success_count = sum(1 for path in records.values() if path)
    logger.info(f"Attachment upload process completed. Successfully uploaded {success_count}/{len(attachments)} attachments")
    
    return records