import re
import uuid
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
from io import BytesIO
from typing import Dict, Tuple, Optional
from core.http_client import AsyncHTTPClient
from core.supabase_storage import get_async_storage_client
from core.config import settings

logger = logging.getLogger(__name__)


def safe_filename(label: str, url: str) -> str:
    """生成安全的文件名，避免重复扩展名和非法字符"""
    path = url.split("?")[0]  # 去掉 query string
    ext = Path(path).suffix or ".pdf"

    # 如果 label 已经以 .pdf 结尾，就不再拼接
    if label.lower().endswith(ext.lower()):
        file_name = label
    else:
        file_name = f"{label}{ext}"

    # 清理非法字符
    file_name = re.sub(r'[\/:*?"<>| ]', "_", file_name)
    return file_name


async def fetch_file(file_url: str, arcname: str, retries: int = 3) -> Tuple[str, Optional[bytes]]:
    """
    异步下载文件，返回 (arcname, content)
    
    Args:
        file_url: 文件 URL 或存储路径
        arcname: ZIP 内的文件名
        retries: 重试次数
        
    Returns:
        (arcname, content) 或 (arcname, None)
    """
    http_client = AsyncHTTPClient.get_client()
    storage_client = get_async_storage_client()
    
    for attempt in range(1, retries + 1):
        try:
            # 判断是 URL 还是存储路径
            if file_url.startswith("http"):
                # 从 URL 下载
                response = await http_client.get(file_url, timeout=30)
                response.raise_for_status()
                content = response.content
            else:
                # 从存储下载
                content = await storage_client.download(file_url)
            
            logger.info(f"✅ Downloaded: {arcname} ({len(content)} bytes)")
            return arcname, content
            
        except Exception as e:
            logger.warning(f"Attempt {attempt}/{retries}: {arcname} - {e}")
            
            if attempt < retries:
                await asyncio.sleep(2 * attempt)  # 指数退避
    
    logger.error(f"❌ All retries failed for {arcname}")
    return arcname, None


async def generate_download_zip(user_id: str, data: Dict) -> str:
    """
    异步生成发票压缩包，上传到存储，返回存储路径
    
    Args:
        user_id: 用户 ID
        data: 文件组织结构
              {
                  buyer: {
                      date: {
                          category: {
                              file_url: label
                          }
                      }
                  }
              }
              
    Returns:
        存储路径
    """
    logger.info("Starting invoice zip generation")
    
    storage_client = get_async_storage_client()

    date_str = datetime.now().strftime("%Y%m%d-%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    zip_name = f"receipt_attachment_{date_str}_{unique_id}.zip"
    upload_path = f"summary/{user_id}/{date_str}/{zip_name}"

    # 收集所有下载任务
    tasks = []
    for buyer, date_dict in data.items():
        for invoice_date, category_dict in date_dict.items():
            for category, file_dict in category_dict.items():
                for file_url, label in file_dict.items():
                    file_name = safe_filename(label, file_url)
                    arcname = f"{buyer}/{invoice_date}/{category}/{file_name}"
                    tasks.append(fetch_file(file_url, arcname))

    logger.info(f"Downloading {len(tasks)} files concurrently...")
    
    # 并发下载所有文件 (限制并发数)
    sem = asyncio.Semaphore(10)  # 最多 10 个并发下载
    
    async def download_with_limit(task):
        async with sem:
            return await task
    
    results = await asyncio.gather(*[download_with_limit(task) for task in tasks])

    # 创建 ZIP 文件 (在内存中)
    logger.info("Creating ZIP archive...")
    zip_buffer = BytesIO()
    
    # 串行写入 ZIP (避免并发写入问题)
    await asyncio.to_thread(create_zip_in_memory, zip_buffer, results)

    # 上传到存储
    logger.info(f"Uploading ZIP to storage: {upload_path}")
    try:
        zip_buffer.seek(0)
        zip_data = zip_buffer.read()
        
        result = await storage_client.upload(
            path=upload_path,
            file_data=zip_data,
            content_type="application/zip"
        )
        
        if result["success"]:
            logger.info(f"✅ Zip created and uploaded successfully: {upload_path}")
            return upload_path
        else:
            raise Exception(f"Upload failed: {result.get('error')}")
            
    except Exception as e:
        logger.error(f"❌ Upload failed: {e}")
        raise


def create_zip_in_memory(zip_buffer: BytesIO, results: list):
    """
    在内存中创建 ZIP 文件 (同步函数，会被包装到线程)
    
    Args:
        zip_buffer: ZIP 缓冲区
        results: [(arcname, content), ...] 列表
    """
    with ZipFile(zip_buffer, "w", ZIP_DEFLATED) as zipf:
        for arcname, content in results:
            if content:
                zipf.writestr(arcname, content)
                logger.debug(f"Added to ZIP: {arcname}")
    
    logger.info(f"ZIP archive created with {len([r for r in results if r[1]])} files")
