import uuid
import logging
import asyncio
from typing import List, Dict
from datetime import datetime
from bs4 import BeautifulSoup
from core.config import settings
from core.http_client import AsyncHTTPClient
from core.supabase_storage import get_async_storage_client

logger = logging.getLogger(__name__)


def extract_pdf_invoice_urls(html: str) -> List[str]:
    """
    从 HTML 中提取 PDF 发票链接 (同步，因为只是解析)
    
    Args:
        html: HTML 内容
        
    Returns:
        PDF 链接列表
    """
    logger.info("Extracting PDF invoice URLs from HTML content")
    soup = BeautifulSoup(html, "html.parser")
    links = soup.find_all("a", string=lambda text: text and "Download PDF invoice" in text)
    urls = [link["href"] for link in links if link.has_attr("href")]
    logger.info(f"Found {len(urls)} PDF invoice URLs")
    return urls


async def download_and_upload_single_pdf(
    pdf_url: str,
    user_id: str,
    show: str,
    index: int
) -> tuple[str, str]:
    """
    异步下载单个 PDF 并上传到存储
    
    Args:
        pdf_url: PDF 下载链接
        user_id: 用户 ID
        show: 显示名称
        index: 索引号
        
    Returns:
        (display_name, storage_path) 或 (display_name, "")
    """
    http_client = AsyncHTTPClient.get_client()
    storage_client = get_async_storage_client()
    
    try:
        logger.info(f"Downloading PDF from: {pdf_url}")
        
        # 异步下载 PDF
        response = await http_client.get(pdf_url)
        response.raise_for_status()
        
        logger.info(f"PDF downloaded successfully, size: {len(response.content)} bytes")

        id_suffix = str(uuid.uuid4())[:8]
        filename = f"save/{user_id}/{datetime.utcnow().date().isoformat()}/eml_att_{datetime.utcnow().timestamp()}_{id_suffix}.pdf"
        logger.info(f"Generated storage filename: {filename}")

        # 异步上传到存储
        logger.info(f"Uploading PDF to storage: {filename}")
        result = await storage_client.upload(
            path=filename,
            file_data=response.content,
            content_type="application/pdf"
        )
        
        if result["success"]:
            logger.info(f"✅ PDF uploaded successfully")
            display_name = f"{show}_{id_suffix}"
            return (display_name, filename)
        else:
            logger.warning(f"❌ Upload failed: {result.get('error')}")
            return (f"{show}_{id_suffix}", "")
        
    except Exception as e:
        logger.exception(f"Failed to process PDF {index}: {pdf_url} - Error: {str(e)}")
        return (f"{show}_{index}", "")


async def upload_invoice_pdf_to_supabase(
    pdf_urls: List[str],
    user_id: str,
    show: str
) -> Dict[str, str]:
    """
    异步批量下载并上传 PDF 发票
    
    Args:
        pdf_urls: PDF 链接列表
        user_id: 用户 ID
        show: 显示名称前缀
        
    Returns:
        {display_name: storage_path} 字典
    """
    logger.info(f"Starting PDF upload process for {len(pdf_urls)} URLs with show: {show}")
    
    # 并发下载和上传所有 PDF
    tasks = [
        download_and_upload_single_pdf(url, user_id, show, i)
        for i, url in enumerate(pdf_urls, 1)
    ]
    
    results = await asyncio.gather(*tasks)
    
    # 组装结果字典
    public_urls = {name: path for name, path in results if path}
    
    logger.info(f"PDF upload process completed. Total files uploaded: {len(public_urls)}")
    return public_urls