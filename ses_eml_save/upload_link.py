import uuid
import logging
import asyncio
import re
from urllib.parse import unquote
from typing import List, Dict
from datetime import datetime
from bs4 import BeautifulSoup
from core.config import settings
from core.http_client import AsyncHTTPClient
from core.supabase_storage import get_async_storage_client

logger = logging.getLogger(__name__)


def extract_pdf_invoice_urls(content: str) -> List[str]:
    """
    从邮件内容中提取 PDF 发票链接（支持 HTML 和纯文本格式）
    
    Args:
        content: 邮件内容（HTML 或纯文本）
        
    Returns:
        PDF 链接列表
    """
    logger.info("Extracting PDF invoice URLs from email content")
    urls = []
    
    # 方法1: 尝试作为 HTML 解析
    try:
        soup = BeautifulSoup(content, "html.parser")
        links = soup.find_all("a", string=lambda text: text and "Download PDF invoice" in text)
        urls.extend([link["href"] for link in links if link.has_attr("href")])
    except Exception as e:
        logger.debug(f"HTML parsing failed or no results: {e}")
    
    # 方法2: 使用正则表达式提取所有 URL（适用于纯文本）
    if not urls:
        # 匹配发票相关的 URL 模式
        # Bolt 的发票 URL 通常包含 'invoice' 关键字
        url_pattern = r'https?://[^\s<>"\'\)]+invoice[^\s<>"\'\)]*'
        found_urls = re.findall(url_pattern, content, re.IGNORECASE)
        
        # 清理 URL（移除可能的尾部标点符号）
        cleaned_urls = []
        for url in found_urls:
            # 移除常见的尾部字符
            url = url.rstrip('.,;:!?')
            # 移除 AWS tracking 包装（如果存在）
            if 'awstrack.me' in url:
                # 提取 L0/ 后面的实际 URL
                match = re.search(r'L0/(https?[^/]+.*?)(?:/\d+/|$)', url)
                if match:
                    # URL 解码
                    actual_url = match.group(1).replace('%2F', '/').replace('%3F', '?').replace('%3D', '=')
                    cleaned_urls.append(actual_url)
                else:
                    cleaned_urls.append(url)
            else:
                cleaned_urls.append(url)
        
        urls.extend(cleaned_urls)
    
    # 去重并记录
    urls = list(dict.fromkeys(urls))  # 保持顺序的去重
    logger.info(f"Found {len(urls)} PDF invoice URL(s)")
    
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