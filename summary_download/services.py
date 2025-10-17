import logging
from typing import Dict, List
from sqlalchemy import insert
from collections import defaultdict
from core.generation import generate_summary
from core.encryption import encrypt_data
from core.models import ReceiptSummaryZipEN
from core.database import AsyncSessionLocal
from core.supabase_storage import get_async_storage_client
from core.config import settings
from summary_download.download_zip import generate_download_zip
from summary_download.normalizing import serialize_for_invoices, render_summary

logger = logging.getLogger(__name__)


def group_invoices(invoices: List[Dict]) -> Dict:
    """按买方、日期、类别分组发票"""
    result = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    
    for invoice in invoices:
        buyer = invoice.get('buyer', 'Unknown_Buyer')
        date = invoice.get('invoice_date', 'Unknown_Date')
        category = invoice.get('category', 'Uncategorized')
        file_url = invoice.get('file_url')
        seller = invoice.get('seller', 'Unknown_Seller').replace(' ', '_')
        total = invoice.get('invoice_total', '0.0')
        currency = invoice.get('currency', 'UNK')
        
        if not file_url:
            continue
            
        filename = f"{seller}_{total}_{currency}"
        result[buyer][date][category][file_url] = filename
    
    return dict(result)


async def get_summary_invoices(user_id: str, title: str, invoices: List[Dict], used_ai: bool = False) -> Dict:
    """
    完全异步的汇总发票处理
    
    Args:
        user_id: 用户 ID
        title: 报告标题
        invoices: 发票列表
        used_ai: 是否使用 AI 生成摘要
        
    Returns:
        {title, summary, download_url} 字典
    """
    logger.info(f"Starting invoice summary generation for user: {user_id}")
    
    storage_client = get_async_storage_client()

    # 1. 异步生成摘要
    logger.info("Generating summary...")
    serialize_json = serialize_for_invoices(invoices)
    
    if used_ai:
        summary_content = await generate_summary(serialize_json)
    else:
        # render_summary 是同步的，但很快，不需要异步
        summary_content = render_summary(serialize_json)
    
    logger.info("Summary generated successfully")

    # 2. 异步生成 ZIP 文件
    logger.info("Generating ZIP file...")
    grouped_invoices = group_invoices(invoices)
    download_url = await generate_download_zip(user_id, grouped_invoices)
    logger.info(f"ZIP file generated: {download_url}")
    
    # 3. 加密敏感字段
    logger.info("Encrypting sensitive fields...")
    insert_data = {
        "user_id": user_id,
        "summary_content": summary_content,
        "title": title,
        "download_url": download_url
    }
    encrypted_insert_data = encrypt_data("receipt_summary_zip_en", insert_data)

    # 4. 插入数据库
    logger.info(f"Inserting receipt_summary_zip_en...")
    async with AsyncSessionLocal() as session:
        await session.execute(
            insert(ReceiptSummaryZipEN).values(encrypted_insert_data)
        )
        await session.commit()
    logger.info(f"Successfully inserted data for receipt_summary_zip_en.")
    
    # 5. 异步生成签名下载 URL（24 小时有效）
    try:
        signed_url = await storage_client.create_signed_url(download_url, expires_in=86400)
        if signed_url:
            download_url = signed_url
            logger.info(f"Generated signed URL, valid for 24 hours.")
        else:
            logger.warning("Failed to generate signed URL, using storage path")
    except Exception as e:
        logger.warning(f"Failed to generate signed URL: {e}")

    logger.info("Invoice summary process completed successfully")
    
    return {
        'title': title,
        'summary': summary_content,
        'download_url': download_url
    }
