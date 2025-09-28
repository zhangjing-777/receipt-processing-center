import os
import logging
from dotenv import load_dotenv
from typing import Dict, List
from collections import defaultdict
from supabase import create_client, Client
from core.generation import generate_summary
from core.encryption import encrypt_data
from summary_download.download_zip import generate_download_zip
from summary_download.normalizing import serialize_for_invoices, render_summary

load_dotenv()

url: str = os.getenv("SUPABASE_URL") or ""
key: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
supabase: Client = create_client(url, key)

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


async def get_summary_invoices(user_id: str, title:str, invoices: List[Dict], used_ai: bool = False) -> Dict:
    """汇总发票数据的主要方法"""

    logger.info("生成摘要")
    serialize_json = serialize_for_invoices(invoices)
    if used_ai:
        summary_content = generate_summary(serialize_json)
    else:
        summary_content = render_summary(serialize_json)

    logger.info("生成ZIP文件")
    grouped_invoices = group_invoices(invoices)
    download_url = await generate_download_zip(user_id, grouped_invoices)
    
    logger.info("加密敏感字段")
    insert_data = {"user_id": user_id,
                    "summary_content": summary_content,
                    "title": title,
                    "download_url": download_url
                    }
    encrypted_insert_data = encrypt_data("receipt_summary_zip_en", insert_data)

    logger.info(f"Inserting receipt_summary_zip_en ...")
    supabase.table("receipt_summary_zip_en").insert(encrypted_insert_data).execute()
    logger.info(f"Successfully inserted data for receipt_summary_zip_en.")
    
    return {
        'title': title,
        'summary': summary_content,
        'download_url': download_url
    }

