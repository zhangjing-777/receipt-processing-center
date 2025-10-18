import logging
from typing import List, Dict
from fastapi import APIRouter, HTTPException
from core.quota import QuotaManager
from summary_download.services import get_summary_invoices



logger = logging.getLogger(__name__)

router = APIRouter(prefix="/summary_download", tags=["生成发票总结报告和下载包"])

def classify(n: int) -> int:
    if n < 10:
        return 1
    if n <= 30:
        return 2
    return 3

@router.post("/summary-download-ai")
async def receipt_summary_download_ai(user_id: str, title:str, invoices: List[Dict]):
    try:
        logger.info("Starting check and reset quato ...")
        quato_manager = QuotaManager(user_id, table="receipt_usage_quota_request_en")
        await quato_manager.check_and_reset()
        logger.info("Check and reset quato successfully")

        result = await get_summary_invoices(user_id, title, invoices, used_ai=True)

        logger.info("Starting update usage count ...")
        if result:
            count = classify(len(invoices))
            await quato_manager.increment_usage(count)
            logger.info("Update usage count successfully")
        
        return result
    
    except Exception as e:
        logger.info(f"Error in receipt_summary_download_ai: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/summary-download")
async def receipt_summary_download(user_id: str, title:str, invoices: List[Dict]):
    try:
        result = await get_summary_invoices(user_id, title, invoices)
        
        return result
    
    except Exception as e:
        logger.info(f"Error in receipt_summary_download: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

