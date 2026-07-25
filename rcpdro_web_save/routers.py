import logging
from typing import List
from fastapi import APIRouter, UploadFile, HTTPException
from core.quota import QuotaManager
from core.send_push import send_push
from rcpdro_web_save.services import upload_to_supabase



logger = logging.getLogger(__name__)

router = APIRouter(prefix="/receiptdrop-web-save", tags=["receiptdrop网页转入"])

@router.post("/receiptdrop-transfer")
async def receiptdrop_transfer(user_id: str, files: List[UploadFile]):
    try:
        logger.info("Starting check and reset quato ...")
        quato_manager = QuotaManager(user_id, table="receipt_usage_quota_receipt_en")
        files_length = len(files)
        await quato_manager.check_and_reset(files_length)
        logger.info("Check and reset quato successfully")

        status, success_count = await upload_to_supabase(user_id, files)

        logger.info("Starting update usage count ...")
        if success_count:
            await quato_manager.increment_usage(success_count)
            logger.info("Update usage count successfully")
        
        await send_push({
            "user_id": user_id,
            "notification_type": "single_success" if files_length==1 else "batch_success"
        })
        return status
    
    except Exception as e:
        logger.info(f"Error in upload_receipts: {str(e)}")
        await send_push({
            "user_id": user_id,
            "notification_type": "failed"
        })
        raise HTTPException(status_code=500, detail=str(e))
