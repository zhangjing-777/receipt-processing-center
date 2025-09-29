import logging
from fastapi import APIRouter
from core.quota import QuotaManager
from ses_eml_save.services import upload_to_supabase

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ses-eml-save", tags=["aws邮箱转入"])


@router.post("/ses-email-transfer")
async def ses_email_transfer(bucket, key, user_id):
    """拉取 S3 并转发给supabase"""
    logger.info("Received webhook request")
    bucket = str(bucket)
    key = str(key)
    user_id = str(user_id)
    try:
        logger.info("Starting check and reset quato ...")
        quato_manager = QuotaManager(user_id, table="receipt_usage_quota_receipt_en")
        quato_manager.check_and_reset()
        logger.info("Check and reset quato successfully")

        status, success_count = await upload_to_supabase(bucket, key, user_id)

        logger.info("Starting update usage count ...")
        if success_count:
            quato_manager.increment_usage(success_count)
            logger.info("Update usage count successfully")
        
        return status

    except Exception as e:
        logger.exception(f"Upload process failed: {str(e)}")
        return {"error": f"Upload process failed: {str(e)}", "status": "error"}

