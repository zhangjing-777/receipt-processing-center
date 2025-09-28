import logging
from fastapi import APIRouter
from core.quota import QuotaManager
from ses_eml_save.models import UpdateReceiptRequest, GetReceiptRequest, DeleteReceiptRequest
from ses_eml_save.services import upload_to_supabase, update_receipt, get_receipt, delete_receipt

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


@router.post("/update-receipt-items")
async def update_receipt_items(request: UpdateReceiptRequest):
    """根据record_id和user_id更新收据信息接口"""
    return await update_receipt(request)


@router.post("/get-receipt-items")
async def get_receipt_items(request: GetReceiptRequest):
    """获取解密后的收据信息"""
    return await get_receipt(request)


@router.delete("/delete-receipt-items")
async def delete_receipt_items(request: DeleteReceiptRequest):
    """根据ind和user_id批量删除收据信息"""
    return await delete_receipt(request)