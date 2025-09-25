import logging
from fastapi import APIRouter
from ses_eml_save.models import UpdateReceiptRequest, GetReceiptRequest, DeleteReceiptRequest
from ses_eml_save.services import upload_to_supabase, update_receipt, get_receipt, delete_receipt

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ses-eml", tags=["aws邮箱转入"])


@router.post("/webhook/ses-email-transfer")
async def ses_email_transfer(bucket, key, user_id):
    """拉取 S3 并转发给supabase"""
    logger.info("Received webhook request")
    bucket = str(bucket)
    key = str(key)
    user_id = str(user_id)
    try:
        logger.info(f"Starting upload process for bucket: {bucket}, key: {key}, user_id: {user_id}")
        result = await upload_to_supabase(bucket, key, user_id)
        logger.info(f"Upload process completed: {result}")
        return {"message": "Email processed successfully", "result": result, "status": "success"}
    except Exception as e:
        logger.exception(f"Upload process failed: {str(e)}")
        return {"error": f"Upload process failed: {str(e)}", "status": "error"}


@router.post("/webhook/update_receipt")
async def update_receipt_items(request: UpdateReceiptRequest):
    """根据record_id和user_id更新收据信息接口"""
    return await update_receipt(request)


@router.post("/webhook/get_receipt")
async def get_receipt_items(request: GetReceiptRequest):
    """获取解密后的收据信息"""
    return await get_receipt(request)


@router.delete("/webhook/delete_receipt")
async def delete_receipt_items(request: DeleteReceiptRequest):
    """根据ind和user_id批量删除收据信息"""
    return await delete_receipt(request)