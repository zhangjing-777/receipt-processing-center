import logging
from fastapi import APIRouter, BackgroundTasks
from core.quota import QuotaManager
from core.send_push import send_push
from ses_eml_save.services import upload_to_supabase

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ses-eml-save", tags=["aws邮箱转入"])

@router.post("/ses-email-transfer")
async def ses_email_transfer(bucket: str, key: str, user_id: str, background_tasks: BackgroundTasks):
    """
    接收 Lambda 的 webhook，立即返回 200，
    后台异步执行上传和额度更新，避免 AWS Lambda 超时重试。
    """
    logger.info(f"✅ Received webhook: bucket={bucket}, key={key}, user_id={user_id}")

    # ✅ 立即响应 Lambda，避免超时
    background_tasks.add_task(process_email_task, bucket, key, user_id)
    return {"status": "received", "message": "Processing in background"}
    

async def process_email_task(bucket: str, key: str, user_id: str):
    """后台执行实际处理逻辑"""
    try:
        logger.info(f"🧩 Start quota check for user {user_id}")
        quato_manager = QuotaManager(user_id, table="receipt_usage_quota_receipt_en")
        await quato_manager.check_and_reset()
        logger.info("Quota check done")

        status, success_count = await upload_to_supabase(bucket, key, user_id)
        logger.info(f"Upload done, success_count={success_count}")

        if success_count:
            await quato_manager.increment_usage(success_count)
            logger.info("Usage updated successfully")

        await send_push({
            "user_id": user_id,
            "notification_type": "single_success"
        })
        logger.info(f"✅ Finished processing email for user {user_id}: {status}")

    except Exception as e:
        await send_push({
            "user_id": user_id,
            "notification_type": "failed"
        })
        logger.exception(f"❌ Background process failed for user {user_id}: {str(e)}")
        

