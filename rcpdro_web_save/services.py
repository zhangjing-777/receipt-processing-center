import logging
from typing import List
from fastapi import UploadFile
from sqlalchemy import insert
from core.encryption import encrypt_data
from core.ocr import ocr_attachment
from core.database import AsyncSessionLocal
from core.models import ReceiptItemsEN, SubscriptionRecords, ReceiptItemsENUploadResult
from core.generation import extract_fields_from_ocr, analyze_and_extract_subscription
from core.upload_files import smart_upload_files
from core.process_files import process_files_parallel
from rcpdro_web_save.insert_data import ReceiptDataPreparer, SubscriptDataPreparer



logger = logging.getLogger(__name__)


async def upload_to_supabase(user_id: str, files: List[UploadFile]):
    logger.info(f"Starting upload_to_supabase for user_id: {user_id}, Total files to process: {len(files)}")
    
    try:
        # 1. 上传文件到 Storage
        public_urls = await smart_upload_files(user_id, files)
        
        # 2. 并行处理 OCR 和字段提取
        successes, success_files, failures, subscription_files = await process_files_parallel(
            public_urls,
            user_id,
            ocr_attachment,
            extract_fields_from_ocr,
            analyze_and_extract_subscription,
            max_concurrent=5  # 根据 API 速率限制调整
        )
        
        # 3. 批量插入数据库
        receipt_records = []
        subscription_records = []
        
        for result in successes:
            # 构建 receipt 数据
            preparer = ReceiptDataPreparer(
                result["fields"], 
                user_id, 
                result["public_url"], 
                result["ocr"]
            )
            receipt_row = preparer.build_receipt_data()
            encrypted_receipt = encrypt_data("receipt_items_en", receipt_row)
            receipt_records.append(encrypted_receipt)
            
            # 处理订阅数据
            if result.get("is_subscription") and result.get("subscription_fields"):
                sub_preparer = SubscriptDataPreparer(
                    result["subscription_fields"],
                    user_id,
                    "web"
                )
                sub_row = sub_preparer.build_subscript_data()
                encrypted_sub = encrypt_data("subscription_records", sub_row)
                subscription_records.append(encrypted_sub)
        
        # 4. 批量插入（一次性插入所有记录）
        async with AsyncSessionLocal() as session:
            if receipt_records:
                await session.execute(
                    insert(ReceiptItemsEN).values(receipt_records)
                )
                logger.info(f"Batch inserted {len(receipt_records)} receipt records")
            
            if subscription_records:
                await session.execute(
                    insert(SubscriptionRecords).values(subscription_records)
                )
                logger.info(f"Batch inserted {len(subscription_records)} subscription records")
            
            await session.commit()
        
        # 5. 生成状态报告
        total_files = len(public_urls)
        success_count = len(success_files)
        failure_count = len(failures)
        subscription_count = len(subscription_files)
        
        status = f"""You uploaded a total of {total_files} files:\n
                     {success_count} succeeded--{success_files}, \n
                     {failure_count} failed--{failures}, \n
                     {subscription_count} subscriptions--{subscription_files}."""
        
        logger.info(f"Processing complete: {success_count} success, {failure_count} failed, {subscription_count} subscriptions")
        
        # 保存上传结果
        async with AsyncSessionLocal() as session:
            await session.execute(
                insert(ReceiptItemsENUploadResult).values({
                    "upload_result": status,
                    "user_id": user_id
                })
            )
            await session.commit()
            logger.info("Successfully saved upload result to database")

        return status, success_count
        
    except Exception as e:
        logger.exception(f"Optimized upload failed: {str(e)}")
        raise
