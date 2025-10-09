import os
import logging
from typing import List
from dotenv import load_dotenv
from fastapi import UploadFile
from supabase import create_client, Client
from core.encryption import encrypt_data
from core.ocr import ocr_attachment
from core.generation import extract_fields_from_ocr, analyze_and_extract_subscription
from core.upload_files import upload_files_to_supabase, smart_upload_files
from core.utils import clean_and_parse_json
from core.process_files import process_files_parallel
from rcpdro_web_save.insert_data import ReceiptDataPreparer, SubscriptDataPreparer

load_dotenv()

url: str = os.getenv("SUPABASE_URL") or ""
key: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
supabase: Client = create_client(url, key)

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
        if receipt_records:
            supabase.table("receipt_items_en").insert(receipt_records).execute()
            logger.info(f"Batch inserted {len(receipt_records)} receipt records")
        
        if subscription_records:
            supabase.table("subscription_records").insert(subscription_records).execute()
            logger.info(f"Batch inserted {len(subscription_records)} subscription records")
        
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
        try:
            logger.info("Saving upload result to database...")
            supabase.table("receipt_items_en_upload_result").insert({"upload_result": status, "user_id": user_id}).execute()
            logger.info("Successfully saved upload result to database")
        except Exception as e:
            logger.exception(f"Failed to save upload result to database: {str(e)}")

        return status, success_count
        
    except Exception as e:
        logger.exception(f"Optimized upload failed: {str(e)}")
        raise


async def upload_to_supabase_old(user_id: str, files: List[UploadFile]):
    logger.info(f"Starting upload_to_supabase for user_id: {user_id}, Total files to process: {len(files)}")
    
    try:
        public_urls = upload_files_to_supabase(user_id, files)       
        
        # 处理每个文件的OCR和数据提取
        successes = []
        failures = []
        subscript = []
        
        for i, (filename, public_url) in enumerate(public_urls.items(), 1):
            logger.info(f"Processing file {i}/{len(public_urls)}: {filename}")

            try:
                logger.info(f"Starting OCR for {filename}...")
                ocr = ocr_attachment(public_url)
                
                logger.info(f"Extracting fields from OCR for {filename}...")
                fields = extract_fields_from_ocr(ocr)

                logger.info(f"Preparing receipt data for {filename}...")
                preparer = ReceiptDataPreparer(fields, user_id, public_url, ocr)
                receipt_row = preparer.build_receipt_data()

                logger.info(f"Inserting receipt_items_en for {filename}...")
                encrypted_receipt_row = encrypt_data("receipt_items_en", receipt_row)               
                supabase.table("receipt_items_en").insert(encrypted_receipt_row).execute()
                logger.info(f"Successfully inserted data for {filename}")

                # 订阅检测
                try:
                    extracted = analyze_and_extract_subscription(ocr)
                    extracted = clean_and_parse_json(extracted)
                    if extracted.get("is_subscription"):
                        subscript.append(filename)
                        sub_pre = SubscriptDataPreparer(extracted.get("subscription_fields"), user_id, "web")
                        subscript_row = sub_pre.build_subscript_data()
                        encrypted_subscript_row = encrypt_data("subscription_records", subscript_row)               
                        supabase.table("subscription_records").insert(encrypted_subscript_row).execute()
                        logger.info(f"Successfully inserted subscription_records data for {filename}")
                        
                except Exception as sub_error:
                    logger.warning(f"Subscription processing failed: {sub_error}")

                successes.append(filename)
                logger.info(f"File {filename} processed successfully")
                
            except Exception as e:
                error_msg = f"{filename} - Error: {str(e)}"
                logger.exception(f"Failed to process file {i}/{len(public_urls)}: {error_msg}")
                failures.append(error_msg)
        
        # 生成状态报告
        total_files = len(successes) + len(failures)
        success_count = len(successes)
        failure_count = len(failures)
        
        status = f"""You uploaded a total of {total_files} files: {success_count} succeeded--{successes}, {failure_count} failed--{failures}.
        Subscript files: {subscript}.
        """
        
        logger.info(f"Processing summary - Total: {total_files}, Success: {success_count}, Failed: {failure_count}")
        
        # 保存上传结果
        try:
            logger.info("Saving upload result to database...")
            supabase.table("receipt_items_en_upload_result").insert({"upload_result": status, "user_id": user_id}).execute()
            logger.info("Successfully saved upload result to database")
        except Exception as e:
            logger.exception(f"Failed to save upload result to database: {str(e)}")
        
        logger.info(f"upload_to_supabase completed successfully. Final status: {status}")
        return status, success_count
        
    except Exception as e:
        logger.exception(f"Critical error in upload_to_supabase for user_id {user_id}: {str(e)}")
        raise

