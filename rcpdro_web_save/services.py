import os
import logging
from typing import List
from dotenv import load_dotenv
from fastapi import UploadFile
from supabase import create_client, Client
from core.encryption import encrypt_data
from rcpdro_web_save.insert_data import ReceiptDataPreparer
from core.ocr import ocr_attachment, extract_fields_from_ocr
from rcpdro_web_save.upload_files import upload_files_to_supabase


load_dotenv()

url: str = os.getenv("SUPABASE_URL") or ""
key: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
supabase: Client = create_client(url, key)

logger = logging.getLogger(__name__)

async def upload_to_supabase(user_id: str, files: List[UploadFile]):
    logger.info(f"Starting upload_to_supabase for user_id: {user_id}, Total files to process: {len(files)}")
    
    try:
        public_urls = upload_files_to_supabase(user_id, files)       
        
        # 处理每个文件的OCR和数据提取
        successes = []
        failures = []
        
        for i, (filename, public_url) in enumerate(public_urls.items(), 1):
            logger.info(f"Processing file {i}/{len(public_urls)}: {filename}")

            try:
                logger.info(f"Starting OCR for {filename}...")
                ocr = ocr_attachment(public_url)
                logger.info(f"OCR completed for {filename}, text length: {len(ocr)} characters")
                
                logger.info(f"Extracting fields from OCR for {filename}...")
                fields = extract_fields_from_ocr(ocr)
                logger.info(f"Field extraction completed for {filename}")

                logger.info(f"Preparing data for {filename}...")
                preparer = ReceiptDataPreparer(fields, user_id, public_url, ocr)
                receipt_row = preparer.build_receipt_data()
                logger.info(f"Data preparation completed for {filename}")

                encrypted_receipt_row = encrypt_data("receipt_items_en", receipt_row)
                logger.info(f"Inserting receipt_items_en for {filename}...")
                supabase.table("receipt_items_en").insert(encrypted_receipt_row).execute()
                logger.info(f"Successfully inserted data for {filename}")

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
        
        status = f"""You uploaded a total of {total_files} files: {success_count} succeeded--{successes}, {failure_count} failed--{failures}."""
        
        logger.info(f"Processing summary - Total: {total_files}, Success: {success_count}, Failed: {failure_count}")
        
        # 保存上传结果
        try:
            logger.info("Saving upload result to database...")
            supabase.table("receipt_items_upload_result").insert({"upload_result": status, "user_id": user_id}).execute()
            logger.info("Successfully saved upload result to database")
        except Exception as e:
            logger.exception(f"Failed to save upload result to database: {str(e)}")
        
        logger.info(f"upload_to_supabase completed successfully. Final status: {status}")
        return status, success_count
        
    except Exception as e:
        logger.exception(f"Critical error in upload_to_supabase for user_id {user_id}: {str(e)}")
        raise
