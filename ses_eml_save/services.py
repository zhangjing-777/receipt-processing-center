import logging
from sqlalchemy import insert
from core.database import AsyncSessionLocal
from core.models import ReceiptItemsEN, SesEmlInfoEN, SubscriptionRecords, ReceiptItemsENUploadResult
from core.encryption import encrypt_data
from core.ocr import ocr_attachment
from core.utils import clean_and_parse_json
from core.generation import extract_fields_from_ocr, analyze_and_extract_subscription
from ses_eml_save.insert_data import ReceiptDataPreparer, SubscriptDataPreparer
from ses_eml_save.eml_parser import load_s3, mail_parser
from ses_eml_save.upload_attachment import upload_attachments_to_storage
from ses_eml_save.upload_string_to_image import render_html_string_to_image_and_upload
from ses_eml_save.upload_link import extract_pdf_invoice_urls, upload_invoice_pdf_to_supabase

logger = logging.getLogger(__name__)


async def upload_to_supabase(bucket, key, user_id):
    """
    完全异步的邮件处理流程
    
    Args:
        bucket: S3 桶名
        key: S3 对象键
        user_id: 用户 ID
        
    Returns:
        (status_message, success_count)
    """
    logger.info(f"Starting upload_to_supabase for user_id: {user_id}, bucket: {bucket}, key: {key}")
    
    try:
        # 1. 异步加载邮件
        logger.info("Loading email from S3...")
        eml_bytes = await load_s3(bucket, key)
        logger.info(f"Successfully loaded email from S3, size: {len(eml_bytes)} bytes")
        
        # 2. 异步解析邮件
        logger.info("Parsing email content...")
        raw_attachments = await mail_parser(eml_bytes)
        logger.info("Email parsing completed")
        
        html_str = raw_attachments['body']
        subject = raw_attachments['subject']
        attachments = raw_attachments['attachments']
        
        logger.info(f"Email subject: {subject}")
        logger.info(f"Found {len(attachments)} attachments")
        logger.info(f"HTML body length: {len(html_str)} characters")
        
        # 3. 处理附件或链接 (异步)
        if len(attachments) > 0:
            logger.info("Processing email attachments...")
            public_urls = await upload_attachments_to_storage(attachments, user_id)
            logger.info(f"Successfully uploaded {len(public_urls)} attachments to storage")
        else:
            logger.info("No attachments found, checking for PDF invoice links...")
            urls = extract_pdf_invoice_urls(html_str)
            if len(urls) > 0:
                logger.info(f"Found {len(urls)} PDF invoice links, downloading and uploading...")
                public_urls = await upload_invoice_pdf_to_supabase(urls, user_id, subject)
                logger.info(f"Successfully processed {len(public_urls)} PDF invoice links")
            else:
                logger.info("No PDF links found, converting HTML body to image...")
                public_urls = await render_html_string_to_image_and_upload(html_str, user_id, subject)
                logger.info("Successfully converted HTML body to image and uploaded")
        
        logger.info(f"Total files to process: {len(public_urls)}")
        
        # 4. 并发处理所有文件的 OCR 和数据提取
        import asyncio
        successes = []
        failures = []
        subscript = []
        
        # 创建所有处理任务
        tasks = [
            process_single_file(filename, public_url, user_id, raw_attachments, bucket, key)
            for filename, public_url in public_urls.items()
        ]
        
        # 并发执行
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 分类结果
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                filename = list(public_urls.keys())[i]
                logger.error(f"Failed to process {filename}: {result}")
                failures.append(f"{filename} - {str(result)}")
            elif result["status"] == "success":
                successes.append(result["filename"])
                if result.get("is_subscription"):
                    subscript.append(result["filename"])
            else:
                failures.append(f"{result['filename']} - {result.get('error', 'Unknown error')}")
        
        # 5. 生成状态报告
        total_files = len(successes) + len(failures)
        success_count = len(successes)
        failure_count = len(failures)
        subscription_count = len(subscript)
        
        status = f"""You uploaded a total of {total_files} files: \n
                        {success_count} succeeded--{successes}, \n
                        {failure_count} failed--{failures}, \n
                        {subscription_count} subscriptions--{subscript}.
                        """
        
        logger.info(f"Processing summary - Total: {total_files}, Success: {success_count}, Failed: {failure_count}")
        
        # 6. 保存上传结果
        async with AsyncSessionLocal() as session:
            await session.execute(
                insert(ReceiptItemsENUploadResult).values({
                    "upload_result": status,
                    "user_id": user_id
                })
            )
            await session.commit()
            logger.info("Successfully saved upload result to database")
        
        logger.info(f"upload_to_supabase completed successfully. Final status: {status}")
        return status, success_count
        
    except Exception as e:
        logger.exception(f"Critical error in upload_to_supabase for user_id {user_id}: {str(e)}")
        raise


async def process_single_file(filename: str, public_url: str, user_id: str, raw_attachments: dict, bucket: str, key: str) -> dict:
    """
    异步处理单个文件
    
    Args:
        filename: 文件名
        public_url: 文件存储路径
        user_id: 用户 ID
        raw_attachments: 原始邮件附件信息
        bucket: S3 桶名
        key: S3 键
        
    Returns:
        处理结果字典
    """
    try:
        logger.info(f"Processing file: {filename}")
        
        # 异步 OCR
        logger.info(f"Starting OCR for {filename}...")
        ocr = await ocr_attachment(public_url)
        logger.info(f"OCR completed for {filename}, text length: {len(ocr)} characters")
        
        # 异步字段提取
        logger.info(f"Extracting fields from OCR for {filename}...")
        fields = await extract_fields_from_ocr(ocr)
        logger.info(f"Field extraction completed for {filename}")

        # 准备数据
        logger.info(f"Preparing data for {filename}...")
        preparer = ReceiptDataPreparer(user_id, fields, raw_attachments, public_url, ocr)
        receipt_row = preparer.build_receipt_data()
        eml_row = preparer.build_eml_data(bucket+'/'+key)
        logger.info(f"Data preparation completed for {filename}")

        # 加密
        encrypted_receipt_row = encrypt_data("receipt_items_en", receipt_row)
        encrypted_eml_row = encrypt_data("ses_eml_info_en", eml_row)
        
        # 插入数据库
        async with AsyncSessionLocal() as session:
            await session.execute(
                insert(ReceiptItemsEN).values(encrypted_receipt_row)
            ) 
            await session.execute(
                insert(SesEmlInfoEN).values(encrypted_eml_row)
            )                           
            await session.commit()
            logger.info(f"Inserted receipt_items_en/ses_eml_info_en for {filename}")

        # 异步订阅检测
        is_subscription = False
        try:
            extracted = await analyze_and_extract_subscription(ocr)
            extracted = clean_and_parse_json(extracted)
            if extracted.get("is_subscription"):
                is_subscription = True
                sub_pre = SubscriptDataPreparer(extracted.get("subscription_fields"), user_id, "email")
                subscript_row = sub_pre.build_subscript_data()
                encrypted_subscript_row = encrypt_data("subscription_records", subscript_row) 
                async with AsyncSessionLocal() as session:
                    await session.execute(
                        insert(SubscriptionRecords).values(encrypted_subscript_row)
                    )                           
                    await session.commit()
                logger.info(f"Successfully inserted subscription_records data for {filename}")
                
        except Exception as sub_error:
            logger.warning(f"Subscription processing failed for {filename}: {sub_error}")

        logger.info(f"File {filename} processed successfully")
        
        return {
            "status": "success",
            "filename": filename,
            "is_subscription": is_subscription
        }
        
    except Exception as e:
        logger.exception(f"Failed to process {filename}: {str(e)}")
        return {
            "status": "error",
            "filename": filename,
            "error": str(e)
        }
