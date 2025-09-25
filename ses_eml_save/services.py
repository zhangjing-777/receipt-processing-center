import os
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
from core.encryption import encrypt_data, decrypt_data
from ses_eml_save.insert_data import ReceiptDataPreparer
from ses_eml_save.eml_parser import load_s3, mail_parser
from core.ocr import ocr_attachment, extract_fields_from_ocr
from ses_eml_save.upload_attachment import upload_attachments_to_storage
from ses_eml_save.upload_string_to_image import render_html_string_to_image_and_upload
from ses_eml_save.upload_link import extract_pdf_invoice_urls, upload_invoice_pdf_to_supabase
from ses_eml_save.models import UpdateReceiptRequest, GetReceiptRequest, DeleteReceiptRequest

load_dotenv()

url: str = os.getenv("SUPABASE_URL") or ""
key: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
supabase: Client = create_client(url, key)
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")

logger = logging.getLogger(__name__)


async def upload_to_supabase(bucket, key, user_id):
    logger.info(f"Starting upload_to_supabase for user_id: {user_id}, bucket: {bucket}, key: {key}")
    
    try:
        logger.info("Loading email from S3...")
        eml_bytes = load_s3(bucket, key)
        logger.info(f"Successfully loaded email from S3, size: {len(eml_bytes)} bytes")
        
        logger.info("Parsing email content...")
        raw_attachments = mail_parser(eml_bytes)
        logger.info("Email parsing completed")
        
        html_str = raw_attachments['body']
        subject = raw_attachments['subject']
        attachments = raw_attachments['attachments']
        
        logger.info(f"Email subject: {subject}")
        logger.info(f"Found {len(attachments)} attachments")
        logger.info(f"HTML body length: {len(html_str)} characters")
        
        # 处理附件或链接
        if len(attachments) > 0:
            logger.info("Processing email attachments...")
            public_urls = upload_attachments_to_storage(attachments, user_id)
            logger.info(f"Successfully uploaded {len(public_urls)} attachments to storage")
        else:
            logger.info("No attachments found, checking for PDF invoice links...")
            urls = extract_pdf_invoice_urls(html_str)
            if len(urls) > 0:
                logger.info(f"Found {len(urls)} PDF invoice links, downloading and uploading...")
                public_urls = upload_invoice_pdf_to_supabase(urls, user_id, subject)
                logger.info(f"Successfully processed {len(public_urls)} PDF invoice links")
            else:
                logger.info("No PDF links found, converting HTML body to image...")
                public_urls = await render_html_string_to_image_and_upload(html_str, user_id, subject)
                logger.info("Successfully converted HTML body to image and uploaded")
        
        logger.info(f"Total files to process: {len(public_urls)}")
        
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
                preparer = ReceiptDataPreparer(user_id, fields, raw_attachments, public_url, ocr)
                receipt_row = preparer.build_receipt_data()
                eml_row = preparer.build_eml_data(bucket+'/'+key)
                logger.info(f"Data preparation completed for {filename}")

                encrypted_receipt_row = encrypt_data("receipt_items_en", receipt_row)
                encrypted_eml_row = encrypt_data("ses_eml_info_en", eml_row)
                logger.info(f"Inserting receipt_items_en for {filename}...")
                supabase.table("receipt_items_en").insert(encrypted_receipt_row).execute()
                logger.info(f"Inserting ses_eml_info_en for {filename}...")
                supabase.table("ses_eml_info_en").insert(encrypted_eml_row).execute()
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
        return status
        
    except Exception as e:
        logger.exception(f"Critical error in upload_to_supabase for user_id {user_id}: {str(e)}")
        raise


async def update_receipt(request: UpdateReceiptRequest):
    """根据record_id和user_id更新收据信息接口"""
    logger.info("Received update_receipt webhook request")
    
    try:
        logger.info(f"Updating receipt for record_id: {request.ind}, user_id: {request.user_id}")
        
        # 构建更新数据，只包含非None的字段
        update_data = {}
        for field, value in request.dict(exclude={'ind', 'user_id'}).items():
            if value != "string" and value:
                update_data[field] = value
        
        if not update_data:
            return {"message": "No data to update", "status": "success"}
        
        logger.info(f"Fields to update: {list(update_data.keys())}")
        
        # 加密敏感字段
        encrypted_update_data = encrypt_data("receipt_items_en", update_data)
        
        # 执行数据库更新
        result = supabase.table("receipt_items_en").update(encrypted_update_data).eq("ind", request.ind).eq("user_id", request.user_id).execute()
        
        if not result.data:
            return {"error": "No matching record found or no permission to update", "status": "error"}
        
        # 解密返回数据中的敏感字段
        decrypted_result = []
        for record in result.data:
            decrypted_record = decrypt_data("receipt_items_en", record)
            decrypted_result.append(decrypted_record)
        
        logger.info(f"Successfully updated {len(result.data)} record(s)")
        return {
            "message": "Receipt information updated successfully", 
            "updated_records": len(result.data),
            "data": decrypted_result,
            "status": "success"
        }
        
    except Exception as e:
        logger.exception(f"Failed to update receipt: {str(e)}")
        return {"error": f"Failed to update receipt information: {str(e)}", "status": "error"}
  

async def get_receipt(request: GetReceiptRequest):
    """根据user_id和可选的id查询收据信息"""
    logger.info(f"Querying receipts for user_id: {request.user_id}, ind: {request.ind}, limit: {request.limit}, offset: {request.offset}")
    
    try:
        # 构建查询
        query = supabase.table("receipt_items_en").select("*").eq("user_id", request.user_id)
        
        # 如果提供了id，则精确查询
        if request.ind:
            query = query.eq("ind", request.ind)
            logger.info(f"Exact query for record id: {request.ind}")
        else:
            # 分页查询，按create_time倒序排列
            query = query.order("create_time", desc=True).range(request.offset, request.offset + request.limit - 1)
            logger.info(f"Paginated query with limit: {request.limit}, offset: {request.offset}, ordered by create_time desc")
        
        # 执行查询
        result = query.execute()
        
        if not result.data:
            return {"message": "No records found", "data": [], "total": 0, "status": "success"}
        
        # 解密返回数据中的敏感字段
        decrypted_result = []
        for record in result.data:
            decrypted_record = decrypt_data("receipt_items_en", record)
            decrypted_result.append(decrypted_record)
        
        for record in decrypted_result:
            if record.get("file_url"):
                try:
                    signed_url_result = supabase.storage.from_(SUPABASE_BUCKET).create_signed_url(
                        record["file_url"], 
                        expires_in=86400  # 24小时
                    )
                    record["file_url"] = signed_url_result.get("signedURL", record["file_url"])
                except Exception as e:
                    logger.warning(f"Failed to generate signed URL for {record['file_url']}: {e}")
        
        return decrypted_result
        
    except Exception as e:
        logger.exception(f"Failed to retrieve receipts: {str(e)}")
        return {"error": f"Failed to retrieve receipts: {str(e)}", "status": "error"}
    

async def delete_receipt(request: DeleteReceiptRequest):
    """根据ind和user_id批量删除收据信息"""
    logger.info(f"Deleting receipts for user_id: {request.user_id}, ind: {request.inds}")
    
    try:
        if not request.inds:
            return {"error": "ind list cannot be empty", "status": "error"}
        
        # 1. 先查询 receipt_items_en 表，获取对应的 id 列表
        receipt_query_result = supabase.table("receipt_items_en").select("id, ind").eq("user_id", request.user_id).in_("ind", request.inds).execute()
        
        if not receipt_query_result.data:
            return {"message": "No matching records found", "deleted_count": 0, "status": "success"}
        
        # 提取 id 列表和实际找到的 ind 列表
        found_ids = [record["id"] for record in receipt_query_result.data]
        found_inds = [record["ind"] for record in receipt_query_result.data]
        
        logger.info(f"Found {len(found_ids)} records to delete with ids: {found_ids}")
        
        # 2. 删除 receipt_items_en 表中的记录
        receipt_delete_result = supabase.table("receipt_items_en").delete().eq("user_id", request.user_id).in_("ind", found_inds).execute()
        receipt_deleted_count = len(receipt_delete_result.data) if receipt_delete_result.data else 0
        
        # 3. 删除 ses_eml_info_en 表中对应的记录（根据 id 匹配）
        eml_delete_result = supabase.table("ses_eml_info_en").delete().eq("user_id", request.user_id).in_("id", found_ids).execute()
        eml_deleted_count = len(eml_delete_result.data) if eml_delete_result.data else 0
        
        # 检查是否有未找到的 ind
        not_found_inds = list(set(request.inds) - set(found_inds))
        
        logger.info(f"Successfully deleted {receipt_deleted_count} records from receipt_items_en and {eml_deleted_count} records from ses_eml_info_en")
        
        response_data = {
            "message": "Records deleted successfully",
            "receipt_deleted_count": receipt_deleted_count,
            "eml_deleted_count": eml_deleted_count,
            "total_deleted_pairs": min(receipt_deleted_count, eml_deleted_count),
            "status": "success"
        }
        
        # 如果有未找到的记录，添加到响应中
        if not_found_inds:
            response_data["not_found_inds"] = not_found_inds
            response_data["message"] += f". {len(not_found_inds)} records not found: {not_found_inds}"
        
        return response_data
        
    except Exception as e:
        logger.exception(f"Failed to delete receipts: {str(e)}")
        return {"error": f"Failed to delete receipts: {str(e)}", "status": "error"}