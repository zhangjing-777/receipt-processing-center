import os
import logging
from dotenv import load_dotenv
from typing import Optional, List
from pydantic import BaseModel, Field
from supabase import create_client, Client
from datetime import datetime
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from core.encryption import encrypt_data, decrypt_data
from core.upload_files import upload_files_to_supabase

load_dotenv()

url: str = os.getenv("SUPABASE_URL") or ""
key: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
supabase: Client = create_client(url, key)
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/receipt-items-en", tags=["receipt_items_en表操作"])

class GetReceiptRequest(BaseModel):
    user_id: str  # 必填
    ind: Optional[int] = None  # 精确查询
    start_time: Optional[str] = None  # YYYY-MM-DD
    end_time: Optional[str] = None    # YYYY-MM-DD
    limit: Optional[int] = 10
    offset: Optional[int] = 0

class UpdateReceiptRequest(BaseModel):
    ind: int = Field(..., description="记录ID")
    user_id: str = Field(..., description="用户ID")
    
    buyer: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    seller: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    invoice_date: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    category: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    invoice_total: Optional[float] = Field(default=None, json_schema_extra={"default": None})
    currency: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    invoice_number: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    address: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    original_info: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    ocr: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    hash_id: Optional[str] = Field(default=None, json_schema_extra={"default": None})
    create_time: Optional[str] = Field(default=None, json_schema_extra={"default": None})

class DeleteReceiptRequest(BaseModel):
    user_id: str  # 必填
    inds: List[int]  # 必填，支持批量删除


@router.post("/get-receipt-items")
async def get_receipt(request: GetReceiptRequest):
    """
    根据 user_id 和条件查询收据信息:
    1. ind 精确查询
    2. create_time 时间范围 (YYYY-MM-DD → timestamptz 范围)
    3. limit+offset 分页查询
    """
    logger.info(
        f"Querying receipts for user_id: {request.user_id}, "
        f"ind: {request.ind}, start_time: {request.start_time}, end_time: {request.end_time}, "
        f"limit: {request.limit}, offset: {request.offset}"
    )

    try:
        query = supabase.table("receipt_items_en").select("*").eq("user_id", request.user_id)

        if request.ind:
            # ① 精确查询
            query = query.eq("ind", request.ind)
            logger.info(f"Exact query for record id: {request.ind}")

        elif request.start_time != "string" or request.end_time != "string":
            # ② 时间范围查询
            if request.start_time != "string":
                try:
                    start_dt = datetime.strptime(request.start_time, "%Y-%m-%d")
                    query = query.gte("create_time", start_dt.isoformat())
                except ValueError:
                    return {"error": "Invalid start_time format, expected YYYY-MM-DD", "status": "error"}

            if request.end_time != "string":
                try:
                    end_dt = datetime.strptime(request.end_time, "%Y-%m-%d")
                    # 包含当天 → 设置为当天的最后一秒
                    end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                    query = query.lte("create_time", end_dt.isoformat())
                except ValueError:
                    return {"error": "Invalid end_time format, expected YYYY-MM-DD", "status": "error"}

            query = query.order("create_time", desc=True)
            logger.info(f"Time range query: {request.start_time} - {request.end_time}")

        else:
            # ③ 分页查询
            query = query.order("create_time", desc=True).range(
                request.offset, request.offset + request.limit - 1
            )
            logger.info(f"Paginated query with limit: {request.limit}, offset: {request.offset}")
        logger.info(f"query is {query}")
        result = query.execute()

        if not result.data:
            return {"message": "No records found", "data": [], "total": 0, "status": "success"}

        decrypted_result = [decrypt_data("receipt_items_en", record) for record in result.data]

        # 生成 signed URL
        for record in decrypted_result:
            if record.get("file_url"):
                try:
                    signed_url_result = supabase.storage.from_(SUPABASE_BUCKET).create_signed_url(
                        record["file_url"], expires_in=86400
                    )
                    record["file_url"] = signed_url_result.get("signedURL", record["file_url"])
                except Exception as e:
                    logger.warning(f"Failed to generate signed URL for {record['file_url']}: {e}")

        return decrypted_result

    except Exception as e:
        logger.exception(f"Failed to retrieve receipts: {str(e)}")
        return {"error": f"Failed to retrieve receipts: {str(e)}", "status": "error"}

@router.post("/update-receipt-items")
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

@router.post("/update-file-url")
async def update_file_url(
    user_id: str = Form(...),
    ind: int = Form(...),
    file: UploadFile = File(...)
):
    """上传新文件到 Storage，并更新数据库里的加密 file_url"""

    try:
        logger.info("上传新文件到 Supabase Storage ...")
        storage_path = upload_files_to_supabase(user_id, [file])[file.filename]

        logger.info("加密存储路径...")
        encrypted_path = encrypt_data("receipt_items_en", {"file_url": storage_path})["file_url"]

        logger.info("更新数据库里的 file_url 字段（存加密值）")
        try:
            update_result = (
                supabase.table("receipt_items_en")
                .update({"file_url": encrypted_path})
                .eq("user_id", user_id)
                .eq("ind", ind)
                .execute()
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to update database: {str(e)}")

        if not update_result.data:
            raise HTTPException(status_code=404, detail="Record not found to update")

        logger.info("返回签名 URL（24小时有效）")
        try:
            signed_url_result = supabase.storage.from_(SUPABASE_BUCKET).create_signed_url(
                storage_path, expires_in=86400
            )
            signed_url = signed_url_result.get("signedURL", storage_path)
        except Exception as e:
            signed_url = storage_path
            logger.warning(f"Failed to create signed URL: {e}")

        return {
            "message": "File uploaded and file_url updated successfully",
            "user_id": user_id,
            "ind": ind,
            "download_url": signed_url,     # 临时下载链接
        }

    except Exception as e:
        logger.exception(f"update_file_url failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"update_file_url failed: {str(e)}")

@router.delete("/delete-receipt-items")
async def delete_receipt(request: DeleteReceiptRequest):
    """根据 ind 和 user_id 批量删除收据信息和 storage 文件"""
    logger.info(f"Deleting receipts for user_id: {request.user_id}, ind: {request.inds}")
    
    try:
        if not request.inds:
            return {"error": "ind list cannot be empty", "status": "error"}
        
        # 1. 查询 receipt_items_en，拿到 id 和 file_url
        receipt_query_result = (
            supabase.table("receipt_items_en")
            .select("id, ind, file_url")
            .eq("user_id", request.user_id)
            .in_("ind", request.inds)
            .execute()
        )
        
        if not receipt_query_result.data:
            return {"message": "No matching records found", "deleted_count": 0, "status": "success"}
        
        found_ids = [record["id"] for record in receipt_query_result.data]
        found_inds = [record["ind"] for record in receipt_query_result.data]
        
        # 2. 删除 receipt_items_en 表中的记录
        receipt_delete_result = (
            supabase.table("receipt_items_en")
            .delete()
            .eq("user_id", request.user_id)
            .in_("ind", found_inds)
            .execute()
        )
        receipt_deleted_count = len(receipt_delete_result.data) if receipt_delete_result.data else 0
        
        # 3. 删除 ses_eml_info_en 表中的对应记录
        eml_delete_result = (
            supabase.table("ses_eml_info_en")
            .delete()
            .eq("user_id", request.user_id)
            .in_("id", found_ids)
            .execute()
        )
        eml_deleted_count = len(eml_delete_result.data) if eml_delete_result.data else 0
        
        # 4. 删除 Supabase Storage 中的文件
        deleted_files = []
        failed_files = []
        
        for record in receipt_query_result.data:
            if record.get("file_url"):
                try:
                    # 解密 file_url
                    decrypted_url = decrypt_data("receipt_items_en", record).get("file_url")
                    
                    if decrypted_url:
                        try:                           
                            # 调用 Supabase Storage API 删除文件
                            supabase.storage.from_(SUPABASE_BUCKET).remove([decrypted_url])
                            deleted_files.append(decrypted_url)
                            logger.info(f"Deleted file from storage: {decrypted_url}")
                        except:
                            failed_files.append(decrypted_url)
                    else:
                        failed_files.append("empty file_url")
                except Exception as e:
                    logger.warning(f"Failed to delete file from storage: {e}")
                    failed_files.append(record.get("file_url"))
        
        # 检查是否有未找到的 ind
        not_found_inds = list(set(request.inds) - set(found_inds))
        
        response_data = {
            "message": "Records deleted successfully",
            "receipt_deleted_count": receipt_deleted_count,
            "eml_deleted_count": eml_deleted_count,
            "deleted_files": deleted_files,
            "failed_files": failed_files,
            "total_deleted_pairs": min(receipt_deleted_count, eml_deleted_count),
            "status": "success"
        }
        
        if not_found_inds:
            response_data["not_found_inds"] = not_found_inds
            response_data["message"] += f". {len(not_found_inds)} records not found: {not_found_inds}"
        
        return response_data
        
    except Exception as e:
        logger.exception(f"Failed to delete receipts: {str(e)}")
        return {"error": f"Failed to delete receipts: {str(e)}", "status": "error"}
