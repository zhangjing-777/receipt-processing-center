import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional, List
from pydantic import BaseModel, Field
from supabase import create_client, Client
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from core.encryption import encrypt_data, decrypt_data
from core.upload_files import upload_files_to_supabase

load_dotenv()

url: str = os.getenv("SUPABASE_URL") or ""
key: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
supabase: Client = create_client(url, key)
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/receipt-summary-zip-en", tags=["receipt_summary_zip_en表操作"])

# ----------- 请求体定义 -----------
class GetSummaryZipRequest(BaseModel):
    user_id: str  # 必填
    id: Optional[int] = None  # 精确查询
    start_time: Optional[str] = None  # YYYY-MM-DD
    end_time: Optional[str] = None    # YYYY-MM-DD
    limit: Optional[int] = 10
    offset: Optional[int] = 0

class UpdateSummaryZipRequest(BaseModel):
    id: int = Field(..., description="记录ID")
    user_id: str = Field(..., description="用户ID")
    
    title: Optional[str] = None
    summary_content: Optional[str] = None

class DeleteSummaryZipRequest(BaseModel):
    user_id: str
    ids: List[int]

# ----------- 查询接口 -----------
@router.post("/get-summary-zip")
async def get_summary_zip(request: GetSummaryZipRequest):
    """
    根据 user_id 和条件查询 summary zip 信息
    """
    try:
        query = supabase.table("receipt_summary_zip_en").select("*").eq("user_id", request.user_id)

        if request.id:
            query = query.eq("id", request.id)
        elif request.start_time != "string" or request.end_time != "string":
            if request.start_time != "string":
                try:
                    start_dt = datetime.strptime(request.start_time, "%Y-%m-%d")
                    query = query.gte("created_at", start_dt.isoformat())
                except ValueError:
                    return {"error": "Invalid start_time format, expected YYYY-MM-DD", "status": "error"}
            if request.end_time != "string":
                try:
                    end_dt = datetime.strptime(request.end_time, "%Y-%m-%d")
                    end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                    query = query.lte("created_at", end_dt.isoformat())
                except ValueError:
                    return {"error": "Invalid end_time format, expected YYYY-MM-DD", "status": "error"}
            query = query.order("created_at", desc=True)
        else:
            query = query.order("created_at", desc=True).range(
                request.offset, request.offset + request.limit - 1
            )

        result = query.execute()

        if not result.data:
            return {"message": "No records found", "data": [], "total": 0, "status": "success"}

        decrypted_result = [decrypt_data("receipt_summary_zip_en", record) for record in result.data]

        # 生成 signed URL
        for record in decrypted_result:
            if record.get("download_url"):
                try:
                    signed_url_result = supabase.storage.from_(SUPABASE_BUCKET).create_signed_url(
                        record["download_url"], expires_in=86400
                    )
                    record["download_url"] = signed_url_result.get("signedURL", record["download_url"])
                except Exception as e:
                    logger.warning(f"Failed to generate signed URL for {record['download_url']}: {e}")

        return decrypted_result

    except Exception as e:
        logger.exception(f"Failed to retrieve summary zip: {str(e)}")
        return {"error": f"Failed to retrieve summary zip: {str(e)}", "status": "error"}


# ----------- 更新接口 -----------
@router.post("/update-summary-zip")
async def update_summary_zip(request: UpdateSummaryZipRequest):
    """根据 id 和 user_id 更新 summary zip 信息"""
    try:
        update_data = {}
        for field, value in request.dict(exclude={'id', 'user_id'}).items():
            if value and value != "string":
                update_data[field] = value

        if not update_data:
            return {"message": "No data to update", "status": "success"}

        encrypted_update_data = encrypt_data("receipt_summary_zip_en", update_data)

        result = (
            supabase.table("receipt_summary_zip_en")
            .update(encrypted_update_data)
            .eq("id", request.id)
            .eq("user_id", request.user_id)
            .execute()
        )

        if not result.data:
            return {"error": "No matching record found or no permission to update", "status": "error"}

        decrypted_result = [decrypt_data("receipt_summary_zip_en", record) for record in result.data]

        # 生成 signed URL
        for record in decrypted_result:
            if record.get("download_url"):
                try:
                    signed_url_result = supabase.storage.from_(SUPABASE_BUCKET).create_signed_url(
                        record["download_url"], expires_in=86400
                    )
                    record["download_url"] = signed_url_result.get("signedURL", record["download_url"])
                except Exception as e:
                    logger.warning(f"Failed to generate signed URL for {record['download_url']}: {e}")

        return {
            "message": "Summary zip updated successfully",
            "updated_records": len(result.data),
            "data": decrypted_result,
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to update summary zip: {str(e)}")
        return {"error": f"Failed to update summary zip: {str(e)}", "status": "error"}

@router.post("/update-download-url")
async def update_summary_zip_download_url(
    user_id: str = Form(...),
    id: int = Form(...),
    file: UploadFile = File(...)
):
    """上传新文件到 Storage，并更新数据库里的加密 download_url，同时删除旧文件"""

    try:
        # 1. 查旧记录
        record_result = (
            supabase.table("receipt_summary_zip_en")
            .select("download_url")
            .eq("user_id", user_id)
            .eq("id", id)
            .execute()
        )
        if not record_result.data:
            raise HTTPException(status_code=404, detail="Record not found")

        old_record = record_result.data[0]
        old_download_url = None
        try:
            decrypted = decrypt_data("receipt_summary_zip_en", old_record)
            old_download_url = decrypted.get("download_url")
        except Exception as e:
            logger.warning(f"Failed to decrypt old download_url: {e}")

        # 2. 上传新文件
        storage_path = upload_files_to_supabase(user_id, [file], "summary")[file.filename]

        # 3. 加密新路径并更新表
        encrypted_path = encrypt_data("receipt_summary_zip_en", {"download_url": storage_path})["download_url"]

        update_result = (
            supabase.table("receipt_summary_zip_en")
            .update({"download_url": encrypted_path})
            .eq("user_id", user_id)
            .eq("id", id)
            .execute()
        )
        if not update_result.data:
            raise HTTPException(status_code=500, detail="Failed to update database")

        # 4. 删除旧文件
        if old_download_url:
            try:
                supabase.storage.from_(SUPABASE_BUCKET).remove([old_download_url])
                logger.info(f"Deleted old file from storage: {old_download_url}")
            except Exception as e:
                logger.warning(f"Failed to delete old file from storage: {e}")

        # 5. 返回签名 URL
        try:
            signed_url_result = supabase.storage.from_(SUPABASE_BUCKET).create_signed_url(
                storage_path, expires_in=86400
            )
            signed_url = signed_url_result.get("signedURL", storage_path)
        except Exception as e:
            signed_url = storage_path
            logger.warning(f"Failed to create signed URL: {e}")

        return {
            "message": "File uploaded and download_url updated successfully",
            "user_id": user_id,
            "id": id,
            "download_url": signed_url,
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"update_summary_zip_file_url failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"update_summary_zip_file_url failed: {str(e)}")

# ----------- 删除接口 -----------
@router.delete("/delete-summary-zip")
async def delete_summary_zip(request: DeleteSummaryZipRequest):
    """根据 id 和 user_id 删除 receipt_summary_zip_en 表内容 + 删除 Storage 文件"""
    try:
        if not request.ids:
            return {"error": "ids list cannot be empty", "status": "error"}

        # 1. 查询记录，拿到 download_url
        records_result = (
            supabase.table("receipt_summary_zip_en")
            .select("id, download_url")
            .eq("user_id", request.user_id)
            .in_("id", request.ids)
            .execute()
        )

        if not records_result.data:
            return {"message": "No matching records found", "deleted_count": 0, "status": "success"}

        # 解密 download_url 并尝试删除 Storage 文件
        deleted_files = []
        failed_files = []
        for record in records_result.data:
            try:
                decrypted = decrypt_data("receipt_summary_zip_en", record)
                if decrypted.get("download_url"):
                    try:
                        supabase.storage.from_(SUPABASE_BUCKET).remove([decrypted["download_url"]])
                        deleted_files.append(decrypted["download_url"])
                        logger.info(f"Deleted file from storage: {decrypted['download_url']}")
                    except Exception as e:
                        failed_files.append(decrypted["download_url"])
                        logger.warning(f"Failed to delete file from storage: {e}")
            except Exception as e:
                failed_files.append(record.get("download_url"))
                logger.warning(f"Failed to decrypt download_url: {e}")

        # 2. 删除数据库记录
        delete_result = (
            supabase.table("receipt_summary_zip_en")
            .delete()
            .eq("user_id", request.user_id)
            .in_("id", request.ids)
            .execute()
        )

        deleted_count = len(delete_result.data) if delete_result.data else 0

        return {
            "message": "Records deleted successfully",
            "deleted_count": deleted_count,
            "deleted_files": deleted_files,
            "failed_files": failed_files,
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to delete summary zip: {str(e)}")
        return {"error": f"Failed to delete summary zip: {str(e)}", "status": "error"}

