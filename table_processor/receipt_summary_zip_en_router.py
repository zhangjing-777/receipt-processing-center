import logging
import asyncio
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field
from supabase import create_client, Client
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from sqlalchemy import select, update, delete, and_
from core.config import settings
from core.database import AsyncSessionLocal
from core.encryption import encrypt_data, decrypt_data, encrypt_value
from core.upload_files import upload_files_to_supabase
from core.models import ReceiptSummaryZipEN
from table_processor.utils import process_record


supabase: Client = create_client(settings.supabase_url, settings.supabase_service_role_key)

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
        async with AsyncSessionLocal() as session:
            query = select(ReceiptSummaryZipEN).where(ReceiptSummaryZipEN.user_id == request.user_id)

            if request.id:
                query = query.where(ReceiptSummaryZipEN.id == request.id)
            elif request.start_time != "string" and request.end_time != "string":
                start_dt = datetime.strptime(request.start_time, "%Y-%m-%d").date()
                end_dt = datetime.strptime(request.end_time, "%Y-%m-%d").date()
                query = query.where(ReceiptSummaryZipEN.created_at >= start_dt,
                                    ReceiptSummaryZipEN.created_at <= end_dt
                                    ).order_by(ReceiptSummaryZipEN.created_at.desc())
            else:
                query = query.order_by(ReceiptSummaryZipEN.created_at.desc()).offset(request.offset).limit(request.limit)

            result = await session.execute(query)
            records = result.mappings().all()

        if not records:
            return {"message": "No records found", "data": [], "total": 0, "status": "success"}

        # 并行执行解密 + 签名
        decrypted_result = await asyncio.gather(*[process_record(r, "receipt_summary_zip_en", "download_url") for r in records])

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

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                update(ReceiptSummaryZipEN)
                .where(and_(ReceiptSummaryZipEN.id == request.id, ReceiptSummaryZipEN.user_id == request.user_id))
                .values(**encrypted_update_data)
                .returning(ReceiptSummaryZipEN)
            )
            await session.commit()
            updated_records = result.mappings().all()

        if not updated_records:
            return {"error": "No matching record found or no permission to update", "status": "error"}

        # 并行执行解密 + 签名
        decrypted_result = await asyncio.gather(*[process_record(r, "receipt_summary_zip_en", "download_url") for r in updated_records])

        return {
            "message": "Summary zip updated successfully",
            "updated_records": len(decrypted_result),
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
        async with AsyncSessionLocal() as session:
            record_result = await session.execute(
                select(ReceiptSummaryZipEN.download_url)
                .where(and_(ReceiptSummaryZipEN.user_id == user_id, ReceiptSummaryZipEN.id == id))
            )
            record_data = record_result.first()
        
        if not record_data:
            raise HTTPException(status_code=404, detail="Record not found")

        old_download_url = None
        try:
            decrypted = decrypt_data("receipt_summary_zip_en", {"download_url": record_data[0]})
            old_download_url = decrypted.get("download_url")
        except Exception as e:
            logger.warning(f"Failed to decrypt old download_url: {e}")

        # 2. 上传新文件
        storage_path = upload_files_to_supabase(user_id, [file], "summary")[file.filename]

        # 3. 加密新路径并更新表
        encrypted_path = encrypt_value(storage_path)

        async with AsyncSessionLocal() as session:
            update_result = await session.execute(
                update(ReceiptSummaryZipEN)
                .where(and_(ReceiptSummaryZipEN.user_id == user_id, ReceiptSummaryZipEN.id == id))
                .values(download_url=encrypted_path)
                .returning(ReceiptSummaryZipEN)
            )
            await session.commit()
            update_result_data = update_result.scalars().all()
        
        if not update_result_data:
            raise HTTPException(status_code=500, detail="Failed to update database")

        # 4. 删除旧文件
        if old_download_url:
            try:
                supabase.storage.from_(settings.supabase_bucket).remove([old_download_url])
                logger.info(f"Deleted old file from storage: {old_download_url}")
            except Exception as e:
                logger.warning(f"Failed to delete old file from storage: {e}")

        # 5. 返回签名 URL
        try:
            signed_url_result = supabase.storage.from_(settings.supabase_bucket).create_signed_url(
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
        async with AsyncSessionLocal() as session:
            records_result = await session.execute(
                select(ReceiptSummaryZipEN.id, ReceiptSummaryZipEN.download_url)
                .where(and_(ReceiptSummaryZipEN.user_id == request.user_id, ReceiptSummaryZipEN.id.in_(request.ids)))
            )
            records = records_result.all()

        if not records:
            return {"message": "No matching records found", "deleted_count": 0, "status": "success"}

        # 解密 download_url 并尝试删除 Storage 文件
        deleted_files = []
        failed_files = []
        for record in records:
            try:
                decrypted = decrypt_data("receipt_summary_zip_en", {"download_url": record.download_url})
                if decrypted.get("download_url"):
                    try:
                        supabase.storage.from_(settings.supabase_bucket).remove([decrypted["download_url"]])
                        deleted_files.append(decrypted["download_url"])
                        logger.info(f"Deleted file from storage: {decrypted['download_url']}")
                    except Exception as e:
                        failed_files.append(decrypted["download_url"])
                        logger.warning(f"Failed to delete file from storage: {e}")
            except Exception as e:
                failed_files.append(str(record.download_url))
                logger.warning(f"Failed to decrypt download_url: {e}")

        # 2. 删除数据库记录
        async with AsyncSessionLocal() as session:
            delete_result = await session.execute(
                delete(ReceiptSummaryZipEN)
                .where(and_(ReceiptSummaryZipEN.user_id == request.user_id, ReceiptSummaryZipEN.id.in_(request.ids)))
                .returning(ReceiptSummaryZipEN)
            )
            await session.commit()
            deleted_data = delete_result.scalars().all()
            deleted_count = len(deleted_data)

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
    