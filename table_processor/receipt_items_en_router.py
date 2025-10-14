import logging
import asyncio
from typing import Optional, List
from pydantic import BaseModel, Field
from supabase import create_client, Client
from datetime import datetime, timedelta
import calendar
from asyncpg.pgproto.pgproto import UUID as AsyncpgUUID
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from sqlalchemy import select, update, delete, and_
from concurrent.futures import ThreadPoolExecutor
from core.database import AsyncSessionLocal
from core.config import settings
from core.models import ReceiptItemsEN, SesEmlInfoEN
from core.encryption import encrypt_data, decrypt_data
from core.upload_files import upload_files_to_supabase
from table_processor.utils import process_record

executor = ThreadPoolExecutor(max_workers=8)
supabase: Client = create_client(settings.supabase_url, settings.supabase_service_role_key)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/receipt-items-en", tags=["receipt_items_en表操作"])

class GetReceiptRequest(BaseModel):
    user_id: str  # 必填
    ind: Optional[int] = None  # 精确查询
    start_time: Optional[str] = None  # YYYY-MM-DD
    end_time: Optional[str] = None    # YYYY-MM-DD
    year: Optional[int] = None        # 查询年份
    month: Optional[int] = None       # 查询月份
    limit: Optional[int] = 0
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
    2. year + month 按月查询 invoice_date
    3. create_time 时间范围 (YYYY-MM-DD → timestamptz 范围)
    4. limit+offset 分页查询
    默认查询“上个月invoice_date”的所有记录
    """
    logger.info(
        f"Querying receipts for user_id: {request.user_id}, "
        f"ind: {request.ind}, start_time: {request.start_time}, end_time: {request.end_time}, "
        f"year: {request.year}, month: {request.month}, limit: {request.limit}, offset: {request.offset}"
    )

    try:
        async with AsyncSessionLocal() as session:
            query = select(
                ReceiptItemsEN.ind,
                ReceiptItemsEN.id,
                ReceiptItemsEN.user_id,
                ReceiptItemsEN.category,
                ReceiptItemsEN.buyer,
                ReceiptItemsEN.seller,
                ReceiptItemsEN.invoice_date,
                ReceiptItemsEN.invoice_total,
                ReceiptItemsEN.currency,
                ReceiptItemsEN.file_url,
                ReceiptItemsEN.address
            ).where(ReceiptItemsEN.user_id == request.user_id)

            # ① 精确查询优先
            if request.ind:
                query = query.where(ReceiptItemsEN.ind == request.ind)
                logger.info(f"Exact query for record id: {request.ind}")

            # ② 年月查询（优先级高于 start/end）
            elif request.year and request.month:               
                year, month = request.year, request.month
                start_dt = datetime(year, month, 1)
                _, last_day = calendar.monthrange(year, month)
                end_dt = datetime(year, month, last_day, 23, 59, 59, 999999)
                query = query.where(
                    ReceiptItemsEN.invoice_date >= start_dt,
                    ReceiptItemsEN.invoice_date <= end_dt
                )
                logger.info(f"Monthly query: {year}-{month:02d}")

            # ③ 普通时间范围查询
            elif request.start_time != "string" and request.end_time != "string":
                start_dt = datetime.strptime(request.start_time, "%Y-%m-%d")
                end_dt = datetime.strptime(request.end_time, "%Y-%m-%d")
                end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                query = query.where(ReceiptItemsEN.create_time >= start_dt,
                                    ReceiptItemsEN.create_time <= end_dt)

                logger.info(f"Custom range query: {request.start_time} - {request.end_time}")

            # 排序 + 分页
            elif request.offset and request.limit:               
                query = query.order_by(ReceiptItemsEN.invoice_date.desc()).offset(request.offset).limit(request.limit)

            # ④ 默认查询上个月
            else:
                today = datetime.utcnow()
                first_of_this_month = datetime(today.year, today.month, 1)
                last_month_end = first_of_this_month - timedelta(seconds=1)
                last_month_start = datetime(last_month_end.year, last_month_end.month, 1)
                query = query.where(
                    ReceiptItemsEN.invoice_date >= last_month_start,
                    ReceiptItemsEN.invoice_date <= last_month_end
                )
                logger.info(f"Default: query last month ({last_month_start.date()} ~ {last_month_end.date()})")


            logger.info(f"Final SQL: {query}")
            result = await session.execute(query)
            records = result.mappings().all()

        if not records:
            return {"message": "No records found", "data": [], "total": 0, "status": "success"}

        # 并行执行解密 + 签名
        decrypted_result = await asyncio.gather(
            *[process_record(r, "receipt_items_en", "file_url") for r in records]
        )

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
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                update(ReceiptItemsEN)
                .where(and_(ReceiptItemsEN.ind == request.ind, ReceiptItemsEN.user_id == request.user_id))
                .values(**encrypted_update_data)
                .returning(ReceiptItemsEN)
            )
            await session.commit()
            updated_records = result.mappings().all() 
        
        if not updated_records:
            return {"error": "No matching record found or no permission to update", "status": "error"}

        # 并行执行解密 
        decrypted_result = await asyncio.gather(*[process_record(r, "receipt_items_en") for r in updated_records])
       
        logger.info(f"Successfully updated {len(updated_records)} record(s)")
        return {
            "message": "Receipt information updated successfully", 
            "updated_records": len(updated_records),
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
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    update(ReceiptItemsEN)
                    .where(and_(ReceiptItemsEN.user_id == user_id, ReceiptItemsEN.ind == ind))
                    .values(file_url=encrypted_path)
                    .returning(ReceiptItemsEN)
                )
                await session.commit()
                update_result_data = result.scalars().all()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to update database: {str(e)}")

        if not update_result_data:
            raise HTTPException(status_code=404, detail="Record not found to update")

        logger.info("返回签名 URL（24小时有效）")
        try:
            signed_url_result = supabase.storage.from_(settings.supabase_bucket).create_signed_url(
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
        async with AsyncSessionLocal() as session:
            receipt_query_result = await session.execute(
                select(ReceiptItemsEN.id, ReceiptItemsEN.ind, ReceiptItemsEN.file_url)
                .where(and_(ReceiptItemsEN.user_id == request.user_id, ReceiptItemsEN.ind.in_(request.inds)))
            )
            records = receipt_query_result.all()
        
        if not records:
            return {"message": "No matching records found", "deleted_count": 0, "status": "success"}
        
        found_ids = [record.id for record in records]
        found_inds = [record.ind for record in records]
        
        # 2. 删除 receipt_items_en 表中的记录
        async with AsyncSessionLocal() as session:
            receipt_delete_result = await session.execute(
                delete(ReceiptItemsEN)
                .where(and_(ReceiptItemsEN.user_id == request.user_id, ReceiptItemsEN.ind.in_(found_inds)))
                .returning(ReceiptItemsEN)
            )
            receipt_deleted_data = receipt_delete_result.scalars().all()
            receipt_deleted_count = len(receipt_deleted_data)
            
            # 3. 删除 ses_eml_info_en 表中的对应记录
            eml_delete_result = await session.execute(
                delete(SesEmlInfoEN)
                .where(and_(SesEmlInfoEN.user_id == request.user_id, SesEmlInfoEN.id.in_(found_ids)))
                .returning(SesEmlInfoEN)
            )
            eml_deleted_data = eml_delete_result.scalars().all()
            eml_deleted_count = len(eml_deleted_data)
            
            await session.commit()
        
        # 4. 删除 Supabase Storage 中的文件
        deleted_files = []
        failed_files = []
        
        for record in records:
            if record.file_url:
                try:
                    # 解密 file_url
                    decrypted_url = decrypt_data("receipt_items_en", {"file_url": record.file_url}).get("file_url")
                    
                    if decrypted_url:
                        try:
                            # 调用 Supabase Storage API 删除文件
                            supabase.storage.from_(settings.supabase_bucket).remove([decrypted_url])
                            deleted_files.append(decrypted_url)
                            logger.info(f"Deleted file from storage: {decrypted_url}")
                        except:
                            failed_files.append(decrypted_url)
                    else:
                        failed_files.append("empty file_url")
                except Exception as e:
                    logger.warning(f"Failed to delete file from storage: {e}")
                    failed_files.append(str(record.file_url))
        
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
    