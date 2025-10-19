import logging
import asyncio
from typing import Optional, List
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
import calendar
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from sqlalchemy import select, update, delete, and_
from core.database import AsyncSessionLocal
from core.config import settings
from core.models import ReceiptItemsEN, SesEmlInfoEN
from core.encryption import encrypt_data, decrypt_data
from core.upload_files import upload_files_to_supabase_async
from core.supabase_storage import get_async_storage_client
from core.batch_operations import BatchOperations
from core.performance_monitor import timer, measure_time
from table_processor.utils import process_record

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/receipt-items-en", tags=["receipt_items_en表操作"])

# ========== 请求模型 ==========

class GetReceiptRequest(BaseModel):
    user_id: str
    ind: Optional[int] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    year: Optional[int] = None
    month: Optional[int] = None
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
    user_id: str
    inds: List[int]


# ========== 查询接口 ==========

@router.post("/get-receipt-items")
@timer("get_receipt_items")
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
        f"ind: {request.ind}, year: {request.year}, month: {request.month}"
    )

    try:
        async with measure_time("database_query"):
            async with AsyncSessionLocal() as session:
                # 构建查询
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

                # 精确查询
                if request.ind:
                    query = query.where(ReceiptItemsEN.ind == request.ind)
                    logger.info(f"Exact query for ind: {request.ind}")

                # 按年月查询
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

                # 时间范围查询
                elif request.start_time != "string" and request.end_time != "string":
                    start_dt = datetime.strptime(request.start_time, "%Y-%m-%d")
                    end_dt = datetime.strptime(request.end_time, "%Y-%m-%d")
                    end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                    query = query.where(
                        ReceiptItemsEN.create_time >= start_dt,
                        ReceiptItemsEN.create_time <= end_dt
                    )
                    logger.info(f"Range query: {request.start_time} - {request.end_time}")

                # 分页查询
                elif request.offset and request.limit:               
                    query = query.order_by(
                        ReceiptItemsEN.invoice_date.desc()
                    ).offset(request.offset).limit(request.limit)

                # 默认查询上个月
                else:
                    today = datetime.utcnow()
                    first_of_this_month = datetime(today.year, today.month, 1)
                    last_month_end = first_of_this_month - timedelta(seconds=1)
                    last_month_start = datetime(last_month_end.year, last_month_end.month, 1)
                    query = query.where(
                        ReceiptItemsEN.invoice_date >= last_month_start,
                        ReceiptItemsEN.invoice_date <= last_month_end
                    )
                    logger.info(f"Default: last month ({last_month_start.date()} ~ {last_month_end.date()})")

                result = await session.execute(query)
                records = result.mappings().all()

        if not records:
            return {"message": "No records found", "data": [], "total": 0, "status": "success"}

        # 并行解密和签名
        async with measure_time("decrypt_and_sign"):
            decrypted_result = await asyncio.gather(
                *[process_record(r, "receipt_items_en", "file_url") for r in records]
            )

        logger.info(f"Query completed: {len(decrypted_result)} records")
        return decrypted_result

    except Exception as e:
        logger.exception(f"Failed to retrieve receipts: {str(e)}")
        return {"error": f"Failed to retrieve receipts: {str(e)}", "status": "error"}


# ========== 更新接口 ==========

@router.post("/update-receipt-items")
@timer("update_receipt_items")
async def update_receipt(request: UpdateReceiptRequest):
    """根据record_id和user_id更新收据信息接口"""
    logger.info(f"Updating receipt: ind={request.ind}, user_id={request.user_id}")
    
    try:
        # 构建更新数据
        update_data = {}
        for field, value in request.dict(exclude={'ind', 'user_id'}).items():
            if value != "string" and value:
                update_data[field] = value
        
        if not update_data:
            return {"message": "No data to update", "status": "success"}
        
        logger.info(f"Fields to update: {list(update_data.keys())}")
        
        # 加密敏感字段
        encrypted_update_data = encrypt_data("receipt_items_en", update_data)
        
        # 执行更新
        async with measure_time("database_update"):
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    update(ReceiptItemsEN)
                    .where(and_(
                        ReceiptItemsEN.ind == request.ind,
                        ReceiptItemsEN.user_id == request.user_id
                    ))
                    .values(**encrypted_update_data)
                    .returning(ReceiptItemsEN)
                )
                await session.commit()
                updated_records = result.mappings().all()
        
        if not updated_records:
            return {"error": "No matching record found", "status": "error"}

        # 并行解密
        decrypted_result = await asyncio.gather(
            *[process_record(r, "receipt_items_en") for r in updated_records]
        )
       
        logger.info(f"Successfully updated {len(updated_records)} record(s)")
        return {
            "message": "Receipt updated successfully", 
            "updated_records": len(updated_records),
            "data": decrypted_result,
            "status": "success"
        }
        
    except Exception as e:
        logger.exception(f"Failed to update receipt: {str(e)}")
        return {"error": f"Failed to update receipt: {str(e)}", "status": "error"}


# ========== 文件更新接口 ==========

@router.post("/update-file-url")
@timer("update_file_url")
async def update_file_url(
    user_id: str = Form(...),
    ind: int = Form(...),
    file: UploadFile = File(...)
):
    """上传新文件到 Storage，并更新数据库里的加密 file_url"""
    storage_client = get_async_storage_client()
    
    try:
        logger.info(f"Uploading new file for user_id={user_id}, ind={ind}")
        
        # 异步上传到 Storage
        async with measure_time("file_upload"):
            result = await upload_files_to_supabase_async(user_id, [file])
            storage_path = result.get(file.filename)
        
        if not storage_path:
            raise HTTPException(status_code=500, detail="File upload failed")

        logger.info(f"File uploaded to: {storage_path}")

        # 加密存储路径
        encrypted_path = encrypt_data("receipt_items_en", {"file_url": storage_path})["file_url"]

        # 更新数据库
        async with measure_time("database_update"):
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    update(ReceiptItemsEN)
                    .where(and_(
                        ReceiptItemsEN.user_id == user_id,
                        ReceiptItemsEN.ind == ind
                    ))
                    .values(file_url=encrypted_path)
                    .returning(ReceiptItemsEN)
                )
                await session.commit()
                update_result_data = result.scalars().all()

        if not update_result_data:
            raise HTTPException(status_code=404, detail="Record not found")

        # 生成签名 URL
        async with measure_time("generate_signed_url"):
            signed_url = await storage_client.create_signed_url(storage_path, expires_in=86400)
            if not signed_url:
                signed_url = storage_path

        logger.info("File URL updated successfully")
        return {
            "message": "File uploaded and URL updated successfully",
            "user_id": user_id,
            "ind": ind,
            "download_url": signed_url,
        }

    except Exception as e:
        logger.exception(f"update_file_url failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== 删除接口 ==========

@router.delete("/delete-receipt-items")
@timer("delete_receipt_items")
async def delete_receipt(request: DeleteReceiptRequest):
    """根据 ind 和 user_id 批量删除收据信息和 storage 文件"""
    logger.info(f"Deleting receipts for user_id: {request.user_id}, inds: {request.inds}")
    
    try:
        if not request.inds:
            return {"error": "ind list cannot be empty", "status": "error"}
        
        storage_client = get_async_storage_client()
        
        # 1. 查询要删除的记录
        async with measure_time("query_records"):
            async with AsyncSessionLocal() as session:
                receipt_query_result = await session.execute(
                    select(ReceiptItemsEN.id, ReceiptItemsEN.ind, ReceiptItemsEN.file_url)
                    .where(and_(
                        ReceiptItemsEN.user_id == request.user_id,
                        ReceiptItemsEN.ind.in_(request.inds)
                    ))
                )
                records = receipt_query_result.all()
        
        if not records:
            return {"message": "No matching records found", "deleted_count": 0, "status": "success"}
        
        found_ids = [record.id for record in records]
        found_inds = [record.ind for record in records]
        
        # 2. 批量删除数据库记录
        batch_ops = BatchOperations()
        
        async with measure_time("batch_delete"):
            receipt_deleted = await batch_ops.batch_delete(
                ReceiptItemsEN,
                found_inds,
                key_field='ind'
            )
            
            # 删除关联的 eml 记录
            eml_deleted = await batch_ops.batch_delete(
                SesEmlInfoEN,
                found_ids,
                key_field='id'
            )
        
        logger.info(f"Deleted {receipt_deleted} receipts, {eml_deleted} eml records")
        
        # 3. 并发删除 Storage 文件
        deleted_files = []
        failed_files = []
        
        async with measure_time("delete_storage_files"):
            delete_tasks = []
            for record in records:
                if record.file_url:
                    try:
                        decrypted_url = decrypt_data(
                            "receipt_items_en",
                            {"file_url": record.file_url}
                        ).get("file_url")
                        
                        if decrypted_url:
                            delete_tasks.append((decrypted_url, record.file_url))
                    except Exception as e:
                        logger.warning(f"Failed to decrypt URL: {e}")
                        failed_files.append(str(record.file_url))
            
            # 批量删除文件
            if delete_tasks:
                paths_to_delete = [path for path, _ in delete_tasks]
                result = await storage_client.delete(paths_to_delete)
                
                if result["success"]:
                    deleted_files.extend(paths_to_delete)
                    logger.info(f"Deleted {len(paths_to_delete)} files from storage")
                else:
                    failed_files.extend(paths_to_delete)
        
        # 检查未找到的记录
        not_found_inds = list(set(request.inds) - set(found_inds))
        
        response_data = {
            "message": "Records deleted successfully",
            "receipt_deleted_count": receipt_deleted,
            "eml_deleted_count": eml_deleted,
            "deleted_files": deleted_files,
            "failed_files": failed_files,
            "status": "success"
        }
        
        if not_found_inds:
            response_data["not_found_inds"] = not_found_inds
            response_data["message"] += f". {len(not_found_inds)} records not found"
        
        return response_data
        
    except Exception as e:
        logger.exception(f"Failed to delete receipts: {str(e)}")
        return {"error": f"Failed to delete receipts: {str(e)}", "status": "error"}

