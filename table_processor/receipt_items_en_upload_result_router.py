from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import logging
import asyncio
from sqlalchemy import select, delete
from core.database import AsyncSessionLocal
from core.models import ReceiptItemsENUploadResult
from core.batch_operations import BatchOperations
from core.performance_monitor import timer, measure_time
from table_processor.utils import process_record

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/receipt-items-en-upload-result", tags=["receipt_items_en_upload_result表操作"])

# ========== 请求模型 ==========

class GetUploadResultRequest(BaseModel):
    user_id: str
    id: Optional[int] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    limit: Optional[int] = 10
    offset: Optional[int] = 0

class DeleteUploadResultRequest(BaseModel):
    user_id: str
    ids: List[int]


# ========== 查询接口 ==========

@router.post("/get-upload-result")
@timer("get_upload_result")
async def get_upload_result(request: GetUploadResultRequest):
    """根据 user_id、id、时间范围或分页查询 receipt_items_en_upload_result 表"""
    try:
        async with measure_time("database_query"):
            async with AsyncSessionLocal() as session:
                query = select(ReceiptItemsENUploadResult).where(
                    ReceiptItemsENUploadResult.user_id == request.user_id
                )

                if request.id:
                    query = query.where(ReceiptItemsENUploadResult.id == request.id)
                elif request.start_time != "string" or request.end_time != "string":
                    if request.start_time != "string":
                        try:
                            start_dt = datetime.strptime(request.start_time, "%Y-%m-%d")
                            query = query.where(ReceiptItemsENUploadResult.created_at >= start_dt)
                        except ValueError:
                            return {"error": "Invalid start_time format", "status": "error"}
                    if request.end_time != "string":
                        try:
                            end_dt = datetime.strptime(request.end_time, "%Y-%m-%d")
                            end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                            query = query.where(ReceiptItemsENUploadResult.created_at <= end_dt)
                        except ValueError:
                            return {"error": "Invalid end_time format", "status": "error"}
                    query = query.order_by(ReceiptItemsENUploadResult.created_at.desc())
                else:
                    query = query.order_by(
                        ReceiptItemsENUploadResult.created_at.desc()
                    ).offset(request.offset).limit(request.limit)

                result = await session.execute(query)
                records = result.mappings().all()
            
        if not records:
            return {"message": "No records found", "data": [], "total": 0, "status": "success"}

        # 并行处理记录
        async with measure_time("process_records"):
            records = await asyncio.gather(
                *[process_record(r) for r in records]
            )
        
        return {
            "message": "Query success",
            "data": records,
            "total": len(records),
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to retrieve upload results: {str(e)}")
        return {"error": f"Failed to retrieve upload results: {str(e)}", "status": "error"}


# ========== 删除接口 ==========

@router.delete("/delete-upload-result")
@timer("delete_upload_result")
async def delete_upload_result(request: DeleteUploadResultRequest):
    """根据 user_id + 主键 id 列表删除 receipt_items_en_upload_result 表记录"""
    try:
        if not request.ids:
            return {"error": "ids list cannot be empty", "status": "error"}

        batch_ops = BatchOperations()
        
        async with measure_time("batch_delete"):
            deleted_count = await batch_ops.batch_delete(
                ReceiptItemsENUploadResult,
                request.ids,
                key_field='id'
            )

        return {
            "message": "Records deleted successfully",
            "deleted_count": deleted_count,
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to delete upload results: {str(e)}")
        return {"error": f"Failed to delete upload results: {str(e)}", "status": "error"}
