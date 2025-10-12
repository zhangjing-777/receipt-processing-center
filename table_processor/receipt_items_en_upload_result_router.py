from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import logging

from sqlalchemy import select, delete
from core.config import settings
from core.database import AsyncSessionLocal
from core.models import ReceiptItemsENUploadResult


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/receipt-items-en-upload-result", tags=["receipt_items_en_upload_result表操作"])


# ----------- 请求体 -----------
class GetUploadResultRequest(BaseModel):
    user_id: str  # 必填
    id: Optional[int] = None  # 精确查询
    start_time: Optional[str] = None  # YYYY-MM-DD
    end_time: Optional[str] = None    # YYYY-MM-DD
    limit: Optional[int] = 10
    offset: Optional[int] = 0


class DeleteUploadResultRequest(BaseModel):
    user_id: str
    ids: List[int]  # 主键 id 列表


# 查询接口 
@router.post("/get-upload-result")
async def get_upload_result(request: GetUploadResultRequest):
    """根据 user_id、id、时间范围或分页查询 receipt_items_en_upload_result 表"""
    try:
        async with AsyncSessionLocal() as session:
            query = select(ReceiptItemsENUploadResult).where(ReceiptItemsENUploadResult.user_id == request.user_id)

            if request.id:
                query = query.where(ReceiptItemsENUploadResult.id == request.id)
            elif request.start_time != "string" or request.end_time != "string":
                if request.start_time != "string":
                    try:
                        start_dt = datetime.strptime(request.start_time, "%Y-%m-%d")
                        query = query.where(ReceiptItemsENUploadResult.created_at >= start_dt)
                    except ValueError:
                        return {"error": "Invalid start_time format, expected YYYY-MM-DD", "status": "error"}
                if request.end_time != "string":
                    try:
                        end_dt = datetime.strptime(request.end_time, "%Y-%m-%d")
                        end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                        query = query.where(ReceiptItemsENUploadResult.created_at <= end_dt)
                    except ValueError:
                        return {"error": "Invalid end_time format, expected YYYY-MM-DD", "status": "error"}
                query = query.order_by(ReceiptItemsENUploadResult.created_at.desc())
            else:
                query = query.order_by(ReceiptItemsENUploadResult.created_at.desc()).offset(request.offset).limit(request.limit)

            result = await session.execute(query)
            records = result.scalars().all()
            
            if not records:
                return {"message": "No records found", "data": [], "total": 0, "status": "success"}

            result_data = []
            for record in records:
                record_dict = {c.name: getattr(record, c.name) for c in record.__table__.columns}
                result_data.append(record_dict)

            return {"message": "Query success", "data": result_data, "total": len(result_data), "status": "success"}

    except Exception as e:
        logger.exception(f"Failed to retrieve upload results: {str(e)}")
        return {"error": f"Failed to retrieve upload results: {str(e)}", "status": "error"}


# 删除接口 
@router.delete("/delete-upload-result")
async def delete_upload_result(request: DeleteUploadResultRequest):
    """根据 user_id + 主键 id 列表删除 receipt_items_en_upload_result 表记录"""
    try:
        if not request.ids:
            return {"error": "ids list cannot be empty", "status": "error"}

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                delete(ReceiptItemsENUploadResult)
                .where(ReceiptItemsENUploadResult.user_id == request.user_id)
                .where(ReceiptItemsENUploadResult.id.in_(request.ids))
                .returning(ReceiptItemsENUploadResult)
            )
            await session.commit()
            deleted_data = result.scalars().all()
            deleted_count = len(deleted_data)

        return {
            "message": "Records deleted successfully",
            "deleted_count": deleted_count,
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to delete upload results: {str(e)}")
        return {"error": f"Failed to delete upload results: {str(e)}", "status": "error"}