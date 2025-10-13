from fastapi import APIRouter
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
import logging
import asyncio

from sqlalchemy import select, update, delete, and_
from core.database import AsyncSessionLocal
from core.models import SesEmlInfoEN
from core.encryption import encrypt_data, decrypt_data


logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ses-eml-info-en", tags=["ses_eml_info_en表操作"])

# ----------- 请求体定义 -----------
class GetEmlInfoRequest(BaseModel):
    user_id: str  # 必填
    ind: Optional[int] = None  # 精确查询
    start_time: Optional[str] = None  # YYYY-MM-DD
    end_time: Optional[str] = None    # YYYY-MM-DD
    limit: Optional[int] = 10
    offset: Optional[int] = 0

class UpdateEmlInfoRequest(BaseModel):
    ind: int = Field(..., description="记录唯一标识")
    user_id: str = Field(..., description="用户ID")

    from_: Optional[str] = Field(default=None, alias="from")
    to: Optional[str] = None
    s3_eml_url: Optional[str] = None
    buyer: Optional[str] = None
    seller: Optional[str] = None
    invoice_date: Optional[str] = None  # YYYY-MM-DD
    create_time: Optional[str] = None   # 可选

class DeleteEmlInfoRequest(BaseModel):
    user_id: str
    inds: List[int]


# 解密返回数据中的敏感字段
async def process_record(record_dict):
    record_dict = dict(record_dict)
    return decrypt_data("ses_eml_info_en", record_dict)

# ----------- 查询接口 -----------
@router.post("/get-eml-info")
async def get_eml_info(request: GetEmlInfoRequest):
    """根据 user_id、ind、时间范围或分页查询 ses_eml_info_en 表"""
    try:
        async with AsyncSessionLocal() as session:
            query = select(SesEmlInfoEN).where(SesEmlInfoEN.user_id == request.user_id)

            if request.ind:
                query = query.where(SesEmlInfoEN.ind == request.ind)
            elif request.start_time != "string" or request.end_time != "string":
                if request.start_time != "string":
                    try:
                        start_dt = datetime.strptime(request.start_time, "%Y-%m-%d")
                        query = query.where(SesEmlInfoEN.create_time >= start_dt)
                    except ValueError:
                        return {"error": "Invalid start_time format, expected YYYY-MM-DD", "status": "error"}
                if request.end_time != "string":
                    try:
                        end_dt = datetime.strptime(request.end_time, "%Y-%m-%d")
                        end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                        query = query.where(SesEmlInfoEN.create_time <= end_dt)
                    except ValueError:
                        return {"error": "Invalid end_time format, expected YYYY-MM-DD", "status": "error"}
                query = query.order_by(SesEmlInfoEN.create_time.desc())
            else:
                query = query.order_by(SesEmlInfoEN.create_time.desc()).offset(request.offset).limit(request.limit)

            result = await session.execute(query)
            records = result.mappings().all()
        
        if not records:
            return {"message": "No records found", "data": [], "total": 0, "status": "success"}

        # 并行执行解密 
        decrypted_result = await asyncio.gather(*[process_record(r) for r in records])
        
        return {"message": "Query success", "data": decrypted_result, "total": len(decrypted_result), "status": "success"}

    except Exception as e:
        logger.exception(f"Failed to retrieve eml info: {str(e)}")
        return {"error": f"Failed to retrieve eml info: {str(e)}", "status": "error"}


# ----------- 更新接口 -----------
@router.post("/update-eml-info")
async def update_eml_info(request: UpdateEmlInfoRequest):
    """根据 ind 和 user_id 更新 ses_eml_info_en 表"""
    try:
        update_data = {}
        for field, value in request.dict(exclude={'ind', 'user_id'}, by_alias=True).items():
            if value and value != "string":
                update_data[field] = value

        if not update_data:
            return {"message": "No data to update", "status": "success"}

        encrypted_update_data = encrypt_data("ses_eml_info_en", update_data)

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                update(SesEmlInfoEN)
                .where(and_(SesEmlInfoEN.ind == request.ind, SesEmlInfoEN.user_id == request.user_id))
                .values(**encrypted_update_data)
                .returning(SesEmlInfoEN)
            )
            await session.commit()
            updated_records = result.mappings().all()

        if not updated_records:
            return {"error": "No matching record found or no permission to update", "status": "error"}

        # 并行执行解密 
        decrypted_result = await asyncio.gather(*[process_record(r) for r in updated_records])
        
        return {
            "message": "Eml info updated successfully",
            "updated_records": len(decrypted_result),
            "data": decrypted_result,
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to update eml info: {str(e)}")
        return {"error": f"Failed to update eml info: {str(e)}", "status": "error"}


# ----------- 删除接口 -----------
@router.delete("/delete-eml-info")
async def delete_eml_info(request: DeleteEmlInfoRequest):
    """根据 ind 和 user_id 删除 ses_eml_info_en 表记录"""
    try:
        if not request.inds:
            return {"error": "inds list cannot be empty", "status": "error"}

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                delete(SesEmlInfoEN)
                .where(and_(SesEmlInfoEN.user_id == request.user_id, SesEmlInfoEN.ind.in_(request.inds)))
                .returning(SesEmlInfoEN)
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
        logger.exception(f"Failed to delete eml info: {str(e)}")
        return {"error": f"Failed to delete eml info: {str(e)}", "status": "error"}