from supabase import create_client, Client
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from dotenv import load_dotenv
import logging
import os

from core.encryption import encrypt_data, decrypt_data


load_dotenv()

url: str = os.getenv("SUPABASE_URL") or ""
key: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
supabase: Client = create_client(url, key)

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

# ----------- 查询接口 -----------
@router.post("/get-eml-info")
async def get_eml_info(request: GetEmlInfoRequest):
    """根据 user_id、ind、时间范围或分页查询 ses_eml_info_en 表"""
    try:
        query = supabase.table("ses_eml_info_en").select("*").eq("user_id", request.user_id)

        if request.ind:
            query = query.eq("ind", request.ind)
        elif request.start_time != "string" or request.end_time != "string":
            if request.start_time != "string":
                try:
                    start_dt = datetime.strptime(request.start_time, "%Y-%m-%d")
                    query = query.gte("create_time", start_dt.isoformat())
                except ValueError:
                    return {"error": "Invalid start_time format, expected YYYY-MM-DD", "status": "error"}
            if request.end_time != "string":
                try:
                    end_dt = datetime.strptime(request.end_time, "%Y-%m-%d")
                    end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                    query = query.lte("create_time", end_dt.isoformat())
                except ValueError:
                    return {"error": "Invalid end_time format, expected YYYY-MM-DD", "status": "error"}
            query = query.order("create_time", desc=True)
        else:
            query = query.order("create_time", desc=True).range(
                request.offset, request.offset + request.limit - 1
            )

        result = query.execute()
        if not result.data:
            return {"message": "No records found", "data": [], "total": 0, "status": "success"}

        decrypted_result = [decrypt_data("ses_eml_info_en", record) for record in result.data]
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

        result = (
            supabase.table("ses_eml_info_en")
            .update(encrypted_update_data)
            .eq("ind", request.ind)
            .eq("user_id", request.user_id)
            .execute()
        )

        if not result.data:
            return {"error": "No matching record found or no permission to update", "status": "error"}

        decrypted_result = [decrypt_data("ses_eml_info_en", record) for record in result.data]
        return {
            "message": "Eml info updated successfully",
            "updated_records": len(result.data),
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

        result = (
            supabase.table("ses_eml_info_en")
            .delete()
            .eq("user_id", request.user_id)
            .in_("ind", request.inds)
            .execute()
        )

        deleted_count = len(result.data) if result.data else 0
        return {
            "message": "Records deleted successfully",
            "deleted_count": deleted_count,
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to delete eml info: {str(e)}")
        return {"error": f"Failed to delete eml info: {str(e)}", "status": "error"}
