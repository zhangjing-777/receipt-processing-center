from supabase import create_client, Client
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from dotenv import load_dotenv
import logging
import os

load_dotenv()

url: str = os.getenv("SUPABASE_URL") or ""
key: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
supabase: Client = create_client(url, key)

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


# ----------- 查询接口 -----------
@router.post("/get-upload-result")
async def get_upload_result(request: GetUploadResultRequest):
    """根据 user_id、id、时间范围或分页查询 receipt_items_en_upload_result 表"""
    try:
        query = supabase.table("receipt_items_en_upload_result").select("*").eq("user_id", request.user_id)

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

        return {"message": "Query success", "data": result.data, "total": len(result.data), "status": "success"}

    except Exception as e:
        logger.exception(f"Failed to retrieve upload results: {str(e)}")
        return {"error": f"Failed to retrieve upload results: {str(e)}", "status": "error"}


# ----------- 删除接口 -----------
@router.delete("/delete-upload-result")
async def delete_upload_result(request: DeleteUploadResultRequest):
    """根据 user_id + 主键 id 列表删除 receipt_items_en_upload_result 表记录"""
    try:
        if not request.ids:
            return {"error": "ids list cannot be empty", "status": "error"}

        result = (
            supabase.table("receipt_items_en_upload_result")
            .delete()
            .eq("user_id", request.user_id)
            .in_("id", request.ids)
            .execute()
        )

        deleted_count = len(result.data) if result.data else 0
        return {
            "message": "Records deleted successfully",
            "deleted_count": deleted_count,
            "status": "success"
        }

    except Exception as e:
        logger.exception(f"Failed to delete upload results: {str(e)}")
        return {"error": f"Failed to delete upload results: {str(e)}", "status": "error"}
