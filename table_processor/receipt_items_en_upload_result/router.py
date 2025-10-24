import logging
from typing import Optional, List
from pydantic import BaseModel
from fastapi import APIRouter, HTTPException
from core.performance_monitor import timer
from table_processor.receipt_items_en_upload_result.service import ReceiptItemsENUploadResultService

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
        result = await ReceiptItemsENUploadResultService.get_upload_results(
            user_id=request.user_id,
            id=request.id,
            start_time=request.start_time,
            end_time=request.end_time,
            limit=request.limit,
            offset=request.offset
        )
        return result
    except Exception as e:
        logger.exception(f"Failed to retrieve upload results: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== 删除接口 ==========

@router.delete("/delete-upload-result")
@timer("delete_upload_result")
async def delete_upload_result(request: DeleteUploadResultRequest):
    """根据 user_id + 主键 id 列表删除 receipt_items_en_upload_result 表记录"""
    try:
        result = await ReceiptItemsENUploadResultService.delete_upload_results(
            user_id=request.user_id,
            ids=request.ids
        )
        return result
    except Exception as e:
        logger.exception(f"Failed to delete upload results: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))