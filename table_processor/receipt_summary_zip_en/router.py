import logging
from typing import Optional, List
from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from core.performance_monitor import timer
from table_processor.receipt_summary_zip_en.service import ReceiptSummaryZipENService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/receipt-summary-zip-en", tags=["receipt_summary_zip_en表操作"])

# ========== 请求模型 ==========

class GetSummaryZipRequest(BaseModel):
    user_id: str
    id: Optional[int] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
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


# ========== 查询接口 ==========

@router.post("/get-summary-zip")
@timer("get_summary_zip")
async def get_summary_zip(request: GetSummaryZipRequest):
    """根据 user_id 和条件查询 summary zip 信息"""
    try:
        result = await ReceiptSummaryZipENService.get_summary_zips(
            user_id=request.user_id,
            id=request.id,
            start_time=request.start_time,
            end_time=request.end_time,
            limit=request.limit,
            offset=request.offset
        )
        return result
    except Exception as e:
        logger.exception(f"Failed to retrieve summary zip: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== 更新接口 ==========

@router.post("/update-summary-zip")
@timer("update_summary_zip")
async def update_summary_zip(request: UpdateSummaryZipRequest):
    """根据 id 和 user_id 更新 summary zip 信息"""
    try:
        # 构建更新数据
        update_data = {}
        for field, value in request.dict(exclude={'id', 'user_id'}).items():
            if value and value != "string":
                update_data[field] = value
        
        result = await ReceiptSummaryZipENService.update_summary_zip(
            id=request.id,
            user_id=request.user_id,
            update_fields=update_data
        )
        return result
    except Exception as e:
        logger.exception(f"Failed to update summary zip: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== 文件更新接口 ==========

@router.post("/update-download-url")
@timer("update_download_url")
async def update_summary_zip_download_url(
    user_id: str = Form(...),
    id: int = Form(...),
    file: UploadFile = File(...)
):
    """上传新文件到 Storage，并更新数据库里的加密 download_url，同时删除旧文件"""
    try:
        result = await ReceiptSummaryZipENService.update_download_url(
            user_id=user_id,
            id=id,
            file=file
        )
        
        if result.get("status") == "error":
            raise HTTPException(status_code=500, detail=result.get("error"))
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"update_download_url failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== 删除接口 ==========

@router.delete("/delete-summary-zip")
@timer("delete_summary_zip")
async def delete_summary_zip(request: DeleteSummaryZipRequest):
    """根据 id 和 user_id 删除 receipt_summary_zip_en 表内容 + 删除 Storage 文件"""
    try:
        result = await ReceiptSummaryZipENService.delete_summary_zips(
            user_id=request.user_id,
            ids=request.ids
        )
        return result
    except Exception as e:
        logger.exception(f"Failed to delete summary zip: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))