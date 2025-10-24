import logging
from typing import Optional, List
from pydantic import BaseModel, Field
from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from core.performance_monitor import timer
from table_processor.receipt_items_en.service import ReceiptItemsENService

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
    try:
        result = await ReceiptItemsENService.get_receipts(
            user_id=request.user_id,
            ind=request.ind,
            start_time=request.start_time,
            end_time=request.end_time,
            year=request.year,
            month=request.month,
            limit=request.limit,
            offset=request.offset
        )
        return result
    except Exception as e:
        logger.exception(f"Failed to retrieve receipts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== 更新接口 ==========

@router.post("/update-receipt-items")
@timer("update_receipt_items")
async def update_receipt(request: UpdateReceiptRequest):
    """根据 ind 和 user_id 更新收据信息"""
    try:
        # 构建更新数据
        update_data = {}
        for field, value in request.dict(exclude={'ind', 'user_id'}).items():
            if value != "string" and value:
                update_data[field] = value
        
        result = await ReceiptItemsENService.update_receipt(
            ind=request.ind,
            user_id=request.user_id,
            update_fields=update_data
        )
        return result
    except Exception as e:
        logger.exception(f"Failed to update receipt: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== 文件更新接口 ==========

@router.post("/update-file-url")
@timer("update_file_url")
async def update_file_url(
    user_id: str = Form(...),
    ind: int = Form(...),
    file: UploadFile = File(...)
):
    """上传新文件到 Storage，并更新数据库里的加密 file_url"""
    try:
        result = await ReceiptItemsENService.update_file_url(
            user_id=user_id,
            ind=ind,
            file=file
        )
        
        if result.get("status") == "error":
            raise HTTPException(status_code=500, detail=result.get("error"))
        
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"update_file_url failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== 删除接口 ==========

@router.delete("/delete-receipt-items")
@timer("delete_receipt_items")
async def delete_receipt(request: DeleteReceiptRequest):
    """根据 ind 和 user_id 批量删除收据信息和 storage 文件"""
    try:
        result = await ReceiptItemsENService.delete_receipts(
            user_id=request.user_id,
            inds=request.inds
        )
        return result
    except Exception as e:
        logger.exception(f"Failed to delete receipts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))