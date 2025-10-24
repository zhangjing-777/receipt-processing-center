import logging
from typing import Optional, List
from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException
from core.performance_monitor import timer
from table_processor.ses_eml_info_en.service import SesEmlInfoENService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ses-eml-info-en", tags=["ses_eml_info_en表操作"])

# ========== 请求模型 ==========

class GetEmlInfoRequest(BaseModel):
    user_id: str
    ind: Optional[int] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
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
    invoice_date: Optional[str] = None
    create_time: Optional[str] = None

class DeleteEmlInfoRequest(BaseModel):
    user_id: str
    inds: List[int]


# ========== 查询接口 ==========

@router.post("/get-eml-info")
@timer("get_eml_info")
async def get_eml_info(request: GetEmlInfoRequest):
    """根据 user_id、ind、时间范围或分页查询 ses_eml_info_en 表"""
    try:
        result = await SesEmlInfoENService.get_eml_infos(
            user_id=request.user_id,
            ind=request.ind,
            start_time=request.start_time,
            end_time=request.end_time,
            limit=request.limit,
            offset=request.offset
        )
        return result
    except Exception as e:
        logger.exception(f"Failed to retrieve eml info: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== 更新接口 ==========

@router.post("/update-eml-info")
@timer("update_eml_info")
async def update_eml_info(request: UpdateEmlInfoRequest):
    """根据 ind 和 user_id 更新 ses_eml_info_en 表"""
    try:
        # 构建更新数据
        update_data = {}
        for field, value in request.dict(exclude={'ind', 'user_id'}, by_alias=True).items():
            if value and value != "string":
                update_data[field] = value
        
        result = await SesEmlInfoENService.update_eml_info(
            ind=request.ind,
            user_id=request.user_id,
            update_fields=update_data
        )
        return result
    except Exception as e:
        logger.exception(f"Failed to update eml info: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ========== 删除接口 ==========

@router.delete("/delete-eml-info")
@timer("delete_eml_info")
async def delete_eml_info(request: DeleteEmlInfoRequest):
    """根据 ind 和 user_id 删除 ses_eml_info_en 表记录"""
    try:
        result = await SesEmlInfoENService.delete_eml_infos(
            user_id=request.user_id,
            inds=request.inds
        )
        return result
    except Exception as e:
        logger.exception(f"Failed to delete eml info: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))