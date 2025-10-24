import logging
import asyncio
from typing import Dict, List, Optional
from datetime import datetime
from sqlalchemy import select, delete
from core.database import AsyncSessionLocal
from core.models import ReceiptItemsENUploadResult
from core.batch_operations import BatchOperations
from core.performance_monitor import measure_time
from table_processor.utils import process_record

logger = logging.getLogger(__name__)


class ReceiptItemsENUploadResultService:
    """receipt_items_en_upload_result 业务逻辑层"""
    
    @staticmethod
    async def get_upload_results(
        user_id: str,
        id: Optional[int] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 10,
        offset: int = 0
    ) -> Dict:
        """
        根据条件查询上传结果
        
        Args:
            user_id: 用户 ID
            id: 精确查询
            start_time: 开始时间
            end_time: 结束时间
            limit: 分页大小
            offset: 分页偏移
            
        Returns:
            查询结果
        """
        async with measure_time("database_query"):
            async with AsyncSessionLocal() as session:
                query = select(ReceiptItemsENUploadResult).where(
                    ReceiptItemsENUploadResult.user_id == user_id
                )

                if id:
                    query = query.where(ReceiptItemsENUploadResult.id == id)
                elif start_time != "string" or end_time != "string":
                    if start_time != "string":
                        try:
                            start_dt = datetime.strptime(start_time, "%Y-%m-%d")
                            query = query.where(ReceiptItemsENUploadResult.created_at >= start_dt)
                        except ValueError:
                            return {"error": "Invalid start_time format", "status": "error"}
                    if end_time != "string":
                        try:
                            end_dt = datetime.strptime(end_time, "%Y-%m-%d")
                            end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                            query = query.where(ReceiptItemsENUploadResult.created_at <= end_dt)
                        except ValueError:
                            return {"error": "Invalid end_time format", "status": "error"}
                    query = query.order_by(ReceiptItemsENUploadResult.created_at.desc())
                else:
                    query = query.order_by(
                        ReceiptItemsENUploadResult.created_at.desc()
                    ).offset(offset).limit(limit)

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
    
    @staticmethod
    async def delete_upload_results(
        user_id: str,
        ids: List[int]
    ) -> Dict:
        """
        批量删除上传结果记录
        
        Args:
            user_id: 用户 ID
            ids: 要删除的记录 ID 列表
            
        Returns:
            删除结果
        """
        if not ids:
            return {"error": "ids list cannot be empty", "status": "error"}

        batch_ops = BatchOperations()
        
        async with measure_time("batch_delete"):
            deleted_count = await batch_ops.batch_delete(
                ReceiptItemsENUploadResult,
                ids,
                key_field='id'
            )

        return {
            "message": "Records deleted successfully",
            "deleted_count": deleted_count,
            "status": "success"
        }
