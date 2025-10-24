import logging
import asyncio
from typing import Dict, List, Optional
from datetime import datetime
from sqlalchemy import select, update, delete, and_
from core.database import AsyncSessionLocal
from core.models import SesEmlInfoEN
from core.encryption import encrypt_data
from core.batch_operations import BatchOperations
from core.performance_monitor import measure_time
from table_processor.utils import process_record

logger = logging.getLogger(__name__)


class SesEmlInfoENService:
    """ses_eml_info_en 业务逻辑层"""
    
    @staticmethod
    async def get_eml_infos(
        user_id: str,
        ind: Optional[int] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 10,
        offset: int = 0
    ) -> Dict:
        """
        根据条件查询 eml info 信息
        
        Args:
            user_id: 用户 ID
            ind: 精确查询
            start_time: 开始时间
            end_time: 结束时间
            limit: 分页大小
            offset: 分页偏移
            
        Returns:
            查询结果
        """
        async with measure_time("database_query"):
            async with AsyncSessionLocal() as session:
                query = select(SesEmlInfoEN).where(SesEmlInfoEN.user_id == user_id)

                if ind:
                    query = query.where(SesEmlInfoEN.ind == ind)
                elif start_time != "string" or end_time != "string":
                    if start_time != "string":
                        try:
                            start_dt = datetime.strptime(start_time, "%Y-%m-%d")
                            query = query.where(SesEmlInfoEN.create_time >= start_dt)
                        except ValueError:
                            return {"error": "Invalid start_time format", "status": "error"}
                    if end_time != "string":
                        try:
                            end_dt = datetime.strptime(end_time, "%Y-%m-%d")
                            end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                            query = query.where(SesEmlInfoEN.create_time <= end_dt)
                        except ValueError:
                            return {"error": "Invalid end_time format", "status": "error"}
                    query = query.order_by(SesEmlInfoEN.create_time.desc())
                else:
                    query = query.order_by(
                        SesEmlInfoEN.create_time.desc()
                    ).offset(offset).limit(limit)

                result = await session.execute(query)
                records = result.mappings().all()
        
        if not records:
            return {"message": "No records found", "data": [], "total": 0, "status": "success"}

        # 并行解密
        async with measure_time("decrypt_records"):
            decrypted_result = await asyncio.gather(
                *[process_record(r, "ses_eml_info_en") for r in records]
            )
        
        return {
            "message": "Query success",
            "data": decrypted_result,
            "total": len(decrypted_result),
            "status": "success"
        }
    
    @staticmethod
    async def update_eml_info(
        ind: int,
        user_id: str,
        update_fields: Dict
    ) -> Dict:
        """
        更新 eml info 信息
        
        Args:
            ind: 记录 ID
            user_id: 用户 ID
            update_fields: 要更新的字段
            
        Returns:
            更新结果
        """
        if not update_fields:
            return {"message": "No data to update", "status": "success"}

        encrypted_update_data = encrypt_data("ses_eml_info_en", update_fields)

        async with measure_time("database_update"):
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    update(SesEmlInfoEN)
                    .where(and_(
                        SesEmlInfoEN.ind == ind,
                        SesEmlInfoEN.user_id == user_id
                    ))
                    .values(**encrypted_update_data)
                    .returning(SesEmlInfoEN)
                )
                await session.commit()
                updated_records = result.mappings().all()

        if not updated_records:
            return {"error": "No matching record found", "status": "error"}

        # 并行解密
        decrypted_result = await asyncio.gather(
            *[process_record(r, "ses_eml_info_en") for r in updated_records]
        )
        
        return {
            "message": "Eml info updated successfully",
            "updated_records": len(decrypted_result),
            "data": decrypted_result,
            "status": "success"
        }
    
    @staticmethod
    async def delete_eml_infos(
        user_id: str,
        inds: List[int]
    ) -> Dict:
        """
        批量删除 eml info 记录
        
        Args:
            user_id: 用户 ID
            inds: 要删除的记录 ID 列表
            
        Returns:
            删除结果
        """
        if not inds:
            return {"error": "inds list cannot be empty", "status": "error"}

        batch_ops = BatchOperations()
        
        async with measure_time("batch_delete"):
            deleted_count = await batch_ops.batch_delete(
                SesEmlInfoEN,
                inds,
                key_field='ind'
            )

        return {
            "message": "Records deleted successfully",
            "deleted_count": deleted_count,
            "status": "success"
        }
