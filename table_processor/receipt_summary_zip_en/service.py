import logging
import asyncio
from typing import Dict, List, Optional
from datetime import datetime
from sqlalchemy import select, update, delete, and_
from fastapi import UploadFile
from core.database import AsyncSessionLocal
from core.models import ReceiptSummaryZipEN
from core.encryption import encrypt_data, decrypt_value, encrypt_value
from core.upload_files import upload_files_to_supabase_async
from core.supabase_storage import get_async_storage_client
from core.batch_operations import BatchOperations
from core.performance_monitor import measure_time
from table_processor.utils import process_record

logger = logging.getLogger(__name__)


class ReceiptSummaryZipENService:
    """receipt_summary_zip_en 业务逻辑层"""
    
    @staticmethod
    async def get_summary_zips(
        user_id: str,
        id: Optional[int] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 10,
        offset: int = 0
    ) -> Dict:
        """
        根据条件查询 summary zip 信息
        
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
                query = select(ReceiptSummaryZipEN).where(
                    ReceiptSummaryZipEN.user_id == user_id
                )

                if id:
                    query = query.where(ReceiptSummaryZipEN.id == id)
                elif start_time != "string" and end_time != "string":
                    start_dt = datetime.strptime(start_time, "%Y-%m-%d").date()
                    end_dt = datetime.strptime(end_time, "%Y-%m-%d").date()
                    query = query.where(
                        ReceiptSummaryZipEN.created_at >= start_dt,
                        ReceiptSummaryZipEN.created_at <= end_dt
                    ).order_by(ReceiptSummaryZipEN.created_at.desc())
                else:
                    query = query.order_by(
                        ReceiptSummaryZipEN.created_at.desc()
                    ).offset(offset).limit(limit)

                result = await session.execute(query)
                records = result.mappings().all()

        if not records:
            return {"message": "No records found", "data": [], "total": 0, "status": "success"}

        # 并行解密和签名
        async with measure_time("decrypt_and_sign"):
            decrypted_result = await asyncio.gather(
                *[process_record(r, "receipt_summary_zip_en", "download_url") for r in records]
            )

        return decrypted_result
    
    @staticmethod
    async def update_summary_zip(
        id: int,
        user_id: str,
        update_fields: Dict
    ) -> Dict:
        """
        更新 summary zip 信息
        
        Args:
            id: 记录 ID
            user_id: 用户 ID
            update_fields: 要更新的字段
            
        Returns:
            更新结果
        """
        if not update_fields:
            return {"message": "No data to update", "status": "success"}

        encrypted_update_data = encrypt_data("receipt_summary_zip_en", update_fields)

        async with measure_time("database_update"):
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    update(ReceiptSummaryZipEN)
                    .where(and_(
                        ReceiptSummaryZipEN.id == id,
                        ReceiptSummaryZipEN.user_id == user_id
                    ))
                    .values(**encrypted_update_data)
                    .returning(ReceiptSummaryZipEN)
                )
                await session.commit()
                updated_records = result.mappings().all()

        if not updated_records:
            return {"error": "No matching record found", "status": "error"}

        # 并行解密
        decrypted_result = await asyncio.gather(
            *[process_record(r, "receipt_summary_zip_en", "download_url") for r in updated_records]
        )

        return {
            "message": "Summary zip updated successfully",
            "updated_records": len(decrypted_result),
            "data": decrypted_result,
            "status": "success"
        }
    
    @staticmethod
    async def update_download_url(
        user_id: str,
        id: int,
        file: UploadFile
    ) -> Dict:
        """
        上传新文件并更新 download_url
        
        Args:
            user_id: 用户 ID
            id: 记录 ID
            file: 上传的文件
            
        Returns:
            更新结果
        """
        storage_client = get_async_storage_client()
        
        # 1. 查询旧记录
        async with AsyncSessionLocal() as session:
            record_result = await session.execute(
                select(ReceiptSummaryZipEN.download_url)
                .where(and_(
                    ReceiptSummaryZipEN.user_id == user_id,
                    ReceiptSummaryZipEN.id == id
                ))
            )
            record_data = record_result.first()
        
        if not record_data:
            return {"error": "Record not found", "status": "error"}

        old_download_url = None
        try:
            old_download_url = decrypt_value(record_data[0])
        except Exception as e:
            logger.warning(f"Failed to decrypt old URL: {e}")

        # 2. 异步上传新文件
        async with measure_time("file_upload"):
            result = await upload_files_to_supabase_async(user_id, [file], "summary")
            storage_path = result.get(file.filename)
        
        if not storage_path:
            return {"error": "File upload failed", "status": "error"}

        # 3. 加密并更新数据库
        encrypted_path = encrypt_value(storage_path)

        async with measure_time("database_update"):
            async with AsyncSessionLocal() as session:
                update_result = await session.execute(
                    update(ReceiptSummaryZipEN)
                    .where(and_(
                        ReceiptSummaryZipEN.user_id == user_id,
                        ReceiptSummaryZipEN.id == id
                    ))
                    .values(download_url=encrypted_path)
                    .returning(ReceiptSummaryZipEN)
                )
                await session.commit()
                update_result_data = update_result.scalars().all()
        
        if not update_result_data:
            return {"error": "Database update failed", "status": "error"}

        # 4. 异步删除旧文件
        if old_download_url:
            asyncio.create_task(storage_client.delete([old_download_url]))

        # 5. 生成签名 URL
        async with measure_time("generate_signed_url"):
            signed_url = await storage_client.create_signed_url(storage_path, expires_in=86400)
            if not signed_url:
                signed_url = storage_path

        return {
            "message": "File uploaded and download_url updated successfully",
            "user_id": user_id,
            "id": id,
            "download_url": signed_url,
            "status": "success"
        }
    
    @staticmethod
    async def delete_summary_zips(
        user_id: str,
        ids: List[int]
    ) -> Dict:
        """
        批量删除 summary zip 记录
        
        Args:
            user_id: 用户 ID
            ids: 要删除的记录 ID 列表
            
        Returns:
            删除结果
        """
        if not ids:
            return {"error": "ids list cannot be empty", "status": "error"}

        storage_client = get_async_storage_client()

        # 1. 查询要删除的记录
        async with measure_time("query_records"):
            async with AsyncSessionLocal() as session:
                records_result = await session.execute(
                    select(ReceiptSummaryZipEN.id, ReceiptSummaryZipEN.download_url)
                    .where(and_(
                        ReceiptSummaryZipEN.user_id == user_id,
                        ReceiptSummaryZipEN.id.in_(ids)
                    ))
                )
                records = records_result.all()

        if not records:
            return {"message": "No matching records found", "deleted_count": 0, "status": "success"}

        # 2. 解密 URLs 并准备删除
        paths_to_delete = []
        failed_paths = []
        
        for record in records:
            try:
                decrypted = decrypt_value(record.download_url)
                if decrypted:
                    paths_to_delete.append(decrypted["download_url"])
            except Exception as e:
                logger.warning(f"Failed to decrypt URL: {e}")
                failed_paths.append(str(record.download_url))

        # 3. 批量删除数据库记录
        batch_ops = BatchOperations()
        
        async with measure_time("batch_delete"):
            deleted_count = await batch_ops.batch_delete(
                ReceiptSummaryZipEN,
                ids,
                key_field='id'
            )

        # 4. 并发删除 Storage 文件
        deleted_files = []
        if paths_to_delete:
            async with measure_time("delete_storage_files"):
                result = await storage_client.delete(paths_to_delete)
                if result["success"]:
                    deleted_files = paths_to_delete
                    logger.info(f"Deleted {len(paths_to_delete)} files from storage")
                else:
                    failed_paths.extend(paths_to_delete)

        return {
            "message": "Records deleted successfully",
            "deleted_count": deleted_count,
            "deleted_files": deleted_files,
            "failed_files": failed_paths,
            "status": "success"
        }
