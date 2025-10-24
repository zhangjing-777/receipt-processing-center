import logging
import asyncio
import calendar
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from sqlalchemy import select, update, delete, and_
from fastapi import UploadFile
from core.database import AsyncSessionLocal
from core.models import ReceiptItemsEN, SesEmlInfoEN
from core.encryption import encrypt_value, encrypt_data, decrypt_value
from core.upload_files import upload_files_to_supabase_async
from core.supabase_storage import get_async_storage_client
from core.batch_operations import BatchOperations
from core.performance_monitor import measure_time
from table_processor.utils import process_record

logger = logging.getLogger(__name__)


class ReceiptItemsENService:
    """receipt_items_en 业务逻辑层"""
    
    @staticmethod
    async def get_receipts(
        user_id: str,
        ind: Optional[int] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        year: Optional[int] = None,
        month: Optional[int] = None,
        limit: int = 0,
        offset: int = 0
    ) -> Dict:
        """
        根据条件查询收据信息
        
        Args:
            user_id: 用户 ID
            ind: 精确查询
            start_time: 开始时间
            end_time: 结束时间
            year: 年份
            month: 月份
            limit: 分页大小
            offset: 分页偏移
            
        Returns:
            查询结果
        """
        logger.info(
            f"Querying receipts for user_id: {user_id}, "
            f"ind: {ind}, year: {year}, month: {month}"
        )

        async with measure_time("database_query"):
            async with AsyncSessionLocal() as session:
                # 构建查询
                query = select(
                    ReceiptItemsEN.ind,
                    ReceiptItemsEN.id,
                    ReceiptItemsEN.user_id,
                    ReceiptItemsEN.category,
                    ReceiptItemsEN.buyer,
                    ReceiptItemsEN.seller,
                    ReceiptItemsEN.invoice_date,
                    ReceiptItemsEN.invoice_total,
                    ReceiptItemsEN.currency,
                    ReceiptItemsEN.file_url,
                    ReceiptItemsEN.address
                ).where(ReceiptItemsEN.user_id == user_id)

                # 精确查询
                if ind:
                    query = query.where(ReceiptItemsEN.ind == ind)
                    logger.info(f"Exact query for ind: {ind}")

                # 按年月查询
                elif year and month:               
                    start_dt = datetime(year, month, 1)
                    _, last_day = calendar.monthrange(year, month)
                    end_dt = datetime(year, month, last_day, 23, 59, 59, 999999)
                    query = query.where(
                        ReceiptItemsEN.invoice_date >= start_dt,
                        ReceiptItemsEN.invoice_date <= end_dt
                    )
                    logger.info(f"Monthly query: {year}-{month:02d}")

                # 时间范围查询
                elif start_time != "string" and end_time != "string":
                    start_dt = datetime.strptime(start_time, "%Y-%m-%d")
                    end_dt = datetime.strptime(end_time, "%Y-%m-%d")
                    end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
                    query = query.where(
                        ReceiptItemsEN.create_time >= start_dt,
                        ReceiptItemsEN.create_time <= end_dt
                    )
                    logger.info(f"Range query: {start_time} - {end_time}")

                # 分页查询
                elif offset and limit:               
                    query = query.order_by(
                        ReceiptItemsEN.invoice_date.desc()
                    ).offset(offset).limit(limit)

                # 默认查询上个月
                else:
                    today = datetime.utcnow()
                    first_of_this_month = datetime(today.year, today.month, 1)
                    last_month_end = first_of_this_month - timedelta(seconds=1)
                    last_month_start = datetime(last_month_end.year, last_month_end.month, 1)
                    query = query.where(
                        ReceiptItemsEN.invoice_date >= last_month_start,
                        ReceiptItemsEN.invoice_date <= last_month_end
                    )
                    logger.info(f"Default: last month ({last_month_start.date()} ~ {last_month_end.date()})")

                result = await session.execute(query)
                records = result.mappings().all()

        if not records:
            return {"message": "No records found", "data": [], "total": 0, "status": "success"}

        # 并行解密和签名
        async with measure_time("decrypt_and_sign"):
            decrypted_result = await asyncio.gather(
                *[process_record(r, "receipt_items_en", "file_url") for r in records]
            )

        logger.info(f"Query completed: {len(decrypted_result)} records")
        return decrypted_result
    
    @staticmethod
    async def update_receipt(
        ind: int,
        user_id: str,
        update_fields: Dict
    ) -> Dict:
        """
        更新收据信息
        
        Args:
            ind: 记录 ID
            user_id: 用户 ID
            update_fields: 要更新的字段
            
        Returns:
            更新结果
        """
        logger.info(f"Updating receipt: ind={ind}, user_id={user_id}")
        
        if not update_fields:
            return {"message": "No data to update", "status": "success"}
        
        logger.info(f"Fields to update: {list(update_fields.keys())}")
        
        # 加密敏感字段
        encrypted_update_data = encrypt_data("receipt_items_en", update_fields)
        
        # 执行更新
        async with measure_time("database_update"):
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    update(ReceiptItemsEN)
                    .where(and_(
                        ReceiptItemsEN.ind == ind,
                        ReceiptItemsEN.user_id == user_id
                    ))
                    .values(**encrypted_update_data)
                    .returning(ReceiptItemsEN)
                )
                await session.commit()
                updated_records = result.mappings().all()
        
        if not updated_records:
            return {"error": "No matching record found", "status": "error"}

        # 并行解密
        decrypted_result = await asyncio.gather(
            *[process_record(r, "receipt_items_en") for r in updated_records]
        )
        
        logger.info(f"Successfully updated {len(updated_records)} record(s)")
        return {
            "message": "Receipt updated successfully", 
            "updated_records": len(updated_records),
            "data": decrypted_result,
            "status": "success"
        }
    
    @staticmethod
    async def update_file_url(
        user_id: str,
        ind: int,
        file: UploadFile
    ) -> Dict:
        """
        上传新文件并更新 file_url
        
        Args:
            user_id: 用户 ID
            ind: 记录 ID
            file: 上传的文件
            
        Returns:
            更新结果
        """
        storage_client = get_async_storage_client()

        logger.info(f"Uploading new file for user_id={user_id}, ind={ind}")
        
        # 1. 查询旧记录
        async with AsyncSessionLocal() as session:
            record_result = await session.execute(
                select(ReceiptItemsEN.file_url)
                .where(and_(
                    ReceiptItemsEN.user_id == user_id,
                    ReceiptItemsEN.ind == ind
                ))
            )
            record_data = record_result.first()
        
        if not record_data:
            return {"error": "Record not found", "status": "error"}

        old_file_url = None
        try:
            old_file_url = decrypt_value(record_data[0])
        except Exception as e:
            logger.warning(f"Failed to decrypt old URL: {e}")
        
        # 2. 异步上传到 Storage
        async with measure_time("file_upload"):
            result = await upload_files_to_supabase_async(user_id, [file])
            storage_path = result.get(file.filename)
        
        if not storage_path:
            return {"error": "File upload failed", "status": "error"}

        logger.info(f"File uploaded to: {storage_path}")

        # 3. 加密存储路径并更新数据库
        encrypted_path = encrypt_value(storage_path)

        async with measure_time("database_update"):
            async with AsyncSessionLocal() as session:
                result = await session.execute(
                    update(ReceiptItemsEN)
                    .where(and_(
                        ReceiptItemsEN.user_id == user_id,
                        ReceiptItemsEN.ind == ind
                    ))
                    .values(file_url=encrypted_path)
                    .returning(ReceiptItemsEN)
                )
                await session.commit()
                update_result_data = result.scalars().all()

        if not update_result_data:
            return {"error": "Database update failed", "status": "error"}

        # 4. 异步删除旧文件
        if old_file_url:
            asyncio.create_task(storage_client.delete([old_file_url]))
            logger.info(f"Scheduled deletion of old file: {old_file_url}")

        # 5. 生成签名 URL
        async with measure_time("generate_signed_url"):
            signed_url = await storage_client.create_signed_url(storage_path, expires_in=86400)
            if not signed_url:
                signed_url = storage_path

        logger.info("File URL updated successfully")
        return {
            "message": "File uploaded and URL updated successfully",
            "user_id": user_id,
            "ind": ind,
            "download_url": signed_url,
        }
    
    @staticmethod
    async def delete_receipts(
        user_id: str,
        inds: List[int]
    ) -> Dict:
        """
        批量删除收据记录
        
        Args:
            user_id: 用户 ID
            inds: 要删除的记录 ID 列表
            
        Returns:
            删除结果
        """
        logger.info(f"Deleting receipts for user_id: {user_id}, inds: {inds}")
    
        if not inds:
            return {"error": "ind list cannot be empty", "status": "error"}
        
        storage_client = get_async_storage_client()
        
        # 1. 查询要删除的记录
        async with measure_time("query_records"):
            async with AsyncSessionLocal() as session:
                receipt_query_result = await session.execute(
                    select(ReceiptItemsEN.id, ReceiptItemsEN.ind, ReceiptItemsEN.file_url)
                    .where(and_(
                        ReceiptItemsEN.user_id == user_id,
                        ReceiptItemsEN.ind.in_(inds)
                    ))
                )
                records = receipt_query_result.all()
        
        if not records:
            return {"message": "No matching records found", "deleted_count": 0, "status": "success"}
        
        found_ids = [record.id for record in records]
        found_inds = [record.ind for record in records]
        
        # 2. 批量删除数据库记录
        batch_ops = BatchOperations()
        
        async with measure_time("batch_delete"):
            receipt_deleted = await batch_ops.batch_delete(
                ReceiptItemsEN,
                found_inds,
                key_field='ind'
            )
            
            # 删除关联的 eml 记录
            eml_deleted = await batch_ops.batch_delete(
                SesEmlInfoEN,
                found_ids,
                key_field='id'
            )
        
        logger.info(f"Deleted {receipt_deleted} receipts, {eml_deleted} eml records")
        
        # 3. 并发删除 Storage 文件
        deleted_files = []
        failed_files = []
        
        async with measure_time("delete_storage_files"):
            delete_tasks = []
            for record in records:
                if record.file_url:
                    try:
                        decrypted_url = decrypt_value(record.file_url)                        
                        if decrypted_url:
                            delete_tasks.append((decrypted_url, record.file_url))
                    except Exception as e:
                        logger.warning(f"Failed to decrypt URL: {e}")
                        failed_files.append(str(record.file_url))
            
            # 批量删除文件
            if delete_tasks:
                paths_to_delete = [path for path, _ in delete_tasks]
                result = await storage_client.delete(paths_to_delete)
                
                if result["success"]:
                    deleted_files.extend(paths_to_delete)
                    logger.info(f"Deleted {len(paths_to_delete)} files from storage")
                else:
                    failed_files.extend(paths_to_delete)
        
        # 检查未找到的记录
        not_found_inds = list(set(inds) - set(found_inds))
        
        response_data = {
            "message": "Records deleted successfully",
            "receipt_deleted_count": receipt_deleted,
            "eml_deleted_count": eml_deleted,
            "deleted_files": deleted_files,
            "failed_files": failed_files,
            "status": "success"
        }
        
        if not_found_inds:
            response_data["not_found_inds"] = not_found_inds
            response_data["message"] += f". {len(not_found_inds)} records not found"
        
        return response_data
        