import logging
from typing import List, Dict, Any
from sqlalchemy import insert, update, delete
from sqlalchemy.dialects.postgresql import insert as pg_insert
from core.database import AsyncSessionLocal
from core.config import settings

logger = logging.getLogger(__name__)


class BatchOperations:
    """批量数据库操作工具"""
    
    @staticmethod
    async def batch_insert(
        model,
        records: List[Dict[str, Any]],
        batch_size: int = None
    ) -> int:
        """
        批量插入记录 (自动分批)
        
        Args:
            model: SQLAlchemy 模型
            records: 记录列表
            batch_size: 每批大小 (None 则使用配置值)
            
        Returns:
            插入的记录数
        """
        if not records:
            return 0
        
        batch_size = batch_size or settings.batch_insert_size
        total_inserted = 0
        
        async with AsyncSessionLocal() as session:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                try:
                    await session.execute(insert(model).values(batch))
                    await session.commit()
                    total_inserted += len(batch)
                    logger.info(f"Batch inserted {len(batch)} records (Total: {total_inserted}/{len(records)})")
                    
                except Exception as e:
                    await session.rollback()
                    logger.error(f"Batch insert failed for batch {i//batch_size + 1}: {e}")
                    raise
        
        logger.info(f"✅ Total inserted: {total_inserted} records")
        return total_inserted
    
    @staticmethod
    async def batch_upsert(
        model,
        records: List[Dict[str, Any]],
        constraint_name: str,
        batch_size: int = None
    ) -> int:
        """
        批量 upsert (插入或更新)
        
        Args:
            model: SQLAlchemy 模型
            records: 记录列表
            constraint_name: 唯一约束名称
            batch_size: 每批大小
            
        Returns:
            处理的记录数
        """
        if not records:
            return 0
        
        batch_size = batch_size or settings.batch_insert_size
        total_processed = 0
        
        async with AsyncSessionLocal() as session:
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                try:
                    stmt = pg_insert(model).values(batch)
                    stmt = stmt.on_conflict_do_update(
                        constraint=constraint_name,
                        set_={k: stmt.excluded[k] for k in batch[0].keys() if k != 'id'}
                    )
                    
                    await session.execute(stmt)
                    await session.commit()
                    total_processed += len(batch)
                    logger.info(f"Batch upserted {len(batch)} records (Total: {total_processed}/{len(records)})")
                    
                except Exception as e:
                    await session.rollback()
                    logger.error(f"Batch upsert failed for batch {i//batch_size + 1}: {e}")
                    raise
        
        logger.info(f"✅ Total upserted: {total_processed} records")
        return total_processed
    
    @staticmethod
    async def batch_update(
        model,
        updates: List[Dict[str, Any]],
        key_field: str = 'id',
        batch_size: int = None
    ) -> int:
        """
        批量更新记录
        
        Args:
            model: SQLAlchemy 模型
            updates: 更新数据列表 (必须包含 key_field)
            key_field: 主键字段名
            batch_size: 每批大小
            
        Returns:
            更新的记录数
        """
        if not updates:
            return 0
        
        batch_size = batch_size or settings.batch_insert_size
        total_updated = 0
        
        async with AsyncSessionLocal() as session:
            for i in range(0, len(updates), batch_size):
                batch = updates[i:i + batch_size]
                
                try:
                    for record in batch:
                        key_value = record.pop(key_field)
                        stmt = (
                            update(model)
                            .where(getattr(model, key_field) == key_value)
                            .values(**record)
                        )
                        await session.execute(stmt)
                    
                    await session.commit()
                    total_updated += len(batch)
                    logger.info(f"Batch updated {len(batch)} records (Total: {total_updated}/{len(updates)})")
                    
                except Exception as e:
                    await session.rollback()
                    logger.error(f"Batch update failed for batch {i//batch_size + 1}: {e}")
                    raise
        
        logger.info(f"✅ Total updated: {total_updated} records")
        return total_updated
    
    @staticmethod
    async def batch_delete(
        model,
        ids: List[Any],
        key_field: str = 'id',
        batch_size: int = None
    ) -> int:
        """
        批量删除记录
        
        Args:
            model: SQLAlchemy 模型
            ids: ID 列表
            key_field: 主键字段名
            batch_size: 每批大小
            
        Returns:
            删除的记录数
        """
        if not ids:
            return 0
        
        batch_size = batch_size or settings.batch_insert_size
        total_deleted = 0
        
        async with AsyncSessionLocal() as session:
            for i in range(0, len(ids), batch_size):
                batch_ids = ids[i:i + batch_size]
                
                try:
                    stmt = delete(model).where(getattr(model, key_field).in_(batch_ids))
                    result = await session.execute(stmt)
                    await session.commit()
                    
                    deleted_count = result.rowcount
                    total_deleted += deleted_count
                    logger.info(f"Batch deleted {deleted_count} records (Total: {total_deleted})")
                    
                except Exception as e:
                    await session.rollback()
                    logger.error(f"Batch delete failed for batch {i//batch_size + 1}: {e}")
                    raise
        
        logger.info(f"✅ Total deleted: {total_deleted} records")
        return total_deleted


class StreamingBatchProcessor:
    """流式批量处理器 (适用于大数据量)"""
    
    def __init__(self, model, batch_size: int = None):
        self.model = model
        self.batch_size = batch_size or settings.batch_insert_size
        self.buffer: List[Dict[str, Any]] = []
        self.total_processed = 0
    
    async def add(self, record: Dict[str, Any]):
        """
        添加一条记录到缓冲区
        
        Args:
            record: 记录字典
        """
        self.buffer.append(record)
        
        if len(self.buffer) >= self.batch_size:
            await self.flush()
    
    async def flush(self):
        """刷新缓冲区 (写入数据库)"""
        if not self.buffer:
            return
        
        async with AsyncSessionLocal() as session:
            try:
                await session.execute(insert(self.model).values(self.buffer))
                await session.commit()
                self.total_processed += len(self.buffer)
                logger.info(f"Flushed {len(self.buffer)} records (Total: {self.total_processed})")
                self.buffer.clear()
                
            except Exception as e:
                await session.rollback()
                logger.error(f"Flush failed: {e}")
                raise
    
    async def close(self):
        """关闭处理器 (刷新剩余数据)"""
        await self.flush()
        logger.info(f"✅ StreamingBatchProcessor closed. Total processed: {self.total_processed}")
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()