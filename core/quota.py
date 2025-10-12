import logging
from datetime import date
from sqlalchemy import select, update
from core.database import AsyncSessionLocal
from core.models import ReceiptUsageQuotaReceiptEN, ReceiptUsageQuotaRequestEN

logger = logging.getLogger(__name__)

class QuotaManager:
    def __init__(self, user_id: str, table: str = "receipt_usage_quota_receipt_en"):
        self.user_id = user_id
        self.table = table
        self.used_month = 0
        self.model = ReceiptUsageQuotaReceiptEN if table == "receipt_usage_quota_receipt_en" else ReceiptUsageQuotaRequestEN

    async def check_and_reset(self):
        """检查并重置配额"""
        async with AsyncSessionLocal() as session:
            # 查询配额
            result = await session.execute(
                select(self.model).where(self.model.user_id == self.user_id)
            )
            quota_data = result.scalar_one_or_none()
            
            if not quota_data:
                raise ValueError(f"Quota record not found for user_id={self.user_id}")

            # 检查是否需要重置
            today = date.today()  # 使用 date.today() 而不是 datetime.now().date()
            today_str = today.isoformat()  # "YYYY-MM-DD"
            current_month = today_str[:7]  # "YYYY-MM"
            
            # 处理 last_reset_date（可能是 date 对象或字符串）
            last_reset_date = quota_data.last_reset_date
            if isinstance(last_reset_date, date):
                last_reset_month = last_reset_date.isoformat()[:7]
            elif isinstance(last_reset_date, str):
                last_reset_month = last_reset_date[:7]
            else:
                last_reset_month = ""
            
            needs_reset = current_month != last_reset_month

            if needs_reset:
                self.used_month = 0
                await session.execute(
                    update(self.model)
                    .where(self.model.user_id == self.user_id)
                    .values(used_month=0, last_reset_date=today)  # 直接传 date 对象
                )
                await session.commit()
            else:
                self.used_month = quota_data.used_month

            month_limit = quota_data.month_limit
            allowed = self.used_month < month_limit

            if not allowed:
                remark = "⚠️ You have reached your month usage limit. Please try next period or upgrade your plan for more quota."
                await session.execute(
                    update(self.model)
                    .where(self.model.user_id == self.user_id)
                    .values(remark=remark)
                )
                await session.commit()
                raise ValueError(remark)

            logger.info(f"user_id:{self.user_id}, used_month:{self.used_month}, month_limit:{month_limit}")

    async def increment_usage(self, success_count: int):
        """增加使用量"""
        new_used = self.used_month + success_count
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(self.model)
                .where(self.model.user_id == self.user_id)
                .values(used_month=new_used)
            )
            await session.commit()
            self.used_month = new_used
            logger.info(f"user_id:{self.user_id}, used_month:{self.used_month}")
