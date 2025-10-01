import os
import logging
from dotenv import load_dotenv
from datetime import datetime
from supabase import create_client, Client

load_dotenv()

url: str = os.getenv("SUPABASE_URL") or ""
key: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
supabase: Client = create_client(url, key)

logger = logging.getLogger(__name__)


class QuotaManager:
    def __init__(self, user_id: str, table: str = "receipt_usage_quota_receipt_en"):
        self.user_id = user_id
        self.table = table
        self.used_month = 0  # 将由 check_and_reset() 重新赋值

    def check_and_reset(self):
        """Check user quota and update if necessary"""
        # Get current quota
        response = supabase.table(self.table).select("*").eq("user_id", self.user_id).execute()
        if not response.data:
            raise ValueError(f"Quota record not found for user_id={self.user_id}")

        quota_data = response.data[0]

        # Check if reset is needed
        today = datetime.now().date().isoformat()
        current_month = today[:7]  # YYYY-MM format
        last_reset_month = (quota_data.get("last_reset_date", "") or "")[:7]

        needs_reset = current_month != last_reset_month

        if needs_reset:
            self.used_month = 0
        else:
            self.used_month = quota_data.get("used_month", 0)

        month_limit = quota_data.get("month_limit", 0)
        logger.info(f"month_limit is {month_limit}")
        allowed = self.used_month < month_limit

        if not allowed:
            remark = "⚠️ You have reached your month usage limit. Please try next period or upgrade your plan for more quota."
            supabase.table(self.table).update({
                "remark": remark
            }).eq("user_id", self.user_id).execute()
            
            raise ValueError(remark)

        if needs_reset:
            # Update reset date and usage
            supabase.table(self.table).update({
                "used_month": 0,
                "last_reset_date": today
            }).eq("user_id", self.user_id).execute()

        logger.info(f"user_id:{self.user_id}, used_month:{self.used_month}, month_limit:{month_limit}")
        return None

    def increment_usage(self, success_count: int):
        new_used = self.used_month + success_count
        supabase.table(self.table).update({
            "used_month": new_used
        }).eq("user_id", self.user_id).execute()
        # 同步内存值，保证后续累加正确
        self.used_month = new_used
        logger.info(f"user_id:{self.user_id}, used_month:{self.used_month}")

         