import os
import logging
from datetime import datetime
from fastapi import FastAPI, Request
from ses_eml_save.routers import router as ses_eml_save_routers
from rcpdro_web_save.routers import router as rcpdro_web_save_routers
from summary_download.routers import router as summary_download_routers
from table_processor.receipt_items_en_router import router as receipt_items_en_routers
from table_processor.receipt_summary_zip_en_router import router as receipt_summary_zip_en_routers
from table_processor.ses_eml_info_en_router import router as ses_eml_info_en_routers

# 创建logs目录
os.makedirs('logs', exist_ok=True)

# 配置日志格式和存储
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(f'logs/app_{datetime.now().strftime("%Y%m%d")}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI()

# 包含路由
app.include_router(ses_eml_save_routers)
app.include_router(rcpdro_web_save_routers)
app.include_router(summary_download_routers)
app.include_router(receipt_items_en_routers)
app.include_router(receipt_summary_zip_en_routers)
app.include_router(ses_eml_info_en_routers)

@app.get("/health")
async def health_check():
    """健康检查接口"""
    logger.info("Health check requested")
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """全局异常处理器"""
    logger.exception(f"Unhandled exception occurred: {str(exc)}")
    return {"error": "Internal server error", "status": "error"}