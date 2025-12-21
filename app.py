import os
import logging
from datetime import datetime
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from core.redis_client import redis_client
from core.http_client import AsyncHTTPClient
from core.database import close_db
from ses_eml_save.routers import router as ses_eml_save_routers
from rcpdro_web_save.routers import router as rcpdro_web_save_routers
from summary_download.routers import router as summary_download_routers
from table_processor.receipt_items_en.router import router as receipt_items_en_routers
from table_processor.receipt_summary_zip_en.router import router as receipt_summary_zip_en_routers
from table_processor.ses_eml_info_en.router import router as ses_eml_info_en_routers
from table_processor.receipt_items_en_upload_result.router import router as receipt_items_en_upload_result_routers

# 创建 logs 目录
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理 (启动和关闭)"""
    # ========== 启动阶段 ==========
    logger.info("🚀 Application startup initiated")
    
    # 初始化 Redis
    try:
        pong = await redis_client.ping()
        if pong:
            logger.info("✅ Redis connected successfully")
    except Exception as e:
        logger.warning(f"⚠️ Redis connection failed: {e}")
    
    # 初始化全局 HTTP 客户端
    try:
        AsyncHTTPClient.get_client()
        logger.info("✅ Global HTTP client initialized")
    except Exception as e:
        logger.warning(f"⚠️ HTTP client initialization failed: {e}")
    
    logger.info("✅ Application startup completed")
    
    yield
    
    # ========== 关闭阶段 ==========
    logger.info("🧹 Application shutdown initiated")
    
    # 关闭 Redis 连接
    try:
        await redis_client.close()
        logger.info("✅ Redis connection closed")
    except Exception as e:
        logger.warning(f"⚠️ Redis cleanup failed: {e}")
    
    # 关闭全局 HTTP 客户端
    try:
        await AsyncHTTPClient.close()
        logger.info("✅ Global HTTP client closed")
    except Exception as e:
        logger.warning(f"⚠️ HTTP client cleanup failed: {e}")
    
    # 关闭数据库连接池
    try:
        await close_db()
        logger.info("✅ Database connection pool closed")
    except Exception as e:
        logger.warning(f"⚠️ Database cleanup failed: {e}")
    
    logger.info("✅ Application shutdown completed")


# 创建 FastAPI 应用 (使用新的 lifespan 模式)
app = FastAPI(
    title="Receipt Processing API",
    version="2.3.0",  # Phase 2 版本号
    lifespan=lifespan
)

# 添加 CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 包含路由
app.include_router(ses_eml_save_routers)
app.include_router(rcpdro_web_save_routers)
app.include_router(summary_download_routers)
app.include_router(receipt_items_en_routers)
app.include_router(receipt_summary_zip_en_routers)
app.include_router(ses_eml_info_en_routers)
app.include_router(receipt_items_en_upload_result_routers)


@app.get("/health")
async def health_check():
    """健康检查接口"""
    logger.info("Health check requested")
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "2.3.0"
    }


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """全局异常处理器"""
    logger.exception(f"Unhandled exception occurred: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error", "status": "error"}
    )
