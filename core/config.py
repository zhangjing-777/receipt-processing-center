from pydantic_settings import BaseSettings
import base64


class Settings(BaseSettings):
    # 数据库连接参数
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str
    db_user: str
    db_password: str

    aws_region: str
    aws_access_key_id: str
    aws_secret_access_key: str

    supabase_url: str
    supabase_service_role_key: str
    supabase_bucket: str

    openrouter_url: str
    openrouter_api_key: str
    model_free: str
    model: str

    deepseek_api_key: str
    deepseek_url: str
  
    encryption_key: str
    
    redis_host: str
    redis_port: str
    redis_password: str
    redis_db: str
    
    # ========== Phase 2 新增配置 ==========
    
    # 并发控制
    max_concurrent_ocr: int = 5          # OCR 最大并发数
    max_concurrent_upload: int = 10      # 上传最大并发数
    max_concurrent_download: int = 10    # 下载最大并发数
    
    # HTTP 客户端配置
    http_timeout: int = 60               # HTTP 请求超时(秒)
    http_connect_timeout: int = 10       # HTTP 连接超时(秒)
    http_pool_connections: int = 100     # 连接池大小
    http_pool_maxsize: int = 100         # 最大连接数
    
    # 数据库连接池配置
    db_pool_size: int = 100              # 数据库连接池大小
    db_max_overflow: int = 50            # 超出后最多再创建
    db_pool_recycle: int = 3600          # 连接回收时间(秒)
    
    # Redis 缓存配置
    redis_cache_ttl: int = 82800         # Redis 缓存时间(23小时)
    supabase_signed_url_ttl: int = 86400 # Supabase 签名 URL 有效期(24小时)
    
    # 性能优化
    enable_batch_insert: bool = True      # 启用批量插入
    batch_insert_size: int = 50           # 批量插入大小
    enable_query_cache: bool = True       # 启用查询缓存
    
    class Config:
        env_file = ".env"
    
    @property
    def encryption_key_bytes(self) -> bytes:
        """Convert base64 encoded key to bytes"""
        return base64.b64decode(self.encryption_key)
    
    @property
    def database_url(self) -> str:
        """Construct database URL from individual components"""
        import urllib.parse
        password = urllib.parse.quote_plus(self.db_password)
        return f"postgresql+asyncpg://{self.db_user}:{password}@{self.db_host}:{self.db_port}/{self.db_name}"


settings = Settings()
