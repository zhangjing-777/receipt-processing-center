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