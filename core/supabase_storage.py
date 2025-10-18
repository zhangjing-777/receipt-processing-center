import os
import logging
from typing import Optional, Dict, Any
from urllib.parse import urljoin, quote
from dotenv import load_dotenv
from core.http_client import AsyncHTTPClient

load_dotenv()

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL") or ""
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")


class AsyncSupabaseStorage:
    """异步 Supabase Storage 客户端"""
    
    def __init__(self, url: str = SUPABASE_URL, key: str = SUPABASE_KEY, bucket: str = SUPABASE_BUCKET):
        self.base_url = f"{url.rstrip('/')}/storage/v1"
        self.bucket = bucket
        self.headers = {
            "Authorization": f"Bearer {key}",
            "apikey": key
        }
    
    async def upload(
        self,
        path: str,
        file_data: bytes,
        content_type: str = "application/octet-stream",
        upsert: bool = False
    ) -> Dict[str, Any]:
        """
        异步上传文件到 Supabase Storage
        
        Args:
            path: 存储路径 (例如: "users/123/file.pdf")
            file_data: 文件二进制数据
            content_type: MIME 类型
            upsert: 是否覆盖已存在的文件
            
        Returns:
            上传结果字典
        """
        url = f"{self.base_url}/object/{self.bucket}/{path}"
        headers = {
            **self.headers,
            "Content-Type": content_type,
            "x-upsert": str(upsert).lower()
        }
        
        client = AsyncHTTPClient.get_client()
        
        try:
            logger.info(f"Uploading file to: {path}")
            response = await client.post(url, content=file_data, headers=headers)
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"✅ Successfully uploaded: {path}")
            return {"success": True, "path": path, "data": result}
            
        except Exception as e:
            logger.exception(f"❌ Upload failed for {path}: {str(e)}")
            return {"success": False, "path": path, "error": str(e)}
    
    async def download(self, path: str) -> bytes:
        """
        异步从 Supabase Storage 下载文件
        
        Args:
            path: 存储路径
            
        Returns:
            文件二进制数据
        """
        url = f"{self.base_url}/object/{self.bucket}/{path}"
        
        client = AsyncHTTPClient.get_client()
        
        try:
            logger.info(f"Downloading file from: {path}")
            response = await client.get(url, headers=self.headers)
            response.raise_for_status()
            
            logger.info(f"✅ Successfully downloaded: {path} ({len(response.content)} bytes)")
            return response.content
            
        except Exception as e:
            logger.exception(f"❌ Download failed for {path}: {str(e)}")
            raise
    
    async def delete(self, paths: list[str]) -> Dict[str, Any]:
        """
        异步删除文件
        
        Args:
            paths: 要删除的文件路径列表
            
        Returns:
            删除结果字典
        """
        url = f"{self.base_url}/object/{self.bucket}"
        
        client = AsyncHTTPClient.get_client()
        
        try:
            logger.info(f"Deleting {len(paths)} file(s)")
            response = await client.delete(
                url,
                headers={**self.headers, "Content-Type": "application/json"},
                json={"prefixes": paths}
            )
            response.raise_for_status()
            
            result = response.json()
            logger.info(f"✅ Successfully deleted {len(paths)} file(s)")
            return {"success": True, "deleted_count": len(paths), "data": result}
            
        except Exception as e:
            logger.exception(f"❌ Delete failed: {str(e)}")
            return {"success": False, "error": str(e)}

    async def create_signed_url(self, path: str, expires_in: int = 3600) -> Optional[str]:
        """
        异步创建签名 URL
        
        Args:
            path: 文件路径
            expires_in: 过期时间(秒)
            
        Returns:
            签名 URL
        """
        # 1) 规范 Storage 基址：确保带 /storage/v1
        storage_base = self.base_url if "/storage/v1" in self.base_url \
            else f"{self.base_url.rstrip('/')}/storage/v1"

        # 2) 规范对象路径：去前导斜杠；若误传含桶名前缀则去重；URL 编码（保留 /）
        object_path = path.lstrip("/")
        bucket_prefix = f"{self.bucket}/"
        if object_path.startswith(bucket_prefix):
            object_path = object_path[len(bucket_prefix):]
        safe_object_path = quote(object_path, safe="/")

        # 3) 签名接口
        sign_url = f"{storage_base}/object/sign/{self.bucket}/{safe_object_path}"

        client = AsyncHTTPClient.get_client()
        try:
            logger.info(f"Creating signed URL for: {path}")
            resp = await client.post(
                sign_url,
                headers={**self.headers, "Content-Type": "application/json"},
                json={"expiresIn": int(expires_in)},
                timeout=15.0,
            )
            resp.raise_for_status()
            data = resp.json()

            # 4) 兼容相对/绝对返回，统一成完整 URL
            signed_path = data.get("signedURL") or data.get("signedUrl") or data.get("signed_url")
            if not signed_path:
                logger.warning(f"No signedURL in response for: {path}")
                return None

            full_url = signed_path if signed_path.startswith(("http://", "https://")) \
                else urljoin(storage_base.rstrip("/") + "/", signed_path.lstrip("/"))

            logger.info(f"✅ Created signed URL for: {path}")
            return full_url

        except Exception as e:
            logger.warning(f"Failed to generate signed URL for {path}: {e}")
            return None
    
    def get_public_url(self, path: str) -> str:
        """
        获取公开 URL (不需要异步)
        
        Args:
            path: 文件路径
            
        Returns:
            公开 URL
        """
        return f"{self.base_url}/object/public/{self.bucket}/{path}"


# 全局单例
_storage_client: Optional[AsyncSupabaseStorage] = None


def get_async_storage_client() -> AsyncSupabaseStorage:
    """获取全局异步 Storage 客户端"""
    global _storage_client
    if _storage_client is None:
        _storage_client = AsyncSupabaseStorage()
        logger.info("✅ Async Supabase Storage client initialized")
    return _storage_client