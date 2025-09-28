import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict
from fastapi import UploadFile
from supabase import create_client, Client
from core.utils import make_safe_storage_path


load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL") or ""
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")

logger = logging.getLogger(__name__)

def upload_files_to_supabase(user_id: str, files: List[UploadFile]) -> Dict[str, str]:
    """ 批量上传文件到Supabase私有存储 """
    # 创建Supabase客户端
    
    result = {}
    
    for file in files:
        try:
            # 读取文件内容
            file_content = file.file.read()

            # 生成安全路径
            safe_filename = make_safe_storage_path(file.filename)
            date_url = datetime.utcnow().date().isoformat()
            timestamp = datetime.utcnow().isoformat()
            storage_path = f"save/{user_id}/{date_url}/{timestamp}_{safe_filename}"
            
            # 上传文件到Supabase存储
            supabase.storage.from_(SUPABASE_BUCKET).upload(
                path=storage_path,
                file=file_content,
                file_options={"content-type": file.content_type}
            )
      
            result[file.filename] = storage_path
            
            # 重置文件指针
            file.file.seek(0)
            
        except Exception as e:
            logger.info(f"上传文件 {file.filename} 失败: {str(e)}")
            result[file.filename] = ""
    logger.info(f"Files upload process completed. Successfully uploaded {len(result)}/{len(files)} files")
    return result


