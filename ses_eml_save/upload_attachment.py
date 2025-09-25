import os
import base64
import logging
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client
from core.utils import make_safe_storage_path


load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL") or ""
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")

logger = logging.getLogger(__name__)

def upload_attachments_to_storage(attachments, user_id, bucket=SUPABASE_BUCKET):
    logger.info(f"Starting attachment upload process for {len(attachments)} attachments to bucket: {bucket}")

    records = {}
    for i, att in enumerate(attachments, 1):
        try:
            filename = att["filename"]
            logger.info(f"Processing attachment {i}/{len(attachments)}: {filename}")
            
            safe_filename = make_safe_storage_path(filename)
            logger.info(f"Safe filename generated: {safe_filename}")
            
            binary = att["binary"]
            if isinstance(binary, bytes):
                binary_data = base64.b64decode(binary)
                logger.info(f"Decoded base64 binary data, size: {len(binary_data)} bytes")
            else:
                binary_data = binary  # 已经是 bytes
                logger.info(f"Binary data already in bytes format, size: {len(binary_data)} bytes")

            date_url = datetime.utcnow().date().isoformat()
            timestamp = datetime.utcnow().isoformat()
            storage_path = f"users/{user_id}/{date_url}/{timestamp}_{safe_filename}"
            logger.info(f"Generated storage path: {storage_path}")

            logger.info(f"Uploading {filename} to storage at {storage_path}")
            supabase.storage.from_(bucket).upload(
                path=storage_path,
                file=binary_data,
                file_options={"content-type": att.get("content_type", "application/octet-stream")}
            )

            # 获取公共URL
            #public_url = supabase.storage.from_(bucket).get_public_url(storage_path).rstrip('?')
            #logger.info(f"Upload successful. Public URL: {public_url}")
            #records[filename] = public_url
            records[filename] = storage_path
        
        except Exception as e:
            logger.exception(f"Failed to upload attachment {i}/{len(attachments)}: {att.get('filename', 'unknown')} - Error: {str(e)}")
            raise
    
    logger.info(f"Attachment upload process completed. Successfully uploaded {len(records)}/{len(attachments)} attachments")
    return records