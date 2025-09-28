import os
import base64
import requests
from dotenv import load_dotenv
from supabase import create_client, Client
import logging

load_dotenv()

logger = logging.getLogger(__name__)



OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
MODEL = os.getenv("MODEL")
MODEL_FREE = os.getenv("MODEL_FREE")
OPENROUTER_URL = os.getenv("OPENROUTER_URL") or ""
url: str = os.getenv("SUPABASE_URL") or ""
key: str = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
supabase: Client = create_client(url, key)
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")

HEADERS = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json"
    }


def openrouter_image_ocr(file_url):
    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "What's in this image?"
                },
                {
                    "type": "image_url",
                    "image_url": {
                        "url": file_url
                    }
                }
            ]
        }
    ]
    
    # 先尝试使用 MODEL_FREE
    payload = {
        "model": MODEL_FREE,
        "messages": messages
    }
    
    try:
        logger.info(f"Trying image OCR with MODEL_FREE: {MODEL_FREE}")
        response = requests.post(OPENROUTER_URL, headers=HEADERS, json=payload)
        response.raise_for_status()
        response_data = response.json()
        
        # 记录 token 使用量
        if "usage" in response_data:
            usage = response_data["usage"]
            logger.info(f"Image OCR token usage (MODEL_FREE) - Prompt: {usage.get('prompt_tokens', 'N/A')}, "
                       f"Completion: {usage.get('completion_tokens', 'N/A')}, "
                       f"Total: {usage.get('total_tokens', 'N/A')}")
        else:
            logger.warning("No usage information found in OpenRouter response")
        
        logger.info(f"Image OCR API response (MODEL_FREE): {response.status_code}")
        return response_data["choices"][0]["message"]["content"]
        
    except Exception as e:
        logger.warning(f"MODEL_FREE failed, trying MODEL: {str(e)}")
        
        # 如果 MODEL_FREE 失败，尝试使用 MODEL
        payload["model"] = MODEL
        try:
            logger.info(f"Trying image OCR with MODEL: {MODEL}")
            response = requests.post(OPENROUTER_URL, headers=HEADERS, json=payload)
            response.raise_for_status()
            response_data = response.json()
            
            # 记录 token 使用量
            if "usage" in response_data:
                usage = response_data["usage"]
                logger.info(f"Image OCR token usage (MODEL) - Prompt: {usage.get('prompt_tokens', 'N/A')}, "
                           f"Completion: {usage.get('completion_tokens', 'N/A')}, "
                           f"Total: {usage.get('total_tokens', 'N/A')}")
            else:
                logger.warning("No usage information found in OpenRouter response")
            
            logger.info(f"Image OCR API response (MODEL): {response.status_code}")
            return response_data["choices"][0]["message"]["content"]
            
        except Exception as e2:
            logger.exception(f"Both MODEL_FREE and MODEL failed for image OCR: {str(e2)}")
            raise

def openrouter_pdf_ocr(file_url):
    logger.info(f"Starting PDF OCR for: {file_url}")
    try:
        response = requests.get(file_url)
        response.raise_for_status()
        base64_pdf = base64.b64encode(response.content).decode('utf-8')
        data_url = f"data:application/pdf;base64,{base64_pdf}"
        messages = [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "What are the main points in this document?"
                    },
                    {
                        "type": "file",
                        "file": {
                            "filename": "invoice.pdf",
                            "file_data": data_url
                        }
                    },
                ]
            }
        ]
        plugins = [
            {
                "id": "file-parser",
                "pdf": {
                    "engine": "pdf-text"  
                }
            }
        ]
        
        # 先尝试使用 MODEL_FREE
        payload = {
            "model": MODEL_FREE,
            "messages": messages,
            "plugins": plugins
        }
        
        try:
            logger.info(f"Trying PDF OCR with MODEL_FREE: {MODEL_FREE}")
            response = requests.post(OPENROUTER_URL, headers=HEADERS, json=payload)
            response.raise_for_status()
            response_data = response.json()
            
            # 记录 token 使用量
            if "usage" in response_data:
                usage = response_data["usage"]
                logger.info(f"PDF OCR token usage (MODEL_FREE) - Prompt: {usage.get('prompt_tokens', 'N/A')}, "
                           f"Completion: {usage.get('completion_tokens', 'N/A')}, "
                           f"Total: {usage.get('total_tokens', 'N/A')}")
            else:
                logger.warning("No usage information found in OpenRouter response")
            
            logger.info(f"PDF OCR API response (MODEL_FREE): {response.status_code}")
            return response_data["choices"][0]["message"]["content"]
            
        except Exception as e:
            logger.warning(f"MODEL_FREE failed, trying MODEL: {str(e)}")
            
            # 如果 MODEL_FREE 失败，尝试使用 MODEL
            payload["model"] = MODEL
            try:
                logger.info(f"Trying PDF OCR with MODEL: {MODEL}")
                response = requests.post(OPENROUTER_URL, headers=HEADERS, json=payload)
                response.raise_for_status()
                response_data = response.json()
                
                # 记录 token 使用量
                if "usage" in response_data:
                    usage = response_data["usage"]
                    logger.info(f"PDF OCR token usage (MODEL) - Prompt: {usage.get('prompt_tokens', 'N/A')}, "
                               f"Completion: {usage.get('completion_tokens', 'N/A')}, "
                               f"Total: {usage.get('total_tokens', 'N/A')}")
                else:
                    logger.warning("No usage information found in OpenRouter response")
                
                logger.info(f"PDF OCR API response (MODEL): {response.status_code}")
                return response_data["choices"][0]["message"]["content"]
                
            except Exception as e2:
                logger.exception(f"Both MODEL_FREE and MODEL failed for PDF OCR: {str(e2)}")
                raise
                
    except Exception as e:
        logger.exception(f"PDF OCR failed: {str(e)}")
        raise

def ocr_pdf_from_storage(storage_path):
    """直接从Supabase存储下载PDF进行OCR"""
    logger.info(f"Downloading PDF from storage: {storage_path}")
    try:
        # 使用Supabase client下载文件
        file_content = supabase.storage.from_(SUPABASE_BUCKET).download(storage_path)
        base64_pdf = base64.b64encode(file_content).decode('utf-8')
        data_url = f"data:application/pdf;base64,{base64_pdf}"
        
        messages = [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "What are the main points in this document?"
                    },
                    {
                        "type": "file",
                        "file": {
                            "filename": "invoice.pdf",
                            "file_data": data_url
                        }
                    },
                ]
            }
        ]
        plugins = [
            {
                "id": "file-parser",
                "pdf": {
                    "engine": "pdf-text"  
                }
            }
        ]
        
        # 先尝试使用 MODEL_FREE
        payload = {
            "model": MODEL_FREE,
            "messages": messages,
            "plugins": plugins
        }
        
        try:
            logger.info(f"Trying PDF OCR with MODEL_FREE: {MODEL_FREE}")
            response = requests.post(OPENROUTER_URL, headers=HEADERS, json=payload)
            response.raise_for_status()
            response_data = response.json()
            
            # 记录 token 使用量
            if "usage" in response_data:
                usage = response_data["usage"]
                logger.info(f"PDF OCR token usage (MODEL_FREE) - Prompt: {usage.get('prompt_tokens', 'N/A')}, "
                           f"Completion: {usage.get('completion_tokens', 'N/A')}, "
                           f"Total: {usage.get('total_tokens', 'N/A')}")
            else:
                logger.warning("No usage information found in OpenRouter response")
            
            logger.info(f"PDF OCR API response (MODEL_FREE): {response.status_code}")
            return response_data["choices"][0]["message"]["content"]
            
        except Exception as e:
            logger.warning(f"MODEL_FREE failed, trying MODEL: {str(e)}")
            
            # 如果 MODEL_FREE 失败，尝试使用 MODEL
            payload["model"] = MODEL
            try:
                logger.info(f"Trying PDF OCR with MODEL: {MODEL}")
                response = requests.post(OPENROUTER_URL, headers=HEADERS, json=payload)
                response.raise_for_status()
                response_data = response.json()
                
                # 记录 token 使用量
                if "usage" in response_data:
                    usage = response_data["usage"]
                    logger.info(f"PDF OCR token usage (MODEL) - Prompt: {usage.get('prompt_tokens', 'N/A')}, "
                               f"Completion: {usage.get('completion_tokens', 'N/A')}, "
                               f"Total: {usage.get('total_tokens', 'N/A')}")
                else:
                    logger.warning("No usage information found in OpenRouter response")
                
                logger.info(f"PDF OCR API response (MODEL): {response.status_code}")
                return response_data["choices"][0]["message"]["content"]
                
            except Exception as e2:
                logger.exception(f"Both MODEL_FREE and MODEL failed for PDF OCR: {str(e2)}")
                raise
                
    except Exception as e:
        logger.exception(f"Storage PDF OCR failed: {str(e)}")
        raise

def ocr_image_from_storage(storage_path):
    """直接从Supabase存储下载图片进行OCR"""
    logger.info(f"Downloading image from storage: {storage_path}")
    try:
        # 使用Supabase client下载文件
        file_content = supabase.storage.from_(SUPABASE_BUCKET).download(storage_path)
        base64_image = base64.b64encode(file_content).decode('utf-8')
        
        # 根据文件扩展名判断content-type
        content_type = "image/jpeg"  # 默认
        if storage_path.lower().endswith('.png'):
            content_type = "image/png"
        elif storage_path.lower().endswith(('.jpg', '.jpeg')):
            content_type = "image/jpeg"
        
        data_url = f"data:{content_type};base64,{base64_image}"
        
        messages = [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "What's in this image?"
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": data_url
                        }
                    }
                ]
            }
        ]
        
        # 使用与原 openrouter_image_ocr 相同的逻辑
        payload = {
            "model": MODEL_FREE,
            "messages": messages
        }
        
        try:
            logger.info(f"Trying image OCR with MODEL_FREE: {MODEL_FREE}")
            response = requests.post(OPENROUTER_URL, headers=HEADERS, json=payload)
            response.raise_for_status()
            response_data = response.json()
            
            # 记录 token 使用量
            if "usage" in response_data:
                usage = response_data["usage"]
                logger.info(f"Image OCR token usage (MODEL_FREE) - Prompt: {usage.get('prompt_tokens', 'N/A')}, "
                           f"Completion: {usage.get('completion_tokens', 'N/A')}, "
                           f"Total: {usage.get('total_tokens', 'N/A')}")
            else:
                logger.warning("No usage information found in OpenRouter response")
            
            logger.info(f"Image OCR API response (MODEL_FREE): {response.status_code}")
            return response_data["choices"][0]["message"]["content"]
            
        except Exception as e:
            logger.warning(f"MODEL_FREE failed, trying MODEL: {str(e)}")
            
            # 如果 MODEL_FREE 失败，尝试使用 MODEL
            payload["model"] = MODEL
            try:
                logger.info(f"Trying image OCR with MODEL: {MODEL}")
                response = requests.post(OPENROUTER_URL, headers=HEADERS, json=payload)
                response.raise_for_status()
                response_data = response.json()
                
                # 记录 token 使用量
                if "usage" in response_data:
                    usage = response_data["usage"]
                    logger.info(f"Image OCR token usage (MODEL) - Prompt: {usage.get('prompt_tokens', 'N/A')}, "
                               f"Completion: {usage.get('completion_tokens', 'N/A')}, "
                               f"Total: {usage.get('total_tokens', 'N/A')}")
                else:
                    logger.warning("No usage information found in OpenRouter response")
                
                logger.info(f"Image OCR API response (MODEL): {response.status_code}")
                return response_data["choices"][0]["message"]["content"]
                
            except Exception as e2:
                logger.exception(f"Both MODEL_FREE and MODEL failed for image OCR: {str(e2)}")
                raise
                
    except Exception as e:
        logger.exception(f"Storage image OCR failed: {str(e)}")
        raise

def ocr_attachment(file_path_or_url) -> str:
    logger.info(f"Starting OCR for attachment: {file_path_or_url}")
    try:
        # 判断是存储路径还是完整URL
        if file_path_or_url.startswith("users/") or (not file_path_or_url.startswith("http")):
            # 是存储路径，需要从Supabase下载
            logger.info(f"Processing storage path: {file_path_or_url}")
            if file_path_or_url.endswith("pdf"):
                return ocr_pdf_from_storage(file_path_or_url)
            else:
                return ocr_image_from_storage(file_path_or_url)
        else:
            # 是完整URL，使用原有逻辑
            logger.info(f"Processing URL: {file_path_or_url}")
            if file_path_or_url.endswith("pdf"):
                return openrouter_pdf_ocr(file_path_or_url)
            else:
                return openrouter_image_ocr(file_path_or_url)
    except Exception as e:
        logger.error(f"OCR failed for {file_path_or_url}: {str(e)}")
        raise   

