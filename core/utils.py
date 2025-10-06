import re
import json
import unicodedata
import hashlib
from pypinyin import lazy_pinyin
import logging
from typing import Any, Dict



logger = logging.getLogger(__name__)

def make_safe_storage_path(filename: str, prefix: str = "") -> str:
    logger.info(f"Sanitizing filename: {filename}")
    # 1. 去除不可见字符 + 正规化为 NFC
    filename = unicodedata.normalize("NFKC", filename)

    # 2. 中文转拼音（只保留文件主名，后缀不处理）
    if "." in filename:
        name_part, ext = filename.rsplit(".", 1)
    else:
        name_part, ext = filename, ""

    # 转为拼音（如：'天翔迪晟（深圳）发票' → 'tianxiangdisheng_shenzhen_fapiao'）
    pinyin_name = "_".join(lazy_pinyin(name_part))

    # 3. 保留英文、数字、下划线、短横线和点，移除非法字符
    pinyin_name = re.sub(r"[^\w.-]", "_", pinyin_name)
    ext = re.sub(r"[^\w]", "", ext)

    # 4. 限长 + 防重复 hash
    if len(pinyin_name) > 80:
        hash_suffix = hashlib.md5(filename.encode()).hexdigest()[:8]
        pinyin_name = pinyin_name[:70] + "_" + hash_suffix

    # 5. 组装最终文件名
    final_filename = f"{pinyin_name}.{ext}" if ext else pinyin_name

    # 6. 可选前缀路径（如 '2025-06-23'）
    if prefix:
        result = f"{prefix}/{final_filename}"
    else:
        result = final_filename
    logger.info(f"Sanitized filename result: {result}")
    return result


def clean_and_parse_json(text: Any) -> Dict:
    """
    清洗并解析 JSON 内容。
    支持以下输入类型：
    - dict：直接返回
    - str：自动清洗 ```json 包裹并解析
    - bytes：先解码再解析
    """
    logger.info("Cleaning and parsing JSON input.")

    # 🧩 情况 1：如果已经是 dict，直接返回
    if isinstance(text, dict):
        logger.info("Input is already a dict, returning directly.")
        return text

    # 🧩 情况 2：如果是 bytes，先转成 str
    if isinstance(text, bytes):
        try:
            text = text.decode("utf-8")
        except Exception as e:
            logger.warning(f"Failed to decode bytes input: {e}")
            raise ValueError("Invalid bytes input for JSON parsing")

    # 🧩 情况 3：如果是字符串，尝试清洗并解析
    if isinstance(text, str):
        try:
            # 去掉 markdown 代码块包装，如 ```json ... ```
            cleaned = re.sub(r"^```(?:json|python)?\s*", "", text.strip(), flags=re.IGNORECASE)
            cleaned = re.sub(r"\s*```$", "", cleaned.strip())

            # 尝试解析 JSON
            result = json.loads(cleaned)
            logger.info("JSON parsed successfully.")
            return result
        except json.JSONDecodeError as e:
            logger.warning(f"Primary JSON decode failed: {e}. Trying literal_eval fallback...")

            # 兼容单引号 JSON 的 fallback
            import ast
            try:
                result = ast.literal_eval(cleaned)
                if isinstance(result, dict):
                    logger.info("Parsed using ast.literal_eval fallback.")
                    return result
                else:
                    raise ValueError("Parsed object is not a dict")
            except Exception as e2:
                logger.exception(f"Failed to clean and parse JSON: {str(e2)}")
                raise ValueError(f"Cannot parse JSON string: {text[:200]}") from e2

    # 🧩 其他类型不支持
    raise TypeError(f"Unsupported input type: {type(text)}")
