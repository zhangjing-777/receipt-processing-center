import os
import requests
from dotenv import load_dotenv
import json
from typing import Dict, List
import logging

load_dotenv()

logger = logging.getLogger(__name__)


DEEPSEEK_URL = os.getenv("DEEPSEEK_URL") or ""
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")

DEEP_HEADERS = {
    "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
    "Content-Type": "application/json"
}



def extract_fields_from_ocr(text):
    logger.info("Extracting fields from OCR text.")
    prompt = f"""This is the raw text extracted from an invoice using OCR. 
    Please extract the following fields and output them as a JSON object, with strict type and format requirements:

    - invoice_number: string
    - invoice_date: string, must be in "YYYY-MM-DD" format (ISO 8601), e.g. "2025-06-23"
    - buyer (purchaser): string
    - seller (vendor): string
    - invoice_total: number (do not include any currency symbols, commas, or quotes, just the numeric value, e.g. 1234.56)
    - currency: string (e.g. "USD", "CNY")
    - category: string
    - address: string

    Return only the JSON object, no extra explanation.

    Example output:
    {{
      "invoice_number": "INV-20250623-001",
      "invoice_date": "2025-06-23",
      "buyer": "Acme Corp",
      "seller": "Widget Inc",
      "invoice_total": 1234.56,
      "currency": "USD",
      "category": "Office Supplies",
      "address": "123 Main St, Springfield"
    }}

    Invoice text is as follows:
    {text}
    """
    data = {
        "model": "deepseek-chat",  
        "messages": [
            {"role": "system", "content": "You are an AI assistant specialized in extracting structured data."},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3,
        "stream": False
    }
    try:
        response = requests.post(DEEPSEEK_URL, headers=DEEP_HEADERS, json=data)
        response.raise_for_status()
        response_data = response.json()
        
        # 记录 token 使用量
        if "usage" in response_data:
            usage = response_data["usage"]
            logger.info(f"Deepseek field extraction token usage - Prompt: {usage.get('prompt_tokens', 'N/A')}, "
                       f"Completion: {usage.get('completion_tokens', 'N/A')}, "
                       f"Total: {usage.get('total_tokens', 'N/A')}")
            logger.info(f"Total: {usage.get('total_tokens', 'N/A')}")
        else:
            logger.warning("No usage information found in Deepseek response")
        
        logger.info(f"Deepseek API response: {response.status_code}")
        return response_data["choices"][0]["message"]["content"]
    except Exception as e:
        logger.exception(f"Field extraction from OCR failed: {str(e)}")
        raise


def generate_summary(invoices_info: Dict):
    logger.info("Starting generate summary ...")
    system_message = """
    You are a receipt reimbursement assistant.

    You will receive pre-computed invoice summaries grouped by buyer.
    Your task is to generate natural language reimbursement summaries and detailed tables,
    strictly following the required output format.

    ----------------------------------
    Expected output format:

    For each **buyer**, generate one independent summary with the following structure:

    ✅ Your business travel reimbursement summary for [Buyer Name] has been generated:
    - [Standardized Category 1]: [amount with currency symbol]
    - [Standardized Category 2]: [amount with currency symbol]
    ...

    Totals by currency:
    - [Currency]: [amount]
    - [Currency]: [amount]
    ...

    Please copy the following description into the reimbursement remarks section:
    During this business trip, the following expenses were incurred:  
    - [Category 1] expenses totaling [amount], **describe in detail using the invoice info**.  
    * For transportation invoices: merge multiple tickets into one description, specify number of rides/tickets, full dates (YYYY-MM-DD), and origin/destination if recognizable from seller (e.g. Uber, Didi, Train, Flight).  
    * For hotel invoices: mention the city or hotel name if available, number of nights, and which days. If multiple hotel invoices exist, summarize them together.  
    * For meals: summarize meals by day or occasion, and mention whether it was canteen, business dinner, or client meal if seller/buyer suggests.  
    * For office/supplies/other expenses: explain what they were used for (e.g. printing, courier, conference materials).  
    - [Category 2] expenses totaling [amount], with a similarly merged and detailed natural explanation.
    ...
    All receipts have been attached. Please proceed with the review.

    Please find the details for [Buyer Name] below:
    (- Return the input in a tabular format.
    - The table should include only the following fields, in this exact order:
    ID, Invoice Date, Category, Seller, Buyer, Invoice Total, Currency, File URL.
    - Sort rows by invoice_date (oldest to newest))

    ----------------------------------
    Rules:
    1. Use the provided "totals_by_category" and "totals_by_currency" **exactly as given**. Do NOT recompute or alter the numbers.
    2. Only include categories and currencies that actually exist (skip zero or missing).
    3. Remarks must use the provided numbers verbatim and provide professional, detailed explanations.
    4. All dates must be written in full format YYYY-MM-DD.
    5. Group results by buyer. If multiple buyers exist, output multiple summaries sequentially, one per buyer.
    6. Keep the output structure exactly as shown in the "Expected output format".
    """

    user_message = f"Input invoices: {json.dumps(invoices_info, indent=2)}"
    data = {
        "model": "deepseek-chat",  
        "messages": [
            {"role": "system", "content": system_message},
            {"role": "user", "content": user_message}
        ],
        "temperature": 0.3,
        "stream": False
    }
    try:
        response = requests.post(DEEPSEEK_URL, headers=DEEP_HEADERS, json=data)
        response.raise_for_status()
        response_data = response.json()
        
        # 记录 token 使用量
        if "usage" in response_data:
            usage = response_data["usage"]
            logger.info(f"Deepseek generate summary token usage - Prompt: {usage.get('prompt_tokens', 'N/A')}, "
                       f"Completion: {usage.get('completion_tokens', 'N/A')}, "
                       f"Total: {usage.get('total_tokens', 'N/A')}")
            logger.info(f"Total: {usage.get('total_tokens', 'N/A')}")
        else:
            logger.warning("No usage information found in Deepseek response")
        
        logger.info(f"Deepseek API response: {response.status_code}")
        return response_data["choices"][0]["message"]["content"]
    except Exception as e:
        logger.exception(f"Generate summary failed: {str(e)}")
        raise