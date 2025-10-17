# Receipt Processing Center

A comprehensive receipt and invoice processing system that automatically extracts, processes, and manages receipts from multiple sources including email (AWS SES) and direct file uploads. The system uses AI-powered OCR and natural language processing to extract structured data from receipts and generate intelligent reimbursement summaries.

## 🌟 Features

### Core Capabilities
- **Multi-source Receipt Processing**: Accept receipts from AWS SES emails, web uploads, and direct file submissions
- **AI-Powered OCR**: Extract text from images (JPG, PNG) and PDF documents using advanced vision models
- **Intelligent Data Extraction**: Automatically extract structured invoice fields (date, amount, buyer, seller, category, etc.)
- **Data Encryption**: End-to-end encryption for sensitive fields using Fernet symmetric encryption
- **Receipt Management**: Full CRUD operations on receipt records with time-based queries
- **Smart Summarization**: AI-generated reimbursement summaries grouped by buyer and category
- **Quota Management**: Monthly usage quota tracking and automatic reset
- **ZIP Download**: Bulk download receipts with organized file structure and summary reports

### Technical Features
- **RESTful API**: FastAPI-based high-performance REST API
- **Database**: Supabase (PostgreSQL) with encrypted storage
- **File Storage**: Supabase Storage with signed URL generation
- **Containerization**: Docker deployment ready with docker-compose
- **Logging**: Comprehensive logging with daily rotation
- **Health Monitoring**: Health check endpoints for service monitoring

## 🏗️ Architecture

### System Components

```
┌─────────────────────────────────────────────────────────────────┐
│                     Receipt Processing Center                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  AWS SES     │  │  Web Upload  │  │  Direct API  │          │
│  │  Email Input │  │  Interface   │  │  Submission  │          │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘          │
│         │                  │                  │                   │
│         └──────────────────┴──────────────────┘                  │
│                            │                                      │
│                            ▼                                      │
│                    ┌───────────────┐                             │
│                    │  Quota Check  │                             │
│                    └───────┬───────┘                             │
│                            │                                      │
│                            ▼                                      │
│         ┌──────────────────────────────────┐                    │
│         │    Receipt Processing Engine     │                    │
│         │  ┌────────────┐  ┌────────────┐  │                    │
│         │  │ OCR Engine │  │ AI Extract │  │                    │
│         │  │ (Vision)   │  │ (Deepseek) │  │                    │
│         │  └────────────┘  └────────────┘  │                    │
│         └──────────────┬───────────────────┘                    │
│                        │                                          │
│                        ▼                                          │
│         ┌──────────────────────────────────┐                    │
│         │    Encryption & Storage Layer    │                    │
│         │  ┌────────────┐  ┌────────────┐  │                    │
│         │  │  Supabase  │  │  Storage   │  │                    │
│         │  │  Database  │  │  Bucket    │  │                    │
│         │  └────────────┘  └────────────┘  │                    │
│         └──────────────────────────────────┘                    │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### Module Structure

- **`core/`**: Core utilities and shared functionality
  - `ocr.py`: OCR processing for images and PDFs
  - `generation.py`: AI-powered field extraction and summary generation
  - `encryption.py`: Data encryption/decryption for sensitive fields
  - `quota.py`: Usage quota management and tracking
  - `upload_files.py`: File upload utilities
  - `utils.py`: Utility functions (filename sanitization, JSON parsing)

- **`ses_eml_save/`**: AWS SES email processing module
  - `routers.py`: API endpoints for SES webhook
  - `services.py`: Email processing orchestration
  - `eml_parser.py`: Email parsing and extraction
  - `upload_attachment.py`: Attachment upload handler
  - `upload_link.py`: PDF link extraction and download
  - `upload_string_to_image.py`: HTML to image conversion
  - `insert_data.py`: Data preparation and insertion

- **`rcpdro_web_save/`**: Web upload processing module
  - `routers.py`: API endpoints for web uploads
  - `services.py`: Upload processing logic
  - `insert_data.py`: Data insertion utilities

- **`table_processor/`**: Database table management
  - `receipt_items_en_router.py`: Receipt CRUD operations
  - `receipt_summary_zip_en_router.py`: Summary generation and ZIP download
  - `ses_eml_info_en_router.py`: Email info management
  - `receipt_items_en_upload_result_router.py`: Upload result tracking

- **`summary_download/`**: Summary and report generation
  - `routers.py`: Summary generation endpoints
  - `services.py`: Summary processing logic
  - `download_zip.py`: ZIP file generation
  - `normalizing.py`: Data normalization and aggregation

## 🚀 Installation

### Prerequisites

- Python 3.11+
- Docker & Docker Compose (for containerized deployment)
- Supabase account (or self-hosted Supabase)
- OpenRouter API key (for OCR)
- Deepseek API key (for AI extraction)

### Local Development Setup

1. **Clone the repository**
```bash
git clone <repository-url>
cd receipt-processing-center
```

2. **Create virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
playwright install --with-deps  # Install browser for HTML rendering
```

4. **Configure environment variables**
Create a `.env` file in the root directory:

```env
# Supabase Configuration
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key
SUPABASE_BUCKET=your-bucket-name

# Encryption Keys
ENCRYPTION_KEY=your-base64-encoded-fernet-key
ENC_KEY=your-database-encryption-key
HMAC_KEY=your-database-hmac-key

# AI API Configuration
OPENROUTER_API_KEY=your-openrouter-api-key
OPENROUTER_URL=https://openrouter.ai/api/v1/chat/completions
MODEL=openai/gpt-4-vision-preview
MODEL_FREE=google/gemini-flash-1.5-8b

# Deepseek Configuration
DEEPSEEK_API_KEY=your-deepseek-api-key
DEEPSEEK_URL=https://api.deepseek.com/v1/chat/completions

# AWS Configuration (for SES email fetching)
AWS_ACCESS_KEY_ID=your-aws-access-key
AWS_SECRET_ACCESS_KEY=your-aws-secret-key
AWS_REGION=us-east-1
```

5. **Run the application**
```bash
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

The API will be available at `http://localhost:8000`

### Docker Deployment

1. **Build and run with Docker Compose**
```bash
docker-compose up -d
```

The service will be available at `http://localhost:8005`

2. **View logs**
```bash
docker-compose logs -f receipt-processing-center
```

3. **Stop the service**
```bash
docker-compose down
```

## 📚 API Documentation

### Health Check
```
GET /health
```
Returns service health status.

### AWS SES Email Processing

#### Transfer Email from S3
```
POST /ses-eml-save/ses-email-transfer
```
**Parameters:**
- `bucket` (string): S3 bucket name
- `key` (string): S3 object key
- `user_id` (string): User identifier

**Response:**
```json
{
  "status": "success",
  "message": "You uploaded a total of 3 files: 3 succeeded--[file1.pdf, file2.jpg, file3.png], 0 failed--[]."
}
```

### Web Upload Processing

#### Upload Receipts
```
POST /receiptdrop-web-save/receiptdrop-transfer
```
**Parameters:**
- `user_id` (string, form): User identifier
- `files` (array[file], form): Receipt files (images or PDFs)

**Response:**
```json
{
  "status": "success",
  "uploaded_count": 3,
  "message": "Files processed successfully"
}
```

### Receipt Management

#### Get Receipt Items
```
POST /receipt-items-en/get-receipt-items
```
**Request Body:**
```json
{
  "user_id": "user-uuid",
  "ind": 123,  // Optional: exact record ID
  "start_time": "2025-01-01",  // Optional: YYYY-MM-DD
  "end_time": "2025-01-31",    // Optional: YYYY-MM-DD
  "limit": 10,
  "offset": 0
}
```

**Response:**
```json
[
  {
    "ind": 123,
    "user_id": "user-uuid",
    "invoice_number": "INV-001",
    "invoice_date": "2025-01-15",
    "buyer": "Acme Corp",
    "seller": "Widget Inc",
    "category": "Office Supplies",
    "invoice_total": 1234.56,
    "currency": "USD",
    "address": "123 Main St",
    "file_url": "https://signed-url...",
    "create_time": "2025-01-15T10:30:00Z"
  }
]
```

#### Update Receipt Items
```
POST /receipt-items-en/update-receipt-items
```
**Request Body:**
```json
{
  "ind": 123,
  "user_id": "user-uuid",
  "buyer": "Updated Buyer",
  "category": "Updated Category",
  "invoice_total": 999.99
}
```

#### Update File URL
```
POST /receipt-items-en/update-file-url
```
**Parameters (multipart/form-data):**
- `user_id` (string): User identifier
- `ind` (integer): Record ID
- `file` (file): New receipt file

#### Delete Receipt Items
```
DELETE /receipt-items-en/delete-receipt-items
```
**Request Body:**
```json
{
  "user_id": "user-uuid",
  "inds": [123, 124, 125]
}
```

### Summary Generation

#### Generate Summary with AI
```
POST /summary_download/summary-download-ai
```
**Parameters:**
- `user_id` (string): User identifier
- `title` (string): Report title
- `invoices` (array[dict]): Invoice data array

**Response:**
```json
{
  "summary": "✅ Your business travel reimbursement summary...",
  "download_url": "https://signed-url-to-zip..."
}
```

#### Generate Summary without AI
```
POST /summary_download/summary-download
```
Same parameters as above, but without AI-generated descriptions.

### Email Info Management

#### Get Email Info
```
POST /ses-eml-info-en/get-eml-info
```

#### Update Email Info
```
POST /ses-eml-info-en/update-eml-info
```

#### Delete Email Info
```
DELETE /ses-eml-info-en/delete-eml-info
```

### Summary ZIP Management

#### Get Summary ZIP
```
POST /receipt-summary-zip-en/get-summary-zip
```

#### Delete Summary ZIP
```
DELETE /receipt-summary-zip-en/delete-summary-zip
```

### Upload Result Tracking

#### Get Upload Result
```
POST /receipt-items-en-upload-result/get-upload-result
```

## 🔐 Security Features

### Data Encryption
All sensitive fields are encrypted before storage using Fernet symmetric encryption:

**Encrypted fields in `receipt_items_en`:**
- buyer, seller, address, file_url, invoice_number, original_info, ocr

**Encrypted fields in `ses_eml_info_en`:**
- from, to, s3_eml_url, buyer, seller

**Encrypted fields in `receipt_summary_zip_en`:**
- summary_content, title, download_url

### File Storage Security
- Files stored in Supabase Storage with path encryption
- Signed URLs with 24-hour expiration for file access
- User-based path isolation

### Quota Management
- Monthly usage limits enforced
- Automatic quota reset on new month
- Different quota types for receipts and summary requests

## 📊 Logging

Logs are stored in the `logs/` directory with daily rotation:
- Filename format: `app_YYYYMMDD.log`
- Log level: INFO
- Includes timestamp, module name, level, and message

## 📝 Database Schema

### Main Tables

**receipt_items_en**: Stores processed receipt information
- `ind` (serial primary key): Auto-increment ID
- `id` (uuid): Unique identifier
- `user_id` (uuid): User reference
- `invoice_number`, `invoice_date`, `buyer`, `seller` (encrypted)
- `category`, `invoice_total`, `currency`
- `address`, `file_url` (encrypted)
- `original_info`, `ocr` (encrypted): Raw data
- `hash_id`: Deduplication hash
- `create_time`: Timestamp

**ses_eml_info_en**: Stores email metadata
- `id` (uuid): Links to receipt_items_en
- `user_id` (uuid)
- `from`, `to`, `s3_eml_url` (encrypted)
- `buyer`, `seller` (encrypted)
- `invoice_date`, `create_time`

**receipt_summary_zip_en**: Stores generated summaries
- `user_id` (uuid)
- `title`, `summary_content` (encrypted)
- `download_url` (encrypted): ZIP file location
- `create_time`

**Quota Tables:**
- `receipt_usage_quota_receipt_en`: Receipt processing quota
- `receipt_usage_quota_request_en`: Summary generation quota

## 🔧 Configuration

### OCR Model Selection
The system uses a fallback mechanism:
1. Try `MODEL_FREE` first (cost-effective)
2. Fall back to `MODEL` if free model fails

### Supported File Types
- **Images**: JPG, JPEG, PNG
- **Documents**: PDF

### HTML to Image Conversion
For emails without attachments or PDF links, the system:
1. Uses Playwright to render HTML
2. Takes a screenshot
3. Uploads as an image for OCR processing

## 📦 Dependencies

Key dependencies (see `requirements.txt` for full list):
- `fastapi` & `uvicorn`: Web framework
- `supabase`: Database and storage client
- `boto3`: AWS S3 integration
- `playwright`: HTML rendering
- `beautifulsoup4`: HTML parsing
- `mail-parser`: Email parsing
- `cryptography`: Encryption
- `pypinyin`: Chinese filename handling
- `pydantic`: Data validation


**Version**: 1.0.0  
**Last Updated**: September 2025

后续计划：

如果用户数量增多，速度变慢，可以考虑将ocr，generation这些运行慢的，后台执行。

多 worker（2个够了）
后台任务队列（先观察）
分布式部署（等规模大了再说）