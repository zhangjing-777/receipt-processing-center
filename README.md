# Receipt Email Processing & OCR System

一个基于 FastAPI 的智能邮件处理系统，专门用于**自动接收 AWS SES 邮件通知**，解析邮件内容，提取附件，进行 OCR 识别，并将发票信息结构化存储到 Supabase 数据库。

融入了原n8n流程：Delete rawcleaned table/ webhook/swap_buyer_seller/ uploads_name_change
---

## 🚀 核心功能

### 📧 邮件处理
- **自动接收 AWS SES 邮件通知**，支持 S3 存储的邮件文件
- **智能邮件解析**：提取邮件主题、正文、附件
- **多格式附件支持**：PDF、图片（PNG、JPG等）
- **HTML 内容处理**：将邮件正文转换为图片存储

### 🔍 OCR 智能识别
- **多模型 OCR 支持**：
  - 优先使用免费模型（`MODEL_FREE`）
  - 失败时自动回退到付费模型（`MODEL`）
- **图像 OCR**：支持各种图片格式的文本识别
- **PDF OCR**：支持 PDF 文档的文本提取
- **发票字段提取**：自动识别发票号、日期、金额、买卖方等关键信息

### 📁 文件存储
- **Supabase Storage 集成**：自动上传文件到云存储
- **多种上传方式**：
  - 直接附件上传
  - PDF 链接下载上传
  - HTML 转图片上传
- **智能文件命名**：基于时间戳和 UUID 的唯一文件名

### 🗄️ 数据管理
- **结构化数据存储**：发票信息存入 `receipt_items_cleaned` 表
- **邮件信息记录**：邮件元数据存入 `ses_eml_info` 表
- **处理结果追踪**：上传结果存入 `receipt_items_upload_result` 表

### 📊 监控与日志
- **详细日志记录**：每个处理步骤都有完整的日志
- **错误追踪**：详细的异常信息和错误上下文
- **性能监控**：文件大小、处理时间、Token 使用量等指标
- **健康检查**：系统状态监控接口

---

## 🏗️ 系统架构

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   AWS SES/S3    │───▶│   FastAPI App   │───▶│  Supabase DB    │
│   (邮件源)       │    │   (处理引擎)     │    │   (数据存储)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │  OCR Services   │
                       │ (OpenRouter/    │
                       │  DeepSeek)      │
                       └─────────────────┘
```

---

## 📁 项目结构

```
receipt-ses-email-transfer/
├── app.py                          # FastAPI 主应用入口
├── requirements.txt                # Python 依赖包
├── dockerfile                      # Docker 构建文件
├── docker-compose.yml              # Docker Compose 配置
├── README.md                       # 项目说明文档
├── ses_eml_save/                   # 核心处理模块
│   ├── main.py                     # 主处理流程
│   ├── eml_parser.py               # 邮件解析器
│   ├── ocr.py                      # OCR 识别服务
│   ├── attachment_upload.py        # 附件上传处理
│   ├── link_upload.py              # 链接文件上传
│   ├── string_to_image_upload.py   # HTML 转图片上传
│   ├── insert_data.py              # 数据插入处理
│   └── util.py                     # 工具函数
├── logs/                           # 日志文件目录
└── venv/                           # Python 虚拟环境
```

---

## ⚙️ 环境配置

### 必需的环境变量

创建 `.env` 文件并配置以下变量：

```env
# Supabase 配置
SUPABASE_URL=你的supabase项目url
SUPABASE_SERVICE_ROLE_KEY=你的supabase服务密钥
OPENROUTER_API_KEY=你的OpenRouter大模型API Key
OPENROUTER_URL=你的OpenRouter API地址
MODEL=你的大模型名称
DEEPSEEK_URL=你的Deepseek API地址
DEEPSEEK_API_KEY=你的Deepseek API Key
AWS_REGION=你的AWS区域
AWS_ACCESS_KEY_ID=你的AWS访问ID
AWS_SECRET_ACCESS_KEY=你的AWS密钥
```

---

## 🚀 快速部署

### 方法一：本地运行

```bash
# 1. 克隆项目
git clone <repository-url>
cd receipt-ses-email-transfer

# 2. 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或 venv\Scripts\activate  # Windows

# 3. 安装依赖
pip install -r requirements.txt

# 4. 配置环境变量
cp .env.example .env
# 编辑 .env 文件，填入你的配置

# 5. 启动服务
uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

### 方法二：Docker 部署

```bash
# 1. 构建镜像
docker build -t receipt-email-processor .

# 2. 启动服务
docker-compose up --build -d

# 3. 查看日志
docker-compose logs -f
```

### 方法三：Docker Compose 一键部署

```bash
# 直接启动所有服务
docker-compose up --build
```

---

## 🔌 API 接口

### 1. 健康检查
```http
GET /health
```
**响应示例：**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00.123456"
}
```

### 2. 邮件处理 Webhook
```http
POST /webhook/ses-email-transfer
```
**请求参数：**
```json
{
  "bucket": "your-s3-bucket",
  "key": "path/to/email.eml",
  "user_id": "user123"
}
```
**响应示例：**
```json
{
  "message": "Email processed successfully",
  "result": "You uploaded a total of 2 files: 2 succeeded--['invoice1.pdf', 'invoice2.pdf'], 0 failed--[]",
  "status": "success"
}
```

---

## 🔄 处理流程

### 1. 邮件接收与解析
- 接收 AWS SES 邮件通知
- 从 S3 下载邮件文件
- 解析邮件内容（主题、正文、附件）

### 2. 文件处理策略
```
邮件附件 → 直接上传到 Supabase Storage
    ↓
无附件但有PDF链接 → 下载PDF并上传
    ↓
无附件无链接 → HTML正文转图片上传
```

### 3. OCR 识别流程
```
文件上传 → 模型选择 → OCR处理 → 字段提取 → 数据存储
    ↓
优先使用免费模型 → 失败时回退到付费模型
```

### 4. 数据存储
- **发票数据** → `receipt_items_cleaned` 表
- **邮件信息** → `ses_eml_info` 表  
- **处理结果** → `receipt_items_upload_result` 表

---

## 📊 日志系统

### 日志级别
- **INFO**: 正常操作流程
- **WARNING**: 警告信息（如模型回退）
- **ERROR**: 错误信息
- **EXCEPTION**: 异常详情

### 日志内容
- 文件处理进度和状态
- OCR 模型使用情况和 Token 消耗
- 数据库操作结果
- 错误追踪和调试信息

### 日志位置
- **控制台输出**: 实时查看
- **文件存储**: `logs/app_YYYYMMDD.log`

---

## 🔧 故障排除

### 常见问题

1. **OCR 识别失败**
   - 检查 OpenRouter API Key 配置
   - 确认模型名称正确
   - 查看网络连接状态

2. **文件上传失败**
   - 验证 Supabase 配置
   - 检查存储桶权限
   - 确认文件格式支持

3. **数据库插入失败**
   - 检查表结构是否正确
   - 验证数据库连接
   - 查看字段格式要求

### 调试技巧

```bash
# 查看实时日志
docker-compose logs -f

# 检查服务状态
curl http://localhost:8000/health

# 查看详细错误信息
tail -f logs/app_$(date +%Y%m%d).log
```

---

## 🤝 贡献指南

1. Fork 项目
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 打开 Pull Request

---

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

---

## 📞 支持

如有问题或建议，请通过以下方式联系：

- 提交 Issue
- 发送邮件至项目维护者
- 查看项目文档和 Wiki

---

**注意**: 请确保在生产环境中正确配置所有环境变量，并定期备份重要数据。

---
