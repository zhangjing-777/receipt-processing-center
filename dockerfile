# 安装 Python 和依赖
FROM python:3.11-slim

# 设定工作目录
WORKDIR /app

# 安装必要工具（playwright 依赖）
RUN apt-get update && apt-get install -y wget gnupg curl libglib2.0-0 libnss3 libgdk-pixbuf2.0-0 libgtk-3-0 libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 libgbm1 libasound2 libxshmfence1 xvfb

# 拷贝依赖
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# 安装浏览器（这一步不能漏）
RUN playwright install --with-deps

# 拷贝代码 
COPY . .

EXPOSE 8000

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

