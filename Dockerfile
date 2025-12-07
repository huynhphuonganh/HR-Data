FROM python:3.10-slim

WORKDIR /app

# Cài đặt dependencies hệ thống cần thiết
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip, setuptools, wheel
RUN pip install --upgrade pip setuptools wheel

# Copy requirements trước
COPY Load_Data/requirements.txt .

# Cài dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code sau khi install dependencies
COPY Load_Data/ /app/Load_Data/

# CMD chạy script
CMD ["python", "/app/Load_Data/Load_data.py"]
