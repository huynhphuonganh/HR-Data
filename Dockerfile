# Sử dụng Python 3.11
FROM python:3.11-slim

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Copy toàn bộ thư mục Load Data vào container
COPY Load_Data/ /app/Load_Data/

# Cài dependencies cần thiết
RUN pip install --no-cache-dir pandas asyncpg python-dotenv

# Chạy Load_data.py khi container start
CMD ["python", "/app/Load_Data/Load_data.py"]