FROM python:3.11-slim

WORKDIR /app

# Copy requirements trước
COPY Load_Data/requirements.txt .

# Cài dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code sau khi install dependencies
COPY Load_Data/ /app/Load_Data/

# CMD chạy script
CMD ["python", "/app/Load_Data/Load_data.py"]
