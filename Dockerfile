FROM python:3.10

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

COPY Load_Data/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
# Thư mục Load_Data
COPY Load_Data/ /app/Load_Data/
# Thư mục Data
COPY Data/ /app/Data/

CMD ["python", "/app/Load_Data/Load_data.py"]
