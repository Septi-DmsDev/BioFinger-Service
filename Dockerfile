FROM python:3.11-slim

WORKDIR /app

ENV TZ=Asia/Jakarta

# Install dependencies & tzdata
RUN apt-get update && apt-get install -y --no-install-recommends tzdata && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy script
COPY main.py .

# Folder persistent untuk SQLite
RUN mkdir -p /var/lib/adms-receiver

EXPOSE 8000

CMD ["uvicorn", "main:app_router", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]
