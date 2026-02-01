FROM python:3.10-slim

# Install system dependencies that yt-dlp needs
RUN apt-get update && \
    apt-get install -y \
    ffmpeg \
    wget \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /tmp/downloads

EXPOSE 5000

CMD gunicorn backend:app --bind 0.0.0.0:$PORT --workers 2 --timeout 300