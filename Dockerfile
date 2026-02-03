FROM python:3.13.7-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    gcc \
    libffi-dev \
    ca-certificates \
    build-essential \
    wget \
    quickjs \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Install required Python dependencies globally
RUN pip install --upgrade pip
RUN pip install \
    yt-dlp \
    flask \
    flask-cors \
    gunicorn \
    pycryptodomex \
    quickjs \
    websockets \
    brotli \
    certifi

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["gunicorn", "-w", "1", "-k", "gthread", "--threads", "4", "--timeout", "300", "-b", "0.0.0.0:8000", "app:app"]
