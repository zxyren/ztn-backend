FROM python:3.13.17-slim

RUN apt-get update && apt-get install -y \
    ffmpeg gcc libffi-dev ca-certificates \
    build-essential wget quickjs xz-utils \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y curl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && node -v

RUN pip install --upgrade pip
RUN pip install \
    --upgrade yt-dlp \
    gallery-dl \
    flask \
    flask-cors \
    gunicorn \
    gevent \
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

# gevent worker handles SSE (long-lived streaming) without killing connections
# timeout 0 = no worker timeout so long downloads never get killed mid-way
CMD ["gunicorn", "-w", "1", "-k", "gevent", "--timeout", "0", "-b", "0.0.0.0:8000", "app:app"]