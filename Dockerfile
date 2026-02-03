FROM python:3.13.7-slim

# Install system dependencies including proper JS runtime
RUN apt-get update && apt-get install -y \
    ffmpeg \
    gcc \
    libffi-dev \
    ca-certificates \
    build-essential \
    wget \
    curl \
    xz-utils \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip

# Install Python dependencies (order matters for curl-cffi)
RUN pip install --no-cache-dir \
    certifi \
    brotli \
    websockets \
    pycryptodomex \
    curl-cffi \
    yt-dlp \
    flask \
    flask-cors \
    gunicorn

WORKDIR /app

COPY cookies.txt /cookies.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["gunicorn", "-w", "2", "-k", "gthread", "--threads", "4", "--timeout", "300", "-b", "0.0.0.0:8000", "app:app"]