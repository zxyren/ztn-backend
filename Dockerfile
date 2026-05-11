FROM python:3.13-slim

# Install system dependencies in one layer
RUN apt-get update && apt-get install -y \
    ffmpeg gcc libffi-dev ca-certificates \
    build-essential wget xz-utils curl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/* \
    && node -v

# Create non-root user with UID 1000 (required by Hugging Face Spaces)
RUN useradd -m -u 1000 user

WORKDIR /app

# Install Python dependencies
RUN pip install --upgrade pip

COPY --chown=user requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY --chown=user . .

# Switch to non-root user
USER user
ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH

EXPOSE 7860

# gevent worker handles SSE (long-lived streaming) without killing connections
# timeout 0 = no worker timeout so long downloads never get killed mid-way
CMD ["gunicorn", "-w", "1", "-k", "gevent", "--timeout", "0", "-b", "0.0.0.0:7860", "app:app"]