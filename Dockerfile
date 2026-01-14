FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_HOST=0.0.0.0 \
    APP_PORT=8000

WORKDIR /app

# System deps:
# - libsnappy-dev, libzstd-dev: for python-snappy / zstandard wheels/runtime linkage
# - build-essential: some environments may still need it for limited packages
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      libsnappy-dev \
      libzstd-dev \
    && rm -rf /var/lib/apt/lists/*

# Install python deps first for better layer caching
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

# Copy application code
COPY app /app/app

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
