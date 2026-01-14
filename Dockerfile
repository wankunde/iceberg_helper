# syntax=docker/dockerfile:1.6

FROM python:3.11-slim AS builder

ENV PIP_NO_CACHE_DIR=1
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      libsnappy-dev \
      libzstd-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt

# Pre-download all deps as wheels (for offline install in runtime stage)
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip \
    && pip wheel -r /app/requirements.txt -w /wheels


FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_HOST=0.0.0.0 \
    APP_PORT=8001

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      libsnappy-dev \
      libzstd-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
COPY --from=builder /wheels /wheels

# Offline install from pre-downloaded wheels
RUN pip install --upgrade pip \
    && pip install --no-index --find-links=/wheels -r /app/requirements.txt

COPY app /app/app

EXPOSE 8001
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
