# syntax=docker/dockerfile:1.6

FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1
ARG PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ARG PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn
ENV PIP_INDEX_URL=${PIP_INDEX_URL} \
    PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST}

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      libsnappy-dev \
      libzstd-dev \
    && rm -rf /var/lib/apt/lists/*

# NEW: venv for builder
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

COPY requirements.txt /app/requirements.txt

# Pre-download all deps as wheels (for offline install in runtime stage)
RUN --mount=type=cache,target=/root/.cache/pip \
    pip wheel -r /app/requirements.txt -w /wheels


FROM python:3.12-slim AS runtime

ARG PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ARG PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn
ENV PIP_INDEX_URL=${PIP_INDEX_URL} \
    PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST} \
    PYTHONDONTWRITEBYTECODE=1 \
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

# NEW: venv for runtime (actual app runs inside it)
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

COPY requirements.txt /app/requirements.txt
COPY --from=builder /wheels /wheels

# Offline install from pre-downloaded wheels into venv
RUN pip install --no-index --find-links=/wheels -r /app/requirements.txt

COPY app /app/app

EXPOSE 8001
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
