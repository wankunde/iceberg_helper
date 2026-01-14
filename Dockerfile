# syntax=docker/dockerfile:1.6

FROM python:3.12-slim

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    APP_HOST=0.0.0.0 \
    APP_PORT=8001

ARG PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ARG PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY
ENV PIP_INDEX_URL=${PIP_INDEX_URL} \
    PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST} \
    HTTP_PROXY=${HTTP_PROXY} \
    HTTPS_PROXY=${HTTPS_PROXY} \
    NO_PROXY=${NO_PROXY}

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      libsnappy-dev \
      libzstd-dev \
    && rm -rf /var/lib/apt/lists/*

# Create venv and use it for everything
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

COPY requirements.txt /app/requirements.txt

# Download wheels first (with progress), then install offline from wheels
RUN --mount=type=cache,target=/root/.cache/pip \
    pip wheel -i "${PIP_INDEX_URL}" --progress-bar on -r /app/requirements.txt -w /wheels \
    && pip install --no-index --find-links=/wheels -r /app/requirements.txt

COPY app /app/app

EXPOSE 8001
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
