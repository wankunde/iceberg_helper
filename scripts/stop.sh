#!/bin/bash

# Iceberg Metadata Viewer 停止脚本

echo "=========================================="
echo "停止 Iceberg Metadata Viewer 服务"
echo "=========================================="

# 查找并终止 uvicorn 进程
PID=$(lsof -ti:8000 2>/dev/null)

if [ -z "$PID" ]; then
    # 尝试使用 pgrep 查找
    PID=$(pgrep -f "uvicorn app.main:app" 2>/dev/null)
fi

if [ -n "$PID" ]; then
    echo "找到进程 PID: $PID"
    kill $PID
    echo "服务已停止"
else
    echo "未找到运行中的服务"
fi

