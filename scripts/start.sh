#!/bin/bash

# Iceberg Metadata Viewer 启动脚本
# 用法: ./start.sh [table root目录路径]

set -e

# 获取脚本所在目录的父目录（项目根目录）
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$PROJECT_ROOT"

echo "=========================================="
echo "Iceberg Metadata Viewer 启动脚本"
echo "=========================================="

# 检查是否传入了 table root 目录路径
DEFAULT_TABLE_ROOT=""
if [ -n "$1" ]; then
    DEFAULT_TABLE_ROOT="$1"
    echo "默认 Table Root: $DEFAULT_TABLE_ROOT"

    # 验证路径是否存在
    if [ ! -d "$DEFAULT_TABLE_ROOT" ]; then
        echo "警告: 指定的目录不存在: $DEFAULT_TABLE_ROOT"
        echo "将使用空值，您可以在页面中手动输入路径"
    fi
else
    echo "提示: 可以通过参数指定默认 Table Root"
    echo "用法: ./start.sh /path/to/table_root"
fi

# 检查 Python 版本
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
echo "检测到 Python 版本: $PYTHON_VERSION"

# 检查虚拟环境
VENV_DIR="$PROJECT_ROOT/.venv"

if [ ! -d "$VENV_DIR" ]; then
    echo "创建虚拟环境..."
    python3 -m venv "$VENV_DIR"
fi

# 激活虚拟环境
echo "激活虚拟环境..."
source "$VENV_DIR/bin/activate"

# 升级 pip
echo "升级 pip..."
pip install --upgrade pip -q

# 安装依赖
echo "安装依赖..."
pip install -r requirements.txt -q

# 启动服务
echo "=========================================="
echo "启动服务..."
echo "访问地址: http://127.0.0.1:8001"
if [ -n "$DEFAULT_TABLE_ROOT" ]; then
    echo "默认 Table Root: $DEFAULT_TABLE_ROOT"
fi
echo "按 Ctrl+C 停止服务"
echo "=========================================="

# 通过环境变量传递默认 table root
export DEFAULT_TABLE_ROOT="$DEFAULT_TABLE_ROOT"

uvicorn app.main:app --reload --host 127.0.0.1 --port 8001
