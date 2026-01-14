#!/bin/bash

# Iceberg Metadata Viewer 启动脚本
# 用法: ./start.sh [metadata目录路径]

set -e

# 获取脚本所在目录的父目录（项目根目录）
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

cd "$PROJECT_ROOT"

echo "=========================================="
echo "Iceberg Metadata Viewer 启动脚本"
echo "=========================================="

# 检查是否传入了 metadata 目录路径
DEFAULT_METADATA_DIR=""
if [ -n "$1" ]; then
    DEFAULT_METADATA_DIR="$1"
    echo "默认 Metadata 目录: $DEFAULT_METADATA_DIR"
    
    # 验证路径是否存在
    if [ ! -d "$DEFAULT_METADATA_DIR" ]; then
        echo "警告: 指定的目录不存在: $DEFAULT_METADATA_DIR"
        echo "将使用空值，您可以在页面中手动输入路径"
    fi
else
    echo "提示: 可以通过参数指定默认 Metadata 目录"
    echo "用法: ./start.sh /path/to/metadata"
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
echo "访问地址: http://127.0.0.1:8000"
if [ -n "$DEFAULT_METADATA_DIR" ]; then
    echo "默认 Metadata 目录: $DEFAULT_METADATA_DIR"
fi
echo "按 Ctrl+C 停止服务"
echo "=========================================="

# 通过环境变量传递默认 metadata 目录
export DEFAULT_METADATA_DIR="$DEFAULT_METADATA_DIR"

uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
