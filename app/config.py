"""应用配置文件"""
import os
from pathlib import Path

# 默认端口
DEFAULT_PORT = 8001

# 默认主机
DEFAULT_HOST = "127.0.0.1"

# 项目根目录
PROJECT_ROOT = Path(__file__).parent.parent

# 模板目录
TEMPLATES_DIR = PROJECT_ROOT / "app" / "templates"

# 静态文件目录
STATIC_DIR = PROJECT_ROOT / "app" / "static"

# 默认 Metadata 目录（从环境变量读取）
DEFAULT_METADATA_DIR = os.getenv("DEFAULT_METADATA_DIR", "")
