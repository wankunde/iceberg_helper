"""应用配置文件"""
import os
from pathlib import Path

# Avro Tools JAR 路径（写死）
AVRO_TOOLS_JAR = "/Users/wankun/apps/avro-tools-1.11.4.jar"

# Java 可执行文件路径（写死）
JAVA_BIN = "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home/bin/java"

# 默认端口
DEFAULT_PORT = 8000

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

