"""JSON 格式化工具"""
import json
from typing import Any


def format_json(data: Any, indent: int = 2, ensure_ascii: bool = False) -> str:
    """格式化 JSON 数据为字符串"""
    try:
        return json.dumps(data, indent=indent, ensure_ascii=ensure_ascii, sort_keys=False)
    except (TypeError, ValueError) as e:
        raise ValueError(f"无法格式化 JSON: {e}")


def parse_json_file(file_path: str) -> dict | list:
    """读取并解析 JSON 文件"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                return {}
            return json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON 解析错误: {e}")
    except FileNotFoundError:
        raise FileNotFoundError(f"文件不存在: {file_path}")
    except Exception as e:
        raise RuntimeError(f"读取文件失败: {e}")

