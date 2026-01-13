"""Iceberg 元数据解析服务"""
import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.config import AVRO_TOOLS_JAR, JAVA_BIN
from app.services.json_utils import format_json, parse_json_file


def parse_avro_file(file_path: str) -> Dict[str, Any]:
    """
    使用 avro-tools.jar 解析 Avro 文件
    
    Returns:
        dict: 包含解析结果的字典，格式为:
        {
            "success": bool,
            "data": list | dict | None,
            "error": str | None,
            "raw_output": str | None
        }
    """
    if not Path(file_path).exists():
        return {
            "success": False,
            "data": None,
            "error": f"文件不存在: {file_path}",
            "raw_output": None
        }
    
    if not Path(AVRO_TOOLS_JAR).exists():
        return {
            "success": False,
            "data": None,
            "error": f"Avro Tools JAR 不存在: {AVRO_TOOLS_JAR}",
            "raw_output": None
        }
    
    if not Path(JAVA_BIN).exists():
        return {
            "success": False,
            "data": None,
            "error": f"Java 可执行文件不存在: {JAVA_BIN}",
            "raw_output": None
        }
    
    try:
        # 调用 avro-tools.jar tojson 命令
        result = subprocess.run(
            [JAVA_BIN, "-jar", AVRO_TOOLS_JAR, "tojson", file_path],
            capture_output=True,
            text=True,
            timeout=30  # 30秒超时
        )
        
        if result.returncode != 0:
            return {
                "success": False,
                "data": None,
                "error": f"Avro Tools 执行失败: {result.stderr}",
                "raw_output": result.stdout
            }
        
        output = result.stdout.strip()
        if not output:
            return {
                "success": False,
                "data": None,
                "error": "Avro Tools 输出为空",
                "raw_output": None
            }
        
        # 尝试解析 JSON 输出
        # avro-tools tojson 可能输出多行 JSON（每行一个 JSON 对象）
        lines = output.split('\n')
        parsed_data = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                parsed_data.append(json.loads(line))
            except json.JSONDecodeError:
                # 如果单行解析失败，尝试整体解析
                try:
                    parsed_data = json.loads(output)
                    break
                except json.JSONDecodeError:
                    pass
        
        # 如果只有一行，直接返回该对象
        if len(parsed_data) == 1:
            parsed_data = parsed_data[0]
        
        return {
            "success": True,
            "data": parsed_data,
            "error": None,
            "raw_output": output
        }
        
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "data": None,
            "error": "Avro Tools 执行超时（30秒）",
            "raw_output": None
        }
    except Exception as e:
        return {
            "success": False,
            "data": None,
            "error": f"解析过程出错: {str(e)}",
            "raw_output": None
        }


def scan_metadata_directory(metadata_dir: str) -> Dict[str, Any]:
    """
    扫描 Iceberg metadata 目录，分类列出文件
    
    Returns:
        dict: 包含文件分类的字典
    """
    metadata_path = Path(metadata_dir)
    
    if not metadata_path.exists():
        return {
            "success": False,
            "error": f"目录不存在: {metadata_dir}",
            "files": {}
        }
    
    if not metadata_path.is_dir():
        return {
            "success": False,
            "error": f"路径不是目录: {metadata_dir}",
            "files": {}
        }
    
    files = {
        "metadata_files": [],     # *.metadata.json 表元数据文件
        "snapshots": [],          # snap-*.avro 快照文件（包含 manifest_paths）
        "data_avro": [],          # *-m*.avro 或其他数据 avro 文件
        "data_parquet": [],       # partition-stats-*.parquet 或其他 parquet 文件
        "other_files": []         # 其他文件
    }
    
    try:
        for file_path in sorted(metadata_path.iterdir()):
            if file_path.is_dir():
                continue
            
            file_name = file_path.name
            
            # 过滤掉 .crc 文件
            if file_name.endswith(".crc"):
                continue
            
            file_info = {
                "name": file_name,
                "path": str(file_path),
                "size": file_path.stat().st_size
            }
            
            # Metadata 文件：*.metadata.json
            if file_name.endswith(".metadata.json"):
                files["metadata_files"].append(file_info)
            # Snapshot 文件：snap-*.avro
            elif file_name.startswith("snap-") and file_name.endswith(".avro"):
                # 尝试解析 snapshot 文件，提取 manifest_path（失败时返回空列表）
                try:
                    manifest_paths = _extract_manifest_paths_from_snapshot(str(file_path))
                    file_info["manifest_paths"] = manifest_paths
                except Exception:
                    # 解析失败时，设置为空列表，不影响其他文件
                    file_info["manifest_paths"] = []
                files["snapshots"].append(file_info)
            # Data Avro 文件：*-m*.avro 或其他 .avro 数据文件
            elif file_name.endswith(".avro") and ("-m" in file_name or not file_name.startswith("snap-")):
                files["data_avro"].append(file_info)
            # Data Parquet 文件：partition-stats-*.parquet 或其他 .parquet 文件
            elif file_name.endswith(".parquet"):
                files["data_parquet"].append(file_info)
            else:
                files["other_files"].append(file_info)
        
        return {
            "success": True,
            "error": None,
            "files": files,
            "latest_version": _get_latest_version(files["metadata_files"])
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"扫描目录失败: {str(e)}",
            "files": {}
        }


def _extract_manifest_paths_from_snapshot(snapshot_path: str) -> List[str]:
    """
    从 snapshot 文件中提取 manifest_path 列表
    
    Args:
        snapshot_path: snapshot 文件路径
    
    Returns:
        list: manifest_path 列表
    """
    try:
        result = parse_avro_file(snapshot_path)
        if not result["success"] or not result["data"]:
            return []
        
        manifest_paths = []
        data = result["data"]
        
        # 如果 data 是列表，遍历每个元素
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    manifest_path = item.get("manifest_path")
                    if manifest_path:
                        manifest_paths.append(manifest_path)
        # 如果 data 是字典，直接提取
        elif isinstance(data, dict):
            manifest_path = data.get("manifest_path")
            if manifest_path:
                manifest_paths.append(manifest_path)
        
        return manifest_paths
    except Exception:
        return []


def _get_latest_version(metadata_files: List[Dict]) -> Optional[str]:
    """获取最新的 metadata 版本文件名"""
    if not metadata_files:
        return None
    
    # 对于 *.metadata.json 文件，按文件名排序（通常是按序号递增）
    # 文件名格式：00000-xxx.metadata.json, 00001-xxx.metadata.json 等
    try:
        # 提取文件名开头的序号进行排序
        versions = []
        for file_info in metadata_files:
            name = file_info["name"]
            # 提取文件名开头的数字部分
            if "-" in name:
                try:
                    version_part = name.split("-")[0]
                    version_num = int(version_part)
                    versions.append((version_num, file_info))
                except ValueError:
                    versions.append((999999, file_info))  # 无法解析的放在最后
            else:
                versions.append((999999, file_info))
        
        if versions:
            versions.sort(key=lambda x: x[0], reverse=True)
            return versions[0][1]["name"]
        else:
            return metadata_files[-1]["name"]
    except Exception:
        # 如果排序失败，返回最后一个文件
        return metadata_files[-1]["name"] if metadata_files else None


def extract_table_metadata_info(metadata_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    从 table metadata 中提取关键信息
    
    Args:
        metadata_data: 解析后的 table metadata JSON 数据
    
    Returns:
        dict: 包含关键信息的字典
    """
    info = {
        "table_uuid": None,
        "location": None,
        "format_version": None,
        "current_snapshot_id": None,
        "schema": None,
        "partition_spec": None,
        "sort_order": None,
        "snapshots": [],
        "properties": {}
    }
    
    if not isinstance(metadata_data, dict):
        return info
    
    # 提取基本信息
    info["table_uuid"] = metadata_data.get("table-uuid") or metadata_data.get("table_uuid")
    info["location"] = metadata_data.get("location")
    info["format_version"] = metadata_data.get("format-version") or metadata_data.get("format_version")
    info["current_snapshot_id"] = metadata_data.get("current-snapshot-id") or metadata_data.get("current_snapshot_id")
    
    # 提取 schema
    schema = metadata_data.get("schema") or metadata_data.get("schemas", [{}])[0] if metadata_data.get("schemas") else {}
    if schema:
        info["schema"] = {
            "type": schema.get("type"),
            "schema_id": schema.get("schema-id") or schema.get("schema_id"),
            "fields": schema.get("fields", [])
        }
    
    # 提取 partition spec
    partition_specs = metadata_data.get("partition-specs") or metadata_data.get("partition_specs", [])
    if partition_specs:
        info["partition_spec"] = partition_specs[0] if partition_specs else {}
    
    # 提取 sort order
    sort_orders = metadata_data.get("sort-orders") or metadata_data.get("sort_orders", [])
    if sort_orders:
        info["sort_order"] = sort_orders[0] if sort_orders else {}
    
    # 提取 snapshots
    snapshots = metadata_data.get("snapshots", [])
    info["snapshots"] = snapshots
    
    # 提取 properties
    info["properties"] = metadata_data.get("properties", {})
    
    return info

