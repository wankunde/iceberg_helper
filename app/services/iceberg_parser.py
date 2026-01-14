"""Iceberg 元数据解析服务"""
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from typing import Tuple
from app.services.json_utils import format_json, parse_json_file


def _bytes_to_text(b: bytes) -> str:
    # 尽量可读；不可读则 base64
    try:
        return b.decode("utf-8")
    except Exception:
        import base64
        return "base64:" + base64.b64encode(b).decode("ascii")


def make_json_safe(obj: Any) -> Any:
    """
    把 Avro/Arrow 等结构转换成可 JSON 序列化的数据：
    - bytes -> str (utf-8 or base64:...)
    - dict/list/tuple/set -> 递归
    - 其他原样返回（FastAPI/JSON 通常可处理 int/float/bool/None/str）
    """
    if obj is None:
        return None
    if isinstance(obj, bytes):
        return _bytes_to_text(obj)
    if isinstance(obj, dict):
        # key 也确保是 str
        return {str(k): make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [make_json_safe(x) for x in obj]
    if isinstance(obj, set):
        return [make_json_safe(x) for x in obj]
    return obj


def parse_avro_file(file_path: str) -> Dict[str, Any]:
    """
    使用 fastavro 解析 Avro 容器文件，返回 Python 数据结构
    """
    try:
        # 统一处理 file: 前缀，避免各处重复处理/漏处理
        raw = file_path or ""
        actual_path = raw.replace("file:", "", 1) if raw.startswith("file:") else raw
        p = Path(actual_path)

        if not p.exists():
            return {
                "success": False,
                "data": None,
                "error": f"文件不存在: {file_path} (resolved: {actual_path})",
                "raw_output": None
            }

        from fastavro import reader
        records: List[Any] = []
        with p.open("rb") as fo:
            for rec in reader(fo):
                records.append(rec)

        data: Any = records[0] if len(records) == 1 else records
        data = make_json_safe(data)  # <-- 关键：避免 bytes 导致 JSON 序列化失败

        return {
            "success": True,
            "data": data,
            "error": None,
            "raw_output": None
        }
    except Exception as e:
        msg = str(e)
        hints: List[str] = []

        # 针对常见编解码器缺失给出明确提示（不要声称“已添加到 requirements.txt”）
        low = msg.lower()
        if "snappy" in low:
            hints.append("可能缺少 python-snappy（pip install python-snappy）或系统 snappy 库")
        if "zstandard" in low or "zstd" in low:
            hints.append("可能缺少 zstandard（pip install zstandard）")

        hint_text = f"；{'；'.join(hints)}" if hints else ""
        return {
            "success": False,
            "data": None,
            "error": f"解析 Avro 失败: {msg}{hint_text}",
            "raw_output": {
                "exception": repr(e),
            },
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


def extract_current_snapshot_manifests(metadata_data: Dict[str, Any]) -> Dict[str, Any]:
    current_snapshot_id = None
    manifest_list = None
    manifest_paths: List[str] = []
    current_snapshot_obj = None
    manifest_list_error: Optional[str] = None

    if isinstance(metadata_data, dict):
        current_snapshot_id = metadata_data.get("current-snapshot-id") or metadata_data.get("current_snapshot_id")
        snapshots = metadata_data.get("snapshots") or []
        if current_snapshot_id and isinstance(snapshots, list):
            for s in snapshots:
                if isinstance(s, dict):
                    sid = s.get("snapshot-id") or s.get("snapshot_id")
                    if sid == current_snapshot_id:
                        current_snapshot_obj = s
                        manifest_list = s.get("manifest-list") or s.get("manifest_list")
                        break

        if manifest_list:
            actual_path = str(manifest_list)
            result = parse_avro_file(actual_path)
            if result.get("success") and result.get("data"):
                data = result["data"]
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            mp = item.get("manifest_path")
                            if mp:
                                manifest_paths.append(mp)
                elif isinstance(data, dict):
                    mp = data.get("manifest_path")
                    if mp:
                        manifest_paths.append(mp)
            else:
                # 关键：把 manifest-list 解析失败原因带回去，便于接口/前端展示
                manifest_list_error = result.get("error")

    return {
        "current_snapshot_id": current_snapshot_id,
        "current_snapshot": current_snapshot_obj,
        "manifest_list": manifest_list,
        "manifest_paths": manifest_paths,
        "manifest_list_error": manifest_list_error,
    }


def _unwrap_union(value: Any) -> Any:
    if isinstance(value, dict) and len(value.keys()) == 1:
        k = next(iter(value.keys()))
        if k in {"long", "int", "float", "double", "string", "bytes", "boolean"}:
            return value[k]
    return value


def _unwrap_array(value: Any) -> Any:
    if isinstance(value, dict) and "array" in value:
        return value["array"]
    return value


def _normalize_partition(partition: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if isinstance(partition, dict):
        for k, v in partition.items():
            result[k] = _unwrap_union(v)
    return result


def extract_manifest_info(manifest_data: Any) -> Dict[str, Any]:
    data_files: List[Dict[str, Any]] = []
    items: List[Dict[str, Any]] = []
    if isinstance(manifest_data, list):
        items = [x for x in manifest_data if isinstance(x, dict)]
    elif isinstance(manifest_data, dict):
        items = [manifest_data]
    for entry in items:
        df = entry.get("data_file") or {}
        if not isinstance(df, dict):
            continue
        file_path = df.get("file_path")
        file_format = df.get("file_format")
        partition = _normalize_partition(df.get("partition") or {})
        record_count = df.get("record_count")
        file_size_in_bytes = df.get("file_size_in_bytes")
        column_files_raw = _unwrap_array(df.get("column_files") or {})
        column_files: List[Dict[str, Any]] = []
        if isinstance(column_files_raw, list):
            for cf in column_files_raw:
                if isinstance(cf, dict):
                    column_files.append({
                        "column_file_path": cf.get("column_file_path"),
                        "column_file_length": cf.get("column_file_length"),
                        "column_file_record_count": cf.get("column_file_record_count"),
                        "column_file_snapshot_id": cf.get("column_file_snapshot_id"),
                        "column_file_ids": cf.get("column_file_ids") or []
                    })
        data_files.append({
            "file_path": file_path,
            "file_format": file_format,
            "partition": partition,
            "record_count": record_count,
            "file_size_in_bytes": file_size_in_bytes,
            "column_files": column_files
        })
    return {
        "entries_count": len(items),
        "data_files": data_files
    }


def _strip_file_prefix(path: str) -> str:
    if isinstance(path, str) and path.startswith("file:"):
        return path.replace("file:", "", 1)
    return path


def read_parquet_rows(file_path: str, limit: int = 100) -> Tuple[List[Dict[str, Any]], List[str]]:
    import pyarrow.parquet as pq
    actual = _strip_file_prefix(file_path)
    table = pq.read_table(actual)
    rows = table.to_pylist()[:limit]
    fields = [f.name for f in table.schema]
    return rows, fields


def read_orc_rows(file_path: str, limit: int = 100) -> Tuple[List[Dict[str, Any]], List[str]]:
    rows: List[Dict[str, Any]] = []
    fields: List[str] = []
    actual = _strip_file_prefix(file_path)
    try:
        import pyarrow.orc as o
        of = o.ORCFile(actual)
        table = of.read()
        rows = table.to_pylist()[:limit]
        fields = [f.name for f in table.schema]
        return rows, fields
    except Exception:
        import pyorc
        with open(actual, "rb") as f:
            reader = pyorc.Reader(f)
            fields = [n for n, _t in reader.schema.fields]
            for i, r in enumerate(reader):
                if i >= limit:
                    break
                if isinstance(r, tuple):
                    rows.append({fields[idx]: r[idx] for idx in range(len(fields))})
                elif isinstance(r, dict):
                    rows.append(r)
                else:
                    rows.append({"value": r})
        return rows, fields
