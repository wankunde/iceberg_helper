from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.security.path_safety import normalize_local_path
from app.services.iceberg_parser import (
    extract_current_snapshot_manifests,
    extract_manifest_info,
    extract_table_metadata_info,
    parse_avro_file,
)
from app.services.json_utils import format_json, parse_json_file

router = APIRouter()


@router.get("/info")
async def get_metadata_info(
    file_path: str = Query(..., description="Metadata 文件路径"),
    file_type: str = Query("json", description="文件类型: json 或 avro"),
):
    try:
        safe_path = normalize_local_path(file_path)
        if file_type == "avro":
            result = parse_avro_file(safe_path)
            if not result["success"]:
                raise HTTPException(status_code=400, detail=result["error"])
            metadata_data = result["data"]
        else:
            metadata_data = parse_json_file(safe_path)

        info = extract_table_metadata_info(metadata_data)
        return {"success": True, "info": info, "formatted": format_json(info)}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"解析元数据失败: {str(e)}")


@router.get("/view")
async def view_metadata(file_path: str = Query(..., description="Metadata JSON 文件路径")):
    try:
        safe_path = normalize_local_path(file_path)
        metadata_data = parse_json_file(safe_path)
        info = extract_table_metadata_info(metadata_data)
        return {"success": True, "metadata": metadata_data, "info": info, "formatted": format_json(metadata_data)}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查看 Metadata 文件失败: {str(e)}")


@router.get("/current-manifests")
async def get_current_manifests(file_path: str = Query(..., description="Metadata JSON 文件路径")):
    try:
        safe_path = normalize_local_path(file_path)
        metadata_data = parse_json_file(safe_path)
        result = extract_current_snapshot_manifests(metadata_data)
        return {"success": True, **result, "formatted": format_json(result)}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取当前快照 manifests 失败: {str(e)}")


@router.get("/snapshot")
async def view_snapshot(file_path: str = Query(..., description="Snapshot Avro 文件路径")):
    try:
        safe_path = normalize_local_path(file_path)
        result = parse_avro_file(safe_path)
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["error"])

        snapshot_data = result["data"]
        snapshot_info = {}
        manifest_paths = []

        if isinstance(snapshot_data, list):
            for item in snapshot_data:
                if isinstance(item, dict):
                    manifest_path = item.get("manifest_path")
                    if manifest_path:
                        manifest_paths.append(manifest_path)
                    if not snapshot_info:
                        snapshot_info = {
                            "manifest_path": manifest_path,
                            "manifest_length": item.get("manifest_length"),
                            "partition_spec_id": item.get("partition_spec_id"),
                            "content": item.get("content"),
                            "sequence_number": item.get("sequence_number"),
                            "min_sequence_number": item.get("min_sequence_number"),
                            "added_snapshot_id": item.get("added_snapshot_id"),
                            "added_data_files_count": item.get("added_data_files_count"),
                            "existing_data_files_count": item.get("existing_data_files_count"),
                            "deleted_data_files_count": item.get("deleted_data_files_count"),
                            "added_rows_count": item.get("added_rows_count"),
                            "existing_rows_count": item.get("existing_rows_count"),
                            "deleted_rows_count": item.get("deleted_rows_count"),
                        }
        elif isinstance(snapshot_data, dict):
            manifest_path = snapshot_data.get("manifest_path")
            if manifest_path:
                manifest_paths.append(manifest_path)
            snapshot_info = {
                "manifest_path": manifest_path,
                "manifest_length": snapshot_data.get("manifest_length"),
                "partition_spec_id": snapshot_data.get("partition_spec_id"),
                "content": snapshot_data.get("content"),
                "sequence_number": snapshot_data.get("sequence_number"),
                "min_sequence_number": snapshot_data.get("min_sequence_number"),
                "added_snapshot_id": snapshot_data.get("added_snapshot_id"),
                "added_data_files_count": snapshot_data.get("added_data_files_count"),
                "existing_data_files_count": snapshot_data.get("existing_data_files_count"),
                "deleted_data_files_count": snapshot_data.get("deleted_data_files_count"),
                "added_rows_count": snapshot_data.get("added_rows_count"),
                "existing_rows_count": snapshot_data.get("existing_rows_count"),
                "deleted_rows_count": snapshot_data.get("deleted_rows_count"),
            }

        return {
            "success": True,
            "snapshot": snapshot_data,
            "info": snapshot_info,
            "manifest_paths": manifest_paths,
            "formatted": format_json(snapshot_data),
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查看 Snapshot 文件失败: {str(e)}")


@router.get("/manifest")
async def view_manifest(file_path: str = Query(..., description="Manifest Avro 文件路径")):
    try:
        safe_path = normalize_local_path(file_path)
        result = parse_avro_file(safe_path)
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["error"])
        manifest_data = result["data"]
        info = extract_manifest_info(manifest_data)
        return {"success": True, "manifest": manifest_data, "info": info, "formatted": format_json(manifest_data)}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查看 Manifest 文件失败: {str(e)}")
