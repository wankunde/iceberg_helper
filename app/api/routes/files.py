from fastapi import APIRouter, HTTPException, Query
from pathlib import Path

from app.security.path_safety import normalize_local_path
from app.services.iceberg_parser import parse_avro_file, scan_metadata_directory, extract_table_metadata_info
from app.services.json_utils import format_json, parse_json_file

router = APIRouter()


@router.get("/list-dir")
async def list_directory(path: str = Query(..., description="表根目录路径 (Table Root)")):
    try:
        safe_dir = normalize_local_path(path)

        # CHANGED: accept table root, append /metadata unless already points to metadata
        p = Path(safe_dir)
        metadata_dir = p if p.name == "metadata" else (p / "metadata")

        result = scan_metadata_directory(str(metadata_dir))
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["error"])
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"扫描目录失败: {str(e)}")


@router.get("/avro")
async def parse_avro(
    file_path: str = Query(..., description="Avro 文件路径"),
    formatted: bool = Query(True, description="是否格式化输出"),
):
    try:
        safe_path = normalize_local_path(file_path)
        result = parse_avro_file(safe_path)
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["error"])

        response_data = {"success": True, "data": result["data"], "raw_output": result["raw_output"]}
        if formatted and result["data"]:
            response_data["formatted"] = format_json(result["data"])
        return response_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"解析 Avro 文件失败: {str(e)}")


@router.get("/json")
async def get_json(
    file_path: str = Query(..., description="JSON 文件路径"),
    formatted: bool = Query(True, description="是否格式化输出"),
):
    try:
        safe_path = normalize_local_path(file_path)
        data = parse_json_file(safe_path)
        response_data = {"success": True, "data": data}
        if formatted:
            response_data["formatted"] = format_json(data)
        return response_data
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取 JSON 文件失败: {str(e)}")


@router.get("/metadata-info")
async def get_metadata_info_compat(
    file_path: str = Query(..., description="Metadata 文件路径"),
    file_type: str = Query("json", description="文件类型: json 或 avro"),
):
    """
    Backward compatible endpoint (old path).
    Frontend still calls /api/metadata-info.
    """
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
