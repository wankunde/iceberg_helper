"""FastAPI 应用主入口"""
import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import DEFAULT_METADATA_DIR, STATIC_DIR, TEMPLATES_DIR
from app.services.iceberg_parser import (
    extract_table_metadata_info,
    parse_avro_file,
    scan_metadata_directory,
    extract_current_snapshot_manifests,
    extract_manifest_info,
)
from app.services.json_utils import format_json, parse_json_file

# 创建 FastAPI 应用
app = FastAPI(title="Iceberg Metadata Viewer", description="Iceberg 表元数据浏览工具")

# 配置模板引擎
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# 挂载静态文件
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """主页面"""
    return templates.TemplateResponse(
        "index.html", 
        {
            "request": request,
            "default_metadata_dir": DEFAULT_METADATA_DIR
        }
    )


@app.get("/api/list-dir")
async def list_directory(path: str = Query(..., description="目录路径")):
    """列出目录下的文件和子目录"""
    try:
        result = scan_metadata_directory(path)
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["error"])
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"扫描目录失败: {str(e)}")


@app.get("/api/avro")
async def parse_avro(
    file_path: str = Query(..., description="Avro 文件路径"),
    formatted: bool = Query(True, description="是否格式化输出"),
):
    """解析 Avro 文件"""
    try:
        result = parse_avro_file(file_path)
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["error"])
        
        response_data = {
            "success": True,
            "data": result["data"],
            "raw_output": result["raw_output"]
        }
        
        if formatted and result["data"]:
            response_data["formatted"] = format_json(result["data"])
        
        return response_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"解析 Avro 文件失败: {str(e)}")


@app.get("/api/json")
async def get_json(
    file_path: str = Query(..., description="JSON 文件路径"),
    formatted: bool = Query(True, description="是否格式化输出"),
):
    """读取并格式化 JSON 文件"""
    try:
        data = parse_json_file(file_path)
        response_data = {
            "success": True,
            "data": data
        }
        
        if formatted:
            response_data["formatted"] = format_json(data)
        
        return response_data
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取 JSON 文件失败: {str(e)}")


@app.get("/api/metadata-info")
async def get_metadata_info(
    file_path: str = Query(..., description="Metadata 文件路径"),
    file_type: str = Query("json", description="文件类型: json 或 avro"),
):
    """获取表元数据概览信息"""
    try:
        if file_type == "avro":
            result = parse_avro_file(file_path)
            if not result["success"]:
                raise HTTPException(status_code=400, detail=result["error"])
            metadata_data = result["data"]
        else:  # json
            metadata_data = parse_json_file(file_path)
        
        # 提取关键信息
        info = extract_table_metadata_info(metadata_data)
        
        return {
            "success": True,
            "info": info,
            "formatted": format_json(info)
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"解析元数据失败: {str(e)}")


@app.get("/api/metadata/view")
async def view_metadata(
    file_path: str = Query(..., description="Metadata JSON 文件路径"),
):
    """查看 Metadata 文件的详细内容（结构化展示）"""
    try:
        metadata_data = parse_json_file(file_path)
        info = extract_table_metadata_info(metadata_data)
        
        return {
            "success": True,
            "metadata": metadata_data,
            "info": info,
            "formatted": format_json(metadata_data)
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查看 Metadata 文件失败: {str(e)}")


@app.get("/api/metadata/current-manifests")
async def get_current_manifests(
    file_path: str = Query(..., description="Metadata JSON 文件路径"),
):
    try:
        metadata_data = parse_json_file(file_path)
        result = extract_current_snapshot_manifests(metadata_data)
        return {
            "success": True,
            **result,
            "formatted": format_json(result)
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取当前快照 manifests 失败: {str(e)}")


@app.get("/api/metadata/snapshot")
async def view_snapshot(
    file_path: str = Query(..., description="Snapshot Avro 文件路径"),
):
    """查看 Snapshot 文件的详细内容"""
    try:
        result = parse_avro_file(file_path)
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["error"])
        
        snapshot_data = result["data"]
        
        # 提取快照关键信息
        snapshot_info = {}
        manifest_paths = []
        
        # 处理 snapshot 数据（可能是列表或字典）
        if isinstance(snapshot_data, list):
            # 如果是列表，提取所有 manifest_path
            for item in snapshot_data:
                if isinstance(item, dict):
                    manifest_path = item.get("manifest_path")
                    if manifest_path:
                        manifest_paths.append(manifest_path)
                    # 提取第一个的关键信息
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
            # 如果是字典，直接提取
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
            "formatted": format_json(snapshot_data)
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查看 Snapshot 文件失败: {str(e)}")


@app.get("/api/metadata/manifest")
async def view_manifest(
    file_path: str = Query(..., description="Manifest Avro 文件路径"),
):
    try:
        result = parse_avro_file(file_path)
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["error"])
        manifest_data = result["data"]
        info = extract_manifest_info(manifest_data)
        return {
            "success": True,
            "manifest": manifest_data,
            "info": info,
            "formatted": format_json(manifest_data)
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查看 Manifest 文件失败: {str(e)}")

@app.get("/api/data/avro")
async def view_data_avro(
    file_path: str = Query(..., description="Data Avro 文件路径"),
    sample_size: int = Query(10, description="数据样本数量", ge=1, le=100),
):
    """查看 Data Avro 文件的详细内容（Schema + 数据样本）"""
    try:
        result = parse_avro_file(file_path)
        if not result["success"]:
            raise HTTPException(status_code=400, detail=result["error"])
        
        data = result["data"]
        
        # 提取数据样本
        samples = []
        if isinstance(data, list):
            samples = data[:sample_size]
        elif isinstance(data, dict):
            samples = [data]
        
        # 尝试提取 schema 信息（如果数据中包含）
        schema_info = None
        if isinstance(data, list) and len(data) > 0:
            # 从第一条记录推断字段
            first_record = data[0]
            if isinstance(first_record, dict):
                schema_info = {
                    "fields": list(first_record.keys()),
                    "field_count": len(first_record.keys()),
                    "total_records": len(data) if isinstance(data, list) else 1
                }
        
        return {
            "success": True,
            "data": data,
            "samples": samples,
            "schema_info": schema_info,
            "total_records": len(data) if isinstance(data, list) else 1,
            "formatted": format_json(data)
        }
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查看 Data Avro 文件失败: {str(e)}")


@app.get("/api/data/parquet")
async def view_data_parquet(
    file_path: str = Query(..., description="Parquet 文件路径"),
):
    """查看 Parquet 文件信息（暂不支持详细解析）"""
    try:
        from pathlib import Path
        file_path_obj = Path(file_path)
        
        if not file_path_obj.exists():
            raise HTTPException(status_code=404, detail=f"文件不存在: {file_path}")
        
        file_info = {
            "path": file_path,
            "name": file_path_obj.name,
            "size": file_path_obj.stat().st_size,
            "note": "Parquet 文件解析功能正在开发中。目前可以通过 parquet-tools 等工具查看。"
        }
        
        return {
            "success": True,
            "info": file_info,
            "formatted": format_json(file_info)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查看 Parquet 文件失败: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    from app.config import DEFAULT_HOST, DEFAULT_PORT
    
    uvicorn.run(app, host=DEFAULT_HOST, port=DEFAULT_PORT)
