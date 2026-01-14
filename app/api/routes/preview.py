from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from app.security.path_safety import normalize_local_path
from app.services.iceberg_parser import read_orc_rows, read_parquet_rows
from app.services.json_utils import format_json

router = APIRouter()


@router.get("/preview/datafile")
async def preview_datafile(
    file_path: str = Query(..., description="数据文件路径（parquet 或 orc）"),
    file_format: Optional[str] = Query(None, description="文件格式: parquet 或 orc（可选，自动识别）"),
    limit: int = Query(100, description="预览行数", ge=1, le=100),
):
    try:
        safe_path = normalize_local_path(file_path)
        fmt = (file_format or "").lower()
        if not fmt:
            if safe_path.lower().endswith(".parquet"):
                fmt = "parquet"
            elif safe_path.lower().endswith(".orc"):
                fmt = "orc"
            else:
                raise HTTPException(status_code=400, detail="无法识别文件格式，请提供 file_format 参数")

        if fmt == "parquet":
            rows, fields = read_parquet_rows(safe_path, limit)
        elif fmt == "orc":
            rows, fields = read_orc_rows(safe_path, limit)
        else:
            raise HTTPException(status_code=400, detail=f"不支持的文件格式: {file_format}")

        data = {
            "path": safe_path,
            "format": fmt,
            "limit": limit,
            "fields": fields,
            "rows_count": len(rows),
            "rows": rows,
        }
        return {"success": True, "data": data, "formatted": format_json(data)}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"文件不存在: {file_path}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"预览数据文件失败: {str(e)}")
