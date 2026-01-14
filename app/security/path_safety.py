from __future__ import annotations

from pathlib import Path
from typing import Optional

from fastapi import HTTPException

# 允许的根目录（可选）。不配置则只做“绝对路径 + 禁止 ..”
ALLOWED_ROOT: Optional[Path] = None


def normalize_local_path(raw: str) -> str:
    """
    - 接受 "/abs/path" 或 "file:/abs/path"
    - 拒绝相对路径/目录穿越
    - 可选：限制在 ALLOWED_ROOT 下
    """
    if not raw or not isinstance(raw, str):
        raise HTTPException(status_code=400, detail="路径不能为空")

    p = raw.strip()
    if p.startswith("file:"):
        p = p.replace("file:", "", 1)

    # 必须是本机绝对路径
    path = Path(p)
    if not path.is_absolute():
        raise HTTPException(status_code=400, detail=f"路径必须是绝对路径: {raw}")

    # 防止目录穿越（同时兼容符号链接不在这里做 realpath，保持轻量）
    parts = path.parts
    if ".." in parts:
        raise HTTPException(status_code=400, detail=f"非法路径(包含..): {raw}")

    if ALLOWED_ROOT is not None:
        try:
            path.relative_to(ALLOWED_ROOT)
        except Exception:
            raise HTTPException(status_code=403, detail=f"路径不在允许范围内: {raw}")

    return str(path)
