"""FastAPI 应用主入口"""
import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import DEFAULT_METADATA_DIR, STATIC_DIR, TEMPLATES_DIR

# NEW: routers
from app.api.routes.files import router as files_router
from app.api.routes.metadata import router as metadata_router
from app.api.routes.preview import router as preview_router

app = FastAPI(title="Iceberg Metadata Viewer", description="Iceberg 表元数据浏览工具")

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "default_metadata_dir": DEFAULT_METADATA_DIR},
    )


# 按功能拆分注册
app.include_router(files_router, prefix="/api", tags=["files"])
app.include_router(metadata_router, prefix="/api/metadata", tags=["metadata"])
app.include_router(preview_router, prefix="/api", tags=["preview"])


if __name__ == "__main__":
    import uvicorn
    from app.config import DEFAULT_HOST, DEFAULT_PORT

    uvicorn.run(app, host=DEFAULT_HOST, port=DEFAULT_PORT)
