from __future__ import annotations

import hashlib
import os
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VENDOR = ROOT / "app" / "static" / "vendor"

# 固定版本（与你模板引用保持一致）
BOOTSTRAP_VERSION = "5.3.2"
BOOTSTRAP_ICONS_VERSION = "1.11.1"
HIGHLIGHT_VERSION = "11.9.0"

# jsdelivr 基础
JSDELIVR = "https://cdn.jsdelivr.net/npm"

ASSETS = [
    # Bootstrap
    (
        f"{JSDELIVR}/bootstrap@{BOOTSTRAP_VERSION}/dist/css/bootstrap.min.css",
        VENDOR / "bootstrap" / "css" / "bootstrap.min.css",
    ),
    (
        f"{JSDELIVR}/bootstrap@{BOOTSTRAP_VERSION}/dist/js/bootstrap.bundle.min.js",
        VENDOR / "bootstrap" / "js" / "bootstrap.bundle.min.js",
    ),
    # Bootstrap Icons (css + fonts)
    (
        f"{JSDELIVR}/bootstrap-icons@{BOOTSTRAP_ICONS_VERSION}/font/bootstrap-icons.css",
        VENDOR / "bootstrap-icons" / "font" / "bootstrap-icons.css",
    ),
    (
        f"{JSDELIVR}/bootstrap-icons@{BOOTSTRAP_ICONS_VERSION}/font/fonts/bootstrap-icons.woff2",
        VENDOR / "bootstrap-icons" / "font" / "fonts" / "bootstrap-icons.woff2",
    ),
    (
        f"{JSDELIVR}/bootstrap-icons@{BOOTSTRAP_ICONS_VERSION}/font/fonts/bootstrap-icons.woff",
        VENDOR / "bootstrap-icons" / "font" / "fonts" / "bootstrap-icons.woff",
    ),
    # highlight.js core + json language + theme
    (
        f"{JSDELIVR}/highlight.js@{HIGHLIGHT_VERSION}/lib/common.min.js",
        VENDOR / "highlightjs" / "highlight.min.js",
    ),
    (
        f"{JSDELIVR}/highlight.js@{HIGHLIGHT_VERSION}/lib/languages/json.min.js",
        VENDOR / "highlightjs" / "languages" / "json.min.js",
    ),
    (
        f"{JSDELIVR}/highlight.js@{HIGHLIGHT_VERSION}/styles/github.min.css",
        VENDOR / "highlightjs" / "styles" / "github.min.css",
    ),
]

PLACEHOLDER_MARKERS = (
    "Place bootstrap",
    "Place bootstrap-icons",
    "Place json.min.js",
    "Place the downloaded",
    "Replace this placeholder",
)

def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": "iceberg_helper-vendor-sync"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = resp.read()
    dest.write_bytes(data)

def _looks_placeholder(p: Path) -> bool:
    try:
        text = p.read_text("utf-8", errors="ignore")
    except Exception:
        return False
    return any(m in text for m in PLACEHOLDER_MARKERS)

def _looks_too_small_or_invalid(p: Path) -> bool:
    # 经验阈值：bootstrap.min.css 通常几十 KB；小于 1KB 基本就是坏的或占位
    try:
        size = p.stat().st_size
    except Exception:
        return True
    if size < 1024:
        return True
    # 明显的“只剩 map 引用”/错误内容
    try:
        text = p.read_text("utf-8", errors="ignore")
        if "sourceMappingURL=bootstrap.min.css.map" in text and len(text.strip().splitlines()) <= 3:
            return True
    except Exception:
        pass
    return False

def main() -> int:
    print(f"[vendor-sync] root={ROOT}")
    print(f"[vendor-sync] vendor_dir={VENDOR}")
    ok = True

    for url, dest in ASSETS:
        print(f"[vendor-sync] GET {url}")
        try:
            _download(url, dest)
            size = dest.stat().st_size
            if size <= 0:
                print(f"  -> FAIL: empty file {dest}")
                ok = False
                continue
            if _looks_placeholder(dest) or _looks_too_small_or_invalid(dest):
                print(f"  -> FAIL: invalid/placeholder content {dest} (size={size})")
                ok = False
                continue
            print(f"  -> OK: {dest} ({size} bytes, sha256={_sha256(dest)[:12]}...)")
        except Exception as e:
            print(f"  -> FAIL: {dest} ({e})")
            ok = False

    if not ok:
        print("[vendor-sync] done with errors")
        return 2

    print("[vendor-sync] done")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
