from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VENDOR = ROOT / "app" / "static" / "vendor"

PLACEHOLDER_MARKERS = (
    "Place bootstrap",
    "Place bootstrap-icons",
    "Place json.min.js",
    "Place the downloaded",
    "Replace this placeholder",
)

def looks_placeholder(p: Path) -> bool:
    try:
        text = p.read_text("utf-8", errors="ignore")
    except Exception:
        return False
    return any(m in text for m in PLACEHOLDER_MARKERS)

def main() -> int:
    if not VENDOR.exists():
        print(f"[vendor-check] missing: {VENDOR}")
        return 2

    bad = []
    for p in VENDOR.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix.lower() not in {".css", ".js", ".woff2", ".woff"}:
            continue

        size = p.stat().st_size
        if size <= 0 or looks_placeholder(p):
            bad.append((p, size))

    if bad:
        print("[vendor-check] BAD files:")
        for p, size in bad:
            print(f"  - {p} ({size} bytes)")
        return 2

    print("[vendor-check] OK")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
