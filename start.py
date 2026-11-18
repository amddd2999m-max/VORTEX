"""نقطة تشغيل موحدة لنظام فورتكس (VORTEX) تعمل على أي استضافة."""
import glob
import importlib
import importlib.util
import os
import subprocess
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)
sys.path.insert(0, BASE_DIR)

_LOCAL_DIRS = glob.glob(os.path.join(BASE_DIR, ".local", "lib", "python*", "site-packages"))
_LOCAL_DIRS.append(os.path.join(BASE_DIR, ".pydeps"))
for _p in _LOCAL_DIRS:
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

from config import PORT  # noqa: E402

_REQUIRED_MODULES = ("aiogram", "fastapi", "uvicorn", "aiohttp", "pydantic")


def _missing_modules():
    importlib.invalidate_caches()
    return [m for m in _REQUIRED_MODULES if importlib.util.find_spec(m) is None]


def _ensure_deps() -> None:
    missing = _missing_modules()
    if not missing:
        return
    print(f"[start] مكتبات ناقصة: {', '.join(missing)}", flush=True)
    if os.getenv("SKIP_AUTO_INSTALL", "").strip().lower() in ("1", "true", "yes"):
        raise SystemExit("[start] SKIP_AUTO_INSTALL مفعّل — ثبّت المكتبات يدوياً: pip install -r requirements.txt")
    req = os.path.join(BASE_DIR, "requirements.txt")
    pydeps = os.path.join(BASE_DIR, ".pydeps")
    base = [sys.executable, "-m", "pip", "install", "--no-cache-dir", "--disable-pip-version-check",
            "--retries", "2", "--timeout", "30", "-r", req]
    for label, cmd in (("pip install", base), ("pip install --target .pydeps", base + ["--target", pydeps])):
        print(f"[start] محاولة: {label} ...", flush=True)
        try:
            subprocess.check_call(cmd)
        except Exception as e:
            print(f"[start] فشلت ({label}): {e}", flush=True)
            continue
        if os.path.isdir(pydeps) and pydeps not in sys.path:
            sys.path.insert(0, pydeps)
        if not _missing_modules():
            print("[start] تم تثبيت المكتبات ✓", flush=True)
            return
    raise SystemExit(
        "[start] تعذّر تثبيت المكتبات تلقائياً.\n"
        "شغّل diagnose.py لتعرف السبب، أو ثبّتها يدوياً: pip install -r requirements.txt"
    )


if __name__ == "__main__":
    print(f"[VORTEX] Python {sys.version.split()[0]} | port {PORT}", flush=True)
    if sys.version_info < (3, 10):
        print("[VORTEX] تحذير: المشروع يحتاج Python 3.10 أو أحدث", flush=True)
    _ensure_deps()
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=PORT,
        log_level="info",
        proxy_headers=True,
        forwarded_allow_ips="*",
        timeout_keep_alive=30,
    )
