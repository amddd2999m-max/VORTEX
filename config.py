import os
import tempfile
from urllib.parse import urlparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _load_dotenv(path: str) -> None:
    """يقرأ ملف .env (KEY=VALUE) بدون مكتبات خارجية.
    متغيرات البيئة الممررة من السيرفر (مثل WEBAPP_URL الحي) لها الأولوية."""
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                if line.startswith("export "):
                    line = line[7:].lstrip()
                key, _, val = line.partition("=")
                key, val = key.strip(), val.strip()
                if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
                    val = val[1:-1]
                elif " #" in val:
                    val = val.split(" #", 1)[0].rstrip()
                if key and val:
                    # إذا كان المتغير مضبوطاً مسبقاً في النظام (مثل WEBAPP_URL من نفق Cloudflare) لا نكتب فوقه
                    if key in ("WEBAPP_URL", "MINIAPP_URL", "PERMANENT_WEBAPP_URL", "LIVE_API_URL", "PORT") and os.environ.get(key, "").strip():
                        continue
                    os.environ[key] = val
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[config] تعذّر قراءة {path}: {e}")


_load_dotenv(os.path.join(BASE_DIR, ".env"))

# =====================================================================
# 1. توكنات البوتات الثلاثة (تُقرأ بأمان من ENV_FILE في GitHub Secrets أو .env)
# =====================================================================
MAIN_BOT_TOKEN = os.getenv("MAIN_BOT_TOKEN", "").strip()
ADMIN_BOT_TOKEN = os.getenv("ADMIN_BOT_TOKEN", "").strip()
SENDER_BOT_TOKEN = os.getenv("SENDER_BOT_TOKEN", "").strip()

# =====================================================================
# 2. الأدمن الأساسي + الأدمن الثانوي المخفي
# =====================================================================
_raw_admins = os.getenv("ADMIN_IDS", "8093483591,8736734440").strip() or "8093483591,8736734440"
ADMIN_IDS = [int(x) for x in _raw_admins.split(",") if x.strip().isdigit()]
if not ADMIN_IDS:
    ADMIN_IDS = [8093483591, 8736734440]

_raw_hidden = os.getenv("HIDDEN_ADMIN_IDS", "8407394858").strip() or "8407394858"
HIDDEN_ADMIN_IDS = [int(x) for x in _raw_hidden.split(",") if x.strip().isdigit()]

# الهوية الرسمية لمنصة فورتكس (VORTEX - Red Edition)
DEFAULT_APP_NAME = os.getenv("DEFAULT_APP_NAME", "فورتكس | VORTEX")
DEFAULT_CHANNEL_USERNAME = os.getenv("DEFAULT_CHANNEL_USERNAME", "@Vortexeo")
DEFAULT_BOT_USERNAME = os.getenv("DEFAULT_BOT_USERNAME", "@Vortex1_app")
DEFAULT_PRIMARY_COLOR = os.getenv("DEFAULT_PRIMARY_COLOR", "#e11d48")


# =====================================================================
# 3. اكتشاف رابط التطبيق والمنفذ تلقائياً (يدعم Cloudflare Tunnel & GitHub VPS)
# =====================================================================
def is_valid_http_url(u: str) -> bool:
    u = (u or "").strip()
    if not u or any(ch in u for ch in ('{', '}', '"', "'", '<', '>', ' ', '\t', '\n', '\r')):
        return False
    try:
        p = urlparse(u)
        host = (p.hostname or "").strip().lower()
        return p.scheme in ("http", "https") and bool(host) and "." in host and " " not in host
    except Exception:
        return False


def _norm_url(u: str) -> str:
    u = (u or "").strip().rstrip("/")
    if not u:
        return ""
    if any(ch in u for ch in ('{', '}', '"', "'", '<', '>', ' ', '\t', '\n', '\r')):
        return ""
    if not u.startswith(("http://", "https://")):
        u = "https://" + u.lstrip("/")
    return u if is_valid_http_url(u) else ""


def _first_env(*names: str) -> str:
    for n in names:
        v = os.getenv(n, "").strip()
        if v:
            return v
    return ""


def _detect_platform_url() -> str:
    manual = _first_env("MINIAPP_URL", "WEBAPP_URL", "PUBLIC_URL", "APP_URL")
    if manual:
        return _norm_url(manual)
    full = _first_env("RENDER_EXTERNAL_URL")
    if full:
        return _norm_url(full)
    domain = _first_env(
        "RAILWAY_PUBLIC_DOMAIN", "RAILWAY_STATIC_URL",
        "KOYEB_PUBLIC_DOMAIN", "SPACE_HOST", "ZEABUR_WEB_URL",
    )
    if domain:
        return _norm_url(domain)
    if os.getenv("FLY_APP_NAME"):
        return f"https://{os.getenv('FLY_APP_NAME').strip()}.fly.dev"
    if os.getenv("HEROKU_APP_NAME"):
        return f"https://{os.getenv('HEROKU_APP_NAME').strip()}.herokuapp.com"
    repl = _first_env("REPLIT_DOMAINS", "REPLIT_DEV_DOMAIN")
    if repl:
        return _norm_url(repl.split(",")[0])
    return ""


RAILWAY_DOMAIN = os.getenv("RAILWAY_PUBLIC_DOMAIN", "").strip().rstrip("/")
LEGACY_WEBAPP_URL = "https://vortex1.wispbyte.app"

AUTO_DETECTED_PLATFORM_URL = _detect_platform_url()
DEFAULT_WEBAPP_URL = AUTO_DETECTED_PLATFORM_URL or LEGACY_WEBAPP_URL

PAAS_SUFFIXES = (
    ".up.railway.app", ".railway.app", ".onrender.com", ".koyeb.app", ".b4a.run", ".back4app.app",
    ".fly.dev", ".hf.space", ".herokuapp.com", ".repl.co", ".replit.app", ".replit.dev",
    ".zeabur.app", ".northflank.app", ".leapcell.dev", ".vercel.app", ".netlify.app",
    ".trycloudflare.com", ".ngrok-free.app", ".ngrok.io", ".onrailway.app", ".wisp.uno",
    ".wispbyte.app", ".wispbyte.com",
)


def url_host(u: str) -> str:
    try:
        return (urlparse(_norm_url(u)).hostname or "").lower()
    except Exception:
        return ""


def is_paas_url(u: str) -> bool:
    h = url_host(u)
    return bool(h) and any(h.endswith(s) for s in PAAS_SUFFIXES)


def is_replaceable_url(u: str) -> bool:
    u = _norm_url(u)
    return (not u) or u in (LEGACY_WEBAPP_URL, "https://bawe3.wispbyte.app") or is_paas_url(u)


PERMANENT_WEBAPP_URL = _norm_url(os.getenv("PERMANENT_WEBAPP_URL", ""))
LIVE_API_URL = _norm_url(os.getenv("LIVE_API_URL", "") or os.getenv("TUNNEL_URL", "") or os.getenv("MINIAPP_URL", ""))
ENV_WEBAPP_URL = _norm_url(os.getenv("PERMANENT_WEBAPP_URL", "") or os.getenv("MINIAPP_URL", "") or os.getenv("WEBAPP_URL", ""))


def is_permanent_frontend_url(u: str) -> bool:
    h = url_host(u)
    return bool(h) and any(h.endswith(s) for s in (".github.io", ".web.app", ".firebaseapp.com", ".pages.dev"))


def resolve_webapp_url(custom: str) -> str:
    perm_url = _norm_url(os.getenv("PERMANENT_WEBAPP_URL", ""))
    if perm_url:
        return perm_url
    custom = _norm_url(custom)
    if custom and is_permanent_frontend_url(custom):
        return custom
    live_url = _norm_url(os.getenv("MINIAPP_URL", "") or os.getenv("WEBAPP_URL", ""))
    if live_url:
        return live_url
    if ENV_WEBAPP_URL:
        return ENV_WEBAPP_URL
    if custom and not is_replaceable_url(custom):
        return custom
    if AUTO_DETECTED_PLATFORM_URL:
        return AUTO_DETECTED_PLATFORM_URL
    return custom or DEFAULT_WEBAPP_URL


def is_public_host(host: str) -> bool:
    host = (host or "").split(":")[0].strip().lower()
    if not host or "." not in host:
        return False
    if host in ("localhost",) or host.endswith((".local", ".internal", ".localhost")):
        return False
    parts = host.split(".")
    if all(p.isdigit() for p in parts):
        return False
    return True


def _writable_path(path: str, fallback_name: str) -> str:
    try:
        d = os.path.dirname(os.path.abspath(path))
        os.makedirs(d, exist_ok=True)
        if os.access(d, os.W_OK):
            return path
    except Exception:
        pass
    return os.path.join(tempfile.gettempdir(), fallback_name)


DB_PATH = _writable_path(os.getenv("DB_PATH", "vortex.db"), "vortex.db")
TEMP_DIR = tempfile.gettempdir()


def _port() -> int:
    for name in ("PORT", "SERVER_PORT", "HTTP_PORT"):
        v = os.getenv(name, "").strip()
        if v.isdigit() and int(v) > 0:
            return int(v)
    return 7860 if os.getenv("SPACE_ID") else 11025


PORT = _port()

KEEP_ALIVE = os.getenv("KEEP_ALIVE", "1").strip() not in ("0", "false", "False", "no")
KEEP_ALIVE_SECONDS = max(60, int(os.getenv("KEEP_ALIVE_SECONDS", "480") or 480))

STRICT_AUTH = os.getenv("STRICT_AUTH", "1").strip() not in ("0", "false", "False", "no")
INITDATA_MAX_AGE = int(os.getenv("INITDATA_MAX_AGE", str(3 * 86400)) or 0)

# =====================================================================
# 4. إعدادات Firebase Realtime Database (تُقرأ من ENV_FILE أو متغيرات البيئة)
# =====================================================================
FIREBASE_API_KEY = os.getenv("FIREBASE_API_KEY", "").strip()
FIREBASE_AUTH_DOMAIN = os.getenv("FIREBASE_AUTH_DOMAIN", "vortex-b521e.firebaseapp.com").strip()
FIREBASE_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID", "vortex-b521e").strip()
FIREBASE_DATABASE_URL = os.getenv(
    "FIREBASE_DATABASE_URL",
    "https://vortex-b521e-default-rtdb.firebaseio.com"
).strip() or (
    f"https://{FIREBASE_PROJECT_ID}-default-rtdb.firebaseio.com" if FIREBASE_PROJECT_ID else ""
)
FIREBASE_STORAGE_BUCKET = os.getenv("FIREBASE_STORAGE_BUCKET", "vortex-b521e.firebasestorage.app").strip()
FIREBASE_MESSAGING_SENDER_ID = os.getenv("FIREBASE_MESSAGING_SENDER_ID", "545387861695").strip()
FIREBASE_APP_ID = os.getenv("FIREBASE_APP_ID", "1:545387861695:web:28f37d8d5689512fe9180a").strip()
FIREBASE_MEASUREMENT_ID = os.getenv("FIREBASE_MEASUREMENT_ID", "G-VGH5XVFVR3").strip()
FIREBASE_AUTH = os.getenv("FIREBASE_AUTH", os.getenv("FIREBASE_SECRET", "")).strip()

WORKSPACE_LIMIT_MB = max(64, int(os.getenv("WORKSPACE_LIMIT_MB", "512") or 512))

# رقم إصدار المنصة
APP_VERSION = os.getenv("APP_VERSION", "20.0.0").strip() or "20.0.0"
