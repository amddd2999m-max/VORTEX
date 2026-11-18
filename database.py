import gc
import os
import sqlite3
import json
import re
import html
import time
import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple, Set
try:
    import aiohttp
except ImportError:
    aiohttp = None
from urllib.parse import urlencode

from config import DB_PATH, ADMIN_IDS, HIDDEN_ADMIN_IDS, FIREBASE_DATABASE_URL, FIREBASE_AUTH

logger = logging.getLogger("vortex.db")


import threading

_DB_TLS = threading.local()
_DB_POOL_LOCK = threading.Lock()
_DB_ACTIVE_CONNS: List["_PooledSQLiteConn"] = []
_DB_POOL_GEN = 0

# كاش الذاكرة العشوائية الفائق السرعة (VORTEX V6 Turbo Cache)
_MEDIA_CACHE: Dict[str, Any] = {
    "items": None,
    "visible_items": None,
    "public_items": None,
    "public_visible_items": None,
    "collections_all": None,
    "collections_active": None,
    "smart_sections_non_empty": None,
    "smart_sections_all": None,
    "counts_visible": None,
    "counts_all": None,
}
_SECTION_DEFS_CACHE: Dict[bool, List[Dict[str, Any]]] = {}
_KNOWN_USERS_CACHE: Dict[int, Tuple[str, str]] = {}
_LEADERBOARD_CACHE: Dict[int, Tuple[float, List[Dict[str, Any]]]] = {}
_STATS_CACHE: Dict[str, Any] = {"ts": 0.0, "data": None}


def invalidate_media_cache() -> None:
    """يمسح كاش الأفلام والمسلسلات والأقسام والسلاسل عند أي إضافة أو تعديل أو حذف."""
    for k in list(_MEDIA_CACHE.keys()):
        _MEDIA_CACHE[k] = None
    _SECTION_DEFS_CACHE.clear()
    _STATS_CACHE["ts"] = 0.0
    _STATS_CACHE["data"] = None


def invalidate_all_caches() -> None:
    """يمسح جميع الكاشات بالذاكرة (يُستدعى عند استعادة نسخة احتياطية أو مزامنة شاملة)."""
    invalidate_media_cache()
    _KNOWN_USERS_CACHE.clear()
    _LEADERBOARD_CACHE.clear()
    try:
        _reload_settings_cache()
    except Exception:
        pass


class _PooledSQLiteConn(sqlite3.Connection):
    """اتصال SQLite دائم بالذاكرة لكل خيط (Thread) يمنع بطء فتح وإغلاق الملف في كل استعلام."""
    def close(self) -> None:
        try:
            if self.in_transaction:
                super().rollback()
        except Exception:
            pass

    def real_close(self) -> None:
        try:
            super().close()
        except Exception:
            pass


def _close_all_pooled_conns() -> None:
    global _DB_POOL_GEN
    with _DB_POOL_LOCK:
        _DB_POOL_GEN += 1
        conns = list(_DB_ACTIVE_CONNS)
        _DB_ACTIVE_CONNS.clear()
    for c in conns:
        c.real_close()


def get_conn() -> sqlite3.Connection:
    conn = getattr(_DB_TLS, "conn", None)
    gen = getattr(_DB_TLS, "gen", -1)
    path = getattr(_DB_TLS, "path", "")
    if conn is not None and gen == _DB_POOL_GEN and path == DB_PATH:
        try:
            conn.execute("SELECT 1")
            return conn
        except Exception:
            try:
                conn.real_close()
            except Exception:
                pass

    conn = sqlite3.connect(
        DB_PATH,
        check_same_thread=False,
        timeout=30,
        factory=_PooledSQLiteConn
    )
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=-32000")
        conn.execute("PRAGMA mmap_size=134217728")
    except sqlite3.Error:
        pass
    _DB_TLS.conn = conn
    _DB_TLS.gen = _DB_POOL_GEN
    _DB_TLS.path = DB_PATH
    with _DB_POOL_LOCK:
        _DB_ACTIVE_CONNS.append(conn)
    return conn


def db_checkpoint() -> None:
    """يدمج ملف WAL داخل ملف القاعدة الرئيسي — لازم قبل أخذ نسخة احتياطية حتى تكون كاملة."""
    try:
        conn = get_conn()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception as e:  # noqa: BLE001
        logger.warning(f"WAL checkpoint warning: {e}")


def db_remove_sidecars() -> None:
    """يغلق الاتصالات المفتوحة ويحذف ملفات -wal و -shm القديمة قبل استبدال القاعدة."""
    _close_all_pooled_conns()
    invalidate_all_caches()
    for suffix in ("-wal", "-shm"):
        try:
            os.remove(DB_PATH + suffix)
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.warning(f"Could not remove {DB_PATH}{suffix}: {e}")


# =====================================================================
# وحدة التزامن الفوري الشامل مع Firebase Realtime Database
#  • حفظ واستعادة كل النصوص والأيديات:
#    (settings, media + published_msg_id, media_files + video_file_id,
#     users + theme, admins, media_requests, media_reports, comments,
#     user_lists, meta)
#  • الفيديوهات والصور نفسها تنخزن على تيليجرام، وأيدياتها ورسائلها بـ Firebase
# =====================================================================
FB_STATS: Dict[str, Any] = {
    "ok": 0, "fail": 0, "dropped": 0,
    "last_ok": 0.0, "last_error": "", "last_error_at": 0.0,
    "last_restore": None,
}
_FB_NODES = (
    "settings", "media", "media_files", "users", "admins",
    "media_requests", "media_reports", "comments", "user_lists", "meta",
    "pull_logs", "share_logs", "scheduled_deletions", "custom_sections", "ad_tasks"
)
_FB_QUEUE: Optional["asyncio.Queue"] = None
_FB_WORKER: Optional["asyncio.Task"] = None
_FB_LOOP: Optional["asyncio.AbstractEventLoop"] = None
_FB_LOG_THROTTLE: Dict[str, float] = {}
_FB_BAD_KEY = re.compile(r"[.$#\[\]/]")


def fb_enabled() -> bool:
    return bool(FIREBASE_DATABASE_URL)


def _fb_url(path: str = "", **params) -> str:
    base = FIREBASE_DATABASE_URL.rstrip("/")
    path = (path or "").strip("/")
    url = f"{base}/{path}.json" if path else f"{base}/.json"
    if FIREBASE_AUTH:
        params["auth"] = FIREBASE_AUTH
    if params:
        url += "?" + urlencode(params)
    return url


def _fb_redact(text: Any) -> str:
    t = str(text)
    if FIREBASE_AUTH:
        t = t.replace(FIREBASE_AUTH, "***")
    return t


def _fb_hint(status: Optional[int]) -> str:
    if status in (401, 403):
        return "قواعد Firebase (Rules) تمنع الوصول، أو مفتاح FIREBASE_AUTH غلط"
    if status == 404:
        return "رابط قاعدة البيانات غلط (راجع FIREBASE_DATABASE_URL والمنطقة)"
    if status == 429:
        return "ضغط كبير على Firebase"
    if status and status >= 500:
        return "خطأ مؤقت من سيرفر Firebase"
    return ""


def _fb_log(key: str, level: int, msg: str):
    now = time.time()
    if now - _FB_LOG_THROTTLE.get(key, 0) >= 60:
        _FB_LOG_THROTTLE[key] = now
        logger.log(level, msg)
    else:
        logger.debug(msg)


def _ts_now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())


def _int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return default


def _float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _str(v: Any, default: str = "") -> str:
    return default if v is None else str(v)


def _as_dict(node: Any) -> Dict[str, Any]:
    if isinstance(node, dict):
        return {str(k): v for k, v in node.items() if v is not None}
    if isinstance(node, list):
        return {str(i): v for i, v in enumerate(node) if v is not None}
    return {}


async def _fb_request(
    session, method: str, path: str, data: Any = None, *,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 15, retries: int = 3
) -> Dict[str, Any]:
    url = _fb_url(path, **(params or {}))
    status: Optional[int] = None
    error = ""
    for attempt in range(1, retries + 1):
        try:
            kwargs: Dict[str, Any] = {"timeout": aiohttp.ClientTimeout(total=timeout)}
            if method in ("PUT", "PATCH"):
                kwargs["json"] = data
            async with session.request(method, url, **kwargs) as resp:
                status = resp.status
                if status == 200:
                    try:
                        body = await resp.json(content_type=None)
                    except Exception:
                        body = None
                    return {"ok": True, "status": 200, "body": body, "error": ""}
                if status != 429 and status < 500:
                    return {"ok": False, "status": status, "body": None, "error": ""}
                error = f"HTTP {status}"
        except Exception as e:
            status = None
            error = f"{type(e).__name__}: {_fb_redact(e)}"[:160]
        if attempt < retries:
            await asyncio.sleep(min(8, 2 ** (attempt - 1)))
    return {"ok": False, "status": status, "body": None, "error": error}


def _fb_record(res: Dict[str, Any], what: str):
    if res["ok"]:
        FB_STATS["ok"] += 1
        FB_STATS["last_ok"] = time.time()
        return
    FB_STATS["fail"] += 1
    reason = _fb_hint(res["status"]) or res["error"] or "سبب غير معروف"
    msg = f"Firebase {what} فشل (status={res['status']}): {reason}"
    FB_STATS["last_error"] = msg[:220]
    FB_STATS["last_error_at"] = time.time()
    _fb_log(f"fail:{res['status']}", logging.WARNING, msg)


async def _fb_worker(queue: "asyncio.Queue"):
    async with aiohttp.ClientSession() as session:
        while True:
            method, path, data = await queue.get()
            try:
                if method == "PUT" and data is None:
                    method = "DELETE"
                res = await _fb_request(session, method, path, data)
                _fb_record(res, f"{method} {path or '/'}")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                FB_STATS["fail"] += 1
                FB_STATS["last_error"] = f"worker: {type(e).__name__}: {_fb_redact(e)}"[:220]
                FB_STATS["last_error_at"] = time.time()
            finally:
                queue.task_done()


def _fb_ensure_worker() -> bool:
    global _FB_QUEUE, _FB_WORKER, _FB_LOOP
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return (_FB_LOOP is not None and _FB_LOOP.is_running() and _FB_QUEUE is not None)
    if _FB_WORKER is None or _FB_WORKER.done() or _FB_LOOP is not loop:
        _FB_LOOP = loop
        _FB_QUEUE = asyncio.Queue(maxsize=20000)
        _FB_WORKER = loop.create_task(_fb_worker(_FB_QUEUE))
    return True


def _fb_put_nowait(item: Tuple[str, str, Any]):
    try:
        _FB_QUEUE.put_nowait(item)
    except asyncio.QueueFull:
        FB_STATS["dropped"] += 1
        _fb_log("queue_full", logging.ERROR, "Firebase: طابور الكتابة ممتلئ، تم تجاهل تحديث!")


def _fb_enqueue(item: Tuple[str, str, Any]):
    if not fb_enabled():
        return
    if not _fb_ensure_worker():
        FB_STATS["dropped"] += 1
        _fb_log("no_loop", logging.WARNING, "Firebase: ما أكو حلقة asyncio شغالة، تحديث ما انرسل")
        return
    try:
        running = asyncio.get_running_loop()
    except RuntimeError:
        running = None
    if running is _FB_LOOP:
        _fb_put_nowait(item)
    else:
        _FB_LOOP.call_soon_threadsafe(_fb_put_nowait, item)


def trigger_firebase_push(node: str, data: Any):
    _fb_enqueue(("PUT", node, data))


def trigger_firebase_update(updates: Dict[str, Any]):
    if updates:
        _fb_enqueue(("PATCH", "", updates))


async def sync_push_to_firebase(node: str, data: Any):
    trigger_firebase_push(node, data)


async def firebase_flush(timeout: float = 60.0):
    if _FB_QUEUE is not None and _FB_LOOP is asyncio.get_running_loop():
        try:
            await asyncio.wait_for(_FB_QUEUE.join(), timeout)
        except asyncio.TimeoutError:
            pass


# ---------------- بناء السجلات الكاملة ----------------
def _row_val(row: sqlite3.Row, key: str, default: Any = "") -> Any:
    try:
        v = row[key]
        return default if v is None else v
    except Exception:
        return default


def _media_payload(row: sqlite3.Row) -> Dict[str, Any]:
    try:
        genres = json.loads(row["genres"]) if row["genres"] else []
    except Exception:
        genres = []
    return {
        "tmdb_id": _row_val(row, "tmdb_id", 0),
        "media_type": _row_val(row, "media_type", "movie"),
        "title": _row_val(row, "title", ""),
        "original_title": _row_val(row, "original_title", ""),
        "year": _row_val(row, "year", ""),
        "duration": _row_val(row, "duration", ""),
        "country": _row_val(row, "country", ""),
        "category_label": _row_val(row, "category_label", ""),
        "rating": _row_val(row, "rating", 0.0),
        "genres": genres,
        "overview": _row_val(row, "overview", ""),
        "poster_url": _row_val(row, "poster_url", ""),
        "backdrop_url": _row_val(row, "backdrop_url", ""),
        "is_tonight": bool(_row_val(row, "is_tonight", 0)),
        "views_count": _row_val(row, "views_count", 0),
        "published_chat_id": _row_val(row, "published_chat_id", ""),
        "published_msg_id": _row_val(row, "published_msg_id", 0),
        "section_pub_chat_id": _row_val(row, "section_pub_chat_id", ""),
        "section_pub_msg_id": _row_val(row, "section_pub_msg_id", 0),
        "collection_id": _row_val(row, "collection_id", 0),
        "collection_name": _row_val(row, "collection_name", ""),
        "collection_poster": _row_val(row, "collection_poster", ""),
        "collection_backdrop": _row_val(row, "collection_backdrop", ""),
        "collection_total_parts": _row_val(row, "collection_total_parts", 0),
        "collection_forced": _row_val(row, "collection_forced", 0),
        "is_hidden": _int(_row_val(row, "is_hidden", 0), 0),
        "section_key": _row_val(row, "section_key", ""),
        "cast_json": _str(_row_val(row, "cast_json", "[]"), "[]"),
        "trailer_url": _str(_row_val(row, "trailer_url", "")),
        "created_at": _row_val(row, "created_at", _ts_now()),
    }


def _file_payload(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "media_id": _row_val(row, "media_id", 0),
        "quality": _row_val(row, "quality", "720"),
        "season": _row_val(row, "season", 1),
        "episode": _row_val(row, "episode", 0),
        "episode_title": _row_val(row, "episode_title", ""),
        "storage_chat_id": _row_val(row, "storage_chat_id", ""),
        "storage_message_id": _row_val(row, "storage_message_id", 0),
        "video_file_id": _row_val(row, "video_file_id", ""),
        "storage_caption": _row_val(row, "storage_caption", ""),
        "file_size_mb": _row_val(row, "file_size_mb", 0.0),
        "duration_sec": _row_val(row, "duration_sec", 0),
    }


def _user_payload(row: sqlite3.Row) -> Dict[str, Any]:
    p: Dict[str, Any] = {
        "first_name": _row_val(row, "first_name", ""),
        "username": _row_val(row, "username", ""),
        "points": _row_val(row, "points", 0),
        "referrals_count": _row_val(row, "referrals_count", 0),
        "referred_by": _row_val(row, "referred_by", 0),
        "referral_rewarded": _row_val(row, "referral_rewarded", 0),
        "referral_rewarded_at": _row_val(row, "referral_rewarded_at", ""),
        "shares_count": _row_val(row, "shares_count", 0),
        "pulls_count": _row_val(row, "pulls_count", 0),
        "vip_until": _row_val(row, "vip_until", 0),
        "theme": _row_val(row, "theme", "vortex"),
        "joined_at": _row_val(row, "joined_at", _ts_now()),
    }
    ch_title = _row_val(row, "custom_channel_title", "")
    ch_url = _row_val(row, "custom_channel_url", "")
    if ch_title or ch_url:
        p["custom_channel"] = {"title": ch_title, "url": ch_url}
    return p


def _admin_payload(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "user_id": _row_val(row, "user_id", 0),
        "first_name": _row_val(row, "first_name", ""),
        "username": _row_val(row, "username", ""),
        "added_by": _row_val(row, "added_by", 0),
        "role_title": _row_val(row, "role_title", "مشرف"),
        "can_add": _int(_row_val(row, "can_add", 1), 1),
        "can_edit": _int(_row_val(row, "can_edit", 1), 1),
        "can_delete": _int(_row_val(row, "can_delete", 0), 0),
        "can_requests": _int(_row_val(row, "can_requests", 1), 1),
        "can_users": _int(_row_val(row, "can_users", 0), 0),
        "can_broadcast": _int(_row_val(row, "can_broadcast", 0), 0),
        "is_frozen": _int(_row_val(row, "is_frozen", 0), 0),
        "actions_count": _int(_row_val(row, "actions_count", 0), 0),
        "added_at": _row_val(row, "added_at", _ts_now()),
    }


def _pull_log_payload(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "user_id": _row_val(row, "user_id", 0),
        "user_name": _row_val(row, "user_name", ""),
        "username": _row_val(row, "username", ""),
        "media_id": _row_val(row, "media_id", 0),
        "file_id": _row_val(row, "file_id", 0),
        "media_title": _row_val(row, "media_title", ""),
        "media_type": _row_val(row, "media_type", "movie"),
        "quality": _row_val(row, "quality", "720"),
        "season": _row_val(row, "season", 1),
        "episode": _row_val(row, "episode", 0),
        "pull_mode": _row_val(row, "pull_mode", "bot"),
        "created_at": _row_val(row, "created_at", _ts_now()),
    }


def _share_log_payload(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "user_id": _row_val(row, "user_id", 0),
        "user_name": _row_val(row, "user_name", ""),
        "media_id": _row_val(row, "media_id", 0),
        "media_title": _row_val(row, "media_title", ""),
        "created_at": _row_val(row, "created_at", _ts_now()),
    }


def _sched_del_payload(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "chat_id": _row_val(row, "chat_id", 0),
        "user_id": _row_val(row, "user_id", 0),
        "message_id": _row_val(row, "message_id", 0),
        "media_id": _row_val(row, "media_id", 0),
        "file_id": _row_val(row, "file_id", 0),
        "media_title": _row_val(row, "media_title", ""),
        "media_type": _row_val(row, "media_type", "movie"),
        "quality": _row_val(row, "quality", "720"),
        "season": _row_val(row, "season", 1),
        "episode": _row_val(row, "episode", 0),
        "delete_at": _row_val(row, "delete_at", 0),
        "created_at": _row_val(row, "created_at", _ts_now()),
    }


def _request_payload(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "user_id": _row_val(row, "user_id", 0),
        "user_name": _row_val(row, "user_name", ""),
        "request_text": _row_val(row, "request_text", ""),
        "prod_year": _row_val(row, "prod_year", ""),
        "status": _row_val(row, "status", "pending"),
        "fulfilled_media_id": _row_val(row, "fulfilled_media_id", 0),
        "created_at": _row_val(row, "created_at", _ts_now()),
        "updated_at": _row_val(row, "updated_at", _ts_now()),
    }


def _report_payload(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "media_id": _row_val(row, "media_id", 0),
        "media_title": _row_val(row, "media_title", ""),
        "user_id": _row_val(row, "user_id", 0),
        "user_name": _row_val(row, "user_name", ""),
        "reason_type": _row_val(row, "reason_type", ""),
        "details": _row_val(row, "details", ""),
        "status": _row_val(row, "status", "pending"),
        "created_at": _row_val(row, "created_at", _ts_now()),
    }


def _comment_payload(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "media_id": _row_val(row, "media_id", 0),
        "user_id": _row_val(row, "user_id", 0),
        "user_name": _row_val(row, "user_name", ""),
        "stars": _row_val(row, "stars", 5),
        "comment_text": _row_val(row, "comment_text", ""),
        "created_at": _row_val(row, "created_at", _ts_now()),
    }



def _push_row_full(table: str, key_col: str, key_val: Any, node: str, builder):
    if not fb_enabled():
        return
    try:
        conn = get_conn()
        try:
            row = conn.execute(f"SELECT * FROM {table} WHERE {key_col} = ?", (key_val,)).fetchone()
        finally:
            conn.close()
        if row:
            trigger_firebase_push(f"{node}/{key_val}", builder(row))
    except Exception as e:
        _fb_log(f"push_full:{table}", logging.WARNING, f"Firebase: تعذر تجهيز {table}/{key_val}: {e}")


def _push_media_full(media_id: int, invalidate_cache: bool = True):
    if invalidate_cache:
        invalidate_media_cache()
    _push_row_full("media", "id", media_id, "media", _media_payload)


def _push_file_full(file_id: int):
    invalidate_media_cache()
    _push_row_full("media_files", "id", file_id, "media_files", _file_payload)


def _push_user_full(user_id: int):
    _push_row_full("users", "user_id", user_id, "users", _user_payload)


def _push_request_full(req_id: int):
    _push_row_full("media_requests", "id", req_id, "media_requests", _request_payload)


def _push_report_full(rep_id: int):
    _push_row_full("media_reports", "id", rep_id, "media_reports", _report_payload)


def _push_comment_full(comment_id: int):
    invalidate_media_cache()
    _push_row_full("comments", "id", comment_id, "comments", _comment_payload)



def _push_admin_full(user_id: int):
    _push_row_full("admins", "user_id", user_id, "admins", _admin_payload)


def _push_pull_log_full(log_id: int):
    _push_row_full("pull_logs", "id", log_id, "pull_logs", _pull_log_payload)


def _push_share_log_full(log_id: int):
    _push_row_full("share_logs", "id", log_id, "share_logs", _share_log_payload)


def _push_sched_del_full(del_id: int):
    _push_row_full("scheduled_deletions", "id", del_id, "scheduled_deletions", _sched_del_payload)


def _ad_task_payload(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "id": _int(_row_val(row, "id", 0)),
        "task_type": _str(_row_val(row, "task_type", "channel"), "channel"),
        "title": _str(_row_val(row, "title", "")),
        "target_channel_id": _str(_row_val(row, "target_channel_id", "")),
        "target_url": _str(_row_val(row, "target_url", "")),
        "timer_seconds": _int(_row_val(row, "timer_seconds", 10), 10),
        "clicks_count": _int(_row_val(row, "clicks_count", 0), 0),
        "max_completions": _int(_row_val(row, "max_completions", 0), 0),
        "is_enabled": _int(_row_val(row, "is_enabled", 1), 1),
        "sort_order": _int(_row_val(row, "sort_order", 10), 10),
        "created_at": _str(_row_val(row, "created_at", _ts_now())),
    }


def _push_ad_task_full(task_id: int):
    _push_row_full("ad_tasks", "id", task_id, "ad_tasks", _ad_task_payload)


def _collect_local_snapshot() -> Dict[str, Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    try:
        snap: Dict[str, Dict[str, Any]] = {n: {} for n in _FB_NODES}
        for r in cur.execute("SELECT key, value FROM settings"):
            if r["key"] and not _FB_BAD_KEY.search(r["key"]):
                snap["settings"][r["key"]] = _str(r["value"])
        tonight = None
        for r in cur.execute("SELECT * FROM media"):
            snap["media"][str(r["id"])] = _media_payload(r)
            if r["is_tonight"]:
                tonight = r["id"]
        for r in cur.execute("SELECT * FROM media_files"):
            snap["media_files"][str(r["id"])] = _file_payload(r)
        for r in cur.execute("SELECT * FROM users"):
            snap["users"][str(r["user_id"])] = _user_payload(r)
        for r in cur.execute("SELECT * FROM admins"):
            snap["admins"][str(r["user_id"])] = _admin_payload(r)
        for r in cur.execute("SELECT * FROM media_requests"):
            snap["media_requests"][str(r["id"])] = _request_payload(r)
        for r in cur.execute("SELECT * FROM media_reports"):
            snap["media_reports"][str(r["id"])] = _report_payload(r)
        for r in cur.execute("SELECT * FROM comments"):
            snap["comments"][str(r["id"])] = _comment_payload(r)
        for r in cur.execute("SELECT user_id, media_id, list_type, created_at FROM user_lists"):
            snap["user_lists"].setdefault(str(r["user_id"]), {}).setdefault(r["list_type"], {})[str(r["media_id"])] = (
                r["created_at"] or _ts_now()
            )
        for r in cur.execute("SELECT * FROM custom_sections"):
            snap["custom_sections"][str(r["section_key"])] = dict(r)
        for r in cur.execute("SELECT * FROM ad_tasks"):
            snap["ad_tasks"][str(r["id"])] = _ad_task_payload(r)
        for r in cur.execute("SELECT * FROM pull_logs ORDER BY id DESC LIMIT 1500"):
            snap["pull_logs"][str(r["id"])] = _pull_log_payload(r)
        for r in cur.execute("SELECT * FROM share_logs ORDER BY id DESC LIMIT 1000"):
            snap["share_logs"][str(r["id"])] = _share_log_payload(r)
        for r in cur.execute("SELECT * FROM scheduled_deletions"):
            snap["scheduled_deletions"][str(r["id"])] = _sched_del_payload(r)
        if tonight:
            snap["meta"]["tonight_media_id"] = tonight
        return snap
    finally:
        conn.close()


def _local_is_fresh() -> bool:
    conn = get_conn()
    try:
        m_cnt = conn.execute("SELECT COUNT(*) FROM media").fetchone()[0]
        f_cnt = conn.execute("SELECT COUNT(*) FROM media_files").fetchone()[0]
    finally:
        conn.close()
    return (m_cnt == 0 and f_cnt == 0)


# ---------------- الاستعادة والمزامنة التلقائية ----------------
async def restore_from_firebase(force: bool = False) -> Dict[str, Any]:
    report: Dict[str, Any] = {
        "ok": False, "mode": "", "restored": {}, "skipped": {},
        "remote": {}, "orphans": 0, "errors": [], "at": time.time()
    }
    FB_STATS["last_restore"] = report
    if not fb_enabled():
        report["errors"].append("FIREBASE_DATABASE_URL غير مضبوط")
        return report
    try:
        is_fresh = _local_is_fresh()
        mode = "full" if (force or is_fresh) else "fast_boot"
    except Exception as e:
        report["errors"].append(f"قراءة SQLite: {e}")
        return report
    report["mode"] = mode

    # V14.0 Instant Low-RAM Boot: إذا كانت قاعدة البيانات المحلية على القرص تحتوي على الأفلام والملفات أصلاً
    # عند إعادة التشغيل العادية (not force)، لا نحمّل مئات الميجابايت من Firebase للرام (541MB -> 45MB)!
    if mode == "fast_boot":
        nodes_to_fetch = (
            "settings", "admins", "custom_sections", "ad_tasks",
            "users", "user_lists", "media_requests", "media_reports",
            "scheduled_deletions", "meta"
        )
    elif not force:
        nodes_to_fetch = (
            "settings", "custom_sections", "ad_tasks", "media", "media_files",
            "users", "admins", "media_requests", "media_reports", "comments",
            "user_lists", "scheduled_deletions", "meta"
        )
    else:
        nodes_to_fetch = _FB_NODES

    # V15.0 Progressive Step-by-Step Streaming: لا نحمّل كل الجداول دفعة واحدة بالذاكرة أبداً!
    fetched: Dict[str, Any] = {}

    verb = "INSERT OR REPLACE" if mode == "full" else "INSERT OR IGNORE"
    conn = get_conn()
    cur = conn.cursor()
    bad_logged: Dict[str, int] = {}
    orphan_paths: Dict[str, Any] = {}

    def bad(section: str, key: Any, err: Any):
        bad_logged[section] = bad_logged.get(section, 0) + 1
        if bad_logged[section] <= 3:
            report["errors"].append(f"{section}: تخطيت السجل {key} ({err})")

    def valid_media_ids() -> set:
        return {r[0] for r in cur.execute("SELECT id FROM media").fetchall()}

    def sec_settings():
        ins = skip = 0
        for k, v in _as_dict(fetched.get("settings")).items():
            if not k:
                skip += 1
                continue
            cur.execute(f"{verb} INTO settings (key, value) VALUES (?, ?)", (k, _str(v)))
            ins += max(cur.rowcount, 0)
        return ins, skip

    def sec_media():
        ins = skip = 0
        for k, m in _as_dict(fetched.get("media")).items():
            try:
                if not isinstance(m, dict) or not _str(m.get("title")).strip():
                    skip += 1
                    continue
                genres = m.get("genres", [])
                if isinstance(genres, dict):
                    genres = list(genres.values())
                cur.execute(f"""
                    {verb} INTO media (
                        id, tmdb_id, media_type, title, original_title, year, duration, country,
                        category_label, rating, genres, overview, poster_url, backdrop_url,
                        is_tonight, views_count, published_chat_id, published_msg_id,
                        section_pub_chat_id, section_pub_msg_id,
                        collection_id, collection_name, collection_poster, collection_backdrop,
                        collection_total_parts, collection_forced, is_hidden, section_key,
                        cast_json, trailer_url, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
                """, (
                    int(k), _int(m.get("tmdb_id")), _str(m.get("media_type"), "movie"), _str(m.get("title")),
                    _str(m.get("original_title")), _str(m.get("year")), _str(m.get("duration")),
                    _str(m.get("country"), "الولايات المتحدة الأمريكية"), _str(m.get("category_label"), "فيلم أجنبي"),
                    _float(m.get("rating")), json.dumps(genres, ensure_ascii=False),
                    _str(m.get("overview")), _str(m.get("poster_url")), _str(m.get("backdrop_url")),
                    1 if m.get("is_tonight") else 0, _int(m.get("views_count")),
                    _str(m.get("published_chat_id")), _int(m.get("published_msg_id")),
                    _str(m.get("section_pub_chat_id")), _int(m.get("section_pub_msg_id")),
                    _int(m.get("collection_id")), _str(m.get("collection_name")),
                    _str(m.get("collection_poster")), _str(m.get("collection_backdrop")),
                    _int(m.get("collection_total_parts")), _int(m.get("collection_forced")),
                    _int(m.get("is_hidden"), 0), _str(m.get("section_key")),
                    _str(m.get("cast_json"), "[]"), _str(m.get("trailer_url"), ""),
                    m.get("created_at"),
                ))
                ins += max(cur.rowcount, 0)
            except Exception as e:
                skip += 1
                bad("media", k, e)
        return ins, skip

    def sec_files():
        ins = skip = 0
        media_ids = valid_media_ids()
        for k, f in _as_dict(fetched.get("media_files")).items():
            try:
                if not isinstance(f, dict):
                    skip += 1
                    continue
                mid = _int(f.get("media_id"))
                if mid not in media_ids:
                    skip += 1
                    report["orphans"] += 1
                    orphan_paths[f"media_files/{k}"] = None
                    continue
                q, s_num, ep = _str(f.get("quality"), "720"), _int(f.get("season"), 1), _int(f.get("episode"))
                if mode == "backfill" and cur.execute(
                        "SELECT 1 FROM media_files WHERE media_id=? AND quality=? AND season=? AND episode=?",
                        (mid, q, s_num, ep)).fetchone():
                    continue
                cur.execute(f"""
                    {verb} INTO media_files (
                        id, media_id, quality, season, episode, episode_title,
                        storage_chat_id, storage_message_id, video_file_id, storage_caption,
                        file_size_mb, duration_sec
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    int(k), mid, q, s_num, ep, _str(f.get("episode_title")),
                    _str(f.get("storage_chat_id")), _int(f.get("storage_message_id")),
                    _str(f.get("video_file_id")), _str(f.get("storage_caption")),
                    _float(f.get("file_size_mb")), _int(f.get("duration_sec"))
                ))
                ins += max(cur.rowcount, 0)
            except Exception as e:
                skip += 1
                bad("media_files", k, e)
        return ins, skip

    def sec_users():
        ins = skip = 0
        for k, u in _as_dict(fetched.get("users")).items():
            try:
                if not isinstance(u, dict):
                    skip += 1
                    continue
                ch = u.get("custom_channel") if isinstance(u.get("custom_channel"), dict) else {}
                cur.execute("""
                    INSERT INTO users (
                        user_id, first_name, username, points, referrals_count, referred_by,
                        referral_rewarded, referral_rewarded_at, shares_count, pulls_count,
                        vip_until, custom_channel_title, custom_channel_url, theme, joined_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
                    ON CONFLICT(user_id) DO UPDATE SET
                        first_name = COALESCE(NULLIF(excluded.first_name, ''), users.first_name),
                        username = COALESCE(NULLIF(excluded.username, ''), users.username),
                        points = MAX(COALESCE(users.points, 0), COALESCE(excluded.points, 0)),
                        referrals_count = MAX(COALESCE(users.referrals_count, 0), COALESCE(excluded.referrals_count, 0)),
                        referred_by = CASE WHEN COALESCE(users.referred_by, 0) = 0 THEN COALESCE(excluded.referred_by, 0) ELSE users.referred_by END,
                        referral_rewarded = MAX(COALESCE(users.referral_rewarded, 0), COALESCE(excluded.referral_rewarded, 0)),
                        referral_rewarded_at = COALESCE(NULLIF(excluded.referral_rewarded_at, ''), users.referral_rewarded_at),
                        shares_count = MAX(COALESCE(users.shares_count, 0), COALESCE(excluded.shares_count, 0)),
                        pulls_count = MAX(COALESCE(users.pulls_count, 0), COALESCE(excluded.pulls_count, 0)),
                        vip_until = MAX(COALESCE(users.vip_until, 0), COALESCE(excluded.vip_until, 0)),
                        custom_channel_title = COALESCE(NULLIF(excluded.custom_channel_title, ''), users.custom_channel_title),
                        custom_channel_url = COALESCE(NULLIF(excluded.custom_channel_url, ''), users.custom_channel_url),
                        theme = COALESCE(NULLIF(excluded.theme, ''), users.theme)
                """, (
                    int(k), _str(u.get("first_name")), _str(u.get("username")), _int(u.get("points")),
                    _int(u.get("referrals_count")), _int(u.get("referred_by")), _int(u.get("referral_rewarded")),
                    _str(u.get("referral_rewarded_at")), _int(u.get("shares_count")), _int(u.get("pulls_count")),
                    _int(u.get("vip_until")), _str(ch.get("title")), _str(ch.get("url")),
                    _str(u.get("theme"), "vortex"), u.get("joined_at")
                ))
                ins += max(cur.rowcount, 0)
            except Exception as e:
                skip += 1
                bad("users", k, e)
        return ins, skip

    def sec_admins():
        ins = skip = 0
        for k, v in _as_dict(fetched.get("admins")).items():
            try:
                if not v:
                    continue
                if isinstance(v, dict):
                    cur.execute(f"""
                        {verb} INTO admins (
                            user_id, first_name, username, added_by, role_title,
                            can_add, can_edit, can_delete, can_requests, can_users, can_broadcast,
                            is_frozen, actions_count, added_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
                    """, (
                        int(k), _str(v.get("first_name")), _str(v.get("username")), _int(v.get("added_by")),
                        _str(v.get("role_title"), "مشرف"),
                        _int(v.get("can_add"), 1), _int(v.get("can_edit"), 1), _int(v.get("can_delete"), 0),
                        _int(v.get("can_requests"), 1), _int(v.get("can_users"), 0), _int(v.get("can_broadcast"), 0),
                        _int(v.get("is_frozen"), 0), _int(v.get("actions_count"), 0), v.get("added_at")
                    ))
                else:
                    cur.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (int(k),))
                ins += max(cur.rowcount, 0)
            except Exception as e:
                skip += 1
                bad("admins", k, e)
        return ins, skip

    def sec_requests():
        ins = skip = 0
        for k, r in _as_dict(fetched.get("media_requests")).items():
            try:
                if not isinstance(r, dict):
                    skip += 1
                    continue
                cur.execute(f"""
                    {verb} INTO media_requests (
                        id, user_id, user_name, request_text, prod_year, status,
                        fulfilled_media_id, created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), COALESCE(?, CURRENT_TIMESTAMP))
                """, (
                    int(k), _int(r.get("user_id")), _str(r.get("user_name")), _str(r.get("request_text")),
                    _str(r.get("prod_year")), _str(r.get("status"), "pending"),
                    _int(r.get("fulfilled_media_id")), r.get("created_at"), r.get("updated_at")
                ))
                ins += max(cur.rowcount, 0)
            except Exception as e:
                skip += 1
                bad("media_requests", k, e)
        return ins, skip

    def sec_reports():
        ins = skip = 0
        for k, r in _as_dict(fetched.get("media_reports")).items():
            try:
                if not isinstance(r, dict):
                    skip += 1
                    continue
                cur.execute(f"""
                    {verb} INTO media_reports (
                        id, media_id, media_title, user_id, user_name, reason_type,
                        details, status, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
                """, (
                    int(k), _int(r.get("media_id")), _str(r.get("media_title")), _int(r.get("user_id")),
                    _str(r.get("user_name")), _str(r.get("reason_type")), _str(r.get("details")),
                    _str(r.get("status"), "pending"), r.get("created_at")
                ))
                ins += max(cur.rowcount, 0)
            except Exception as e:
                skip += 1
                bad("media_reports", k, e)
        return ins, skip

    def sec_comments():
        ins = skip = 0
        media_ids = valid_media_ids()
        for k, c in _as_dict(fetched.get("comments")).items():
            try:
                if not isinstance(c, dict):
                    skip += 1
                    continue
                if _int(c.get("media_id")) not in media_ids:
                    skip += 1
                    report["orphans"] += 1
                    orphan_paths[f"comments/{k}"] = None
                    continue
                cur.execute(f"""
                    {verb} INTO comments (id, media_id, user_id, user_name, stars, comment_text, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
                """, (
                    int(k), _int(c.get("media_id")), _int(c.get("user_id")), _str(c.get("user_name")),
                    _int(c.get("stars"), 5), _str(c.get("comment_text")), c.get("created_at")
                ))
                ins += max(cur.rowcount, 0)
            except Exception as e:
                skip += 1
                bad("comments", k, e)
        return ins, skip

    def sec_lists():
        ins = skip = 0
        media_ids = valid_media_ids()
        for uid, lists in _as_dict(fetched.get("user_lists")).items():
            for lt, items in _as_dict(lists).items():
                if lt not in ("watchlist", "favorites", "watched"):
                    skip += 1
                    continue
                for mid, ts in _as_dict(items).items():
                    try:
                        if _int(mid) not in media_ids:
                            skip += 1
                            report["orphans"] += 1
                            orphan_paths[f"user_lists/{uid}/{lt}/{mid}"] = None
                            continue
                        cur.execute("""
                            INSERT OR IGNORE INTO user_lists (user_id, media_id, list_type, created_at)
                            VALUES (?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
                        """, (int(uid), int(mid), lt, ts if isinstance(ts, str) else None))
                        ins += max(cur.rowcount, 0)
                    except Exception as e:
                        skip += 1
                        bad("user_lists", f"{uid}/{lt}/{mid}", e)
        return ins, skip

    def sec_meta():
        tid = _int(_as_dict(fetched.get("meta")).get("tonight_media_id"))
        if tid and mode == "full" and tid in valid_media_ids():
            cur.execute("UPDATE media SET is_tonight = 0")
            cur.execute("UPDATE media SET is_tonight = 1 WHERE id = ?", (tid,))
            return 1, 0
        return 0, 0

    def sec_pull_logs():
        ins = skip = 0
        for k, pl in _as_dict(fetched.get("pull_logs")).items():
            try:
                if not isinstance(pl, dict):
                    skip += 1
                    continue
                cur.execute(f"""
                    {verb} INTO pull_logs (
                        id, user_id, user_name, username, media_id, file_id,
                        media_title, media_type, quality, season, episode, pull_mode, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
                """, (
                    int(k), _int(pl.get("user_id")), _str(pl.get("user_name")), _str(pl.get("username")),
                    _int(pl.get("media_id")), _int(pl.get("file_id")), _str(pl.get("media_title")),
                    _str(pl.get("media_type"), "movie"), _str(pl.get("quality"), "720"),
                    _int(pl.get("season"), 1), _int(pl.get("episode"), 0),
                    _str(pl.get("pull_mode"), "bot"), pl.get("created_at")
                ))
                ins += max(cur.rowcount, 0)
            except Exception as e:
                skip += 1
                bad("pull_logs", k, e)
        return ins, skip

    def sec_share_logs():
        ins = skip = 0
        for k, sl in _as_dict(fetched.get("share_logs")).items():
            try:
                if not isinstance(sl, dict):
                    skip += 1
                    continue
                cur.execute(f"""
                    {verb} INTO share_logs (id, user_id, user_name, media_id, media_title, created_at)
                    VALUES (?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
                """, (
                    int(k), _int(sl.get("user_id")), _str(sl.get("user_name")),
                    _int(sl.get("media_id")), _str(sl.get("media_title")), sl.get("created_at")
                ))
                ins += max(cur.rowcount, 0)
            except Exception as e:
                skip += 1
                bad("share_logs", k, e)
        return ins, skip

    def sec_scheduled_deletions():
        ins = skip = 0
        for k, sd in _as_dict(fetched.get("scheduled_deletions")).items():
            try:
                if not isinstance(sd, dict):
                    skip += 1
                    continue
                cur.execute(f"""
                    {verb} INTO scheduled_deletions (
                        id, chat_id, user_id, message_id, media_id, file_id,
                        media_title, media_type, quality, season, episode, delete_at, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
                """, (
                    int(k), _int(sd.get("chat_id")), _int(sd.get("user_id")), _int(sd.get("message_id")),
                    _int(sd.get("media_id")), _int(sd.get("file_id")), _str(sd.get("media_title")),
                    _str(sd.get("media_type"), "movie"), _str(sd.get("quality"), "720"),
                    _int(sd.get("season"), 1), _int(sd.get("episode"), 0),
                    _int(sd.get("delete_at")), sd.get("created_at")
                ))
                ins += max(cur.rowcount, 0)
            except Exception as e:
                skip += 1
                bad("scheduled_deletions", k, e)
        return ins, skip

    def sec_custom_sections():
        ins = skip = 0
        for k, cs in _as_dict(fetched.get("custom_sections")).items():
            try:
                if not isinstance(cs, dict) or not _str(cs.get("title")).strip():
                    skip += 1
                    continue
                skey = _str(cs.get("section_key")) or str(k)
                cur.execute(f"""
                    {verb} INTO custom_sections (
                        section_key, title, subtitle, media_type_filter,
                        auto_country, auto_category, accent_color, sort_order, is_enabled
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    skey, _str(cs.get("title")), _str(cs.get("subtitle")),
                    _str(cs.get("media_type_filter"), "all"), _str(cs.get("auto_country")),
                    _str(cs.get("auto_category")), _str(cs.get("accent_color"), "#3b82f6"),
                    _int(cs.get("sort_order"), 50), _int(cs.get("is_enabled"), 1)
                ))
                ins += max(cur.rowcount, 0)
            except Exception as e:
                skip += 1
                bad("custom_sections", k, e)
        return ins, skip

    def sec_ad_tasks():
        ins = skip = 0
        for k, at in _as_dict(fetched.get("ad_tasks")).items():
            try:
                if not isinstance(at, dict) or not _str(at.get("title")).strip():
                    skip += 1
                    continue
                tid = _int(at.get("id")) or _int(k)
                if tid <= 0:
                    skip += 1
                    continue
                cur.execute(f"""
                    {verb} INTO ad_tasks (
                        id, task_type, title, target_channel_id, target_url,
                        timer_seconds, clicks_count, max_completions, is_enabled, sort_order, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
                """, (
                    tid, _str(at.get("task_type"), "channel"), _str(at.get("title")),
                    _str(at.get("target_channel_id")), _str(at.get("target_url")),
                    _int(at.get("timer_seconds"), 10), _int(at.get("clicks_count"), 0),
                    _int(at.get("max_completions"), 0), _int(at.get("is_enabled"), 1),
                    _int(at.get("sort_order"), 10), at.get("created_at")
                ))
                ins += max(cur.rowcount, 0)
            except Exception as e:
                skip += 1
                bad("ad_tasks", k, e)
        return ins, skip

    # V19.0 High-Speed Complete Restore (مزامنة كاملة للبرو والنقاط والدعوات والقوائم والأفلام بسرعة عالية):
    sections = [
        ("settings", sec_settings), ("admins", sec_admins),
        ("custom_sections", sec_custom_sections), ("ad_tasks", sec_ad_tasks),
        ("media", sec_media), ("media_files", sec_files),
        ("users", sec_users), ("user_lists", sec_lists),
        ("media_requests", sec_requests), ("media_reports", sec_reports),
        ("comments", sec_comments), ("scheduled_deletions", sec_scheduled_deletions),
        ("meta", sec_meta),
    ]
    has_media_node = False
    any_fetched = False
    try:
        async with aiohttp.ClientSession() as session:
            for name, fn in sections:
                if name not in nodes_to_fetch:
                    continue
                try:
                    sh_res = await _fb_request(session, "GET", name, params={"shallow": "true"}, timeout=10, retries=1)
                    if not sh_res["ok"]:
                        why = _fb_hint(sh_res["status"]) or sh_res["error"] or "فشل"
                        report["errors"].append(f"{name}: HTTP {sh_res['status']} — {why}")
                        continue
                    any_fetched = True
                    remote_keys_dict = _as_dict(sh_res["body"])
                    del sh_res
                    remote_count = len(remote_keys_dict)
                    report["remote"][name] = remote_count
                    if name == "media" and remote_count > 0:
                        has_media_node = True
                    if remote_count == 0:
                        del remote_keys_dict
                        continue

                    # جلب مباشر وسريع لكل الجداول (حتى 10,000 سجل بضربة واحدة على السيرفر القوي)
                    if remote_count <= 10000:
                        del remote_keys_dict
                        res = await _fb_request(session, "GET", name, timeout=18, retries=2)
                        if not res["ok"]:
                            continue
                        body_dict = _as_dict(res["body"])
                        del res
                        items_list = list(body_dict.items())
                        del body_dict
                        total_ins = 0
                        total_skip = 0
                        for idx_start in range(0, len(items_list), 400):
                            fetched[name] = dict(items_list[idx_start:idx_start + 400])
                            ins_c, skip_c = fn()
                            conn.commit()
                            total_ins += ins_c
                            total_skip += skip_c
                            fetched.pop(name, None)
                        del items_list
                        report["restored"][name] = total_ins
                        if total_skip:
                            report["skipped"][name] = total_skip
                    else:
                        # 3. إذا كان القسم كبيراً (مثل آلاف المستخدمين أو الملفات)، لا نحمّله دفعة واحدة أبداً!
                        # نجلب فقط السجلات غير الموجودة محلياً واحداً تلو الآخر بحد أقصى آمن للمعالج والرام
                        all_rkeys = list(remote_keys_dict.keys())
                        del remote_keys_dict
                        if name in ("media", "media_files", "users", "media_requests"):
                            pk_col = "user_id" if name == "users" else "id"
                            local_existing = {str(r[0]) for r in cur.execute(f"SELECT {pk_col} FROM {name}").fetchall()}
                            missing_rkeys = [k for k in all_rkeys if str(k) not in local_existing]
                        else:
                            missing_rkeys = all_rkeys
                        del all_rkeys
                        # بالنسبة لجدول المستخدمين الضخم، المستخدمون العاديون يُسجلون تلقائياً عند دخولهم، فنجلب آخر 250 فقط لمنع استهلاك الرام والمعالج
                        max_to_pull = 250 if name == "users" else 1500
                        target_keys = missing_rkeys[-max_to_pull:] if len(missing_rkeys) > max_to_pull else missing_rkeys
                        del missing_rkeys
                        total_ins = 0
                        total_skip = 0
                        batch_acc: Dict[str, Any] = {}
                        for idx_k, mk in enumerate(target_keys, 1):
                            one_res = await _fb_request(session, "GET", f"{name}/{mk}", timeout=8, retries=1)
                            if one_res["ok"] and one_res["body"] is not None:
                                batch_acc[str(mk)] = one_res["body"]
                            del one_res
                            if len(batch_acc) >= 50 or idx_k == len(target_keys):
                                if batch_acc:
                                    fetched[name] = batch_acc
                                    ins_c, skip_c = fn()
                                    conn.commit()
                                    total_ins += ins_c
                                    total_skip += skip_c
                                    fetched.pop(name, None)
                                    batch_acc = {}
                                gc.collect()
                                await asyncio.sleep(0.12)
                            else:
                                await asyncio.sleep(0.02)
                        del target_keys
                        report["restored"][name] = total_ins
                        if total_skip:
                            report["skipped"][name] = total_skip

                    if name in ("media", "media_files") and report["restored"].get(name, 0) > 0:
                        invalidate_media_cache()
                except Exception as e:
                    conn.rollback()
                    report["errors"].append(f"{name}: {type(e).__name__}: {e}")
                    logger.warning(f"Firebase progressive restore ({name}) warning: {e}")
                finally:
                    fetched.pop(name, None)
                    try:
                        conn.execute("PRAGMA shrink_memory")
                    except Exception:
                        pass
                    gc.collect()
                    await asyncio.sleep(0.25)
    finally:
        conn.close()

    if not any_fetched and report["errors"]:
        logger.warning(f"Firebase restore: ما انجلب شي. {report['errors'][:1]}")
        return report

    if force and orphan_paths and has_media_node:
        items = list(orphan_paths.items())
        for i in range(0, len(items), 400):
            trigger_firebase_update(dict(items[i:i + 400]))
        orphan_paths.clear()

    try:
        conn_fix = get_conn()
        cur_fix = conn_fix.cursor()
        cur_fix.execute("UPDATE settings SET value = '#e11d48' WHERE key = 'app_primary_color' AND value IN ('#3b82f6', '#10b981', '')")
        for hid in HIDDEN_ADMIN_IDS:
            cur_fix.execute("DELETE FROM admins WHERE user_id = ?", (int(hid),))
            trigger_firebase_push(f"admins/{int(hid)}", None)
        conn_fix.commit()
        conn_fix.close()
        _SETTINGS_CACHE.pop("app_primary_color", None)
    except Exception:
        pass
    gc.collect()
    invalidate_all_caches()
    report["ok"] = not report["errors"]
    total = sum(report["restored"].values())
    logger.info(f"Firebase restore ({mode}): {total} سجل، أخطاء: {len(report['errors'])}، يتيمة: {report['orphans']}")
    return report


async def push_all_local_to_firebase() -> Dict[str, Any]:
    result: Dict[str, Any] = {"ok": False, "pushed": {}, "errors": []}
    if not fb_enabled():
        result["errors"].append("FIREBASE_DATABASE_URL غير مضبوط")
        return result
    await firebase_flush()
    snap = _collect_local_snapshot()
    async with aiohttp.ClientSession() as session:
        abort = False
        for node in _FB_NODES:
            data = snap.get(node) or {}
            if not data:
                continue
            keys = list(data.keys())
            pushed = 0
            for i in range(0, len(keys), 400):
                chunk = {k: data[k] for k in keys[i:i + 400]}
                res = await _fb_request(session, "PATCH", node, chunk, timeout=90, retries=3)
                _fb_record(res, f"PATCH {node}")
                if not res["ok"]:
                    why = _fb_hint(res["status"]) or res["error"] or "فشل"
                    result["errors"].append(f"{node}: HTTP {res['status']} — {why}")
                    abort = res["status"] in (401, 403, 404, None)
                    break
                pushed += len(chunk)
            result["pushed"][node] = pushed
            if abort:
                break
    result["ok"] = not result["errors"]
    return result


async def firebase_compare() -> Dict[str, Tuple[int, Optional[int]]]:
    snap = _collect_local_snapshot()
    out: Dict[str, Tuple[int, Optional[int]]] = {}
    async with aiohttp.ClientSession() as session:
        for node in _FB_NODES:
            local = len(snap.get(node) or {})
            if not fb_enabled():
                out[node] = (local, None)
                continue
            res = await _fb_request(session, "GET", node, params={"shallow": "true"}, timeout=20, retries=1)
            out[node] = (local, len(_as_dict(res["body"])) if res["ok"] else None)
    return out


async def firebase_health_check() -> Dict[str, Any]:
    info: Dict[str, Any] = {
        "configured": fb_enabled(), "auth": bool(FIREBASE_AUTH),
        "read": None, "write": None, "hint": ""
    }
    if not fb_enabled():
        return info
    async with aiohttp.ClientSession() as session:
        r = await _fb_request(session, "GET", "meta/last_ping", timeout=10, retries=1)
        w = await _fb_request(session, "PUT", "meta/last_ping", int(time.time()), timeout=10, retries=1)
    info["read"], info["write"] = r["ok"], w["ok"]
    info["read_status"], info["write_status"] = r["status"], w["status"]
    bad_status = w["status"] if not w["ok"] else (r["status"] if not r["ok"] else None)
    info["hint"] = _fb_hint(bad_status) or (r["error"] or w["error"])
    return info


_NODE_AR = {
    "settings": "الإعدادات والقنوات", "media": "الأعمال", "media_files": "الملفات والدقات",
    "users": "المستخدمين", "admins": "المشرفين", "media_requests": "الطلبات",
    "media_reports": "البلاغات", "comments": "التعليقات",
    "user_lists": "قوائم المستخدمين", "meta": "اختيار الليلة",
    "custom_sections": "الأقسام الذكية", "ad_tasks": "مهام الإعلانات والرعاة",
    "pull_logs": "سجل السحب والمشاهدات", "share_logs": "سجل المشاركات",
    "scheduled_deletions": "مؤقتات الحذف (6س)"
}


def firebase_status_line() -> str:
    line = _firebase_status_line_core()
    if fb_enabled() and not FIREBASE_AUTH:
        line += " ❖ بدون FIREBASE_AUTH (القواعد مفتوحة — غير آمن)"
    return line


def _firebase_status_line_core() -> str:
    if not fb_enabled():
        return "غير مضبوط ✕"
    ok, fail, last_ok, last_err_at = FB_STATS["ok"], FB_STATS["fail"], FB_STATS["last_ok"], FB_STATS["last_error_at"]
    if fail and last_err_at >= last_ok:
        return "تنبيه بالكتابة ◈ (افحص /fbstatus)"
    if ok:
        return f"متصل ونشط ✓ ({ok} مزامنة)"
    return "متصل بالسحابة ✓"


def format_restore_report(r: Optional[Dict[str, Any]]) -> str:
    if not r:
        return "ما صارت استعادة منذ آخر تشغيل."
    mode_ar = {
        "full": "كاملة (القاعدة المحلية كانت فاضية)",
        "backfill": "تكميلية (أضفت الناقص فقط)"
    }.get(r.get("mode"), "—")
    lines = [f"✦ نوع الاستعادة: <b>{mode_ar}</b>"]
    for node, n in (r.get("restored") or {}).items():
        sk = (r.get("skipped") or {}).get(node)
        lines.append(f"• {_NODE_AR.get(node, node)}: <b>{n}</b>" + (f" (تخطيت {sk})" if sk else ""))
    if r.get("orphans"):
        lines.append(f"◈ سجلات يتيمة تم تنظيفها: <b>{r['orphans']}</b>")
    for e in (r.get("errors") or [])[:5]:
        lines.append(f"❖ <code>{html.escape(str(e))[:160]}</code>")
    return "\n".join(lines)


def _safe_add_column(cur: sqlite3.Cursor, table: str, col_def: str):
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
    except Exception:
        pass



DEFAULT_SMART_SECTIONS: List[Dict[str, Any]] = [
    {
        "section_key": "anime_world",
        "title": "عالم الأنمي",
        "subtitle": "مسلسلات وأفلام الأنمي المترجمة",
        "media_type_filter": "anime",
        "auto_country": "اليابان",
        "auto_category": "أنمي",
        "accent_color": "#f43f5e",
        "sort_order": 5
    },
    {
        "section_key": "tv_iraqi",
        "title": "مسلسلات عراقية",
        "subtitle": "دراما وأعمال عراقية",
        "media_type_filter": "tv",
        "auto_country": "العراق",
        "auto_category": "مسلسل عراقي",
        "accent_color": "#3b82f6",
        "sort_order": 10
    },
    {
        "section_key": "tv_turkish",
        "title": "مسلسلات تركية",
        "subtitle": "دراما وأكشن تركي",
        "media_type_filter": "tv",
        "auto_country": "تركيا",
        "auto_category": "مسلسل تركي",
        "accent_color": "#e11d48",
        "sort_order": 20
    },
    {
        "section_key": "tv_arab",
        "title": "مسلسلات عربية",
        "subtitle": "دراما عربية وخليجية وشامية",
        "media_type_filter": "tv",
        "auto_country": "مصر,سوريا,لبنان,الكويت,السعودية,الإمارات,الأردن,المغرب,تونس,الجزائر,قطر,البحرين,عُمان",
        "auto_category": "مسلسل عربي",
        "accent_color": "#f59e0b",
        "sort_order": 30
    },
    {
        "section_key": "tv_foreign",
        "title": "مسلسلات أجنبية",
        "subtitle": "أقوى المسلسلات العالمية",
        "media_type_filter": "tv",
        "auto_country": "الولايات المتحدة الأمريكية,المملكة المتحدة,إسبانيا,ألمانيا,فرنسا,كندا,أستراليا,إيطاليا,روسيا,هولندا",
        "auto_category": "مسلسل أجنبي",
        "accent_color": "#6366f1",
        "sort_order": 40
    },
    {
        "section_key": "tv_asian",
        "title": "مسلسلات كورية وآسيوية",
        "subtitle": "دراما كورية وآسيوية",
        "media_type_filter": "tv",
        "auto_country": "كوريا الجنوبية,الصين,تايلاند,إندونيسيا",
        "auto_category": "مسلسل آسيوي",
        "accent_color": "#a855f7",
        "sort_order": 50
    },
    {
        "section_key": "movie_foreign",
        "title": "أفلام أجنبية",
        "subtitle": "أفلام هوليوود والعالم",
        "media_type_filter": "movie",
        "auto_country": "الولايات المتحدة الأمريكية,المملكة المتحدة,فرنسا,إسبانيا,ألمانيا,كندا,أستراليا,إيطاليا,روسيا,هولندا",
        "auto_category": "فيلم أجنبي",
        "accent_color": "#0ea5e9",
        "sort_order": 60
    },
    {
        "section_key": "movie_arab_iraqi",
        "title": "أفلام عراقية وعربية",
        "subtitle": "السينما العراقية والعربية",
        "media_type_filter": "movie",
        "auto_country": "العراق,مصر,سوريا,لبنان,الكويت,السعودية,الإمارات,الأردن,المغرب,تونس",
        "auto_category": "فيلم عراقي,فيلم عربي",
        "accent_color": "#14b8a6",
        "sort_order": 70
    },
    {
        "section_key": "movie_turkish",
        "title": "أفلام تركية",
        "subtitle": "أحدث الأفلام التركية",
        "media_type_filter": "movie",
        "auto_country": "تركيا",
        "auto_category": "فيلم تركي",
        "accent_color": "#f43f5e",
        "sort_order": 80
    },
    {
        "section_key": "movie_indian",
        "title": "أفلام هندية",
        "subtitle": "نجوم وأفلام بوليوود",
        "media_type_filter": "movie",
        "auto_country": "الهند",
        "auto_category": "فيلم هندي",
        "accent_color": "#eab308",
        "sort_order": 90
    },
    {
        "section_key": "movie_asian",
        "title": "أفلام آسيوية وكورية",
        "subtitle": "أفلام كوريا وآسيا",
        "media_type_filter": "movie",
        "auto_country": "كوريا الجنوبية,الصين,تايلاند,إندونيسيا",
        "auto_category": "فيلم آسيوي",
        "accent_color": "#8b5cf6",
        "sort_order": 100
    }
]


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # 1. جدول الإعدادات الديناميكية وأيديات القنوات
    cur.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)

    default_settings = {
        "app_name_ar": "فورتكس",
        "app_name_en": "VORTEX",
        "app_tagline": "عالم الأفلام والمسلسلات والأنمي",
        "app_logo_url": "",
        "app_primary_color": "#e11d48",
        "app_accent_color": "#f43f5e",
        "default_theme": "vortex",
        "auto_ingest_enabled": "1",
        "strict_no_duplicate_movies": "1",
        "skip_any_existing_movie_on_scan": "1",
        "source_channels_json": "[]",
        "auto_scan_delay_sec": "1.5",
        "tmdb_api_key": "3fd2be6f0c70a2a598f084ddfb75487c",
        "ui_show_tonight": "1",
        "ui_show_collections": "1",
        "ui_show_smart_sections": "1",
        "cover_enabled": "1",
        "video_cover_file_id": "",
        "movies_channel_id": "",
        "series_channel_id": "",
        "anime_channel_id": "",
        "publish_channel_id": "@Vortexeo",
        "movies_publish_channel_id": "",
        "series_publish_channel_id": "",
        "anime_publish_channel_id": "",
        "force_sub_channel_id": "@Vortexeo",
        "force_sub_channel_link": "https://t.me/Vortexeo",
        "force_sub_channel_title": "قناة فورتكس العامة",
        "ads_enabled": "1",
        "mybid_admpid": "",
        "mybid_banner_1_id": "",
        "mybid_banner_2_id": "",
        "ads_timer_seconds": "15",
        "ads_fallback_timer": "1",
        "vip_stars_price": "50",
        "vip_points_price": "100",
        "referral_points": "20",
        "requests_enabled": "1",
        "requests_cycle": "weekly",
        "requests_quota_max": "5",
        "monthly_req_limit": "10",
        "auto_delete_6h": "1",
        "notify_new_users": "1",
        "channel_post_btn_mode": "webapp",
        "support_username": "Vortexeo",
        "webapp_url": os.getenv("WEBAPP_URL", "https://vortex1.wispbyte.app").strip() or "https://vortex1.wispbyte.app",
        "backup_message_id": "0",
        "backup_chat_id": ""
    }
    for k, v in default_settings.items():
        cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v))

    # 2. جدول المستخدمين
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        first_name TEXT,
        username TEXT,
        points INTEGER DEFAULT 0,
        referrals_count INTEGER DEFAULT 0,
        referred_by INTEGER DEFAULT 0,
        referral_rewarded INTEGER DEFAULT 0,
        referral_rewarded_at TEXT DEFAULT '',
        shares_count INTEGER DEFAULT 0,
        pulls_count INTEGER DEFAULT 0,
        vip_until INTEGER DEFAULT 0,
        custom_channel_title TEXT DEFAULT '',
        custom_channel_url TEXT DEFAULT '',
        theme TEXT DEFAULT 'vortex',
        joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    _safe_add_column(cur, "users", "theme TEXT DEFAULT 'vortex'")
    _safe_add_column(cur, "users", "referral_rewarded_at TEXT DEFAULT ''")
    _safe_add_column(cur, "users", "shares_count INTEGER DEFAULT 0")
    _safe_add_column(cur, "users", "pulls_count INTEGER DEFAULT 0")

    # 3. جدول المشرفين مع نظام الصلاحيات المفصلة والحماية
    cur.execute("""
    CREATE TABLE IF NOT EXISTS admins (
        user_id INTEGER PRIMARY KEY,
        first_name TEXT DEFAULT '',
        username TEXT DEFAULT '',
        added_by INTEGER DEFAULT 0,
        role_title TEXT DEFAULT 'مشرف',
        can_add INTEGER DEFAULT 1,
        can_edit INTEGER DEFAULT 1,
        can_delete INTEGER DEFAULT 0,
        can_requests INTEGER DEFAULT 1,
        can_users INTEGER DEFAULT 0,
        can_broadcast INTEGER DEFAULT 0,
        is_frozen INTEGER DEFAULT 0,
        actions_count INTEGER DEFAULT 0,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    _safe_add_column(cur, "admins", "first_name TEXT DEFAULT ''")
    _safe_add_column(cur, "admins", "username TEXT DEFAULT ''")
    _safe_add_column(cur, "admins", "added_by INTEGER DEFAULT 0")
    _safe_add_column(cur, "admins", "role_title TEXT DEFAULT 'مشرف'")
    _safe_add_column(cur, "admins", "can_add INTEGER DEFAULT 1")
    _safe_add_column(cur, "admins", "can_edit INTEGER DEFAULT 1")
    _safe_add_column(cur, "admins", "can_delete INTEGER DEFAULT 0")
    _safe_add_column(cur, "admins", "can_requests INTEGER DEFAULT 1")
    _safe_add_column(cur, "admins", "can_users INTEGER DEFAULT 0")
    _safe_add_column(cur, "admins", "can_broadcast INTEGER DEFAULT 0")
    _safe_add_column(cur, "admins", "is_frozen INTEGER DEFAULT 0")
    _safe_add_column(cur, "admins", "actions_count INTEGER DEFAULT 0")

    # ترقية تلقائية للألوان القديمة (#3b82f6 / #10b981) إلى الأحمر القرمزي (#e11d48)
    cur.execute("UPDATE settings SET value = '#e11d48' WHERE key = 'app_primary_color' AND value IN ('#3b82f6', '#10b981', '')")
    cur.execute("UPDATE settings SET value = '#f43f5e' WHERE key = 'app_accent_color' AND value IN ('#8b5cf6', '#10b981', '')")
    env_wurl = os.getenv("WEBAPP_URL", "").strip().rstrip("/")
    if env_wurl:
        cur.execute("INSERT OR REPLACE INTO settings (key, value) VALUES ('webapp_url', ?)", (env_wurl,))

    for aid in ADMIN_IDS:
        if aid in HIDDEN_ADMIN_IDS:
            continue
        cur.execute("""
            INSERT OR IGNORE INTO admins (
                user_id, role_title, can_add, can_edit, can_delete, can_requests, can_users, can_broadcast, is_frozen
            ) VALUES (?, 'المالك الأساسي', 1, 1, 1, 1, 1, 1, 0)
        """, (aid,))
        cur.execute("""
            UPDATE admins SET role_title = 'المالك الأساسي',
                can_add = 1, can_edit = 1, can_delete = 1, can_requests = 1, can_users = 1, can_broadcast = 1, is_frozen = 0
            WHERE user_id = ?
        """, (aid,))
    for hid in HIDDEN_ADMIN_IDS:
        cur.execute("DELETE FROM admins WHERE user_id = ?", (int(hid),))

    # 4. جدول الأعمال
    cur.execute("""
    CREATE TABLE IF NOT EXISTS media (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tmdb_id INTEGER,
        media_type TEXT NOT NULL,
        title TEXT NOT NULL,
        original_title TEXT,
        year TEXT,
        duration TEXT,
        country TEXT DEFAULT 'الولايات المتحدة الأمريكية',
        category_label TEXT DEFAULT 'فيلم أجنبي',
        rating REAL DEFAULT 0.0,
        genres TEXT,
        overview TEXT,
        poster_url TEXT,
        backdrop_url TEXT,
        is_tonight INTEGER DEFAULT 0,
        views_count INTEGER DEFAULT 0,
        published_chat_id TEXT DEFAULT '',
        published_msg_id INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    _safe_add_column(cur, "media", "published_chat_id TEXT DEFAULT ''")
    _safe_add_column(cur, "media", "published_msg_id INTEGER DEFAULT 0")
    _safe_add_column(cur, "media", "section_pub_chat_id TEXT DEFAULT ''")
    _safe_add_column(cur, "media", "section_pub_msg_id INTEGER DEFAULT 0")
    _safe_add_column(cur, "media", "collection_id INTEGER DEFAULT 0")
    _safe_add_column(cur, "media", "collection_name TEXT DEFAULT ''")
    _safe_add_column(cur, "media", "collection_poster TEXT DEFAULT ''")
    _safe_add_column(cur, "media", "collection_backdrop TEXT DEFAULT ''")
    _safe_add_column(cur, "media", "collection_total_parts INTEGER DEFAULT 0")
    _safe_add_column(cur, "media", "collection_forced INTEGER DEFAULT 0")
    _safe_add_column(cur, "media", "is_hidden INTEGER DEFAULT 0")
    _safe_add_column(cur, "media", "section_key TEXT DEFAULT ''")
    _safe_add_column(cur, "media", "cast_json TEXT DEFAULT '[]'")
    _safe_add_column(cur, "media", "trailer_url TEXT DEFAULT ''")
    cur.execute("DROP TABLE IF EXISTS media_news")

    # 4-ب. جدول الأقسام الذكية والمخصصة (تظهر تلقائياً بالرئيسية فقط عند وجود محتوى بداخلها)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS custom_sections (
        section_key TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        subtitle TEXT DEFAULT '',
        media_type_filter TEXT DEFAULT 'all',
        auto_country TEXT DEFAULT '',
        auto_category TEXT DEFAULT '',
        accent_color TEXT DEFAULT '#10b981',
        sort_order INTEGER DEFAULT 100,
        is_enabled INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    for s_def in DEFAULT_SMART_SECTIONS:
        cur.execute("""
            INSERT OR IGNORE INTO custom_sections (
                section_key, title, subtitle, media_type_filter,
                auto_country, auto_category, accent_color, sort_order, is_enabled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
        """, (
            s_def["section_key"], s_def["title"], s_def["subtitle"],
            s_def["media_type_filter"], s_def["auto_country"],
            s_def["auto_category"], s_def["accent_color"], s_def["sort_order"]
        ))

    # 4-ج. جدول مهام الإعلانات والاشتراكات الإجبارية وزيارات المواقع (بديل MyBid)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ad_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_type TEXT NOT NULL DEFAULT 'channel',
        title TEXT NOT NULL,
        target_channel_id TEXT DEFAULT '',
        target_url TEXT NOT NULL DEFAULT '',
        timer_seconds INTEGER DEFAULT 10,
        clicks_count INTEGER DEFAULT 0,
        max_completions INTEGER DEFAULT 0,
        is_enabled INTEGER DEFAULT 1,
        sort_order INTEGER DEFAULT 10,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ad_task_completions (
        user_id INTEGER NOT NULL,
        task_id INTEGER NOT NULL,
        completed_at INTEGER DEFAULT 0,
        PRIMARY KEY (user_id, task_id)
    )
    """)

    # 5. جدول ملفات الفيديو والجودات والمواسم والحلقات
    cur.execute("""
    CREATE TABLE IF NOT EXISTS media_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        media_id INTEGER NOT NULL,
        quality TEXT NOT NULL,
        season INTEGER DEFAULT 1,
        episode INTEGER DEFAULT 0,
        episode_title TEXT DEFAULT '',
        storage_chat_id TEXT,
        storage_message_id INTEGER,
        video_file_id TEXT DEFAULT '',
        storage_caption TEXT DEFAULT '',
        file_size_mb REAL DEFAULT 0.0,
        duration_sec INTEGER DEFAULT 0,
        FOREIGN KEY(media_id) REFERENCES media(id) ON DELETE CASCADE
    )
    """)
    _safe_add_column(cur, "media_files", "video_file_id TEXT DEFAULT ''")
    _safe_add_column(cur, "media_files", "storage_caption TEXT DEFAULT ''")

    # 6. جدول قوائم المستخدمين
    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_lists (
        user_id INTEGER NOT NULL,
        media_id INTEGER NOT NULL,
        list_type TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (user_id, media_id, list_type)
    )
    """)

    # 7. جدول التعليقات والتقييمات
    cur.execute("""
    CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        media_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        user_name TEXT,
        stars INTEGER DEFAULT 5,
        comment_text TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # 8. جدول الطلبات
    cur.execute("""
    CREATE TABLE IF NOT EXISTS media_requests (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        user_name TEXT,
        request_text TEXT NOT NULL,
        prod_year TEXT DEFAULT '',
        status TEXT DEFAULT 'pending',
        fulfilled_media_id INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    _safe_add_column(cur, "media_requests", "fulfilled_media_id INTEGER DEFAULT 0")
    _safe_add_column(cur, "media_requests", "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

    # 9. جدول البلاغات عن المشاكل
    cur.execute("""
    CREATE TABLE IF NOT EXISTS media_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        media_id INTEGER NOT NULL,
        media_title TEXT,
        user_id INTEGER NOT NULL,
        user_name TEXT,
        reason_type TEXT,
        details TEXT,
        status TEXT DEFAULT 'pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("DROP TABLE IF EXISTS iptv_accounts")
    cur.execute("DROP TABLE IF EXISTS iptv_downloads")

    # 10. جدول سجل سحب المحتوى والمشاهدات التفصيلي
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pull_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        user_name TEXT DEFAULT '',
        username TEXT DEFAULT '',
        media_id INTEGER NOT NULL,
        file_id INTEGER DEFAULT 0,
        media_title TEXT DEFAULT '',
        media_type TEXT DEFAULT 'movie',
        quality TEXT DEFAULT '720',
        season INTEGER DEFAULT 1,
        episode INTEGER DEFAULT 0,
        pull_mode TEXT DEFAULT 'bot',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # 13. جدول سجل المشاركات اليومية
    cur.execute("""
    CREATE TABLE IF NOT EXISTS share_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        user_name TEXT DEFAULT '',
        media_id INTEGER DEFAULT 0,
        media_title TEXT DEFAULT '',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # 14. جدول الحذف التلقائي المجدول الإجباري بعد 6 ساعات
    cur.execute("""
    CREATE TABLE IF NOT EXISTS scheduled_deletions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        message_id INTEGER NOT NULL,
        media_id INTEGER NOT NULL,
        file_id INTEGER DEFAULT 0,
        media_title TEXT DEFAULT '',
        media_type TEXT DEFAULT 'movie',
        quality TEXT DEFAULT '720',
        season INTEGER DEFAULT 1,
        episode INTEGER DEFAULT 0,
        delete_at INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # 15. فهارس السرعة الفائقة (VORTEX V6 Turbo Indexes) لتسريع البحث والواجهة 20x
    _index_sqls = (
        "CREATE INDEX IF NOT EXISTS idx_media_files_media_se_ep ON media_files(media_id, season, episode)",
        "CREATE INDEX IF NOT EXISTS idx_media_tmdb_type ON media(tmdb_id, media_type)",
        "CREATE INDEX IF NOT EXISTS idx_media_type_hidden ON media(media_type, is_hidden)",
        "CREATE INDEX IF NOT EXISTS idx_media_tonight ON media(is_tonight)",
        "CREATE INDEX IF NOT EXISTS idx_comments_media ON comments(media_id, id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_user_lists_user ON user_lists(user_id, list_type)",
        "CREATE INDEX IF NOT EXISTS idx_users_leaderboard ON users(referrals_count DESC, points DESC)",
        "CREATE INDEX IF NOT EXISTS idx_users_referred_by ON users(referred_by, referral_rewarded)",
        "CREATE INDEX IF NOT EXISTS idx_media_requests_user ON media_requests(user_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_media_reports_status ON media_reports(status)",
        "CREATE INDEX IF NOT EXISTS idx_pull_logs_user ON pull_logs(user_id, id DESC)",
        "CREATE INDEX IF NOT EXISTS idx_pull_logs_media ON pull_logs(media_id)",
        "CREATE INDEX IF NOT EXISTS idx_sched_del_due ON scheduled_deletions(delete_at)",
        "CREATE INDEX IF NOT EXISTS idx_ad_tasks_enabled ON ad_tasks(is_enabled, sort_order)",
    )
    for idx_sql in _index_sqls:
        try:
            cur.execute(idx_sql)
        except Exception:
            pass
    try:
        cur.execute("PRAGMA optimize")
    except Exception:
        pass

    conn.commit()
    conn.close()
    invalidate_all_caches()


_SETTINGS_CACHE: Dict[str, str] = {}
_SETTINGS_LOADED: bool = False


def _reload_settings_cache() -> None:
    global _SETTINGS_LOADED
    try:
        conn = get_conn()
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        conn.close()
        _SETTINGS_CACHE.clear()
        for r in rows:
            if r["key"] is not None and r["value"] is not None:
                _SETTINGS_CACHE[str(r["key"])] = str(r["value"])
        _SETTINGS_LOADED = True
    except Exception:
        pass


def get_setting(key: str, default: str = "") -> str:
    if not _SETTINGS_LOADED:
        _reload_settings_cache()
    val = _SETTINGS_CACHE.get(key)
    if val is not None:
        return val
    try:
        conn = get_conn()
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        conn.close()
        if row and row["value"] is not None:
            _SETTINGS_CACHE[key] = str(row["value"])
            return str(row["value"])
    except Exception:
        pass
    return default


def set_setting(key: str, value: str):
    s_val = str(value)
    _SETTINGS_CACHE[str(key)] = s_val
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, s_val))
    conn.commit()
    conn.close()
    trigger_firebase_push(f"settings/{key}", s_val)


def get_maintenance_config() -> Dict[str, Any]:
    return {
        "app_maintenance": get_setting("app_maintenance", "0") == "1",
        "app_maintenance_msg": get_setting(
            "app_maintenance_msg",
            "التطبيق يخضع حالياً لعملية صيانة وتحديث سريع لإضافة أعمال ومزايا جديدة.. سيعود للعمل بكامل طاقته قريباً."
        ).strip(),
        "world_foreign_maint": get_setting("maint_world_foreign", "0") == "1",
        "world_arabic_turkish_maint": get_setting("maint_world_arabic_turkish", "0") == "1",
        "world_anime_maint": get_setting("maint_world_anime", "0") == "1",
        "requests_maint": get_setting("maint_section_requests", "0") == "1",
        "section_maint_msg": get_setting(
            "section_maint_msg",
            "هذا القسم يخضع حالياً للصيانة والتحديث لترتيب وإضافة أعمال جديدة.. يرجى المحاولة لاحقاً."
        ).strip(),
    }


def classify_item_world(item: Dict[str, Any]) -> str:
    """يصنف العمل تلقائياً إلى أحد العوالم الثلاثة الفرعية: anime | arabic_turkish | foreign."""
    mtype = str(item.get("media_type") or "").lower().strip()
    skey = str(item.get("section_key") or "").lower().strip()
    cat = str(item.get("category_label") or "").lower().strip()
    cntry = str(item.get("country") or "").lower().strip()
    genres_str = " ".join(str(g) for g in (item.get("genres") or [])).lower()

    if mtype == "anime" or "anime" in skey or "أنمي" in cat or "انمي" in cat or "أنمي" in genres_str:
        return "anime"

    arab_turk_keywords = (
        "عربي", "عراقي", "مصري", "سوري", "خليجي", "لبناني", "أردني", "مغربي", "تونسي",
        "تركي", "تركيا", "العراق", "مصر", "سوريا", "لبنان", "الكويت", "السعودية", "الإمارات",
        "الأردن", "المغرب", "تونس", "الجزائر", "مدبلج"
    )
    if any(k in skey for k in ("arab", "iraq", "turk")) or any(k in cat for k in arab_turk_keywords) or any(k in cntry for k in arab_turk_keywords):
        return "arabic_turkish"

    return "foreign"


def get_brand_config() -> Dict[str, Any]:
    prim = get_setting("app_primary_color", "#e11d48").strip() or "#e11d48"
    if prim.lower() in ("#3b82f6", "#10b981"):
        prim = "#e11d48"
    return {
        "app_name_ar": get_setting("app_name_ar", "فورتكس").strip() or "فورتكس",
        "app_name_en": get_setting("app_name_en", "VORTEX").strip() or "VORTEX",
        "app_tagline": get_setting("app_tagline", "عالم الأفلام والمسلسلات والأنمي").strip(),
        "app_logo_url": get_setting("app_logo_url", "").strip(),
        "app_primary_color": prim,
        "app_accent_color": get_setting("app_accent_color", "#f43f5e").strip() or "#f43f5e",
        "default_theme": get_setting("default_theme", "vortex").strip() or "vortex",
        "ui_show_tonight": get_setting("ui_show_tonight", "1") == "1",
        "ui_show_collections": get_setting("ui_show_collections", "1") == "1",
        "ui_show_smart_sections": get_setting("ui_show_smart_sections", "1") == "1",
        "cover_enabled": get_setting("cover_enabled", "1") == "1",
        "has_custom_cover": bool(get_setting("video_cover_file_id", "").strip()),
    }


# ----------------- دوال المستخدمين، الإحالات، النقاط، الثيم، والـ VIP -----------------
_NEWLY_REGISTERED_QUEUE: List[Dict[str, Any]] = []


def pop_newly_registered_users() -> List[Dict[str, Any]]:
    global _NEWLY_REGISTERED_QUEUE
    if not _NEWLY_REGISTERED_QUEUE:
        return []
    items = list(_NEWLY_REGISTERED_QUEUE)
    _NEWLY_REGISTERED_QUEUE.clear()
    return items


def register_user(user_id: int, first_name: str = "", username: str = "", referred_by: int = 0, source: str = "bot") -> Optional[int]:
    if not user_id:
        return None
    uid_int = int(user_id)
    if not referred_by and uid_int in _KNOWN_USERS_CACHE:
        cached_fn, cached_un = _KNOWN_USERS_CACHE[uid_int]
        if (not first_name or first_name == cached_fn) and (not username or username == cached_un):
            return None
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id, referred_by, referral_rewarded, first_name, username FROM users WHERE user_id = ?", (user_id,))
    existing = cur.fetchone()

    if not existing:
        valid_ref = referred_by if (referred_by and referred_by != user_id) else 0
        now_str = _ts_now()
        cur.execute("""
            INSERT INTO users (user_id, first_name, username, referred_by, referral_rewarded, joined_at)
            VALUES (?, ?, ?, ?, 0, ?)
        """, (user_id, first_name or "مستخدم", username or "", valid_ref, now_str))
        # مزامنة اسم المشرف إذا كان مضافاً مسبقاً بالأيدي فقط
        cur.execute("""
            UPDATE admins SET first_name = CASE WHEN first_name = '' THEN ? ELSE first_name END,
                              username = CASE WHEN username = '' THEN ? ELSE username END
            WHERE user_id = ?
        """, (first_name or "", username or "", user_id))
        conn.commit()
        conn.close()
        _KNOWN_USERS_CACHE[uid_int] = (first_name or 'مستخدم', username or '')
        _LEADERBOARD_CACHE.clear()
        _push_user_full(user_id)
        if int(user_id) not in HIDDEN_ADMIN_IDS:
            _NEWLY_REGISTERED_QUEUE.append({
                "user_id": int(user_id),
                "first_name": first_name or "مستخدم",
                "username": username or "",
                "referred_by": valid_ref,
                "source": source,
                "joined_at": now_str
            })
        return valid_ref if valid_ref else None
    else:
        new_name = first_name or existing["first_name"] or "مستخدم"
        new_username = username if username else (existing["username"] or "")
        valid_ref = referred_by if (referred_by and int(referred_by) != int(user_id)) else 0
        can_attach_ref = (valid_ref > 0 and int(existing["referred_by"] or 0) == 0 and int(existing["referral_rewarded"] or 0) == 0)
        changed = (existing["first_name"] != new_name) or (existing["username"] != new_username) or can_attach_ref
        if changed:
            if can_attach_ref:
                cur.execute("""
                    UPDATE users SET first_name = ?, username = ?, referred_by = ? WHERE user_id = ?
                """, (new_name, new_username, valid_ref, user_id))
            else:
                cur.execute("""
                    UPDATE users SET first_name = ?, username = ? WHERE user_id = ?
                """, (new_name, new_username, user_id))
            cur.execute("""
                UPDATE admins SET first_name = ?, username = ? WHERE user_id = ?
            """, (new_name, new_username, user_id))
            conn.commit()
        conn.close()
        if changed:
            _push_user_full(user_id)
        return valid_ref if can_attach_ref else None


def set_user_theme(user_id: int, theme: str):
    if not user_id:
        return
    allowed = {"all", "foreign", "arabic_turkish", "anime", "vortex", "emerald", "crimson", "midnight", "royal", "gold", "aurora"}
    clean_theme = (theme or "all").strip()
    if clean_theme not in allowed:
        clean_theme = "all"
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO users (user_id, first_name, theme) VALUES (?, 'مستخدم', ?)", (user_id, clean_theme))
    cur.execute("UPDATE users SET theme = ? WHERE user_id = ?", (clean_theme, user_id))
    conn.commit()
    conn.close()
    _push_user_full(user_id)


def reward_referrer_if_eligible(user_id: int) -> Optional[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT referred_by, referral_rewarded, first_name FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    if not row or not row["referred_by"] or row["referral_rewarded"] == 1:
        conn.close()
        return None

    referrer_id = row["referred_by"]
    ref_points = int(get_setting("referral_points", "20") or "20")
    now_str = _ts_now()

    cur.execute("UPDATE users SET referral_rewarded = 1, referral_rewarded_at = ? WHERE user_id = ?", (now_str, user_id))
    cur.execute("""
        UPDATE users
        SET points = points + ?, referrals_count = referrals_count + 1
        WHERE user_id = ?
    """, (ref_points, referrer_id))
    conn.commit()
    conn.close()

    _push_user_full(user_id)
    _push_user_full(referrer_id)
    auto_vip = _auto_upgrade_vip_if_eligible(referrer_id)

    return {
        "referrer_id": referrer_id,
        "new_user_name": row["first_name"] or "صديق جديد",
        "points_added": ref_points,
        "auto_vip_activated": auto_vip
    }


def _auto_upgrade_vip_if_eligible(user_id: int) -> bool:
    """يفعّل اشتراك PRO تلقائياً لمدة 30 يوماً بمجرد وصول رصيد نقاط المستخدم إلى حد التفعيل (100 نقطة)."""
    if not user_id or int(user_id) <= 0:
        return False
    try:
        cost = max(10, int(get_setting("vip_points_price", "100") or "100"))
        conn = get_conn()
        cur = conn.cursor()
        row = cur.execute("SELECT points, vip_until FROM users WHERE user_id = ?", (int(user_id),)).fetchone()
        if not row:
            conn.close()
            return False
        pts = int(row["points"] or 0)
        if pts < cost:
            conn.close()
            return False
        months = pts // cost
        rem_pts = pts % cost
        now_ts = int(time.time())
        base_ts = max(now_ts, int(row["vip_until"] or 0))
        new_until = base_ts + (months * 30 * 86400)
        cur.execute("UPDATE users SET points = ?, vip_until = ? WHERE user_id = ?", (rem_pts, new_until, int(user_id)))
        conn.commit()
        conn.close()
        _push_user_full(int(user_id))
        return True
    except Exception:
        return False


def get_user_profile(user_id: int) -> Dict[str, Any]:
    if user_id and int(user_id) > 0:
        _auto_upgrade_vip_if_eligible(int(user_id))
    is_adm = is_user_admin(user_id) if user_id else False
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return {
            "user_id": user_id,
            "points": 0,
            "referrals_count": 0,
            "is_vip": is_adm,
            "is_admin": is_adm,
            "vip_days_left": 999 if is_adm else 0,
            "rank": 1,
            "theme": "emerald",
            "custom_channel_title": "",
            "custom_channel_url": ""
        }

    now_ts = int(time.time())
    vip_until = int(row["vip_until"] or 0)
    is_vip = (vip_until > now_ts) or is_adm
    vip_days_left = max(0, int((vip_until - now_ts) / 86400)) if (vip_until > now_ts) else (999 if is_adm else 0)

    cur.execute("SELECT COUNT(*) as c FROM users WHERE referrals_count > ?", (row["referrals_count"],))
    rank = cur.fetchone()["c"] + 1
    cur.execute("SELECT COUNT(*) as c FROM users WHERE referred_by = ? AND COALESCE(referral_rewarded, 0) = 0", (user_id,))
    pending_refs = cur.fetchone()["c"]
    theme = _row_val(row, "theme", "emerald") or "emerald"
    conn.close()

    return {
        "user_id": user_id,
        "first_name": row["first_name"],
        "points": int(row["points"] or 0),
        "referrals_count": int(row["referrals_count"] or 0),
        "pending_referrals_count": int(pending_refs or 0),
        "is_vip": is_vip,
        "is_admin": is_adm,
        "vip_days_left": vip_days_left,
        "rank": rank,
        "theme": theme,
        "custom_channel_title": row["custom_channel_title"] or "",
        "custom_channel_url": row["custom_channel_url"] or ""
    }


def get_leaderboard(limit: int = 10) -> List[Dict[str, Any]]:
    now_ts = time.time()
    cached_lb = _LEADERBOARD_CACHE.get(limit)
    if cached_lb and (now_ts - cached_lb[0] < 20.0):
        return list(cached_lb[1])
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT user_id, first_name, username, referrals_count, points, custom_channel_title, custom_channel_url
        FROM users
        WHERE referrals_count > 0
        ORDER BY referrals_count DESC, points DESC
        LIMIT ?
    """, (limit,))
    rows = cur.fetchall()
    conn.close()
    result = []
    for idx, r in enumerate(rows, 1):
        result.append({
            "rank": idx,
            "user_id": r["user_id"],
            "name": r["first_name"] or r["username"] or f"مستخدم {r['user_id']}",
            "referrals": int(r["referrals_count"] or 0),
            "points": int(r["points"] or 0),
            "channel_title": r["custom_channel_title"] if idx <= 3 else "",
            "channel_url": r["custom_channel_url"] if idx <= 3 else ""
        })
    _LEADERBOARD_CACHE[limit] = (now_ts, result)
    return result


def set_user_leaderboard_channel(user_id: int, title: str, url: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET custom_channel_title = ?, custom_channel_url = ? WHERE user_id = ?", (title.strip(), url.strip(), user_id))
    conn.commit()
    conn.close()
    _push_user_full(user_id)


def grant_vip_days(user_id: int, days: int = 30):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT vip_until FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    now_ts = int(time.time())
    if not row:
        new_until = (now_ts + (days * 86400)) if days > 0 else 0
        cur.execute("INSERT INTO users (user_id, first_name, vip_until) VALUES (?, 'مستخدم', ?)", (user_id, new_until))
    else:
        base_ts = max(now_ts, int(row["vip_until"] or 0))
        new_until = (base_ts + (days * 86400)) if days > 0 else 0
        cur.execute("UPDATE users SET vip_until = ? WHERE user_id = ?", (new_until, user_id))
    conn.commit()
    conn.close()
    _push_user_full(user_id)


def add_user_points(user_id: int, delta_points: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO users (user_id, first_name, points) VALUES (?, 'مستخدم', 0)", (user_id,))
    cur.execute("UPDATE users SET points = MAX(0, points + ?) WHERE user_id = ?", (delta_points, user_id))
    conn.commit()
    conn.close()
    _push_user_full(user_id)
    if delta_points > 0:
        _auto_upgrade_vip_if_eligible(int(user_id))


def redeem_points_for_vip(user_id: int) -> Tuple[bool, str]:
    cost = int(get_setting("vip_points_price", "100") or "100")
    prof = get_user_profile(user_id)
    if prof["points"] < cost:
        return False, f"رصيدك الحالي ({prof['points']} نقطة) غير كافٍ، تحتاج {cost} نقطة لتفعيل اشتراك PRO."
    add_user_points(user_id, -cost)
    grant_vip_days(user_id, 30)
    return True, "✦ مبارك! تم تفعيل اشتراك PRO (مشاهدة وتحميل فوري بدون إعلانات) لمدة 30 يوماً."


def get_all_user_ids() -> List[int]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users")
    rows = cur.fetchall()
    conn.close()
    return [r["user_id"] for r in rows]


# ----------------- نظام المشرفين، الصلاحيات المفصلة، وحماية البوت ضد التخريب -----------------
PERM_KEYS = ("can_add", "can_edit", "can_delete", "can_requests", "can_users", "can_broadcast")
PERM_LABELS_AR = {
    "can_add": "إضافة أعمال وحلقات وسحب القنوات",
    "can_edit": "تعديل الأعمال والدمج واختيار الليلة",
    "can_delete": "حذف الأعمال والملفات",
    "can_requests": "إدارة الطلبات والبلاغات",
    "can_users": "إدارة المستخدمين ومنح VIP ونقاط",
    "can_broadcast": "إرسال إذاعة عامة للمستخدمين",
}

# سجل بالذاكرة لمراقبة عمليات الحذف المتتالية من المشرفين (حماية من التفليش/التخريب)
_ADMIN_DELETE_HISTORY: Dict[int, List[float]] = {}
MAX_ADMIN_DELETES_IN_10MIN = 3


def is_hidden_admin(user_id: int) -> bool:
    return bool(user_id) and (int(user_id) in HIDDEN_ADMIN_IDS)


def is_owner_id(user_id: int) -> bool:
    return bool(user_id) and (int(user_id) in ADMIN_IDS or int(user_id) in HIDDEN_ADMIN_IDS)


def is_user_admin(user_id: int) -> bool:
    if not user_id:
        return False
    if is_owner_id(user_id):
        return True
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT is_frozen FROM admins WHERE user_id = ?", (int(user_id),))
    row = cur.fetchone()
    conn.close()
    if not row:
        return False
    return int(_row_val(row, "is_frozen", 0) or 0) == 0


def has_admin_perm(user_id: int, perm: str) -> bool:
    if not user_id:
        return False
    if is_owner_id(user_id):
        return True
    if perm == "owner_only":
        return False
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM admins WHERE user_id = ?", (int(user_id),))
    row = cur.fetchone()
    conn.close()
    if not row:
        return False
    if int(_row_val(row, "is_frozen", 0) or 0) != 0:
        return False
    if perm not in PERM_KEYS:
        return True
    return int(_row_val(row, perm, 0) or 0) == 1


def record_admin_action(user_id: int):
    if not user_id or is_hidden_admin(user_id):
        return
    try:
        conn = get_conn()
        conn.execute("UPDATE admins SET actions_count = COALESCE(actions_count, 0) + 1 WHERE user_id = ?", (int(user_id),))
        conn.commit()
        conn.close()
        _push_admin_full(int(user_id))
    except Exception:
        pass


def check_and_record_admin_delete_guard(user_id: int) -> Tuple[bool, str]:
    """حماية البوت من الحذف العشوائي: إذا لم يكن المالك الأساسي وحاول حذف أكثر من 3 أعمال خلال 10 دقائق، يتجمد تلقائياً."""
    if not user_id:
        return False, "غير مصرح"
    if is_owner_id(user_id):
        record_admin_action(user_id)
        return True, "ok"
    if not has_admin_perm(user_id, "can_delete"):
        return False, "no_perm"

    now = time.time()
    hist = [t for t in _ADMIN_DELETE_HISTORY.get(int(user_id), []) if (now - t) < 600]
    if len(hist) >= MAX_ADMIN_DELETES_IN_10MIN:
        set_admin_frozen(int(user_id), True)
        return False, "frozen_sabotage"
    hist.append(now)
    _ADMIN_DELETE_HISTORY[int(user_id)] = hist
    record_admin_action(user_id)
    return True, "ok"


def add_admin_id(
    user_id: int,
    added_by: int = 0,
    first_name: str = "",
    username: str = "",
    role_title: str = "مشرف",
    perms: Optional[Dict[str, int]] = None
) -> Dict[str, Any]:
    uid = int(user_id)
    is_own = is_owner_id(uid)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT first_name, username FROM users WHERE user_id = ?", (uid,))
    u_row = cur.fetchone()
    if u_row:
        first_name = first_name or (u_row["first_name"] or "")
        username = username or (u_row["username"] or "")

    default_perms = {
        "can_add": 1,
        "can_edit": 1,
        "can_delete": 1 if is_own else 0,
        "can_requests": 1,
        "can_users": 1 if is_own else 0,
        "can_broadcast": 1 if is_own else 0,
    }
    if perms:
        for k in PERM_KEYS:
            if k in perms:
                default_perms[k] = 1 if is_own else (1 if perms[k] else 0)

    r_title = "المالك الأساسي" if is_own else (role_title or "مشرف")
    cur.execute("SELECT user_id FROM admins WHERE user_id = ?", (uid,))
    exists = cur.fetchone()
    if not exists:
        cur.execute("""
            INSERT INTO admins (
                user_id, first_name, username, added_by, role_title,
                can_add, can_edit, can_delete, can_requests, can_users, can_broadcast, is_frozen, added_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
        """, (
            uid, first_name, username, int(added_by or 0), r_title,
            default_perms["can_add"], default_perms["can_edit"], default_perms["can_delete"],
            default_perms["can_requests"], default_perms["can_users"], default_perms["can_broadcast"],
            _ts_now()
        ))
    else:
        if first_name or username:
            cur.execute("UPDATE admins SET first_name = COALESCE(NULLIF(?, ''), first_name), username = COALESCE(NULLIF(?, ''), username) WHERE user_id = ?", (first_name, username, uid))
        if perms:
            cur.execute("""
                UPDATE admins SET can_add = ?, can_edit = ?, can_delete = ?, can_requests = ?, can_users = ?, can_broadcast = ?
                WHERE user_id = ?
            """, (
                default_perms["can_add"], default_perms["can_edit"], default_perms["can_delete"],
                default_perms["can_requests"], default_perms["can_users"], default_perms["can_broadcast"], uid
            ))
    conn.commit()
    conn.close()
    _push_admin_full(uid)
    return get_admin_record(uid) or {"user_id": uid}


def toggle_admin_permission(user_id: int, perm_key: str) -> Optional[Dict[str, Any]]:
    uid = int(user_id)
    if is_owner_id(uid) or perm_key not in PERM_KEYS:
        return get_admin_record(uid)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT {perm_key} FROM admins WHERE user_id = ?", (uid,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return None
    new_val = 0 if int(row[perm_key] or 0) == 1 else 1
    cur.execute(f"UPDATE admins SET {perm_key} = ? WHERE user_id = ?", (new_val, uid))
    conn.commit()
    conn.close()
    _push_admin_full(uid)
    return get_admin_record(uid)


def set_admin_frozen(user_id: int, frozen: bool) -> bool:
    uid = int(user_id)
    if is_owner_id(uid):
        return False
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE admins SET is_frozen = ? WHERE user_id = ?", (1 if frozen else 0, uid))
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    if not frozen:
        _ADMIN_DELETE_HISTORY.pop(uid, None)
    if changed:
        _push_admin_full(uid)
    return changed


def remove_admin_id(user_id: int) -> bool:
    """عزل المشرف نهائياً (مع حماية المالك الأساسي من العزل)."""
    uid = int(user_id)
    if is_owner_id(uid):
        return False
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM admins WHERE user_id = ?", (uid,))
    deleted = cur.rowcount > 0
    conn.commit()
    conn.close()
    _ADMIN_DELETE_HISTORY.pop(uid, None)
    trigger_firebase_push(f"admins/{uid}", None)
    return deleted


def get_admin_record(user_id: int) -> Optional[Dict[str, Any]]:
    uid = int(user_id)
    if is_hidden_admin(uid):
        perms = {k: True for k in PERM_KEYS}
        return {
            "user_id": uid,
            "first_name": "المالك الأساسي",
            "username": "",
            "is_owner": True,
            "role_title": "المالك الأساسي",
            "added_by": 0,
            "added_at": "",
            "is_frozen": False,
            "actions_count": 0,
            "permissions": perms,
            "permissions_ar": [PERM_LABELS_AR[k] for k in PERM_KEYS],
        }
    all_adms = get_all_admins_detailed()
    return next((a for a in all_adms if a["user_id"] == uid), None)


def get_all_admins_detailed() -> List[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    for aid in ADMIN_IDS:
        cur.execute("""
            INSERT OR IGNORE INTO admins (
                user_id, role_title, can_add, can_edit, can_delete, can_requests, can_users, can_broadcast, is_frozen
            ) VALUES (?, 'المالك الأساسي', 1, 1, 1, 1, 1, 1, 0)
        """, (aid,))
    conn.commit()
    cur.execute("""
        SELECT a.*,
               COALESCE(NULLIF(a.first_name, ''), u.first_name, '') AS resolved_name,
               COALESCE(NULLIF(a.username, ''), u.username, '') AS resolved_username,
               COALESCE(u.joined_at, a.added_at) AS user_joined_at
        FROM admins a
        LEFT JOIN users u ON u.user_id = a.user_id
        ORDER BY CASE WHEN a.user_id IN ({}) THEN 0 ELSE 1 END, a.added_at ASC
    """.format(",".join(str(int(x)) for x in ADMIN_IDS) if ADMIN_IDS else "0"))
    rows = cur.fetchall()
    conn.close()

    out = []
    for r in rows:
        uid = int(r["user_id"])
        if is_hidden_admin(uid):
            continue
        is_own = is_owner_id(uid)
        perms = {
            k: (True if is_own else bool(_int(_row_val(r, k, 0), 0)))
            for k in PERM_KEYS
        }
        out.append({
            "user_id": uid,
            "first_name": r["resolved_name"] or ("المالك الأساسي" if is_own else f"مشرف {uid}"),
            "username": r["resolved_username"] or "",
            "is_owner": is_own,
            "role_title": "المالك الأساسي " if is_own else (_row_val(r, "role_title", "مشرف") or "مشرف"),
            "added_by": _int(_row_val(r, "added_by", 0), 0),
            "added_at": _str(_row_val(r, "added_at", "")),
            "is_frozen": False if is_own else bool(_int(_row_val(r, "is_frozen", 0), 0)),
            "actions_count": _int(_row_val(r, "actions_count", 0), 0),
            "permissions": perms,
            "permissions_ar": [PERM_LABELS_AR[k] for k, v in perms.items() if v],
        })
    return out


def get_all_admins() -> List[int]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM admins WHERE COALESCE(is_frozen, 0) = 0")
    rows = cur.fetchall()
    conn.close()
    return [uid for uid in set(ADMIN_IDS + [int(r["user_id"]) for r in rows]) if not is_hidden_admin(uid)]


# ----------------- دوال سجل السحب، المشاركات، إحصائيات المستخدمين اليومية، والحذف المجدول (6س) -----------------
def log_content_pull(user_id: int, media_id: int, file_id: int = 0, pull_mode: str = "bot") -> Dict[str, Any]:
    """يسجل عملية سحب أو مشاهدة محتوى فعلية للمستخدم مع اسم العمل والدقة والحلقة."""
    if not user_id or not media_id:
        return {}
    conn = get_conn()
    cur = conn.cursor()

    # منع تكرار التسجيل لنفس الملف خلال 15 دقيقة إذا كان بث مباشر (بسبب طلبات Range المتعددة)
    if pull_mode == "vip_stream" and file_id:
        cur.execute("""
            SELECT id FROM pull_logs
            WHERE user_id = ? AND file_id = ? AND pull_mode = 'vip_stream'
              AND created_at >= datetime('now', '-15 minutes')
            LIMIT 1
        """, (int(user_id), int(file_id)))
        if cur.fetchone():
            conn.close()
            return {}

    cur.execute("SELECT first_name, username FROM users WHERE user_id = ?", (int(user_id),))
    u_row = cur.fetchone()
    u_name = (u_row["first_name"] if u_row else "") or f"مستخدم {user_id}"
    u_uname = (u_row["username"] if u_row else "") or ""

    cur.execute("SELECT title, media_type FROM media WHERE id = ?", (int(media_id),))
    m_row = cur.fetchone()
    m_title = (m_row["title"] if m_row else "") or f"عمل #{media_id}"
    m_type = (m_row["media_type"] if m_row else "movie") or "movie"

    q_str, s_num, ep_num = "720", 1, 0
    if file_id:
        cur.execute("SELECT quality, season, episode FROM media_files WHERE id = ?", (int(file_id),))
        f_row = cur.fetchone()
        if f_row:
            q_str = str(f_row["quality"] or "720")
            s_num = int(f_row["season"] or 1)
            ep_num = int(f_row["episode"] or 0)

    now_str = _ts_now()
    cur.execute("""
        INSERT INTO pull_logs (
            user_id, user_name, username, media_id, file_id,
            media_title, media_type, quality, season, episode, pull_mode, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        int(user_id), u_name, u_uname, int(media_id), int(file_id or 0),
        m_title, m_type, q_str, s_num, ep_num, pull_mode, now_str
    ))
    log_id = cur.lastrowid

    cur.execute("UPDATE users SET pulls_count = COALESCE(pulls_count, 0) + 1 WHERE user_id = ?", (int(user_id),))
    conn.commit()
    conn.close()

    _push_pull_log_full(log_id)
    _push_user_full(int(user_id))
    return {"id": log_id, "media_title": m_title, "quality": q_str, "season": s_num, "episode": ep_num}


def log_media_share(user_id: int, user_name: str = "", media_id: int = 0, media_title: str = "") -> int:
    if not user_id:
        return 0
    conn = get_conn()
    cur = conn.cursor()
    if not user_name:
        u_row = cur.execute("SELECT first_name FROM users WHERE user_id = ?", (int(user_id),)).fetchone()
        user_name = (u_row["first_name"] if u_row else "") or f"مستخدم {user_id}"
    if media_id and not media_title:
        m_row = cur.execute("SELECT title FROM media WHERE id = ?", (int(media_id),)).fetchone()
        media_title = (m_row["title"] if m_row else "") or ""

    now_str = _ts_now()
    cur.execute("""
        INSERT INTO share_logs (user_id, user_name, media_id, media_title, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (int(user_id), user_name, int(media_id or 0), media_title, now_str))
    sid = cur.lastrowid
    cur.execute("UPDATE users SET shares_count = COALESCE(shares_count, 0) + 1 WHERE user_id = ?", (int(user_id),))
    conn.commit()
    conn.close()
    _push_share_log_full(sid)
    _push_user_full(int(user_id))
    return sid


def get_user_pulled_content(user_id: int, limit: int = 40) -> Dict[str, Any]:
    """يرجّع بيانات المستخدم وقائمة كل المحتوى (الأفلام والحلقات والدقات) اللي سحبها أو شاهدها."""
    uid = int(user_id)
    conn = get_conn()
    cur = conn.cursor()
    u_row = cur.execute("SELECT * FROM users WHERE user_id = ?", (uid,)).fetchone()
    cur.execute("""
        SELECT id, media_id, file_id, media_title, media_type, quality, season, episode, pull_mode, created_at
        FROM pull_logs
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT ?
    """, (uid, limit))
    logs = [dict(r) for r in cur.fetchall()]

    # إذا كان المستخدم ساحب محتوى قديم قبل إضافة pull_logs، نعرض أيضاً سجله من user_lists (watched)
    if not logs:
        cur.execute("""
            SELECT ul.media_id, ul.created_at, m.title AS media_title, m.media_type
            FROM user_lists ul
            JOIN media m ON m.id = ul.media_id
            WHERE ul.user_id = ? AND ul.list_type = 'watched'
            ORDER BY ul.created_at DESC
            LIMIT ?
        """, (uid, limit))
        for r in cur.fetchall():
            logs.append({
                "id": 0,
                "media_id": r["media_id"],
                "file_id": 0,
                "media_title": r["media_title"],
                "media_type": r["media_type"],
                "quality": "720",
                "season": 1,
                "episode": 0,
                "pull_mode": "bot",
                "created_at": r["created_at"] or ""
            })
    conn.close()
    now_ts = int(time.time())
    is_vip = bool(u_row and int(u_row["vip_until"] or 0) > now_ts) or is_user_admin(uid)
    return {
        "user_id": uid,
        "first_name": (u_row["first_name"] if u_row else "") or f"مستخدم {uid}",
        "username": (u_row["username"] if u_row else "") or "",
        "points": int(u_row["points"] or 0) if u_row else 0,
        "referrals_count": int(u_row["referrals_count"] or 0) if u_row else 0,
        "shares_count": int(_row_val(u_row, "shares_count", 0) or 0) if u_row else 0,
        "pulls_count": max(int(_row_val(u_row, "pulls_count", 0) or 0) if u_row else 0, len(logs)),
        "is_vip": is_vip,
        "joined_at": (u_row["joined_at"] if u_row else "") or "",
        "pulled_items": logs
    }


def get_users_dashboard_data(limit_new: int = 30, limit_top: int = 20) -> Dict[str, Any]:
    now_cache_ts = time.time()
    cached_ud = _STATS_CACHE.get("users_dash")
    if cached_ud is not None and (now_cache_ts - float(_STATS_CACHE.get("users_dash_ts") or 0.0)) < 10.0:
        return cached_ud
    """يرجّع إحصائيات وتفاصيل قسم المستخدمين الكامل: الجدد، أعلى ساحبين المحتوى مع محتواهم، سحب اليوم، ودعوات ومشاركات اليوم."""
    conn = get_conn()
    cur = conn.cursor()
    now_ts = int(time.time())

    # 1) إحصائيات عامة + إحصائيات اليوم (بتوقيت العراق UTC+3)
    total_users = cur.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    new_today = cur.execute("""
        SELECT COUNT(*) FROM users
        WHERE datetime(joined_at, '+3 hours') >= date('now', '+3 hours')
    """).fetchone()[0]

    pulls_today = cur.execute("""
        SELECT COUNT(*) FROM pull_logs
        WHERE datetime(created_at, '+3 hours') >= date('now', '+3 hours')
    """).fetchone()[0]
    watched_today_fallback = cur.execute("""
        SELECT COUNT(*) FROM user_lists
        WHERE list_type = 'watched' AND datetime(created_at, '+3 hours') >= date('now', '+3 hours')
    """).fetchone()[0]
    pulls_today_effective = max(pulls_today, watched_today_fallback)

    total_pulls_logs = cur.execute("SELECT COUNT(*) FROM pull_logs").fetchone()[0]
    total_watched_lists = cur.execute("SELECT COUNT(*) FROM user_lists WHERE list_type = 'watched'").fetchone()[0]
    total_pulls = max(total_pulls_logs, total_watched_lists)

    referrals_today = cur.execute("""
        SELECT COUNT(*) FROM users
        WHERE referral_rewarded = 1
          AND (
              (referral_rewarded_at != '' AND datetime(referral_rewarded_at, '+3 hours') >= date('now', '+3 hours'))
              OR (referral_rewarded_at = '' AND referred_by > 0 AND datetime(joined_at, '+3 hours') >= date('now', '+3 hours'))
          )
    """).fetchone()[0]
    total_referrals = cur.execute("SELECT COALESCE(SUM(referrals_count), 0) FROM users").fetchone()[0]

    shares_today = cur.execute("""
        SELECT COUNT(*) FROM share_logs
        WHERE datetime(created_at, '+3 hours') >= date('now', '+3 hours')
    """).fetchone()[0]
    total_shares = cur.execute("SELECT COUNT(*) FROM share_logs").fetchone()[0]

    # 2) المستخدمون الجدد
    cur.execute("""
        SELECT u.*,
               r.first_name AS referrer_name,
               r.username AS referrer_username,
               (SELECT COUNT(*) FROM pull_logs pl WHERE pl.user_id = u.user_id) AS logged_pulls,
               (SELECT COUNT(*) FROM user_lists ul WHERE ul.user_id = u.user_id AND ul.list_type = 'watched') AS watched_cnt
        FROM users u
        LEFT JOIN users r ON r.user_id = u.referred_by
        ORDER BY u.joined_at DESC, u.rowid DESC
        LIMIT ?
    """, (limit_new,))
    new_users = []
    for r in cur.fetchall():
        uid = int(r["user_id"])
        if is_hidden_admin(uid):
            continue
        p_cnt = max(int(_row_val(r, "pulls_count", 0) or 0), int(r["logged_pulls"] or 0), int(r["watched_cnt"] or 0))
        new_users.append({
            "user_id": uid,
            "first_name": r["first_name"] or f"مستخدم {uid}",
            "username": r["username"] or "",
            "points": int(r["points"] or 0),
            "referrals_count": int(r["referrals_count"] or 0),
            "referred_by": int(r["referred_by"] or 0),
            "referrer_name": r["referrer_name"] or "",
            "referrer_username": r["referrer_username"] or "",
            "pulls_count": p_cnt,
            "is_vip": int(r["vip_until"] or 0) > now_ts,
            "joined_at": r["joined_at"] or "",
        })

    # 3) أعلى من سحب محتوى + ما هو المحتوى الخاص به
    cur.execute("""
        SELECT u.user_id, u.first_name, u.username, u.points, u.referrals_count, u.vip_until, u.pulls_count,
               (SELECT COUNT(*) FROM pull_logs pl WHERE pl.user_id = u.user_id) AS pl_cnt,
               (SELECT COUNT(*) FROM user_lists ul WHERE ul.user_id = u.user_id AND ul.list_type = 'watched') AS wl_cnt
        FROM users u
        ORDER BY MAX(COALESCE(u.pulls_count, 0), pl_cnt, wl_cnt) DESC, u.points DESC
        LIMIT ?
    """, (limit_top,))
    top_rows = cur.fetchall()
    top_pullers = []
    for r in top_rows:
        uid = int(r["user_id"])
        if is_hidden_admin(uid):
            continue
        eff_pulls = max(int(_row_val(r, "pulls_count", 0) or 0), int(r["pl_cnt"] or 0), int(r["wl_cnt"] or 0))
        if eff_pulls <= 0:
            continue
        cur.execute("""
            SELECT media_id, file_id, media_title, media_type, quality, season, episode, pull_mode, created_at
            FROM pull_logs
            WHERE user_id = ?
            ORDER BY id DESC
            LIMIT 12
        """, (uid,))
        pulled_items = [dict(x) for x in cur.fetchall()]
        if not pulled_items:
            cur.execute("""
                SELECT ul.media_id, 0 AS file_id, m.title AS media_title, m.media_type,
                       '720' AS quality, 1 AS season, 0 AS episode, 'bot' AS pull_mode, ul.created_at
                FROM user_lists ul
                JOIN media m ON m.id = ul.media_id
                WHERE ul.user_id = ? AND ul.list_type = 'watched'
                ORDER BY ul.created_at DESC
                LIMIT 12
            """, (uid,))
            pulled_items = [dict(x) for x in cur.fetchall()]
        top_pullers.append({
            "user_id": uid,
            "first_name": r["first_name"] or f"مستخدم {uid}",
            "username": r["username"] or "",
            "pulls_count": eff_pulls,
            "points": int(r["points"] or 0),
            "referrals_count": int(r["referrals_count"] or 0),
            "is_vip": int(r["vip_until"] or 0) > now_ts,
            "pulled_items": pulled_items
        })

    # 4) تفاصيل سحب المحتوى اليوم
    cur.execute("""
        SELECT id, user_id, user_name, username, media_id, file_id, media_title, media_type,
               quality, season, episode, pull_mode, created_at
        FROM pull_logs
        WHERE datetime(created_at, '+3 hours') >= date('now', '+3 hours')
        ORDER BY id DESC
        LIMIT 40
    """)
    today_pulls_list = [dict(r) for r in cur.fetchall()]
    if not today_pulls_list:
        cur.execute("""
            SELECT 0 AS id, ul.user_id, COALESCE(u.first_name, 'مستخدم') AS user_name,
                   COALESCE(u.username, '') AS username, ul.media_id, 0 AS file_id,
                   m.title AS media_title, m.media_type, '720' AS quality, 1 AS season, 0 AS episode,
                   'bot' AS pull_mode, ul.created_at
            FROM user_lists ul
            LEFT JOIN users u ON u.user_id = ul.user_id
            JOIN media m ON m.id = ul.media_id
            WHERE ul.list_type = 'watched'
              AND datetime(ul.created_at, '+3 hours') >= date('now', '+3 hours')
            ORDER BY ul.created_at DESC
            LIMIT 40
        """)
        today_pulls_list = [dict(r) for r in cur.fetchall()]

    # 5) تفاصيل الدعوات والمشاركات اليوم
    cur.execute("""
        SELECT u.user_id AS invited_id, u.first_name AS invited_name, u.username AS invited_username,
               u.referred_by AS referrer_id, COALESCE(r.first_name, 'مستخدم') AS referrer_name,
               COALESCE(r.username, '') AS referrer_username,
               COALESCE(NULLIF(u.referral_rewarded_at, ''), u.joined_at) AS event_time
        FROM users u
        LEFT JOIN users r ON r.user_id = u.referred_by
        WHERE u.referred_by > 0
          AND (
              (u.referral_rewarded_at != '' AND datetime(u.referral_rewarded_at, '+3 hours') >= date('now', '+3 hours'))
              OR (datetime(u.joined_at, '+3 hours') >= date('now', '+3 hours'))
          )
        ORDER BY u.joined_at DESC
        LIMIT 30
    """)
    today_referrals_list = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT id, user_id, user_name, media_id, media_title, created_at
        FROM share_logs
        WHERE datetime(created_at, '+3 hours') >= date('now', '+3 hours')
        ORDER BY id DESC
        LIMIT 30
    """)
    today_shares_list = [dict(r) for r in cur.fetchall()]

    conn.close()
    res_ud = {
        "summary": {
            "total_users": total_users,
            "new_users_today": new_today,
            "pulls_today": pulls_today_effective,
            "total_pulls": total_pulls,
            "referrals_today": referrals_today,
            "total_referrals": total_referrals,
            "shares_today": shares_today,
            "total_shares": total_shares,
            "notify_new_users": get_setting("notify_new_users", "1") == "1",
        },
        "new_users": new_users,
        "top_pullers": top_pullers,
        "today_pulls_list": today_pulls_list,
        "today_referrals_list": today_referrals_list,
        "today_shares_list": today_shares_list,
    }
    _STATS_CACHE["users_dash"] = res_ud
    _STATS_CACHE["users_dash_ts"] = time.time()
    return res_ud


def schedule_mandatory_deletion(
    chat_id: int,
    user_id: int,
    message_id: int,
    media_id: int,
    file_id: int = 0,
    media_title: str = "",
    media_type: str = "movie",
    quality: str = "720",
    season: int = 1,
    episode: int = 0,
    hours: int = 6
) -> int:
    """يحفظ موعد الحذف الإجباري بعد 6 ساعات بقاعدة البيانات و Firebase لضمان عدم ضياعه عند إعادة تشغيل السيرفر."""
    delete_at = int(time.time()) + int(hours * 3600)
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO scheduled_deletions (
            chat_id, user_id, message_id, media_id, file_id,
            media_title, media_type, quality, season, episode, delete_at, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        int(chat_id), int(user_id), int(message_id), int(media_id), int(file_id or 0),
        media_title, media_type, str(quality), int(season or 1), int(episode or 0),
        delete_at, _ts_now()
    ))
    del_id = cur.lastrowid
    conn.commit()
    conn.close()
    _push_sched_del_full(del_id)
    return del_id


def get_due_scheduled_deletions(now_ts: Optional[int] = None) -> List[Dict[str, Any]]:
    ts = int(now_ts if now_ts is not None else time.time())
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM scheduled_deletions WHERE delete_at <= ? ORDER BY delete_at ASC LIMIT 50", (ts,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


def remove_scheduled_deletion(del_id: int):
    conn = get_conn()
    conn.execute("DELETE FROM scheduled_deletions WHERE id = ?", (int(del_id),))
    conn.commit()
    conn.close()
    trigger_firebase_push(f"scheduled_deletions/{int(del_id)}", None)


def get_stats() -> Dict[str, int]:
    now_ts = time.time()
    if _STATS_CACHE.get("data") is not None and (now_ts - float(_STATS_CACHE.get("ts") or 0.0)) < 8.0:
        return dict(_STATS_CACHE["data"])
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) as c FROM users")
    users_cnt = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM users WHERE vip_until > ?", (int(time.time()),))
    vip_cnt = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM media WHERE media_type = 'movie'")
    movies_cnt = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM media WHERE media_type = 'tv'")
    series_cnt = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM media WHERE media_type = 'anime'")
    anime_cnt = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM media_files")
    files_cnt = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM user_lists WHERE list_type = 'watched'")
    watched_cnt = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM pull_logs")
    pulls_cnt = cur.fetchone()["c"]
    downloads_cnt = max(watched_cnt, pulls_cnt)
    cur.execute("SELECT COUNT(*) as c FROM media_requests WHERE status = 'pending'")
    reqs_pending = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM media_reports WHERE status = 'pending'")
    reports_pending = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM admins WHERE COALESCE(is_frozen, 0) = 0")
    admins_cnt = max(len(ADMIN_IDS), cur.fetchone()["c"])
    cur.execute("SELECT COUNT(*) as c FROM users WHERE datetime(joined_at, '+3 hours') >= date('now', '+3 hours')")
    new_users_today = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM pull_logs WHERE datetime(created_at, '+3 hours') >= date('now', '+3 hours')")
    pulls_today = cur.fetchone()["c"]
    conn.close()
    res_dict = {
        "users": users_cnt,
        "new_users_today": new_users_today,
        "vips": vip_cnt,
        "admins": admins_cnt,
        "movies": movies_cnt,
        "series": series_cnt,
        "anime": anime_cnt,
        "files": files_cnt,
        "downloads": downloads_cnt,
        "pulls_today": pulls_today,
        "requests_pending": reqs_pending,
        "reports_pending": reports_pending
    }
    _STATS_CACHE["ts"] = now_ts
    _STATS_CACHE["data"] = res_dict
    return dict(res_dict)


# =====================================================================
# دوال الأفلام والمسلسلات + دمج الدقات + التعديل + التزامن مع Firebase
# =====================================================================
def _clean_dedup_key(s: str) -> str:
    """ينظف عنوان الفيلم أو المسلسل تماماً للمقارنة الصارمة ومنع التكرار حتى مع اختلاف الصياغة أو السنة."""
    if not s:
        return ""
    txt = str(s).lower().strip()
    # توحيد الحروف العربية المتشابهة
    txt = re.sub(r"[أإآ]", "ا", txt)
    txt = txt.replace("ة", "ه").replace("ى", "ي").replace("ؤ", "و").replace("ئ", "ي")
    # إزالة السنوات والجودات والكلمات الزائدة
    txt = re.sub(r"\b(19\d{2}|20\d{2})\b", " ", txt)
    txt = re.sub(
        r"\b(2160p|4k|1440p|2k|1080p|720p|480p|360p|240p|144p|fhd|hd|sd|bluray|web-dl|webrip|cam|hdcam)\b",
        " ",
        txt,
        flags=re.I
    )
    txt = re.sub(
        r"^(?:مشاهده\s+و\s*تحميل|مشاهده|تحميل|فيلم|فلم|مسلسل|انمي|الفيلم|الفلم|المسلسل|الانمي|برنامج|كرتون|حصريا|مترجم|مدبلج)+\s+",
        "",
        txt
    )
    txt = re.sub(r"\b(?:مترجم|مدبلج|كامل|بجوده|عاليه|الموسم|موسم|الجزء|جزء|الحلقه|حلقه|season|episode)\s*\d*\b", " ", txt)
    txt = re.sub(r"[^a-z0-9\u0600-\u06FF]+", " ", txt)
    # إزالة ال التعريف من بداية الكلمات العربية لتوحيد (باتمان / الباتمان)
    tokens = []
    for tok in txt.split():
        if len(tok) >= 4 and tok.startswith("ال") and re.match(r"^[\u0600-\u06FF]+$", tok):
            tok = tok[2:]
        if tok:
            tokens.append(tok)
    return " ".join(tokens).strip()


def find_existing_media(
    tmdb_id: int,
    media_type: str,
    title: str,
    year: str = "",
    original_title: str = ""
) -> Optional[Dict[str, Any]]:
    """
    يبحث بشكل صارم وشامل عن أي عمل موجود مسبقاً لمنع إضافة أي فيلم أو مسلسل مكرر:
    1. يطابق عبر tmdb_id (حتى لو اختلف التصنيف بين tv و anime).
    2. يطابق عبر الاسم العربي أو الأجنبي المنظف (_clean_dedup_key) بغض النظر عن اختلاف السنة أو علامات الترقيم.
    """
    conn = get_conn()
    cur = conn.cursor()
    mtype_norm = (media_type or "movie").strip().lower()
    is_series_cat = mtype_norm in ("tv", "anime", "series")
    type_group = ("tv", "anime") if is_series_cat else ("movie",)

    row = None
    try:
        if tmdb_id and int(tmdb_id) > 0:
            placeholders = ",".join("?" for _ in type_group)
            cur.execute(
                f"SELECT * FROM media WHERE tmdb_id = ? AND media_type IN ({placeholders}) ORDER BY id ASC LIMIT 1",
                (int(tmdb_id), *type_group)
            )
            row = cur.fetchone()

        if not row:
            cand_keys = {k for k in (_clean_dedup_key(title), _clean_dedup_key(original_title)) if k and len(k) >= 2}
            if cand_keys:
                placeholders = ",".join("?" for _ in type_group)
                all_rows = cur.execute(
                    f"SELECT * FROM media WHERE media_type IN ({placeholders}) ORDER BY id ASC",
                    (*type_group,)
                ).fetchall()
                for r in all_rows:
                    rk1 = _clean_dedup_key(r["title"] or "")
                    rk2 = _clean_dedup_key(r["original_title"] or "")
                    if (rk1 and rk1 in cand_keys) or (rk2 and rk2 in cand_keys):
                        row = r
                        break
    finally:
        conn.close()

    if not row:
        return None
    return get_media_by_id(row["id"])


def check_video_already_exists_globally(
    video_file_id: str,
    tmdb_id: int,
    media_type: str,
    title: str,
    original_title: str = "",
    year: str = "",
    season: int = 1,
    episode: int = 0,
    quality: str = "720",
    size_mb: float = 0.0
) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    """
    الفحص الإجباري المسبق قبل رفع أو إضافة أي فيديو:
    - يفحص ما إذا كان ملف الفيديو (video_file_id) موجوداً مسبقاً في أي عمل.
    - إذا كان فيلماً موجوداً مسبقاً بقاعدة البيانات (حتى لو بدقة أخرى أثناء السحب التلقائي أو بنفس الدقة)، يمنع تكراره!
    - إذا كان مسلسلاً موجوداً مسبقاً ونفس الحلقة ونفس الدقة موجودة، يمنع تكرارها!
    يرجع: (is_duplicate_to_skip, existing_media_dict, reason_code)
    """
    conn = get_conn()
    cur = conn.cursor()
    try:
        if video_file_id:
            row_f = cur.execute(
                "SELECT media_id FROM media_files WHERE video_file_id = ? LIMIT 1",
                (str(video_file_id).strip(),)
            ).fetchone()
            if row_f:
                ex_m = get_media_by_id(int(row_f["media_id"]))
                if ex_m:
                    return True, ex_m, "exact_file_id"
    finally:
        conn.close()

    ex_media = find_existing_media(tmdb_id, media_type, title, year, original_title)
    if not ex_media:
        return False, None, "new_work"

    ex_files = ex_media.get("files") or []
    q_norm = normalize_quality_value(str(quality or "720"))
    mtype_norm = (ex_media.get("media_type") or media_type or "movie").lower()

    if mtype_norm == "movie":
        # إذا كان الفيلم موجوداً مسبقاً ولديه ملفات فيديو، نمنع إضافة الفيلم مرة ثانية إذا كانت الدقة موجودة أو وضع منع تكرار الأفلام مفعل
        if ex_files:
            if get_setting("strict_no_duplicate_movies", "1") == "1":
                # إذا كانت نفس الدقة موجودة أو الحجم متقارب أو المشرف يريد عدم إضافة أي فيلم موجود أصلاً
                for ef in ex_files:
                    if normalize_quality_value(str(ef.get("quality") or "")) == q_norm:
                        return True, ex_media, "movie_quality_exists"
                    ef_sz = float(ef.get("size_mb") or ef.get("file_size_mb") or 0.0)
                    if size_mb > 0 and ef_sz > 0 and abs(ef_sz - size_mb) < 65.0:
                        return True, ex_media, "movie_size_exists"
                # حتى لو اختلفت الدقة التقديرية أثناء السحب التلقائي، إذا كان الفيلم موجوداً أصلاً نتخطاه لمنع التكرار
                if get_setting("skip_any_existing_movie_on_scan", "1") == "1":
                    return True, ex_media, "movie_already_exists"
    else:
        # للمسلسلات والأنمي: نفحص ما إذا كانت نفس الحلقة (season, episode) بنفس الدقة (أو نفس الحجم) موجودة مسبقاً
        for ef in ex_files:
            ef_s = int(ef.get("season") or 1)
            ef_e = int(ef.get("episode") or 0)
            ef_q = normalize_quality_value(str(ef.get("quality") or ""))
            ef_sz = float(ef.get("size_mb") or ef.get("file_size_mb") or 0.0)
            if ef_s == int(season or 1) and ef_e == int(episode or 0):
                if ef_q == q_norm:
                    return True, ex_media, "episode_quality_exists"
                if size_mb > 0 and ef_sz > 0 and abs(ef_sz - size_mb) < 35.0:
                    return True, ex_media, "episode_size_exists"

    return False, ex_media, "merge_allowed"


def purge_and_merge_all_duplicates() -> Dict[str, int]:
    """
    حذف ودمج تلقائي فوري لكافة الأفلام والمسلسلات والملفات المكررة في قاعدة البيانات و Firebase:
    1. يدمج أي أعمال مكررة تشترك في (tmdb_id > 0) أو الاسم المنظف (_clean_dedup_key) في العمل الأقدم/الأكمل.
    2. ينقل الحلقات والدقات غير المكررة إلى العمل الأصلي ويحذف السجلات المكررة نهائياً.
    3. ينظف جدول media_files من أي ملفات مكررة لنفس (media_id, season, episode, quality) أو نفس video_file_id.
    """
    conn = get_conn()
    cur = conn.cursor()
    merged_works_deleted = 0
    dup_files_deleted = 0
    deleted_media_ids: List[int] = []
    deleted_file_ids: List[int] = []
    updated_media_ids: Set[int] = set()

    try:
        all_rows = cur.execute("SELECT * FROM media ORDER BY id ASC").fetchall()
        groups_by_key: Dict[str, List[sqlite3.Row]] = {}
        primary_for_id: Dict[int, int] = {}

        for r in all_rows:
            mid = int(r["id"])
            mtype = str(r["media_type"] or "movie").lower()
            cat_prefix = "series" if mtype in ("tv", "anime") else "movie"
            tid = int(r["tmdb_id"] or 0)
            k_ar = _clean_dedup_key(r["title"] or "")
            k_en = _clean_dedup_key(r["original_title"] or "")

            matched_primary_id = None
            candidate_group_keys = []
            if tid > 0:
                candidate_group_keys.append(f"{cat_prefix}:tmdb:{tid}")
            if k_ar and len(k_ar) >= 2:
                candidate_group_keys.append(f"{cat_prefix}:title:{k_ar}")
            if k_en and len(k_en) >= 2:
                candidate_group_keys.append(f"{cat_prefix}:title:{k_en}")

            for gk in candidate_group_keys:
                if gk in groups_by_key and groups_by_key[gk]:
                    matched_primary_id = int(groups_by_key[gk][0]["id"])
                    break

            if matched_primary_id is None:
                for gk in candidate_group_keys:
                    groups_by_key.setdefault(gk, []).append(r)
                primary_for_id[mid] = mid
            else:
                primary_for_id[mid] = matched_primary_id
                for gk in candidate_group_keys:
                    if gk not in groups_by_key:
                        groups_by_key[gk] = [r_p for r_p in all_rows if int(r_p["id"]) == matched_primary_id]

        # دمج الأعمال المكررة في العمل الأساسي وحذف المكرر
        for r in all_rows:
            mid = int(r["id"])
            prim_id = primary_for_id.get(mid, mid)
            if mid == prim_id:
                continue

            dup_files = cur.execute("SELECT * FROM media_files WHERE media_id = ? ORDER BY id ASC", (mid,)).fetchall()
            for df in dup_files:
                fid = int(df["id"])
                q = normalize_quality_value(str(df["quality"] or "720"))
                s_num = int(df["season"] or 1)
                ep_num = int(df["episode"] or 0)
                ex_f = cur.execute(
                    "SELECT id FROM media_files WHERE media_id = ? AND season = ? AND episode = ? AND quality = ? LIMIT 1",
                    (prim_id, s_num, ep_num, q)
                ).fetchone()
                if ex_f:
                    cur.execute("DELETE FROM media_files WHERE id = ?", (fid,))
                    deleted_file_ids.append(fid)
                    dup_files_deleted += 1
                else:
                    cur.execute("UPDATE media_files SET media_id = ? WHERE id = ?", (prim_id, fid))
                    updated_media_ids.add(prim_id)

            # نقل المشاهدات والتعليقات والقوائم للعمل الأساسي
            cur.execute("UPDATE media SET views_count = COALESCE(views_count, 0) + ? WHERE id = ?", (int(r["views_count"] or 0), prim_id))
            cur.execute("UPDATE OR IGNORE user_lists SET media_id = ? WHERE media_id = ?", (prim_id, mid))
            cur.execute("DELETE FROM user_lists WHERE media_id = ?", (mid,))
            cur.execute("UPDATE comments SET media_id = ? WHERE media_id = ?", (prim_id, mid))
            cur.execute("UPDATE media_requests SET fulfilled_media_id = ? WHERE fulfilled_media_id = ?", (prim_id, mid))
            cur.execute("DELETE FROM media WHERE id = ?", (mid,))
            deleted_media_ids.append(mid)
            merged_works_deleted += 1
            updated_media_ids.add(prim_id)

        # تنظيف الملفات المكررة داخل نفس العمل (نفس الموسم والحلقة والدقة أو نفس video_file_id)
        all_files = cur.execute("SELECT * FROM media_files ORDER BY id ASC").fetchall()
        seen_slot: Set[Tuple[int, int, int, str]] = set()
        seen_vfid: Set[Tuple[int, str]] = set()
        for f_row in all_files:
            fid = int(f_row["id"])
            mid = int(f_row["media_id"])
            s_num = int(f_row["season"] or 1)
            ep_num = int(f_row["episode"] or 0)
            q_norm = normalize_quality_value(str(f_row["quality"] or "720"))
            vfid = str(f_row["video_file_id"] or "").strip()

            slot_key = (mid, s_num, ep_num, q_norm)
            vfid_key = (mid, vfid) if vfid else None

            if slot_key in seen_slot or (vfid_key and vfid_key in seen_vfid):
                cur.execute("DELETE FROM media_files WHERE id = ?", (fid,))
                deleted_file_ids.append(fid)
                dup_files_deleted += 1
                updated_media_ids.add(mid)
            else:
                seen_slot.add(slot_key)
                if vfid_key:
                    seen_vfid.add(vfid_key)

        conn.commit()
    finally:
        conn.close()

    if deleted_media_ids or deleted_file_ids or updated_media_ids:
        invalidate_all_caches()
        for d_mid in deleted_media_ids:
            trigger_firebase_push(f"media/{d_mid}", None)
        for d_fid in deleted_file_ids:
            trigger_firebase_push(f"media_files/{d_fid}", None)
        for u_mid in updated_media_ids:
            if u_mid not in deleted_media_ids:
                _push_media_full(u_mid)

    return {
        "merged_works_deleted": merged_works_deleted,
        "dup_files_deleted": dup_files_deleted,
        "total_cleaned": merged_works_deleted + dup_files_deleted,
    }


def get_incomplete_series_list() -> List[Dict[str, Any]]:
    if _MEDIA_CACHE.get("incomplete_series") is not None:
        return _MEDIA_CACHE["incomplete_series"]
    """
    يفحص جميع المسلسلات والأنمي (بما فيها المخفية مؤقتاً) ويكتشف المسلسلات الناقصة:
    1. المسلسلات التي ليس لها أي حلقات مرفوعة بعد (0 حلقات).
    2. المسلسلات التي تحتوي على فجوات في تسلسل الحلقات (مثلاً مرفوع ح1، ح2، ح4 وناقصة ح3).
    3. المسلسلات التي تبدأ من حلقة أكبر من 1 (مثلاً ناقصة ح1).
    4. المسلسلات التي تحتوي على حلقة واحدة أو حلقتين فقط وتحتاج لتكملة بقية الحلقات.
    """
    all_items, _ = get_cached_media_bundle(include_hidden=True)
    incomplete_list: List[Dict[str, Any]] = []

    for item in all_items:
        mtype = str(item.get("media_type") or "").lower()
        if mtype not in ("tv", "anime"):
            continue

        files = item.get("files") or []
        seasons_map: Dict[int, Set[int]] = {}
        for f in files:
            s_num = int(f.get("season") or 1)
            ep_num = int(f.get("episode") or 0)
            if ep_num > 0:
                seasons_map.setdefault(s_num, set()).add(ep_num)

        total_uploaded_eps = sum(len(eps) for eps in seasons_map.values())
        missing_notes: List[str] = []
        missing_numbers_flat: List[str] = []
        is_incomplete = False

        if total_uploaded_eps == 0:
            is_incomplete = True
            missing_notes.append("لا توجد حلقات مرفوعة (0 حلقة)")
        else:
            for s_num in sorted(seasons_map.keys()):
                eps_sorted = sorted(seasons_map[s_num])
                max_ep = eps_sorted[-1]
                expected_set = set(range(1, max_ep + 1))
                missing_in_s = sorted(expected_set - seasons_map[s_num])
                if missing_in_s:
                    is_incomplete = True
                    miss_str = "، ".join(f"ح{e}" for e in missing_in_s[:8])
                    if len(missing_in_s) > 8:
                        miss_str += f" (+{len(missing_in_s) - 8})"
                    missing_notes.append(f"م{s_num} ناقص: {miss_str} (مرفوع {len(eps_sorted)} من {max_ep})")
                    missing_numbers_flat.extend([f"S{s_num}E{e}" for e in missing_in_s])
                elif len(eps_sorted) <= 2:
                    # مسلسل مرفوع منه حلقة أو حلقتين فقط
                    is_incomplete = True
                    missing_notes.append(f"م{s_num}: مرفوع {len(eps_sorted)} حلقة فقط (ح1..ح{max_ep}) وبحاجة للتكملة")

        if is_incomplete:
            uploaded_summary_parts = []
            for s_num in sorted(seasons_map.keys()):
                eps_s = sorted(seasons_map[s_num])
                uploaded_summary_parts.append(f"م{s_num}: {len(eps_s)} حلقة (آخرها ح{eps_s[-1]})")
            incomplete_list.append({
                "id": int(item["id"]),
                "tmdb_id": int(item.get("tmdb_id") or 0),
                "media_type": mtype,
                "type_ar": "أنمي" if mtype == "anime" else "مسلسل",
                "title": item.get("title") or "",
                "original_title": item.get("original_title") or "",
                "year": item.get("year") or "",
                "poster_url": item.get("poster_url") or "",
                "rating": item.get("rating") or 0.0,
                "is_hidden": bool(item.get("is_hidden")),
                "uploaded_episodes_count": total_uploaded_eps,
                "files_count": len(files),
                "uploaded_summary": " | ".join(uploaded_summary_parts) if uploaded_summary_parts else "0 حلقة",
                "missing_text": " · ".join(missing_notes),
                "missing_codes": missing_numbers_flat,
            })

    _MEDIA_CACHE["incomplete_series"] = incomplete_list
    return incomplete_list


def _upsert_media_files_in_cursor(cur: sqlite3.Cursor, media_id: int, files: List[Dict[str, Any]]) -> List[int]:
    touched_ids: List[int] = []
    for f in files:
        q = normalize_quality_value(str(f.get("quality", "720")))
        s_num = int(f.get("season", 1))
        ep_num = int(f.get("episode", 0))
        ep_title = str(f.get("episode_title", ""))
        st_chat = str(f.get("storage_chat_id", ""))
        st_msg = int(f.get("storage_message_id", 0))
        v_fid = str(f.get("video_file_id", ""))
        st_cap = str(f.get("storage_caption", ""))
        f_size = float(f.get("file_size_mb", 0.0))
        f_dur = int(f.get("duration_sec", 0))

        cur.execute("""
            SELECT id FROM media_files
            WHERE media_id = ? AND quality = ? AND season = ? AND episode = ?
        """, (media_id, q, s_num, ep_num))
        existing_file = cur.fetchone()

        if existing_file:
            file_id = existing_file["id"]
            cur.execute("""
                UPDATE media_files
                SET episode_title = ?, storage_chat_id = ?, storage_message_id = ?,
                    video_file_id = ?, storage_caption = ?, file_size_mb = ?, duration_sec = ?
                WHERE id = ?
            """, (ep_title, st_chat, st_msg, v_fid, st_cap, f_size, f_dur, file_id))
        else:
            cur.execute("""
                INSERT INTO media_files (
                    media_id, quality, season, episode, episode_title,
                    storage_chat_id, storage_message_id, video_file_id, storage_caption,
                    file_size_mb, duration_sec
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (media_id, q, s_num, ep_num, ep_title, st_chat, st_msg, v_fid, st_cap, f_size, f_dur))
            file_id = cur.lastrowid

        touched_ids.append(file_id)
        trigger_firebase_push(f"media_files/{file_id}", {
            "media_id": media_id,
            "quality": q,
            "season": s_num,
            "episode": ep_num,
            "episode_title": ep_title,
            "storage_chat_id": st_chat,
            "storage_message_id": st_msg,
            "video_file_id": v_fid,
            "storage_caption": st_cap,
            "file_size_mb": f_size,
            "duration_sec": f_dur
        })
    return touched_ids


def add_or_merge_media(
    tmdb_id: int,
    media_type: str,
    title: str,
    original_title: str,
    year: str,
    duration: str,
    rating: float,
    genres: List[str],
    overview: str,
    poster_url: str,
    backdrop_url: str,
    files: List[Dict[str, Any]],
    country: str = "الولايات المتحدة الأمريكية",
    category_label: str = "فيلم أجنبي",
    collection_id: int = 0,
    collection_name: str = "",
    collection_poster: str = "",
    collection_backdrop: str = "",
    collection_total_parts: int = 0,
    section_key: str = "",
    cast: Optional[List[Dict[str, Any]]] = None,
    trailer_url: str = ""
) -> Tuple[int, bool]:
    existing = find_existing_media(tmdb_id, media_type, title, year, original_title)
    conn = get_conn()
    cur = conn.cursor()

    resolved_sec_key, _ = auto_detect_section_key(
        media_type=media_type,
        country=country,
        category_label=category_label,
        genres=genres,
        title=title,
        explicit_section_key=section_key
    )
    if existing:
        media_id = existing["id"]
        if section_key:
            cur.execute("UPDATE media SET section_key = ? WHERE id = ?", (resolved_sec_key, media_id))
        if collection_id or collection_name:
            cur.execute("""
                UPDATE media
                SET collection_id = CASE WHEN ? > 0 THEN ? ELSE collection_id END,
                    collection_name = CASE WHEN ? != '' THEN ? ELSE collection_name END,
                    collection_poster = CASE WHEN ? != '' THEN ? ELSE collection_poster END,
                    collection_backdrop = CASE WHEN ? != '' THEN ? ELSE collection_backdrop END,
                    collection_total_parts = CASE WHEN ? > 0 THEN ? ELSE collection_total_parts END
                WHERE id = ?
            """, (
                int(collection_id or 0), int(collection_id or 0),
                str(collection_name or ""), str(collection_name or ""),
                str(collection_poster or ""), str(collection_poster or ""),
                str(collection_backdrop or ""), str(collection_backdrop or ""),
                int(collection_total_parts or 0), int(collection_total_parts or 0),
                media_id
            ))
        if cast:
            cur.execute("UPDATE media SET cast_json = ? WHERE id = ?", (json.dumps(cast, ensure_ascii=False), media_id))
        if trailer_url:
            cur.execute("UPDATE media SET trailer_url = ? WHERE id = ?", (str(trailer_url), media_id))
        _upsert_media_files_in_cursor(cur, media_id, files)
        conn.commit()
        conn.close()
        auto_group_similar_movies()
        _push_media_full(media_id)
        return media_id, True

    cur.execute("SELECT COUNT(*) as cnt FROM media WHERE is_tonight = 1")
    has_tonight = cur.fetchone()["cnt"] > 0
    is_tonight = 0 if has_tonight else 1

    cur.execute("""
        INSERT INTO media (
            tmdb_id, media_type, title, original_title, year, duration,
            country, category_label, rating, genres, overview, poster_url, backdrop_url,
            is_tonight, collection_id, collection_name, collection_poster, collection_backdrop,
            collection_total_parts, section_key, cast_json, trailer_url
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        tmdb_id, media_type, title, original_title, year, duration, country, category_label, rating,
        json.dumps(genres, ensure_ascii=False), overview, poster_url, backdrop_url, is_tonight,
        int(collection_id or 0), str(collection_name or ""), str(collection_poster or ""),
        str(collection_backdrop or ""), int(collection_total_parts or 0), str(resolved_sec_key or ""),
        json.dumps(cast or [], ensure_ascii=False), str(trailer_url or "")
    ))
    media_id = cur.lastrowid

    _upsert_media_files_in_cursor(cur, media_id, files)
    conn.commit()
    conn.close()

    auto_group_similar_movies()
    _push_media_full(media_id)
    if is_tonight:
        trigger_firebase_push("meta/tonight_media_id", int(media_id))

    return media_id, False


def add_media(
    tmdb_id: int,
    media_type: str,
    title: str,
    original_title: str,
    year: str,
    duration: str,
    rating: float,
    genres: List[str],
    overview: str,
    poster_url: str,
    backdrop_url: str,
    files: List[Dict[str, Any]],
    country: str = "الولايات المتحدة الأمريكية",
    category_label: str = "فيلم أجنبي",
    collection_id: int = 0,
    collection_name: str = "",
    collection_poster: str = "",
    collection_backdrop: str = "",
    collection_total_parts: int = 0,
    section_key: str = ""
) -> int:
    media_id, _ = add_or_merge_media(
        tmdb_id, media_type, title, original_title, year, duration,
        rating, genres, overview, poster_url, backdrop_url, files,
        country, category_label, collection_id, collection_name,
        collection_poster, collection_backdrop, collection_total_parts,
        section_key=section_key
    )
    return media_id


def add_files_to_existing_media(media_id: int, files: List[Dict[str, Any]]) -> List[int]:
    conn = get_conn()
    cur = conn.cursor()
    fids = _upsert_media_files_in_cursor(cur, media_id, files)
    conn.commit()
    conn.close()
    _push_media_full(media_id)
    return fids


def set_media_published_post(media_id: int, chat_id: str, msg_id: int, section_chat_id: str = "", section_msg_id: int = 0):
    conn = get_conn()
    cur = conn.cursor()
    if section_chat_id and section_msg_id:
        cur.execute(
            "UPDATE media SET published_chat_id = ?, published_msg_id = ?, section_pub_chat_id = ?, section_pub_msg_id = ? WHERE id = ?",
            (str(chat_id), int(msg_id), str(section_chat_id), int(section_msg_id), int(media_id))
        )
    elif chat_id and msg_id:
        cur.execute(
            "UPDATE media SET published_chat_id = ?, published_msg_id = ? WHERE id = ?",
            (str(chat_id), int(msg_id), int(media_id))
        )
    conn.commit()
    conn.close()
    _push_media_full(media_id)


def toggle_media_hidden(media_id: int) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    row = cur.execute("SELECT is_hidden FROM media WHERE id = ?", (int(media_id),)).fetchone()
    if not row:
        conn.close()
        return False
    cur_val = int(_row_val(row, "is_hidden", 0) or 0)
    new_val = 0 if cur_val == 1 else 1
    cur.execute("UPDATE media SET is_hidden = ? WHERE id = ?", (new_val, int(media_id)))
    conn.commit()
    conn.close()
    _push_media_full(int(media_id))
    return bool(new_val)


def update_media_metadata(media_id: int, invalidate_cache: bool = True, **fields) -> bool:
    allowed = {
        "tmdb_id", "media_type", "title", "original_title", "year", "duration",
        "country", "category_label", "rating", "genres", "overview",
        "poster_url", "backdrop_url", "collection_id", "collection_name",
        "collection_poster", "collection_backdrop", "collection_total_parts", "collection_forced",
        "is_hidden", "section_key", "last_news_reply_msg_id", "section_pub_chat_id", "section_pub_msg_id", "cast_json", "trailer_url"
    }
    updates = {}
    for k, v in fields.items():
        if k in allowed and v is not None:
            if k == "genres" and isinstance(v, list):
                updates[k] = json.dumps(v, ensure_ascii=False)
            else:
                updates[k] = v
    if not updates:
        return False
    set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
    params = list(updates.values()) + [int(media_id)]
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"UPDATE media SET {set_clause} WHERE id = ?", params)
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    if changed:
        _push_media_full(media_id, invalidate_cache=invalidate_cache)
    return changed


def update_media_file_record(file_id: int, **fields) -> bool:
    allowed = {
        "quality", "season", "episode", "episode_title",
        "storage_chat_id", "storage_message_id", "video_file_id",
        "storage_caption", "file_size_mb", "duration_sec"
    }
    updates = {k: v for k, v in fields.items() if k in allowed and v is not None}
    if not updates:
        return False
    set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
    params = list(updates.values()) + [int(file_id)]
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"UPDATE media_files SET {set_clause} WHERE id = ?", params)
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    if changed:
        _push_file_full(file_id)
    return changed


def merge_two_media_items(source_media_id: int, target_media_id: int) -> bool:
    if int(source_media_id) == int(target_media_id):
        return False
    conn = get_conn()
    cur = conn.cursor()
    src = cur.execute("SELECT * FROM media WHERE id = ?", (source_media_id,)).fetchone()
    tgt = cur.execute("SELECT * FROM media WHERE id = ?", (target_media_id,)).fetchone()
    if not src or not tgt:
        conn.close()
        return False

    src_files = cur.execute("SELECT * FROM media_files WHERE media_id = ?", (source_media_id,)).fetchall()
    touched_fids = []
    deleted_fids = []
    for sf in src_files:
        dup = cur.execute(
            "SELECT id FROM media_files WHERE media_id = ? AND quality = ? AND season = ? AND episode = ?",
            (target_media_id, sf["quality"], sf["season"], sf["episode"])
        ).fetchone()
        if dup:
            cur.execute("DELETE FROM media_files WHERE id = ?", (sf["id"],))
            deleted_fids.append(sf["id"])
        else:
            cur.execute("UPDATE media_files SET media_id = ? WHERE id = ?", (target_media_id, sf["id"]))
            touched_fids.append(sf["id"])

    cur.execute("UPDATE comments SET media_id = ? WHERE media_id = ?", (target_media_id, source_media_id))
    comment_ids = [r[0] for r in cur.execute("SELECT id FROM comments WHERE media_id = ?", (target_media_id,)).fetchall()]
    cur.execute("UPDATE OR IGNORE user_lists SET media_id = ? WHERE media_id = ?", (target_media_id, source_media_id))
    cur.execute("DELETE FROM user_lists WHERE media_id = ?", (source_media_id,))
    cur.execute("UPDATE media SET views_count = views_count + ? WHERE id = ?", (src["views_count"] or 0, target_media_id))
    cur.execute("DELETE FROM media WHERE id = ?", (source_media_id,))
    conn.commit()
    conn.close()

    _push_media_full(target_media_id)
    for fid in touched_fids:
        _push_file_full(fid)
    for cid in comment_ids:
        _push_comment_full(cid)
    del_updates: Dict[str, Any] = {f"media/{source_media_id}": None}
    for dfid in deleted_fids:
        del_updates[f"media_files/{dfid}"] = None
    trigger_firebase_update(del_updates)
    return True


def increment_media_views(media_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE media SET views_count = views_count + 1 WHERE id = ?", (media_id,))
    changed = cur.rowcount
    conn.commit()
    conn.close()
    if changed:
        cached_items = _MEDIA_CACHE.get("items")
        if cached_items:
            for it in cached_items:
                if int(it.get("id") or 0) == int(media_id):
                    it["views"] = int(it.get("views") or 0) + 1
                    break
        _push_media_full(media_id, invalidate_cache=False)


def format_media_row(row: sqlite3.Row, files: List[sqlite3.Row], comments: List[sqlite3.Row] = None) -> Dict[str, Any]:
    genres_list = []
    try:
        genres_list = json.loads(row["genres"] or "[]")
    except Exception:
        genres_list = []

    formatted_files = []
    for f in files:
        size_mb = f["file_size_mb"] or 0
        size_gb = round(size_mb / 1024, 1) if size_mb >= 100 else round(size_mb / 1024, 2)
        q = normalize_quality_value(str(f["quality"]))
        dur_mins = max(1, round(int(f["duration_sec"]) / 60)) if f["duration_sec"] else 0
        size_disp = f"{round(size_mb)} ميجا" if size_mb < 900 else f"{size_gb} جيجا"
        if q in ("4K", "2160"):
            label = f"فائقة 4K UHD · {size_disp}"
        elif q == "1080":
            label = f"عالية 1080p FHD · {size_disp}"
        elif q == "720":
            label = f"متوسطة 720p HD · {size_disp}"
        elif q == "480":
            label = f"خفيفة 480p SD · {size_disp}"
        elif q == "360":
            label = f"اقتصادية 360p · {size_disp}"
        elif q == "240":
            label = f"ضعيفة جداً 240p · {size_disp}"
        elif q == "144":
            label = f"توفير أقصى 144p · {size_disp}"
        else:
            label = f"دقة {q}p · {size_disp}"

        ep_num = f["episode"] or 0
        season_num = _row_val(f, "season", 1) or 1
        ep_title = _row_val(f, "episode_title", "") or (f"الحلقة {ep_num}" if ep_num > 0 else "")

        formatted_files.append({
            "id": f["id"],
            "quality": q,
            "season": season_num,
            "episode": ep_num,
            "episode_title": ep_title,
            "storage_chat_id": _row_val(f, "storage_chat_id", ""),
            "storage_message_id": _row_val(f, "storage_message_id", 0),
            "video_file_id": _row_val(f, "video_file_id", ""),
            "storage_caption": _row_val(f, "storage_caption", ""),
            "duration_mins": dur_mins,
            "size_mb": round(size_mb, 1),
            "size_gb": size_gb,
            "label": label
        })

    formatted_comments = []
    if comments:
        for c in comments:
            formatted_comments.append({
                "id": c["id"],
                "user_name": c["user_name"] or "مستخدم",
                "stars": c["stars"] or 5,
                "text": c["comment_text"],
                "date": (c["created_at"] or "")[:10]
            })

    country = _row_val(row, "country", "الولايات المتحدة الأمريكية") or "الولايات المتحدة الأمريكية"
    cat_label = _row_val(row, "category_label", "") or ("فيلم أجنبي" if row["media_type"] == "movie" else "مسلسل أجنبي")
    views = _row_val(row, "views_count", 0) or 0

    return {
        "id": row["id"],
        "tmdb_id": row["tmdb_id"],
        "media_type": row["media_type"],
        "type_ar": "أنمي" if row["media_type"] == "anime" else ("فيلم" if row["media_type"] == "movie" else "مسلسل"),
        "title": row["title"],
        "original_title": row["original_title"],
        "year": row["year"],
        "duration": row["duration"],
        "country": country,
        "category_label": cat_label,
        "views_count": int(views),
        "rating": round(float(row["rating"] or 0.0), 1),
        "genres": genres_list,
        "overview": row["overview"],
        "poster_url": row["poster_url"],
        "backdrop_url": row["backdrop_url"] or row["poster_url"],
        "is_tonight": bool(row["is_tonight"]),
        "published_chat_id": _row_val(row, "published_chat_id", ""),
        "published_msg_id": int(_row_val(row, "published_msg_id", 0) or 0),
        "section_pub_chat_id": _row_val(row, "section_pub_chat_id", ""),
        "section_pub_msg_id": int(_row_val(row, "section_pub_msg_id", 0) or 0),
        "collection_id": int(_row_val(row, "collection_id", 0) or 0),
        "collection_name": _row_val(row, "collection_name", ""),
        "collection_poster": _row_val(row, "collection_poster", ""),
        "collection_backdrop": _row_val(row, "collection_backdrop", ""),
        "collection_total_parts": int(_row_val(row, "collection_total_parts", 0) or 0),
        "collection_forced": int(_row_val(row, "collection_forced", 0) or 0),
        "is_hidden": bool(int(_row_val(row, "is_hidden", 0) or 0)),
        "section_key": _row_val(row, "section_key", ""),
        "last_news_reply_msg_id": int(_row_val(row, "last_news_reply_msg_id", 0) or 0),
        "cast": (json.loads(_row_val(row, "cast_json", "[]") or "[]") if str(_row_val(row, "cast_json", "[]") or "").startswith("[") else []),
        "trailer_url": _row_val(row, "trailer_url", ""),
        "files": formatted_files,
        "comments": formatted_comments
    }


def _extract_franchise_base_title(title: str) -> str:
    if not title:
        return ""
    s = title.strip()
    # إزالة الجزء/الأرقام الرومانية أو العربية واللواحق الشائعة
    s = re.sub(r"\s*(?::|-|–)\s*.*$", "", s)
    s = re.sub(r"\s+(?:part|chapter|vol|volume|الجزء)\s*[0-9IVX]+.*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+(?:[2-9]|1[0-5]|II|III|IV|V|VI|الأول|الثاني|الثالث|الرابع|الخامس)\s*$", "", s, flags=re.IGNORECASE)
    return s.strip()


def auto_group_similar_movies() -> int:
    """
    يفحص الأفلام الموجودة بقاعدة البيانات: إذا وجد 2 أو أكثر من الأفلام تشترك بنفس الاسم الأساسي
    (مثل Escape Plan 1, Escape Plan 2, Escape Plan 3) وما بيها collection_name، يربطها تلقائياً بسلسلة موحدة!
    """
    conn = get_conn()
    cur = conn.cursor()
    rows = cur.execute("SELECT id, title, original_title, year, collection_id, collection_name, poster_url, backdrop_url FROM media WHERE media_type = 'movie'").fetchall()

    # 1) توحيد السلسلة للأفلام التي تحمل نفس collection_id
    by_cid: Dict[int, List[sqlite3.Row]] = {}
    for r in rows:
        cid = int(r["collection_id"] or 0)
        if cid > 0:
            by_cid.setdefault(cid, []).append(r)

    # 2) مطابقة الاسم الأساسي للأفلام التي ليس لها collection_id ولا collection_name
    by_base: Dict[str, List[sqlite3.Row]] = {}
    for r in rows:
        base_en = _extract_franchise_base_title(r["original_title"] or "").lower()
        base_ar = _extract_franchise_base_title(r["title"] or "").lower()
        key = base_en if len(base_en) >= 4 else (base_ar if len(base_ar) >= 4 else "")
        if key:
            by_base.setdefault(key, []).append(r)

    updated_ids = []
    for key, group in by_base.items():
        if len(group) < 2:
            continue
        # إذا كان أحد الأفلام بالمجموعة يملك اسم سلسلة أو collection_id، ننسخه للبقية
        existing_cname = next((g["collection_name"] for g in group if g["collection_name"]), "")
        existing_cid = next((int(g["collection_id"] or 0) for g in group if int(g["collection_id"] or 0) > 0), 0)
        if not existing_cname:
            sample = group[0]
            clean_base = _extract_franchise_base_title(sample["original_title"] or sample["title"])
            existing_cname = f"سلسلة أفلام {clean_base}"
        for g in group:
            if not g["collection_name"]:
                cur.execute(
                    "UPDATE media SET collection_name = ?, collection_id = CASE WHEN collection_id = 0 THEN ? ELSE collection_id END WHERE id = ?",
                    (existing_cname, existing_cid, g["id"])
                )
                updated_ids.append(g["id"])

    conn.commit()
    conn.close()
    for mid in updated_ids:
        _push_media_full(mid)
    return len(updated_ids)


def _enrich_items_with_collections(items: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    يجمع سلاسل الأفلام ويرتب أجزاء كل سلسلة زمنياً (من الأقدم للأحدث)،
    ويفعّل السلسلة تلقائياً إذا وصلت 3 أفلام فأكثر (أو اكتملت أجزاؤها أو فعّلها الأدمن يدوياً).
    """
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for it in items:
        if it.get("media_type") != "movie":
            continue
        cid = int(it.get("collection_id") or 0)
        cname = (it.get("collection_name") or "").strip()
        if cid > 0:
            gkey = f"cid_{cid}"
        elif cname:
            gkey = f"cname_{cname.lower()}"
        else:
            continue
        groups.setdefault(gkey, []).append(it)

    collections_list: List[Dict[str, Any]] = []
    item_collection_map: Dict[int, Dict[str, Any]] = {}

    for gkey, parts in groups.items():
        if len(parts) > 15:
            # تجاهل أي مجموعة ضخمة غير حقيقية لمنع تضخم الذاكرة
            continue
        # ترتيب الأجزاء زمنياً من الجزء الأول للأخير حسب سنة الإنتاج ثم رقم الإضافة
        sorted_parts = sorted(
            parts,
            key=lambda x: (int(x["year"]) if str(x.get("year") or "").isdigit() else 9999, x["id"])
        )
        parts_count = len(sorted_parts)
        first_p = sorted_parts[0]
        last_p = sorted_parts[-1]

        cid = max((int(p.get("collection_id") or 0) for p in sorted_parts), default=0)
        cname = next((p["collection_name"] for p in sorted_parts if p.get("collection_name")), f"سلسلة {first_p['title']}")
        cposter = next((p["collection_poster"] for p in sorted_parts if p.get("collection_poster")), last_p.get("poster_url") or first_p.get("poster_url"))
        cbackdrop = next((p["collection_backdrop"] for p in sorted_parts if p.get("collection_backdrop")), last_p.get("backdrop_url") or cposter)
        total_parts = max((int(p.get("collection_total_parts") or 0) for p in sorted_parts), default=0)
        forced = any(int(p.get("collection_forced") or 0) == 1 for p in sorted_parts)

        # تتفعل السلسلة تلقائياً عند 3 أفلام فأكثر، أو عند اكتمال كل أجزاء السلسلة (2+)، أو بتفعيل الأدمن
        is_complete = (parts_count >= 3) or (total_parts >= 2 and parts_count >= total_parts)
        is_active = is_complete or forced

        avg_rating = round(sum(float(p.get("rating") or 0.0) for p in sorted_parts) / max(1, parts_count), 1)
        years = [str(p.get("year")) for p in sorted_parts if p.get("year")]
        years_range = f"{years[0]} – {years[-1]}" if len(years) > 1 and years[0] != years[-1] else (years[0] if years else "")

        parts_summary = []
        for idx, p in enumerate(sorted_parts, 1):
            parts_summary.append({
                "id": p["id"],
                "part_number": idx,
                "title": p["title"],
                "original_title": p.get("original_title") or p["title"],
                "year": p.get("year") or "",
                "rating": p.get("rating") or 0.0,
                "duration": p.get("duration") or "",
                "poster_url": p.get("poster_url") or "",
                "backdrop_url": p.get("backdrop_url") or "",
                "overview": p.get("overview") or "",
                "qualities": sorted(list({str(f["quality"]) for f in (p.get("files") or [])}), reverse=True)
            })

        col_obj = {
            "collection_key": gkey,
            "collection_id": cid,
            "name": cname,
            "poster_url": cposter,
            "backdrop_url": cbackdrop,
            "parts_count": parts_count,
            "total_parts": max(parts_count, total_parts),
            "is_active": is_active,
            "is_complete": is_complete,
            "forced": forced,
            "rating": avg_rating,
            "years_range": years_range,
            "genres": first_p.get("genres") or [],
            "parts": parts_summary
        }
        collections_list.append(col_obj)

        for idx, p in enumerate(sorted_parts, 1):
            item_collection_map[p["id"]] = {
                "collection_key": gkey,
                "collection_id": cid,
                "name": cname,
                "part_number": idx,
                "parts_count": parts_count,
                "total_parts": max(parts_count, total_parts),
                "is_active": is_active,
                "is_complete": is_complete,
                "parts": parts_summary
            }

    for it in items:
        it["collection_info"] = item_collection_map.get(it["id"])

    collections_list.sort(key=lambda c: (1 if c["is_active"] else 0, c["parts_count"], c["rating"]), reverse=True)
    return items, collections_list


def _compute_media_counts(items_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    movies_count = 0
    series_count = 0
    anime_count = 0
    genre_counts: Dict[str, int] = {}
    for x in items_list:
        mt = x.get("media_type")
        if mt == "movie":
            movies_count += 1
        elif mt == "tv":
            series_count += 1
        elif mt == "anime":
            anime_count += 1
        for g in (x.get("genres") or ()):
            genre_counts[g] = genre_counts.get(g, 0) + 1
    return {
        "movies": movies_count,
        "series": series_count,
        "anime": anime_count,
        "genres": genre_counts,
    }


def _make_lightweight_public_item(it: Dict[str, Any]) -> Dict[str, Any]:
    """يزيل حقول الخزن الداخلي الثقيلة (storage_caption, video_file_id) لتسريع /api/bootstrap بنسبة 80%."""
    slim_files = [
        {
            "id": f["id"],
            "quality": f["quality"],
            "season": f.get("season", 1),
            "episode": f.get("episode", 0),
            "episode_title": f.get("episode_title", ""),
            "duration_mins": f.get("duration_mins", 0),
            "size_mb": f.get("size_mb", 0),
            "size_gb": f.get("size_gb", 0),
            "label": f.get("label", ""),
        }
        for f in (it.get("files") or [])
    ]
    out = dict(it)
    out["files"] = slim_files
    return out


def get_cached_media_bundle(include_hidden: bool = False, slim_for_webapp: bool = False) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """يرجع (الأعمال، الإحصائيات) من الكاش الفوري بالذاكرة بدون إعادة الحساب في كل طلب."""
    all_items = get_all_media()
    if include_hidden:
        if _MEDIA_CACHE.get("counts_all") is None:
            _MEDIA_CACHE["counts_all"] = _compute_media_counts(all_items)
        if slim_for_webapp:
            if _MEDIA_CACHE.get("public_items") is None:
                _MEDIA_CACHE["public_items"] = [_make_lightweight_public_item(x) for x in all_items]
            return _MEDIA_CACHE["public_items"], _MEDIA_CACHE["counts_all"]
        return all_items, _MEDIA_CACHE["counts_all"]
    if _MEDIA_CACHE.get("visible_items") is None:
        _MEDIA_CACHE["visible_items"] = [x for x in all_items if not x.get("is_hidden")]
    if _MEDIA_CACHE.get("counts_visible") is None:
        _MEDIA_CACHE["counts_visible"] = _compute_media_counts(_MEDIA_CACHE["visible_items"])
    if slim_for_webapp:
        if _MEDIA_CACHE.get("public_visible_items") is None:
            _MEDIA_CACHE["public_visible_items"] = [_make_lightweight_public_item(x) for x in _MEDIA_CACHE["visible_items"]]
        return _MEDIA_CACHE["public_visible_items"], _MEDIA_CACHE["counts_visible"]
    return _MEDIA_CACHE["visible_items"], _MEDIA_CACHE["counts_visible"]


def get_all_media() -> List[Dict[str, Any]]:
    """يجلب كل الأعمال مع ملفاتها وتعليقاتها من كاش الذاكرة الفائق السرعة أو بـ 3 استعلامات مفهرسة."""
    cached_items = _MEDIA_CACHE.get("items")
    if cached_items is not None:
        return cached_items

    conn = get_conn()
    cur = conn.cursor()
    rows = cur.execute("SELECT * FROM media ORDER BY id DESC").fetchall()
    if not rows:
        conn.close()
        _MEDIA_CACHE["items"] = []
        _MEDIA_CACHE["visible_items"] = []
        _MEDIA_CACHE["collections_all"] = []
        _MEDIA_CACHE["collections_active"] = []
        return []

    all_files = cur.execute(
        "SELECT * FROM media_files ORDER BY media_id ASC, season ASC, episode ASC, CAST(quality AS INTEGER) DESC"
    ).fetchall()
    all_comments = cur.execute("SELECT * FROM comments ORDER BY id DESC").fetchall()
    conn.close()

    files_by_media: Dict[int, List[sqlite3.Row]] = {}
    for f in all_files:
        files_by_media.setdefault(int(f["media_id"]), []).append(f)

    comments_by_media: Dict[int, List[sqlite3.Row]] = {}
    for c in all_comments:
        mid = int(c["media_id"])
        lst = comments_by_media.setdefault(mid, [])
        if len(lst) < 20:
            lst.append(c)

    result = []
    for r in rows:
        mid = int(r["id"])
        result.append(format_media_row(r, files_by_media.get(mid, []), comments_by_media.get(mid, [])))

    sec_defs = get_all_section_defs(only_enabled=False)
    for it in result:
        s_key, s_title = auto_detect_section_key(
            media_type=it.get("media_type", "movie"),
            country=it.get("country", ""),
            category_label=it.get("category_label", ""),
            genres=it.get("genres", []),
            title=it.get("title", ""),
            explicit_section_key=it.get("section_key", ""),
            section_defs=sec_defs
        )
        it["section_key"] = s_key
        it["section_title"] = s_title
        it["world_key"] = classify_item_world(it)
    enriched, cols = _enrich_items_with_collections(result)
    _MEDIA_CACHE["items"] = enriched
    _MEDIA_CACHE["visible_items"] = [x for x in enriched if not x.get("is_hidden")]
    _MEDIA_CACHE["collections_all"] = cols
    _MEDIA_CACHE["collections_active"] = [c for c in cols if c["is_active"]]
    return enriched


def get_all_collections(only_active: bool = False, items: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    get_all_media()
    if only_active and _MEDIA_CACHE.get("collections_active") is not None:
        return _MEDIA_CACHE["collections_active"]
    if not only_active and _MEDIA_CACHE.get("collections_all") is not None:
        return _MEDIA_CACHE["collections_all"]
    if items is None:
        items = _MEDIA_CACHE.get("items") or []
    _, cols = _enrich_items_with_collections(items)
    if only_active:
        return [c for c in cols if c["is_active"]]
    return cols


def get_media_by_id(media_id: int) -> Optional[Dict[str, Any]]:
    """استعلام مباشر وسريع جداً للعمل المطلوب فقط بدلاً من تحميل قاعدة البيانات كاملة."""
    conn = get_conn()
    cur = conn.cursor()
    row = cur.execute("SELECT * FROM media WHERE id = ?", (int(media_id),)).fetchone()
    if not row:
        conn.close()
        return None
    files = cur.execute(
        "SELECT * FROM media_files WHERE media_id = ? ORDER BY season ASC, episode ASC, CAST(quality AS INTEGER) DESC",
        (int(media_id),)
    ).fetchall()
    comments = cur.execute(
        "SELECT * FROM comments WHERE media_id = ? ORDER BY id DESC LIMIT 20",
        (int(media_id),)
    ).fetchall()
    conn.close()

    item = format_media_row(row, files, comments)
    sec_defs = get_all_section_defs(only_enabled=False)
    s_key, s_title = auto_detect_section_key(
        media_type=item.get("media_type", "movie"),
        country=item.get("country", ""),
        category_label=item.get("category_label", ""),
        genres=item.get("genres", []),
        title=item.get("title", ""),
        explicit_section_key=item.get("section_key", ""),
        section_defs=sec_defs
    )
    item["section_key"] = s_key
    item["section_title"] = s_title
    return item


def set_movies_collection(media_ids: List[int], collection_name: str, force_active: bool = True) -> int:
    clean_name = collection_name.strip()
    if not clean_name.startswith("سلسلة"):
        clean_name = f"سلسلة {clean_name}"
    conn = get_conn()
    cur = conn.cursor()
    touched = 0
    for mid in media_ids:
        cur.execute(
            "UPDATE media SET collection_name = ?, collection_forced = ? WHERE id = ?",
            (clean_name, 1 if force_active else 0, int(mid))
        )
        if cur.rowcount > 0:
            touched += 1
    conn.commit()
    conn.close()
    for mid in media_ids:
        _push_media_full(int(mid))
    return touched


def remove_movie_from_collection(media_id: int) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE media SET collection_id = 0, collection_name = '', collection_poster = '', collection_backdrop = '', collection_total_parts = 0, collection_forced = 0 WHERE id = ?",
        (int(media_id),)
    )
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    if changed:
        _push_media_full(int(media_id))
    return changed


def get_file_by_id(file_id: int) -> Optional[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT mf.*, m.title, m.year, m.media_type
        FROM media_files mf
        JOIN media m ON m.id = mf.media_id
        WHERE mf.id = ?
    """, (file_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_media_files_with_titles() -> List[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT mf.*, m.title, m.original_title, m.year, m.media_type
        FROM media_files mf
        JOIN media m ON m.id = mf.media_id
        ORDER BY mf.id DESC
    """)
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def set_tonight_media(media_id: int):
    invalidate_media_cache()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE media SET is_tonight = 0")
    cur.execute("UPDATE media SET is_tonight = 1 WHERE id = ?", (media_id,))
    conn.commit()
    conn.close()
    trigger_firebase_push("meta/tonight_media_id", int(media_id))
    _push_media_full(media_id)


def delete_media(media_id: int) -> Optional[Dict[str, Any]]:
    invalidate_media_cache()
    conn = get_conn()
    cur = conn.cursor()
    media_row = cur.execute("SELECT * FROM media WHERE id = ?", (media_id,)).fetchone()
    file_ids = [r[0] for r in cur.execute("SELECT id FROM media_files WHERE media_id = ?", (media_id,)).fetchall()]
    comment_ids = [r[0] for r in cur.execute("SELECT id FROM comments WHERE media_id = ?", (media_id,)).fetchall()]
    list_rows = cur.execute("SELECT user_id, list_type FROM user_lists WHERE media_id = ?", (media_id,)).fetchall()
    was_tonight = media_row["is_tonight"] if media_row else 0
    cur.execute("DELETE FROM media_files WHERE media_id = ?", (media_id,))
    cur.execute("DELETE FROM user_lists WHERE media_id = ?", (media_id,))
    cur.execute("DELETE FROM comments WHERE media_id = ?", (media_id,))
    cur.execute("DELETE FROM media WHERE id = ?", (media_id,))
    conn.commit()
    conn.close()

    updates: Dict[str, Any] = {f"media/{media_id}": None}
    for fid in file_ids:
        updates[f"media_files/{fid}"] = None
    for cid in comment_ids:
        updates[f"comments/{cid}"] = None
    for r in list_rows:
        updates[f"user_lists/{r['user_id']}/{r['list_type']}/{media_id}"] = None
    if was_tonight:
        updates["meta/tonight_media_id"] = None
    items = list(updates.items())
    for i in range(0, len(items), 400):
        trigger_firebase_update(dict(items[i:i + 400]))
    return dict(media_row) if media_row else None


def delete_single_media_file(file_id: int):
    invalidate_media_cache()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM media_files WHERE id = ?", (file_id,))
    conn.commit()
    conn.close()
    trigger_firebase_push(f"media_files/{file_id}", None)


def search_media_by_title(query: str) -> List[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM media WHERE title LIKE ? OR original_title LIKE ? ORDER BY id DESC LIMIT 15", (f"%{query}%", f"%{query}%"))
    rows = cur.fetchall()
    res = []
    for r in rows:
        cur.execute("SELECT * FROM media_files WHERE media_id = ?", (r["id"],))
        files = cur.fetchall()
        res.append(format_media_row(r, files))
    conn.close()
    return res


def toggle_user_list(user_id: int, media_id: int, list_type: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM user_lists WHERE user_id = ? AND media_id = ? AND list_type = ?", (user_id, media_id, list_type))
    exists = cur.fetchone()
    if exists:
        if list_type != "watched":
            cur.execute("DELETE FROM user_lists WHERE user_id = ? AND media_id = ? AND list_type = ?", (user_id, media_id, list_type))
            conn.commit()
            conn.close()
            trigger_firebase_push(f"user_lists/{user_id}/{list_type}/{media_id}", None)
            return False
        conn.close()
        return True
    else:
        cur.execute("INSERT OR IGNORE INTO user_lists (user_id, media_id, list_type) VALUES (?, ?, ?)", (user_id, media_id, list_type))
        if list_type == "watched":
            cur.execute("UPDATE media SET views_count = views_count + 1 WHERE id = ?", (media_id,))
        conn.commit()
        conn.close()
        trigger_firebase_push(f"user_lists/{user_id}/{list_type}/{media_id}", _ts_now())
        if list_type == "watched":
            _push_media_full(media_id)
        return True


def get_user_lists(user_id: int) -> Dict[str, List[int]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT media_id, list_type FROM user_lists WHERE user_id = ?", (user_id,))
    rows = cur.fetchall()
    conn.close()
    out = {"watchlist": [], "favorites": [], "watched": []}
    for r in rows:
        lt = r["list_type"]
        if lt in out:
            out[lt].append(r["media_id"])
    return out


def add_comment(media_id: int, user_id: int, user_name: str, stars: int, text: str) -> Dict[str, Any]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO comments (media_id, user_id, user_name, stars, comment_text)
        VALUES (?, ?, ?, ?, ?)
    """, (media_id, user_id, user_name or "مستخدم", max(1, min(5, stars)), text.strip()))
    cid = cur.lastrowid
    conn.commit()
    conn.close()
    _push_comment_full(cid)
    return {
        "id": cid,
        "user_name": user_name or "مستخدم",
        "stars": stars,
        "text": text.strip(),
        "date": datetime.utcnow().strftime("%Y-%m-%d")
    }


# =====================================================================
# نظام الطلبات المتقدم (منع التكرار، طلباتي، تجميع الطلبات، إشعار التلبية)
# =====================================================================
def _norm_title(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9\u0600-\u06FF]+", " ", (s or "").lower()).strip()


def _titles_match_for_request(req_text: str, media_title: str, media_orig_title: str) -> bool:
    """يطابق عنوان الطلب مع اسم العمل العربي أو الأصلي بمرونة عالية وبغض النظر عن سنة الإنتاج."""
    rq_clean = _clean_dedup_key(req_text)
    rq_norm = _norm_title(req_text)
    if not rq_clean and not rq_norm:
        return False

    for cand in (media_title, media_orig_title):
        if not cand:
            continue
        c_clean = _clean_dedup_key(cand)
        c_norm = _norm_title(cand)
        if rq_clean and c_clean and rq_clean == c_clean:
            return True
        if rq_norm and c_norm and rq_norm == c_norm:
            return True
        if len(rq_clean) >= 4 and c_clean and (rq_clean in c_clean or c_clean in rq_clean):
            return True
        if len(rq_norm) >= 4 and c_norm and (rq_norm in c_norm or c_norm in rq_norm):
            return True
        # مطابقة الكلمات الأساسية إذا كان العنوان مكوناً من كلمتين أو أكثر
        rq_words = [w for w in rq_clean.split() if len(w) >= 2]
        c_words = [w for w in c_clean.split() if len(w) >= 2]
        if len(rq_words) >= 2 and len(c_words) >= 2:
            common = set(rq_words) & set(c_words)
            if len(common) >= max(2, min(len(rq_words), len(c_words))):
                return True
    return False


def find_matching_media_for_request(request_text: str, prod_year: str = "") -> Optional[Dict[str, Any]]:
    """
    يبحث عن العمل المطلوب في قاعدة البيانات:
    يطابق الاسم أولاً، وحتى لو كتب المستخدم سنة إنتاج خاطئة (prod_year مختلفة) يرجع العمل الموجود!
    """
    if not (request_text or "").strip():
        return None
    conn = get_conn()
    cur = conn.cursor()
    rows = cur.execute("SELECT * FROM media WHERE COALESCE(is_hidden, 0) = 0 ORDER BY id DESC").fetchall()
    conn.close()

    # الأولوية للعمل المطابق بالاسم والسنة معاً، ثم المطابق بالاسم حتى لو السنة مختلفة
    fallback_match_id = None
    for r in rows:
        if _titles_match_for_request(request_text, r["title"] or "", r["original_title"] or ""):
            if not prod_year or not r["year"] or str(r["year"]).strip() == str(prod_year).strip():
                return get_media_by_id(r["id"])
            if fallback_match_id is None:
                fallback_match_id = int(r["id"])

    if fallback_match_id is not None:
        return get_media_by_id(fallback_match_id)
    return None


def get_user_request_quota(user_id: int) -> Dict[str, Any]:
    req_enabled = get_setting("requests_enabled", "1") == "1"
    cycle = get_setting("requests_cycle", "weekly") or "weekly"
    cycle_max = int(get_setting("requests_quota_max", "5") or "5")
    monthly_max = int(get_setting("monthly_req_limit", "20") or "20")

    cycle_clause = "-7 days"
    cycle_ar = "هذا الأسبوع"
    renew_ar = "كل 7 أيام"
    if cycle == "daily":
        cycle_clause = "-1 day"
        cycle_ar = "اليوم"
        renew_ar = "كل 24 ساعة"
    elif cycle == "monthly":
        cycle_clause = "-30 days"
        cycle_ar = "هذا الشهر"
        renew_ar = "كل 30 يوم"

    conn = get_conn()
    cur = conn.cursor()
    if user_id:
        cur.execute(f"SELECT COUNT(*) as c FROM media_requests WHERE user_id = ? AND created_at >= datetime('now', '{cycle_clause}')", (int(user_id),))
        used_cycle = cur.fetchone()["c"]
        cur.execute("SELECT COUNT(*) as c FROM media_requests WHERE user_id = ? AND created_at >= datetime('now', '-30 days')", (int(user_id),))
        used_month = cur.fetchone()["c"]
    else:
        used_cycle = 0
        used_month = 0
    conn.close()

    cycle_left = max(0, cycle_max - used_cycle)
    month_left = max(0, monthly_max - used_month)

    return {
        "enabled": req_enabled,
        "cycle": cycle,
        "cycle_ar": cycle_ar,
        "renew_ar": renew_ar,
        "used_cycle": used_cycle,
        "cycle_max": cycle_max,
        "cycle_left": cycle_left,
        "daily_max": cycle_max,
        "daily_left": cycle_left,
        "monthly_max": monthly_max,
        "monthly_left": month_left
    }


def create_media_request(user_id: int, user_name: str, request_text: str, prod_year: str = "") -> Dict[str, Any]:
    """
    يرجع قاموس:
      - ok: bool
      - already_exists: bool
      - existing_media: dict | None
      - message: str
      - req_id: int
      - total_requesters: int
    """
    clean_title = (request_text or "").strip()
    clean_year = (prod_year or "").strip()

    # التحقق الصارم: العنوان بالإنجليزي فقط بدون أحرف عربية
    if not clean_title or re.search(r"[\u0600-\u06FF]", clean_title) or not re.search(r"[a-zA-Z]", clean_title):
        return {
            "ok": False, "already_exists": False, "existing_media": None,
            "message": "يرجى كتابة اسم العمل بالحروف الإنجليزية فقط بالحقل العلوي.",
            "req_id": 0, "total_requesters": 0
        }

    # منع دمج التاريخ مع العنوان في الحقل العلوي
    if re.search(r"\b(19\d{2}|20\d{2})\b", clean_title):
        return {
            "ok": False, "already_exists": False, "existing_media": None,
            "message": "لا تدمج السنة ويا الاسم! اكتب اسم العمل فقط بالحقل الفوق، واكتب السنة (4 أرقام) بالحقل الجوة.",
            "req_id": 0, "total_requesters": 0
        }

    # التحقق الصارم: السنة 4 أرقام فقط في الحقل السفلي
    if not re.match(r"^(19\d{2}|20\d{2})$", clean_year):
        return {
            "ok": False, "already_exists": False, "existing_media": None,
            "message": "يرجى كتابة سنة الإنتاج (4 أرقام فقط مثل 2024) بالحقل السفلي المخصص للتاريخ.",
            "req_id": 0, "total_requesters": 0
        }

    # 1) فحص إذا العمل موجود أصلاً بقاعدة البيانات
    existing_media = find_matching_media_for_request(request_text, prod_year)
    if existing_media:
        return {
            "ok": False,
            "already_exists": True,
            "existing_media": existing_media,
            "message": f"✦ العمل «{existing_media['title']} ({existing_media['year']})» متوفر أصلاً بالتطبيق! تكدر تفتحه وتفورتكسه هسة.",
            "req_id": 0,
            "total_requesters": 0
        }

    quota = get_user_request_quota(user_id)
    if not quota["enabled"] and not is_user_admin(user_id):
        return {
            "ok": False, "already_exists": False, "existing_media": None,
            "message": "استقبال الطلبات مغلق حالياً من قبل الإدارة.",
            "req_id": 0, "total_requesters": 0
        }

    if quota["cycle_left"] <= 0:
        return {
            "ok": False, "already_exists": False, "existing_media": None,
            "message": f"استنفدت حد طلباتك {quota['cycle_ar']} ({quota['cycle_max']} طلبات). يتجدد رصيدك تلقائياً {quota.get('renew_ar', 'كل 7 أيام')}!",
            "req_id": 0, "total_requesters": 0
        }

    conn = get_conn()
    cur = conn.cursor()
    # فحص إذا نفس المستخدم طالب نفس العمل وهو بعده قيد الانتظار
    dup_self = cur.execute(
        "SELECT id FROM media_requests WHERE user_id = ? AND LOWER(request_text) = LOWER(?) AND status = 'pending'",
        (user_id, request_text.strip())
    ).fetchone()
    if dup_self:
        conn.close()
        return {
            "ok": False, "already_exists": False, "existing_media": None,
            "message": "أنت راسل طلب لهذا العمل وموجود حالياً بقائمة الانتظار، راح يوصلك إشعار أول ما ينزل!",
            "req_id": dup_self["id"], "total_requesters": 1
        }

    cur.execute(
        "INSERT INTO media_requests (user_id, user_name, request_text, prod_year, status, updated_at) VALUES (?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP)",
        (user_id, user_name or "مستخدم", request_text.strip(), prod_year.strip())
    )
    req_id = cur.lastrowid
    total_req = cur.execute(
        "SELECT COUNT(DISTINCT user_id) as c FROM media_requests WHERE LOWER(request_text) = LOWER(?) AND status = 'pending'",
        (request_text.strip(),)
    ).fetchone()["c"]
    conn.commit()
    conn.close()

    _push_request_full(req_id)
    return {
        "ok": True,
        "already_exists": False,
        "existing_media": None,
        "message": "وصل طلبك للأدمنية وراح يوصلك إشعار بالبوت أول ما يتوفر العمل!",
        "req_id": req_id,
        "total_requesters": total_req or 1
    }


def get_user_media_requests(user_id: int, limit: int = 25) -> List[Dict[str, Any]]:
    if not user_id:
        return []
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM media_requests WHERE user_id = ? ORDER BY id DESC LIMIT ?",
        (int(user_id), limit)
    )
    rows = cur.fetchall()
    conn.close()
    status_ar_map = {
        "pending": "◈ قيد الانتظار",
        "fulfilled": "✓ تم النشر",
        "rejected": "✕ مرفوض"
    }
    out = []
    for r in rows:
        st = r["status"] or "pending"
        f_mid = int(_row_val(r, "fulfilled_media_id", 0) or 0)
        if not f_mid and st == "fulfilled":
            matched = find_matching_media_for_request(r["request_text"], r["prod_year"])
            if matched:
                f_mid = matched["id"]
        out.append({
            "id": r["id"],
            "request_text": r["request_text"],
            "prod_year": r["prod_year"] or "",
            "status": st,
            "status_ar": status_ar_map.get(st, "⏳ قيد الانتظار"),
            "fulfilled_media_id": f_mid,
            "created_at": (r["created_at"] or "")[:16]
        })
    return out


def get_pending_media_requests(limit: int = 30) -> List[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    now_ts = int(time.time())
    cur.execute("""
        SELECT mr.*,
               CASE WHEN COALESCE(u.vip_until, 0) > ? THEN 1 ELSE 0 END AS is_vip_req
        FROM media_requests mr
        LEFT JOIN users u ON u.user_id = mr.user_id
        WHERE mr.status = 'pending'
        ORDER BY is_vip_req DESC, mr.id DESC
        LIMIT ?
    """, (now_ts, limit))
    rows = cur.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["is_vip_req"] = bool(r["is_vip_req"])
        cnt = cur.execute(
            "SELECT COUNT(DISTINCT user_id) as c FROM media_requests WHERE LOWER(request_text) = LOWER(?) AND status = 'pending'",
            (r["request_text"],)
        ).fetchone()["c"]
        d["requesters_count"] = cnt or 1
        result.append(d)
    conn.close()
    return result


def mark_media_request_fulfilled(request_id: int, fulfilled_media_id: int = 0) -> List[Dict[str, Any]]:
    """
    يحدد الطلب (وأي طلبات معلقة لنفس اسم العمل) كمكتمل، ويحدث Firebase،
    ويرجع قائمة الطلبات التي تمت تلبيتها لإرسال إشعار لكل أصحابها.
    """
    conn = get_conn()
    cur = conn.cursor()
    target = cur.execute("SELECT * FROM media_requests WHERE id = ?", (int(request_id),)).fetchone()
    if not target:
        conn.close()
        return []

    if not fulfilled_media_id:
        matched = find_matching_media_for_request(target["request_text"], target["prod_year"])
        if matched:
            fulfilled_media_id = matched["id"]

    same_rows = cur.execute(
        "SELECT * FROM media_requests WHERE status = 'pending' AND (id = ? OR LOWER(request_text) = LOWER(?))",
        (int(request_id), target["request_text"])
    ).fetchall()

    fulfilled_list = []
    for r in same_rows:
        cur.execute(
            "UPDATE media_requests SET status = 'fulfilled', fulfilled_media_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (int(fulfilled_media_id or 0), r["id"])
        )
        d = dict(r)
        d["status"] = "fulfilled"
        d["fulfilled_media_id"] = int(fulfilled_media_id or 0)
        fulfilled_list.append(d)

    conn.commit()
    conn.close()

    for d in fulfilled_list:
        _push_request_full(d["id"])
    return fulfilled_list


def mark_media_request_rejected(request_id: int) -> Optional[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    target = cur.execute("SELECT * FROM media_requests WHERE id = ?", (int(request_id),)).fetchone()
    if not target:
        conn.close()
        return None
    cur.execute(
        "UPDATE media_requests SET status = 'rejected', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (int(request_id),)
    )
    conn.commit()
    conn.close()
    _push_request_full(int(request_id))
    d = dict(target)
    d["status"] = "rejected"
    return d


def auto_fulfill_matching_requests_for_media(media_id: int) -> List[Dict[str, Any]]:
    """
    عند نشر أو إضافة أي عمل، يفحص كل الطلبات المعلقة فإذا طابق الاسم أو الاسم الأصلي
    (حتى لو كانت السنة التي كتبها المستخدم في الطلب خاطئة أو مختلفة!)
    يحددها كـ fulfilled تلقائياً ويرجعها لإشعار المستخدمين فوراً بدون الحاجة لأي زر!
    """
    media = get_media_by_id(media_id)
    if not media:
        return []
    t_ar = str(media.get("title") or "")
    t_en = str(media.get("original_title") or "")

    conn = get_conn()
    cur = conn.cursor()
    pending = cur.execute("SELECT * FROM media_requests WHERE status = 'pending'").fetchall()
    matched_ids = []
    matched_rows = []
    for r in pending:
        rq_txt = str(r["request_text"] or "").strip()
        if not rq_txt:
            continue
        if _titles_match_for_request(rq_txt, t_ar, t_en):
            matched_ids.append(r["id"])
            d = dict(r)
            d["status"] = "fulfilled"
            d["fulfilled_media_id"] = int(media_id)
            matched_rows.append(d)

    for rid in matched_ids:
        cur.execute(
            "UPDATE media_requests SET status = 'fulfilled', fulfilled_media_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (int(media_id), rid)
        )
    conn.commit()
    conn.close()

    for rid in matched_ids:
        _push_request_full(rid)
    return matched_rows


def sweep_and_fulfill_all_pending_requests() -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """
    يفحص جميع الطلبات المعلقة حالياً ويقارنها بكل الأفلام والمسلسلات المضافة في المكتبة (بغض النظر عن اختلاف السنة):
    أي طلب يتوفر عمله في المكتبة يتم تحويله تلقائياً إلى مكتمل (fulfilled) وإرجاع قائمة (request_dict, media_dict) لإشعار أصحابها فوراً!
    """
    conn = get_conn()
    cur = conn.cursor()
    pending = cur.execute("SELECT * FROM media_requests WHERE status = 'pending' ORDER BY id ASC").fetchall()
    if not pending:
        conn.close()
        return []
    media_rows = cur.execute("SELECT id, title, original_title, year FROM media WHERE COALESCE(is_hidden, 0) = 0 ORDER BY id DESC").fetchall()
    fulfilled_pairs: List[Tuple[int, int, Dict[str, Any]]] = []

    for req in pending:
        rq_txt = str(req["request_text"] or "").strip()
        if not rq_txt:
            continue
        matched_mid = None
        for mr in media_rows:
            if _titles_match_for_request(rq_txt, mr["title"] or "", mr["original_title"] or ""):
                matched_mid = int(mr["id"])
                break
        if matched_mid is not None:
            rid = int(req["id"])
            cur.execute(
                "UPDATE media_requests SET status = 'fulfilled', fulfilled_media_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (matched_mid, rid)
            )
            req_d = dict(req)
            req_d["status"] = "fulfilled"
            req_d["fulfilled_media_id"] = matched_mid
            fulfilled_pairs.append((rid, matched_mid, req_d))

    conn.commit()
    conn.close()

    results: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for rid, mid, req_d in fulfilled_pairs:
        _push_request_full(rid)
        m_item = get_media_by_id(mid)
        if m_item:
            results.append((req_d, m_item))
    return results


# =====================================================================
# نظام البلاغات المفصّلة عن المشاكل
# =====================================================================
def create_media_report(media_id: int, media_title: str, user_id: int, user_name: str, reason_type: str, details: str) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO media_reports (media_id, media_title, user_id, user_name, reason_type, details, status)
        VALUES (?, ?, ?, ?, ?, ?, 'pending')
    """, (media_id, media_title, user_id, user_name, reason_type, details.strip()))
    rep_id = cur.lastrowid
    conn.commit()
    conn.close()
    _push_report_full(rep_id)
    return rep_id


def get_pending_media_reports(limit: int = 25) -> List[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM media_reports WHERE status = 'pending' ORDER BY id DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def resolve_media_report(report_id: int) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE media_reports SET status = 'resolved' WHERE id = ?", (int(report_id),))
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    if changed:
        _push_report_full(int(report_id))
    return changed


# =====================================================================
# نظام الأقسام الذكية والمربعات المتقلبة (Smart Sections Engine - V11)
# =====================================================================
def get_all_section_defs(only_enabled: bool = False) -> List[Dict[str, Any]]:
    if only_enabled in _SECTION_DEFS_CACHE:
        return _SECTION_DEFS_CACHE[only_enabled]
    conn = get_conn()
    cur = conn.cursor()
    try:
        if only_enabled:
            rows = cur.execute("SELECT * FROM custom_sections WHERE is_enabled = 1 ORDER BY sort_order ASC, created_at ASC").fetchall()
        else:
            rows = cur.execute("SELECT * FROM custom_sections ORDER BY sort_order ASC, created_at ASC").fetchall()
        res = [dict(r) for r in rows]
    except Exception:
        res = [dict(x, is_enabled=1) for x in DEFAULT_SMART_SECTIONS]
    finally:
        conn.close()
    _SECTION_DEFS_CACHE[only_enabled] = res
    return res


def auto_detect_section_key(
    media_type: str,
    country: str,
    category_label: str,
    genres: Optional[List[str]] = None,
    title: str = "",
    explicit_section_key: str = "",
    section_defs: Optional[List[Dict[str, Any]]] = None
) -> Tuple[str, str]:
    """
    يحدد القسم الذكي للعمل تلقائياً بناءً على التخصيص اليدوي (إن وجد) أو الدولة/التصنيف/النوع.
    """
    defs = section_defs if section_defs is not None else get_all_section_defs(only_enabled=False)
    defs_by_key = {d["section_key"]: d for d in defs}

    exp_key = (explicit_section_key or "").strip()
    if exp_key and exp_key in defs_by_key:
        return exp_key, defs_by_key[exp_key]["title"]

    mtype = (media_type or "movie").strip().lower()
    cntry = (country or "").strip()
    cat = (category_label or "").strip()
    g_list = genres or []

    # 1. فحص الأنمي أولاً (سواء كان media_type == 'anime' أو تصنيفه أنمي)
    if mtype == "anime" or cat == "أنمي" or ("أنميشن" in g_list and ("اليابان" in cntry or "أنمي" in cat)):
        if "anime_world" in defs_by_key:
            return "anime_world", defs_by_key["anime_world"]["title"]
        return "anime_world", "عالم الأنمي"

    # 2. مطابقة الأقسام المخصصة أو الافتراضية حسب قواعد الربط التلقائي (auto_category / auto_country)
    for d in defs:
        s_mtype = (d.get("media_type_filter") or "all").strip().lower()
        if s_mtype not in ("all", mtype):
            continue
        auto_cats = [x.strip() for x in (d.get("auto_category") or "").split(",") if x.strip()]
        auto_cnts = [x.strip() for x in (d.get("auto_country") or "").split(",") if x.strip()]

        if auto_cats and cat in auto_cats:
            return d["section_key"], d["title"]
        if auto_cnts and any(ac in cntry for ac in auto_cnts):
            return d["section_key"], d["title"]

    # 3. مطابقة احتياطية ذكية
    if mtype == "tv":
        if "العراق" in cntry or "عراقي" in cat:
            return "tv_iraqi", defs_by_key.get("tv_iraqi", {}).get("title", "مسلسلات عراقية")
        if "تركيا" in cntry or "تركي" in cat:
            return "tv_turkish", defs_by_key.get("tv_turkish", {}).get("title", "مسلسلات تركية")
        if "عربي" in cat or any(c in cntry for c in ("مصر", "سوريا", "لبنان", "الكويت", "السعودية", "الإمارات", "الأردن")):
            return "tv_arab", defs_by_key.get("tv_arab", {}).get("title", "مسلسلات عربية")
        if "آسيوي" in cat or any(c in cntry for c in ("كوريا", "اليابان", "الصين")):
            return "tv_asian", defs_by_key.get("tv_asian", {}).get("title", "مسلسلات كورية وآسيوية")
        return "tv_foreign", defs_by_key.get("tv_foreign", {}).get("title", "مسلسلات أجنبية")
    else:
        if "العراق" in cntry or "عراقي" in cat or "عربي" in cat or any(c in cntry for c in ("مصر", "سوريا", "لبنان", "الكويت", "السعودية", "الإمارات")):
            return "movie_arab_iraqi", defs_by_key.get("movie_arab_iraqi", {}).get("title", "أفلام عراقية وعربية")
        if "تركيا" in cntry or "تركي" in cat:
            return "movie_turkish", defs_by_key.get("movie_turkish", {}).get("title", "أفلام تركية")
        if "الهند" in cntry or "هندي" in cat:
            return "movie_indian", defs_by_key.get("movie_indian", {}).get("title", "أفلام هندية")
        if "آسيوي" in cat or any(c in cntry for c in ("كوريا", "اليابان", "الصين")):
            return "movie_asian", defs_by_key.get("movie_asian", {}).get("title", "أفلام آسيوية وكورية")
        return "movie_foreign", defs_by_key.get("movie_foreign", {}).get("title", "أفلام أجنبية")


def get_smart_sections(only_non_empty: bool = True, items: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """
    يرجع قائمة الأقسام الذكية مع البوسترات المتقلبة وعدد الأعمال (مع دعم الكاش الفوري بالذاكرة).
    """
    cache_slot = "smart_sections_non_empty" if only_non_empty else "smart_sections_all"
    if _MEDIA_CACHE.get(cache_slot) is not None:
        return _MEDIA_CACHE[cache_slot]
    if items is None:
        all_items, _ = get_cached_media_bundle(include_hidden=False)
    else:
        all_items = [x for x in items if not x.get("is_hidden")]

    defs = get_all_section_defs(only_enabled=only_non_empty)
    newest_global_ids = {x["id"] for x in all_items[:10]}

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for it in all_items:
        skey = it.get("section_key") or ""
        if not skey:
            skey, _ = auto_detect_section_key(
                it.get("media_type", "movie"),
                it.get("country", ""),
                it.get("category_label", ""),
                it.get("genres", []),
                it.get("title", ""),
                section_defs=defs
            )
        grouped.setdefault(skey, []).append(it)

    output: List[Dict[str, Any]] = []
    for d in defs:
        skey = d["section_key"]
        sec_items = grouped.get(skey, [])
        cnt = len(sec_items)
        if only_non_empty and (cnt == 0 or not int(d.get("is_enabled", 1))):
            continue

        posters = [x["poster_url"] for x in sec_items if x.get("poster_url")][:8]
        backdrops = [x.get("backdrop_url") or x.get("poster_url") for x in sec_items if (x.get("backdrop_url") or x.get("poster_url"))][:8]
        latest_item = sec_items[0] if sec_items else None
        has_new = any(x["id"] in newest_global_ids for x in sec_items[:3]) if sec_items else False

        output.append({
            "section_key": skey,
            "title": d["title"],
            "subtitle": d.get("subtitle") or "",
            "media_type_filter": d.get("media_type_filter") or "all",
            "auto_country": d.get("auto_country") or "",
            "auto_category": d.get("auto_category") or "",
            "accent_color": d.get("accent_color") or "#10b981",
            "sort_order": int(d.get("sort_order") or 100),
            "is_enabled": bool(int(d.get("is_enabled", 1))),
            "items_count": cnt,
            "has_new": has_new,
            "latest_title": latest_item["title"] if latest_item else "",
            "latest_year": latest_item.get("year", "") if latest_item else "",
            "posters": posters,
            "backdrops": backdrops,
            "item_ids": [x["id"] for x in sec_items]
        })

    _MEDIA_CACHE[cache_slot] = output
    return output


def upsert_custom_section(
    title: str,
    subtitle: str = "",
    media_type_filter: str = "all",
    auto_country: str = "",
    auto_category: str = "",
    accent_color: str = "#10b981",
    sort_order: int = 45,
    section_key: str = ""
) -> Dict[str, Any]:
    clean_title = title.strip()
    if not section_key:
        slug = re.sub(r"[^a-zA-Z0-9_]+", "", clean_title.lower().replace(" ", "_"))
        if not slug or len(slug) < 3:
            slug = f"sec_{int(time.time())}"
        section_key = f"custom_{slug}"

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO custom_sections (
            section_key, title, subtitle, media_type_filter,
            auto_country, auto_category, accent_color, sort_order, is_enabled
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(section_key) DO UPDATE SET
            title = excluded.title,
            subtitle = excluded.subtitle,
            media_type_filter = excluded.media_type_filter,
            auto_country = excluded.auto_country,
            auto_category = excluded.auto_category,
            accent_color = excluded.accent_color,
            sort_order = excluded.sort_order
    """, (
        section_key, clean_title, subtitle.strip(), media_type_filter.strip() or "all",
        auto_country.strip(), auto_category.strip(), accent_color.strip() or "#10b981", int(sort_order or 45)
    ))
    conn.commit()
    row = cur.execute("SELECT * FROM custom_sections WHERE section_key = ?", (section_key,)).fetchone()
    conn.close()
    res = dict(row) if row else {"section_key": section_key, "title": clean_title}
    trigger_firebase_push(f"custom_sections/{section_key}", res)
    return res


def toggle_custom_section(section_key: str) -> Optional[bool]:
    invalidate_media_cache()
    conn = get_conn()
    cur = conn.cursor()
    row = cur.execute("SELECT is_enabled FROM custom_sections WHERE section_key = ?", (section_key,)).fetchone()
    if not row:
        conn.close()
        return None
    new_val = 0 if int(row["is_enabled"] or 0) == 1 else 1
    cur.execute("UPDATE custom_sections SET is_enabled = ? WHERE section_key = ?", (new_val, section_key))
    conn.commit()
    full_row = cur.execute("SELECT * FROM custom_sections WHERE section_key = ?", (section_key,)).fetchone()
    conn.close()
    if full_row:
        trigger_firebase_push(f"custom_sections/{section_key}", dict(full_row))
    return bool(new_val)


def delete_custom_section(section_key: str) -> bool:
    invalidate_media_cache()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM custom_sections WHERE section_key = ?", (section_key,))
    changed = cur.rowcount > 0
    cur.execute("UPDATE media SET section_key = '' WHERE section_key = ?", (section_key,))
    conn.commit()
    conn.close()
    if changed:
        trigger_firebase_push(f"custom_sections/{section_key}", None)
    return changed


def set_media_section(media_id: int, section_key: str) -> Optional[Dict[str, Any]]:
    update_media_metadata(int(media_id), section_key=str(section_key or "").strip())
    return get_media_by_id(int(media_id))


# =====================================================================
# توحيد وترتيب الدقات السبع (144, 240, 360, 480, 720, 1080, 4K) + نظام الأخبار التلقائي
# =====================================================================
QUALITY_RANK_MAP: Dict[str, int] = {
    "4K": 2160,
    "2160": 2160,
    "1080": 1080,
    "720": 720,
    "480": 480,
    "360": 360,
    "240": 240,
    "144": 144,
}


def normalize_quality_value(raw_q: str) -> str:
    s = str(raw_q or "").strip().upper().rstrip("P")
    if s in ("4K", "2160", "UHD"):
        return "4K"
    if s in ("1080", "720", "480", "360", "240", "144"):
        return s
    if s.isdigit():
        val = int(s)
        if val >= 1800:
            return "4K"
        if val >= 950:
            return "1080"
        if val >= 640:
            return "720"
        if val >= 420:
            return "480"
        if val >= 300:
            return "360"
        if val >= 200:
            return "240"
        return "144"
    return "720"


def quality_sort_key(raw_q: str) -> int:
    q = normalize_quality_value(raw_q)
    return QUALITY_RANK_MAP.get(q, 720)


def format_quality_badge(raw_q: str) -> str:
    q = normalize_quality_value(raw_q)
    return "4K" if q == "4K" else f"{q}p"



# =====================================================================
# محرك إعلانات فورتكس الخاص (اشتراكات القنوات/الكروبات الإجبارية + زيارات المواقع)
# =====================================================================
def get_ad_tasks(only_enabled: bool = True) -> List[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    try:
        if only_enabled:
            rows = cur.execute(
                "SELECT * FROM ad_tasks WHERE is_enabled = 1 ORDER BY sort_order ASC, id ASC"
            ).fetchall()
        else:
            rows = cur.execute(
                "SELECT * FROM ad_tasks ORDER BY is_enabled DESC, sort_order ASC, id DESC"
            ).fetchall()
        items = []
        for r in rows:
            d = dict(r)
            d["is_enabled"] = bool(int(d.get("is_enabled") or 0))
            d["clicks_count"] = int(d.get("clicks_count") or 0)
            d["max_completions"] = int(d.get("max_completions") or 0)
            d["timer_seconds"] = max(0, int(d.get("timer_seconds") or 10))
            items.append(d)
        return items
    except Exception:
        return []
    finally:
        conn.close()


def get_ad_task_by_id(task_id: int) -> Optional[Dict[str, Any]]:
    conn = get_conn()
    cur = conn.cursor()
    try:
        row = cur.execute("SELECT * FROM ad_tasks WHERE id = ?", (int(task_id),)).fetchone()
        if not row:
            return None
        d = dict(row)
        d["is_enabled"] = bool(int(d.get("is_enabled") or 0))
        d["clicks_count"] = int(d.get("clicks_count") or 0)
        d["max_completions"] = int(d.get("max_completions") or 0)
        d["timer_seconds"] = max(0, int(d.get("timer_seconds") or 10))
        return d
    finally:
        conn.close()


def add_ad_task(
    task_type: str,
    title: str,
    target_url: str,
    target_channel_id: str = "",
    timer_seconds: int = 10,
    max_completions: int = 0,
    sort_order: int = 10
) -> Dict[str, Any]:
    ttype = "channel" if str(task_type or "").strip().lower() == "channel" else "link"
    clean_title = str(title or "").strip() or ("اشتراك بقناة الراعي" if ttype == "channel" else "زيارة موقع الراعي")
    clean_url = str(target_url or "").strip()
    clean_ch = str(target_channel_id or "").strip()

    # استنتاج معرف القناة من الرابط إذا كان عاماً ولم يُمرر المعرف
    if ttype == "channel" and not clean_ch and "t.me/" in clean_url:
        tail = clean_url.split("t.me/", 1)[1].split("?")[0].strip("/")
        if tail and not tail.startswith("+") and not tail.startswith("joinchat") and "/" not in tail:
            clean_ch = "@" + tail.lstrip("@")
    if ttype == "channel" and clean_ch and not clean_url:
        if clean_ch.startswith("@"):
            clean_url = f"https://t.me/{clean_ch.lstrip('@')}"

    t_sec = max(3, min(120, int(timer_seconds or 10))) if ttype == "link" else 0
    max_c = max(0, int(max_completions or 0))

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO ad_tasks (
            task_type, title, target_channel_id, target_url,
            timer_seconds, clicks_count, max_completions, is_enabled, sort_order
        ) VALUES (?, ?, ?, ?, ?, 0, ?, 1, ?)
    """, (ttype, clean_title, clean_ch, clean_url, t_sec, max_c, int(sort_order or 10)))
    tid = cur.lastrowid
    conn.commit()
    conn.close()
    _push_ad_task_full(tid)
    return get_ad_task_by_id(tid) or {"id": tid, "title": clean_title}


def update_ad_task(
    task_id: int,
    title: Optional[str] = None,
    target_url: Optional[str] = None,
    target_channel_id: Optional[str] = None,
    timer_seconds: Optional[int] = None,
    max_completions: Optional[int] = None
) -> Optional[Dict[str, Any]]:
    existing = get_ad_task_by_id(task_id)
    if not existing:
        return None
    new_title = str(title).strip() if title is not None else existing["title"]
    new_url = str(target_url).strip() if target_url is not None else existing["target_url"]
    new_ch = str(target_channel_id).strip() if target_channel_id is not None else existing["target_channel_id"]
    new_timer = int(timer_seconds) if timer_seconds is not None else existing["timer_seconds"]
    new_max = int(max_completions) if max_completions is not None else existing["max_completions"]

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE ad_tasks
        SET title = ?, target_url = ?, target_channel_id = ?, timer_seconds = ?, max_completions = ?
        WHERE id = ?
    """, (new_title, new_url, new_ch, new_timer, new_max, int(task_id)))
    conn.commit()
    conn.close()
    _push_ad_task_full(int(task_id))
    return get_ad_task_by_id(int(task_id))


def toggle_ad_task(task_id: int) -> Optional[bool]:
    conn = get_conn()
    cur = conn.cursor()
    row = cur.execute("SELECT is_enabled FROM ad_tasks WHERE id = ?", (int(task_id),)).fetchone()
    if not row:
        conn.close()
        return None
    new_val = 0 if int(row["is_enabled"] or 0) == 1 else 1
    cur.execute("UPDATE ad_tasks SET is_enabled = ? WHERE id = ?", (new_val, int(task_id)))
    conn.commit()
    conn.close()
    _push_ad_task_full(int(task_id))
    return bool(new_val)


def delete_ad_task(task_id: int) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM ad_tasks WHERE id = ?", (int(task_id),))
    changed = cur.rowcount > 0
    cur.execute("DELETE FROM ad_task_completions WHERE task_id = ?", (int(task_id),))
    conn.commit()
    conn.close()
    if changed:
        trigger_firebase_push(f"ad_tasks/{int(task_id)}", None)
    return changed


def record_ad_task_completion(user_id: int, task_id: int, is_link_visit: bool = False) -> Dict[str, Any]:
    """
    يسجل إتمام المهمة الإعلانية للمستخدم ويزيد عداد الزيارات/الاشتراكات (clicks_count).
    - لمهام القنوات (channel): يحسب كل مستخدم مرة واحدة فقط في عداد القناة حتى لا يتكرر العداد عند كل فحص.
    - لمهام الروابط (link): يزيد العداد عند إتمام المؤقت ويحفظ طابع وقت الإتمام.
    - إذا وصل clicks_count إلى max_completions (> 0) يتم إيقاف المهمة تلقائياً لأنها حققت الهدف.
    """
    uid = int(user_id or 0)
    tid = int(task_id or 0)
    now_ts = int(time.time())
    conn = get_conn()
    cur = conn.cursor()
    try:
        task_row = cur.execute("SELECT * FROM ad_tasks WHERE id = ?", (tid,)).fetchone()
        if not task_row:
            return {"ok": False, "clicks_count": 0}
        existing = cur.execute(
            "SELECT completed_at FROM ad_task_completions WHERE user_id = ? AND task_id = ?",
            (uid, tid)
        ).fetchone()
        incremented = False
        if not existing:
            cur.execute(
                "INSERT OR REPLACE INTO ad_task_completions (user_id, task_id, completed_at) VALUES (?, ?, ?)",
                (uid, tid, now_ts)
            )
            cur.execute("UPDATE ad_tasks SET clicks_count = COALESCE(clicks_count, 0) + 1 WHERE id = ?", (tid,))
            incremented = True
        elif is_link_visit:
            # للروابط: إذا مر أكثر من 5 دقائق على آخر زيارة لنفس الرابط، نحدث الطابع ونزيد العداد
            last_ts = int(existing["completed_at"] or 0)
            cur.execute(
                "UPDATE ad_task_completions SET completed_at = ? WHERE user_id = ? AND task_id = ?",
                (now_ts, uid, tid)
            )
            if (now_ts - last_ts) >= 60:
                cur.execute("UPDATE ad_tasks SET clicks_count = COALESCE(clicks_count, 0) + 1 WHERE id = ?", (tid,))
                incremented = True

        updated = cur.execute("SELECT clicks_count, max_completions, is_enabled FROM ad_tasks WHERE id = ?", (tid,)).fetchone()
        clicks = int(updated["clicks_count"] or 0) if updated else 0
        max_c = int(updated["max_completions"] or 0) if updated else 0
        if max_c > 0 and clicks >= max_c and int(updated["is_enabled"] or 0) == 1:
            cur.execute("UPDATE ad_tasks SET is_enabled = 0 WHERE id = ?", (tid,))
            incremented = True
        conn.commit()
    finally:
        conn.close()

    if incremented:
        _push_ad_task_full(tid)
    return {"ok": True, "clicks_count": clicks, "incremented": incremented}


def has_recent_link_completion(user_id: int, task_id: int, max_age_seconds: int = 900) -> bool:
    """يتحقق ما إذا كان المستخدم قد أتم زيارة الرابط الإعلاني مؤخراً (خلال جلسة السحب الحالية)."""
    uid = int(user_id or 0)
    tid = int(task_id or 0)
    if uid <= 0 or tid <= 0:
        return False
    conn = get_conn()
    cur = conn.cursor()
    try:
        row = cur.execute(
            "SELECT completed_at FROM ad_task_completions WHERE user_id = ? AND task_id = ?",
            (uid, tid)
        ).fetchone()
        if not row:
            return False
        return (int(time.time()) - int(row["completed_at"] or 0)) <= max_age_seconds
    finally:
        conn.close()


async def sync_public_bridge_to_firebase(api_url: str, permanent_url: str = "") -> bool:
    """يرفع رابط النفق الحي (API) إلى عقدة عامة القراءة في Firebase (/public_bridge)
    ويضبط قواعد القراءة تلقائياً ليعمل الرابط الثابت للأبد بدون أي تدخل يدوي."""
    if not fb_enabled() or aiohttp is None:
        return False
    api_url = (api_url or "").strip().rstrip("/")
    permanent_url = (permanent_url or "").strip().rstrip("/")
    if not api_url.startswith("https://") or any(c in api_url for c in ('{', '}', '"', ' ')):
        return False
    if permanent_url and (not permanent_url.startswith("https://") or any(c in permanent_url for c in ('{', '}', '"', ' '))):
        permanent_url = ""
    try:
        timeout = aiohttp.ClientTimeout(total=12)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # 1. تحديث قواعد Firebase للسماح بقراءة public_bridge فقط للواجهة الثابتة
            if FIREBASE_AUTH:
                rules_payload = {
                    "rules": {
                        ".read": False,
                        ".write": False,
                        "public_bridge": {
                            ".read": True
                        }
                    }
                }
                rules_url = f"{FIREBASE_DATABASE_URL.rstrip('/')}/.settings/rules.json?auth={FIREBASE_AUTH}"
                try:
                    async with session.put(rules_url, json=rules_payload) as _:
                        pass
                except Exception:
                    pass

            # 2. حفظ عنوان النفق الحي في /public_bridge وفي /settings/live_api_url
            bridge_data = {
                "api_url": api_url,
                "permanent_url": permanent_url,
                "updated_at": int(time.time()),
                "updated_iso": _ts_now(),
            }
            await _fb_request(session, "PUT", "public_bridge", bridge_data)
            await _fb_request(session, "PUT", "settings/live_api_url", api_url)
            if permanent_url:
                await _fb_request(session, "PUT", "settings/permanent_webapp_url", permanent_url)
            return True
    except Exception as e:
        logger.warning(f"sync_public_bridge_to_firebase warning: {e}")
        return False
