import json
import gc
import os
import re
import html
import shutil
import asyncio
import logging
import aiohttp
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional, Tuple, List, Set

import time
import hmac
import hashlib
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    WebAppInfo, BotCommand, MenuButtonWebApp, LabeledPrice, PreCheckoutQuery,
    FSInputFile, BufferedInputFile, InputMediaDocument
)
from aiogram.filters import CommandStart, Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

from config import (
    MAIN_BOT_TOKEN, ADMIN_BOT_TOKEN, SENDER_BOT_TOKEN,
    DEFAULT_WEBAPP_URL, AUTO_DETECTED_PLATFORM_URL, DB_PATH, PORT, BASE_DIR,
    KEEP_ALIVE, KEEP_ALIVE_SECONDS, STRICT_AUTH, INITDATA_MAX_AGE,
    ADMIN_IDS, TEMP_DIR, APP_VERSION,
    is_replaceable_url, resolve_webapp_url, is_public_host, _norm_url, is_valid_http_url,
)
from tg_auth import validate_init_data, CURRENT_TG_USER
from database import (
    get_maintenance_config, classify_item_world, sync_public_bridge_to_firebase,
    init_db, get_conn, invalidate_media_cache, restore_from_firebase, firebase_status_line, firebase_health_check, firebase_compare,
    push_all_local_to_firebase, format_restore_report, firebase_flush, FB_STATS,
    get_setting, set_setting, get_brand_config, add_media, add_or_merge_media,
    find_existing_media, check_video_already_exists_globally, purge_and_merge_all_duplicates, get_incomplete_series_list, sweep_and_fulfill_all_pending_requests, add_files_to_existing_media,
    set_media_published_post, update_media_metadata, toggle_media_hidden, update_media_file_record,
    merge_two_media_items, get_all_media_files_with_titles,
    auto_group_similar_movies, get_all_collections, set_movies_collection, remove_movie_from_collection,
    get_smart_sections, get_all_section_defs, auto_detect_section_key,
    upsert_custom_section, toggle_custom_section, delete_custom_section, set_media_section,
    get_ad_tasks, get_ad_task_by_id, add_ad_task, update_ad_task, toggle_ad_task,
    delete_ad_task, record_ad_task_completion, has_recent_link_completion,
    normalize_quality_value, quality_sort_key, format_quality_badge,
    get_all_media, get_cached_media_bundle, get_media_by_id, get_file_by_id,
    set_tonight_media, delete_media, delete_single_media_file, search_media_by_title,
    toggle_user_list, get_user_lists, register_user, set_user_theme, reward_referrer_if_eligible,
    get_user_profile, get_leaderboard, set_user_leaderboard_channel,
    grant_vip_days, add_user_points, redeem_points_for_vip,
    get_all_user_ids, is_user_admin, add_admin_id, get_all_admins,
    pop_newly_registered_users, is_owner_id, has_admin_perm, PERM_KEYS, PERM_LABELS_AR,
    record_admin_action, check_and_record_admin_delete_guard, get_admin_record,
    get_all_admins_detailed, toggle_admin_permission, set_admin_frozen, remove_admin_id,
    log_content_pull, log_media_share, get_user_pulled_content, get_users_dashboard_data,
    schedule_mandatory_deletion, get_due_scheduled_deletions, remove_scheduled_deletion,
    db_checkpoint, db_remove_sidecars,
    get_stats, add_comment, get_user_request_quota, create_media_request,
    get_user_media_requests, get_pending_media_requests, mark_media_request_fulfilled,
    mark_media_request_rejected, auto_fulfill_matching_requests_for_media,
    create_media_report, get_pending_media_reports, resolve_media_report,
)
from tmdb_helper import search_tmdb, get_tmdb_details, get_tmdb_season_episodes, get_tmdb_episode_info, verify_series_episodes_with_tmdb

try:
    os.chdir(BASE_DIR)
except Exception:
    pass

# تفعيل دعم خاصية تلوين الأزرار الرسمية في Telegram Bot API 9.4 (style: danger | primary | success)
try:
    InlineKeyboardButton.model_config["extra"] = "allow"
    InlineKeyboardButton.model_rebuild(force=True)
    KeyboardButton.model_config["extra"] = "allow"
    KeyboardButton.model_rebuild(force=True)
except Exception:
    pass


def ikb(text: str, style: str = "", **kwargs) -> InlineKeyboardButton:
    """ينشئ زر Inline ملون (danger=أحمر للمهم، primary=أزرق للمرتب، success=أخضر للإعدادات)."""
    if style in ("danger", "primary", "success"):
        try:
            return InlineKeyboardButton(text=text, style=style, **kwargs)
        except Exception:
            pass
    return InlineKeyboardButton(text=text, **kwargs)


def rkb(text: str, style: str = "", **kwargs) -> KeyboardButton:
    """ينشئ زر كيبورد سفلي ملون (danger=أحمر، primary=أزرق، success=أخضر)."""
    if style in ("danger", "primary", "success"):
        try:
            return KeyboardButton(text=text, style=style, **kwargs)
        except Exception:
            pass
    return KeyboardButton(text=text, **kwargs)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("vortex")

main_bot = Bot(token=MAIN_BOT_TOKEN)
admin_bot = Bot(token=ADMIN_BOT_TOKEN)
sender_bot = Bot(token=SENDER_BOT_TOKEN)

main_dp = Dispatcher(storage=MemoryStorage())
admin_dp = Dispatcher(storage=MemoryStorage())
sender_dp = Dispatcher(storage=MemoryStorage())

main_router = Router()
admin_router = Router()
sender_router = Router()

main_dp.include_router(main_router)
admin_dp.include_router(admin_router)
sender_dp.include_router(sender_router)


@main_dp.error()
async def _main_bot_global_error_handler(event):
    logger.warning(f"MainBot handled error safely: {getattr(event, 'exception', event)}")
    return True


@admin_dp.error()
async def _admin_bot_global_error_handler(event):
    logger.warning(f"AdminBot handled error safely: {getattr(event, 'exception', event)}")
    return True


@sender_dp.error()
async def _sender_bot_global_error_handler(event):
    logger.warning(f"SenderBot handled error safely: {getattr(event, 'exception', event)}")
    return True

BOT_INFO: Dict[str, Any] = {
    "main_username": "Vortex1_app",
    "sender_username": "Vortexeserver_Bot",
    "admin_username": "VortexeAdmin_Bot",
    "main_id": 9073252184,
    "sender_id": 9245596135,
    "admin_id": 9124179798
}


def get_storage_channel_for_type(media_type: str) -> str:
    """يرجع قناة الخزن المخصصة لكل قسم (أفلام / مسلسلات / أنمي) مع بديل تلقائي."""
    mtype = (media_type or "movie").strip().lower()
    if mtype == "anime":
        return (
            get_setting("anime_channel_id", "").strip()
            or get_setting("series_channel_id", "").strip()
            or get_setting("movies_channel_id", "").strip()
        )
    if mtype == "tv":
        return (
            get_setting("series_channel_id", "").strip()
            or get_setting("movies_channel_id", "").strip()
            or get_setting("anime_channel_id", "").strip()
        )
    return (
        get_setting("movies_channel_id", "").strip()
        or get_setting("series_channel_id", "").strip()
        or get_setting("anime_channel_id", "").strip()
    )


def get_section_publish_channel_for_type(media_type: str) -> str:
    """يرجع قناة النشر المخصصة للقسم (قناة نشر الأفلام / قناة نشر المسلسلات / قناة نشر الأنمي)."""
    mtype = (media_type or "movie").strip().lower()
    if mtype == "anime":
        return get_setting("anime_publish_channel_id", "").strip()
    if mtype == "tv":
        return get_setting("series_publish_channel_id", "").strip()
    return get_setting("movies_publish_channel_id", "").strip()


async def get_or_create_dynamic_cover_file_id(chat_id_for_temp=None, media_details: Optional[Dict[str, Any]] = None) -> str:
    """
    يرجع معرف الغلاف الخاص الثابت المحفوظ على تيليجرام (video_cover_file_id) بصفر استهلاك للذاكرة (RAM).
    """
    if get_setting("cover_enabled", "1") != "1":
        return ""
    return get_setting("video_cover_file_id", "").strip()


def build_admin_reply_keyboard() -> ReplyKeyboardMarkup:
    """كيبورد سفلي دائم وملون لبوت الإدارة يغني عن كتابة أي أوامر نصية (/)."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                rkb("＋ إضافة عمل جديد", style="danger"),
                rkb("＋ إضافة حلقات / دقات", style="danger"),
            ],
            [
                rkb("⚡ الإضافة التلقائية والسحب", style="danger"),
            ],
            [
                rkb("◈ اللوحة الرئيسية", style="primary"),
                rkb("◈ الطلبات والبلاغات", style="primary"),
                rkb("✎ تعديل أو حذف", style="primary"),
            ],
            [
                rkb("❖ الإعدادات والقنوات", style="success"),
                rkb("▣ الغلاف والنسخ الاحتياطي", style="success"),
            ]
        ],
        resize_keyboard=True,
        is_persistent=True
    )


def build_main_bot_reply_keyboard(webapp_url: str, user=None) -> ReplyKeyboardMarkup:
    """كيبورد سفلي ملون ومرتب للبوت الرئيسي مع تمرير معرف المستخدم للويب آب."""
    from urllib.parse import quote
    app_name = get_setting("app_name_ar", "فورتكس")
    row1 = []
    if webapp_url.startswith("https://"):
        u_url = webapp_url
        if user and getattr(user, "id", 0):
            sep = "&" if "?" in u_url else "?"
            fn = quote(str(getattr(user, "first_name", "") or ""))
            un = quote(str(getattr(user, "username", "") or ""))
            u_url = f"{u_url}{sep}uid={user.id}&fn={fn}&un={un}"
        row1.append(rkb(f"◈ فتح تطبيق {app_name}", style="danger", web_app=WebAppInfo(url=u_url)))
    else:
        row1.append(rkb(f"◈ فتح تطبيق {app_name}", style="danger"))
    return ReplyKeyboardMarkup(
        keyboard=[
            row1,
            [
                rkb("◈ طلب فيلم / مسلسل / أنمي", style="primary"),
                rkb("▣ طلباتي وحالتها", style="primary"),
            ],
            [
                rkb("★ اشتراك PRO ونقاطي", style="success"),
                rkb("◈ قناتنا والدعم", style="success"),
            ]
        ],
        resize_keyboard=True,
        is_persistent=True
    )


def get_webapp_url() -> str:
    custom = get_setting("webapp_url", "").strip().rstrip("/")
    target = resolve_webapp_url(custom).rstrip("/")
    if target and target != custom:
        try:
            set_setting("webapp_url", target)
        except Exception:
            pass
    return target


def get_support_url() -> str:
    sup = get_setting("support_username", "").strip().lstrip("@")
    if sup:
        return sup if sup.startswith("http") else f"https://t.me/{sup}"
    return f"https://t.me/{BOT_INFO['main_username']}?start=support"


# =====================================================================
# النسخ الاحتياطي الذكي لقاعدة البيانات (تحديث نفس الرسالة بدل إغراق القناة)
# =====================================================================
async def send_auto_backup_to_storage(reason: str = "تحديث جديد"):
    storage_ch = get_setting("movies_channel_id", "") or get_setting("series_channel_id", "")
    if not storage_ch or not os.path.exists(DB_PATH):
        return
    try:
        chat_target = int(storage_ch) if re.match(r"^-?\d+$", str(storage_ch)) else storage_ch
        caption = (
            f"▣ <b>النسخة الاحتياطية المحدثة لقاعدة بيانات فورتكس</b>\n"
            f"✦ آخر عملية: {html.escape(reason)}\n"
            f"✦ الوقت: <code>{asyncio.get_event_loop().time():.0f}</code>\n"
            f"✦ كل النصوص والأيديات محفوظة تلقائياً بسحابة Firebase Realtime Database."
        )
        saved_msg_id = int(get_setting("backup_message_id", "0") or "0")
        saved_chat_id = get_setting("backup_chat_id", "").strip()

        db_checkpoint()
        doc = FSInputFile(DB_PATH, filename="vortex_backup.db")
        if saved_msg_id > 0 and str(saved_chat_id) == str(storage_ch):
            try:
                await admin_bot.edit_message_media(
                    chat_id=chat_target,
                    message_id=saved_msg_id,
                    media=InputMediaDocument(media=doc, caption=caption, parse_mode="HTML")
                )
                return
            except Exception as edit_err:
                logger.debug(f"Could not edit existing backup msg {saved_msg_id}, sending new: {edit_err}")

        sent = await admin_bot.send_document(
            chat_id=chat_target,
            document=FSInputFile(DB_PATH, filename="vortex_backup.db"),
            caption=caption,
            parse_mode="HTML"
        )
        set_setting("backup_message_id", str(sent.message_id))
        set_setting("backup_chat_id", str(storage_ch))
        try:
            await admin_bot.pin_chat_message(chat_id=chat_target, message_id=sent.message_id, disable_notification=True)
        except Exception:
            pass
    except Exception as e:
        logger.warning(f"Auto backup warning: {e}")


# =====================================================================
# فحص الاشتراك الإجباري الصارم (مع استثناء المشرفين تلقائياً)
# =====================================================================
async def ensure_fresh_invite_link(chat_target) -> str:
    saved_link = get_setting("force_sub_channel_link", "").strip()
    for bot_instance in (main_bot, admin_bot):
        try:
            chat_obj = await bot_instance.get_chat(chat_target)
            if chat_obj.username:
                fresh = f"https://t.me/{chat_obj.username}"
                set_setting("force_sub_channel_link", fresh)
                return fresh
            if chat_obj.invite_link:
                set_setting("force_sub_channel_link", chat_obj.invite_link)
                return chat_obj.invite_link
            created = await bot_instance.create_chat_invite_link(chat_target, name="Vortex Force Sub")
            if created and created.invite_link:
                set_setting("force_sub_channel_link", created.invite_link)
                return created.invite_link
        except Exception:
            continue
    return saved_link or f"https://t.me/{str(chat_target).lstrip('@')}"


def clean_and_normalize_channel_input(raw_input: str) -> Tuple[str, int]:
    """
    ينظف ويوحد أي مدخل لقناة تيليجرام (آيدي رقمي مع أو بدون -100، يوزر @، رابط قناة، أو رابط منشور t.me/c/.../msg_id):
    - يزيل علامات الاتجاه المخفية (RTL/LTR marks) والمسافات والاقتباسات.
    - يحول الأرقام العربية/الفارسية إلى أرقام إنجليزية.
    - يرجع (channel_target_str, extracted_msg_id).
    """
    if not raw_input:
        return "", 0
    s = str(raw_input)
    # تحويل الأرقام العربية والفارسية إلى إنجليزية
    trans_map = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹", "01234567890123456789")
    s = s.translate(trans_map)
    # إزالة رموز الاتجاه المخفية وعلامات التنصيص
    s = re.sub(r"[\u200b-\u200f\u202a-\u202e\u2060-\u206f\ufeff`'\"<>\[\]\(\)]", " ", s).strip()
    if not s:
        return "", 0

    # 1. فحص روابط القنوات الخاصة لمنشور: https://t.me/c/2134567890/55
    m_priv = re.search(r"(?:https?://)?t\.me/c/(\d{7,})(?:/(\d+))?", s, re.IGNORECASE)
    if m_priv:
        cid_digits = m_priv.group(1)
        mid_val = int(m_priv.group(2)) if m_priv.group(2) else 0
        return f"-100{cid_digits}", mid_val

    # 2. فحص روابط القنوات العامة: https://t.me/ChannelName/55 أو https://t.me/ChannelName
    m_pub = re.search(r"(?:https?://)?t\.me/([a-zA-Z0-9_]{4,})(?:/(\d+))?", s, re.IGNORECASE)
    if m_pub:
        uname_part = m_pub.group(1)
        mid_val = int(m_pub.group(2)) if m_pub.group(2) else 0
        if uname_part.lower() not in ("joinchat", "addstickers", "share", "c", "iv"):
            return "@" + uname_part.lstrip("@"), mid_val

    # 3. فحص وجود آيدي رقمي (مع أو بدون -100، حتى لو كان بجانبه مسافات بعد السالب)
    s_compact_num = re.sub(r"-\s+", "-", s.strip())
    m_num = re.search(r"(?<!\w)(-?\d{8,20})(?!\w)", s_compact_num)
    if m_num:
        num_str = m_num.group(1)
        if num_str.startswith("-100") and len(num_str) >= 11:
            return num_str, 0
        digits_only = num_str.lstrip("-")
        if digits_only.startswith("100") and len(digits_only) >= 12:
            return "-" + digits_only, 0
        if len(digits_only) >= 8:
            return "-100" + digits_only, 0

    # 4. فحص وجود يوزر @ChannelName
    m_at = re.search(r"@([a-zA-Z0-9_]{4,})", s)
    if m_at:
        return "@" + m_at.group(1), 0

    # 5. إذا كتب المستخدم يوزر القناة بدون @ (كلمة إنجليزية واحدة)
    first_tok = s.split()[0].strip() if s.split() else ""
    if re.match(r"^[a-zA-Z][a-zA-Z0-9_]{3,}$", first_tok):
        return "@" + first_tok, 0

    return "", 0


def normalize_chat_target(raw_ch: str):
    raw_ch = str(raw_ch or "").strip()
    if not raw_ch:
        return ""
    norm_str, _ = clean_and_normalize_channel_input(raw_ch)
    target = norm_str or raw_ch
    if re.match(r"^-?\d+$", target):
        return int(target)
    return target


_SUB_VERIFIED_CACHE: Dict[Tuple[int, str], Tuple[float, str, str, bool]] = {}
_SUB_CACHE_TTL = 45.0  # كاش سريع 45 ثانية لضمان استجابة فورية للميني آب


async def check_user_subscription(user_id: int, force_refresh: bool = False, allow_anonymous: bool = False) -> Tuple[bool, str, str]:
    force_ch = get_setting("force_sub_channel_id", "").strip()
    force_link = get_setting("force_sub_channel_link", "").strip()
    force_title = get_setting("force_sub_channel_title", "قناة فورتكس الرسمية").strip() or "قناة فورتكس الرسمية"

    if user_id and int(user_id) > 0 and is_user_admin(int(user_id)):
        return True, force_link, force_title

    if not force_ch:
        if user_id and int(user_id) > 0:
            asyncio.create_task(process_referral_reward_notification(int(user_id)))
        return True, "", ""

    # إذا لم يمرر عميل تيليجرام معرف المستخدم عند فتح الميني آب، لا نقفل الشاشة عليه أبداً
    if not user_id or int(user_id) <= 0:
        return (True if allow_anonymous else False), force_link, force_title

    chat_target = normalize_chat_target(force_ch)
    if isinstance(chat_target, str) and chat_target.startswith("@"):
        uname_clean = chat_target.lstrip("@")
        if not force_link or ("t.me/" in force_link and "+" not in force_link and "joinchat" not in force_link and not force_link.lower().rstrip("/").endswith(uname_clean.lower())):
            force_link = f"https://t.me/{uname_clean}"
    elif not force_link:
        force_link = await ensure_fresh_invite_link(chat_target)

    if len(_SUB_VERIFIED_CACHE) > 2000:
        _SUB_VERIFIED_CACHE.clear()

    cache_k = (int(user_id), str(chat_target))
    now_ts = time.time()
    if not force_refresh and cache_k in _SUB_VERIFIED_CACHE:
        cached_entry = _SUB_VERIFIED_CACHE[cache_k]
        ts_cached, lnk_cached, tit_cached = cached_entry[0], cached_entry[1], cached_entry[2]
        is_sub_cached = cached_entry[3] if len(cached_entry) > 3 else True
        # نحفظ في الكاش فقط حالة الاشتراك المؤكد (True) حتى لو اشترك المستخدم للتو يفتح له التطبيق بنفس اللحظة
        if is_sub_cached and (now_ts - ts_cached < 120.0):
            return True, lnk_cached or force_link, tit_cached or force_title

    explicitly_left = False
    for bot_instance in (main_bot, admin_bot, sender_bot):
        try:
            member = await asyncio.wait_for(
                bot_instance.get_chat_member(chat_id=chat_target, user_id=int(user_id)),
                timeout=2.8
            )
            status_val = getattr(member.status, "value", str(member.status)).lower()
            if status_val in ("member", "administrator", "creator") or (status_val == "restricted" and getattr(member, "is_member", True)):
                _SUB_VERIFIED_CACHE[cache_k] = (now_ts, force_link, force_title, True)
                asyncio.create_task(process_referral_reward_notification(int(user_id)))
                return True, force_link, force_title
            if status_val in ("left", "kicked"):
                explicitly_left = True
        except Exception as e:
            err_s = str(e).lower()
            if "user_not_participant" in err_s:
                explicitly_left = True
            continue

    if explicitly_left:
        return False, force_link, force_title

    # في حال لم يكن أي بوت مشرفاً في قناة الاشتراك الإجباري أو حدث بطء من تيليجرام، نفتح التطبيق تلقائياً بدلاً من تعليقه
    _SUB_VERIFIED_CACHE[cache_k] = (now_ts, force_link, force_title, True)
    return True, force_link, force_title


async def check_single_ad_channel(user_id: int, target_channel_id: str) -> bool:
    """يتحقق بشكل حي ومباشر في كل مرة من اشتراك المستخدم بقناة أو كروب الإعلان."""
    raw_ch = (target_channel_id or "").strip()
    if not raw_ch:
        return True
    if not user_id or int(user_id) <= 0:
        return False
    if is_user_admin(user_id):
        return True
    chat_target = normalize_chat_target(raw_ch)
    for b_inst in (main_bot, admin_bot, sender_bot):
        try:
            member = await b_inst.get_chat_member(chat_id=chat_target, user_id=int(user_id))
            st = getattr(member.status, "value", str(member.status)).lower()
            if st in ("creator", "administrator", "member"):
                return True
            if st == "restricted" and getattr(member, "is_member", False):
                return True
            return False
        except Exception as e:
            err_s = str(e).lower()
            if any(k in err_s for k in ("user not found", "participant_id_invalid", "user_not_participant")):
                return False
            continue
    return False


async def evaluate_user_ad_tasks(user_id: int) -> Dict[str, Any]:
    """يفحص جميع مهام الإعلانات النشطة (قنوات إجبارية لفتح الفيلم + روابط زيارة) للمستخدم بالوقت الفعلي."""
    prof = get_user_profile(user_id) if user_id else {}
    is_privileged = bool(prof.get("is_vip") or prof.get("is_admin") or (user_id and is_user_admin(user_id)))
    if is_privileged:
        return {
            "ads_enabled": False,
            "all_completed": True,
            "tasks": [],
            "pending_channels": [],
            "pending_links": []
        }

    active_tasks = get_ad_tasks(only_enabled=True)
    ads_enabled = (get_setting("ads_enabled", "1") == "1") or (len(active_tasks) > 0)
    if not ads_enabled:
        return {
            "ads_enabled": False,
            "all_completed": True,
            "tasks": [],
            "pending_channels": [],
            "pending_links": []
        }

    evaluated = []
    pending_channels = []
    pending_links = []

    for t in active_tasks:
        t_copy = dict(t)
        ttype = str(t.get("task_type") or "channel").lower()
        if ttype == "channel":
            ch_id_or_url = t.get("target_channel_id", "").strip() or t.get("target_url", "").strip()
            is_ok = await check_single_ad_channel(user_id, ch_id_or_url)
            if is_ok and user_id:
                res_c = record_ad_task_completion(user_id, int(t["id"]), is_link_visit=False)
                t_copy["clicks_count"] = res_c.get("clicks_count", t_copy.get("clicks_count", 0))
            t_copy["is_completed"] = is_ok
            if not is_ok:
                pending_channels.append(t_copy)
        else:
            is_ok = has_recent_link_completion(user_id, int(t["id"]), max_age_seconds=600)
            t_copy["is_completed"] = is_ok
            if not is_ok:
                pending_links.append(t_copy)
        evaluated.append(t_copy)

    all_done = all(x.get("is_completed") for x in evaluated)
    return {
        "ads_enabled": True,
        "all_completed": all_done,
        "tasks": evaluated,
        "pending_channels": pending_channels,
        "pending_links": pending_links
    }


def build_ad_channels_wall_keyboard(
    pending_channels: List[Dict[str, Any]],
    retry_cb: str,
    main_force_link: str = "",
    main_force_title: str = ""
) -> InlineKeyboardMarkup:
    rows = []
    if main_force_link:
        rows.append([ikb(text=f"▸ 1. اشترك في: {main_force_title or 'القناة الرسمية'}", style="danger", url=main_force_link)])
    for idx, ch in enumerate(pending_channels, start=(2 if main_force_link else 1)):
        url = (ch.get("target_url") or "").strip()
        if not url and ch.get("target_channel_id", "").startswith("@"):
            url = "https://t.me/" + ch["target_channel_id"].lstrip("@")
        if url:
            rows.append([ikb(text=f"▸ {idx}. اشترك في: {ch.get('title', 'قناة الراعي')}", style="danger", url=url)])
    rows.append([ikb(text="✓ اشتركت بالقنوات — أرسل الفيديو الآن", style="success", callback_data=retry_cb)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def process_referral_reward_notification(user_id: int):
    reward_info = reward_referrer_if_eligible(user_id)
    if reward_info:
        try:
            ref_id = reward_info["referrer_id"]
            pts = reward_info["points_added"]
            new_name = reward_info["new_user_name"]
            prof = get_user_profile(ref_id)
            await main_bot.send_message(
                chat_id=ref_id,
                text=(
                    f"✦ <b>عاشت إيدك! اجتك دعوة ناجحة جديدة</b>\n\n"
                    f"◈ دخل صاحبك: <b>{html.escape(str(new_name))}</b> من رابط دعوتك\n"
                    f"＋ ضفنالك: <b>{pts} نقطة</b> لرصيدك!\n"
                    f"❖ مجموع نقاطك هسة: <b>{prof['points']} نقطة</b> (عدد دعواتك: {prof['referrals_count']})"
                ),
                parse_mode="HTML"
            )
        except Exception:
            pass


async def notify_admins_of_new_users():
    """يرسل إشعار فوري للمشرفين عند دخول مستخدم جديد لأول مرة إذا كان الإشعار مفعّلاً."""
    new_list = pop_newly_registered_users()
    if not new_list:
        return
    if get_setting("notify_new_users", "1") != "1":
        return
    stats = get_stats()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="◈ قسم المستخدمين", callback_data="menu_users"),
            InlineKeyboardButton(text="○ إيقاف إشعار الجدد", callback_data="toggle_new_user_notify")
        ]
    ])
    src_ar = {"bot": "البوت الرئيسي (/start)", "webapp": "واجهة الميني آب", "sender": "بوت السيرفر"}
    for u in new_list:
        uname_str = f"@{u['username']}" if u.get("username") else "بدون يوزر"
        ref_line = f"\n✦ <b>دعوة من الأيدي:</b> <code>{u['referred_by']}</code>" if u.get("referred_by") else ""
        text = (
            f"◈ <b>دخول مستخدم جديد للبوت ✦</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"◈ <b>الاسم:</b> {html.escape(str(u.get('first_name') or 'مستخدم'))}\n"
            f"◈ <b>اليوزر:</b> {html.escape(uname_str)}\n"
            f"◈ <b>الأيدي:</b> <code>{u['user_id']}</code>\n"
            f"◈ <b>طريقة الدخول:</b> {src_ar.get(u.get('source', 'bot'), 'البوت')}"
            f"{ref_line}\n"
            f"━━━━━━━━━━━━━━\n"
            f" الجدد اليوم: <b>{stats.get('new_users_today', 1)}</b>  |  الإجمالي: <b>{stats.get('users', 1)}</b>"
        )
        for aid in get_all_admins():
            try:
                await admin_bot.send_message(chat_id=aid, text=text, reply_markup=kb, parse_mode="HTML")
            except Exception:
                pass


async def alert_owners_sabotage_attempt(offender_id: int, action_desc: str = "حذف متكرر"):
    """يرسل إنذار أمني فوري للمالك الأساسي عند تجميد مشرف حاول التخريب."""
    rec = get_admin_record(offender_id) or {}
    name = rec.get("first_name") or f"مشرف {offender_id}"
    uname = f" (@{rec['username']})" if rec.get("username") else ""
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✕ عزل المشرف نهائياً", callback_data=f"adm_remove_{offender_id}"),
            InlineKeyboardButton(text="✓ فك التجميد عنه", callback_data=f"adm_unfreeze_{offender_id}")
        ],
        [InlineKeyboardButton(text="⬡ فتح قسم المشرفين", callback_data="menu_admins")]
    ])
    text = (
        f"❖ <b>إنذار حماية البوت (Anti-Sabotage)</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"○ <b>تم تجميد صلاحيات المشرف تلقائياً!</b>\n"
        f"◈ <b>المشرف:</b> {html.escape(name)}{html.escape(uname)} (<code>{offender_id}</code>)\n"
        f"◈ <b>السبب:</b> محاولة {html.escape(action_desc)} لأكثر من 3 أعمال خلال 10 دقائق.\n\n"
        f"✦ البوت محمي ولم يتم تنفيذ عملية الحذف الأخيرة. اختار الإجراء من الأسفل:"
    )
    for owner_id in ADMIN_IDS:
        try:
            await admin_bot.send_message(chat_id=owner_id, text=text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            pass


def build_force_sub_keyboard(channel_link: str, callback_payload: str = "check_sub_main:") -> InlineKeyboardMarkup:
    url = channel_link if channel_link.startswith("http") else f"https://t.me/{channel_link.lstrip('@')}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [ikb(text="◈ اشترك الآن بقناة فورتكس الرسمية", style="danger", url=url)],
        [ikb(text="✓ اشتركت، تحقق هسة وافتح البوت", style="success", callback_data=callback_payload)]
    ])


# =====================================================================
# إشعار المستخدمين عند تلبية طلباتهم (مطابق للفيديو 100%)
# =====================================================================
async def notify_users_request_fulfilled(fulfilled_reqs: List[Dict[str, Any]], media_item: Optional[Dict[str, Any]] = None) -> int:
    """يرسل إشعاراً فورياً مع بوستر العمل وزر المشاهدة المباشر لكل مستخدم تم توفير طلبه تلقائياً."""
    if not fulfilled_reqs:
        return 0
    webapp_url = get_webapp_url()
    main_user = BOT_INFO["main_username"]
    support_url = get_support_url()
    notified_users = set()
    sent_count = 0

    for req in fulfilled_reqs:
        uid = int(req.get("user_id") or 0)
        if not uid or uid in notified_users:
            continue
        notified_users.add(uid)

        m_item = media_item
        if not m_item and req.get("fulfilled_media_id"):
            m_item = get_media_by_id(int(req["fulfilled_media_id"]))

        if m_item:
            title_display = f"{m_item['title']} ({m_item.get('year', '')})"
            mid = m_item["id"]
            kb_rows = []
            if webapp_url.startswith("https://"):
                kb_rows.append([ikb(text="◈ فتح ومشاهدة بالميني آب الآن", style="danger", web_app=WebAppInfo(url=f"{webapp_url}?media_id={mid}"))])
            kb_rows.append([ikb(text="📥 استلام ومشاهدة مباشرة بالبوت", style="primary", url=f"https://t.me/{main_user}?start=watch_{mid}")])
            kb_rows.append([ikb(text="◈ مراسلة الدعم", style="success", url=support_url)])
            kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
            text = (
                f"🎉 <b>تم توفير طلبك تلقائياً في فورتكس ✓</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"🎬 <b>العمل:</b> {html.escape(title_display)}\n"
                f"⭐ <b>التقييم:</b> {m_item.get('rating', 7.5)}  |  <b>القسم:</b> {html.escape(str(m_item.get('category_label') or 'فورتكس'))}\n"
                f"━━━━━━━━━━━━━━\n"
                f"✦ صار طلبك مكتمل ومتوفر هسة! اضغط على الزر بالأسفل للمشاهدة أو التحميل المباشر ▾"
            )
            poster_u = str(m_item.get("poster_url") or "").strip()
            try:
                if poster_u.startswith("http"):
                    await main_bot.send_photo(chat_id=uid, photo=poster_u, caption=text, reply_markup=kb, parse_mode="HTML")
                else:
                    await main_bot.send_message(chat_id=uid, text=text, reply_markup=kb, parse_mode="HTML")
                sent_count += 1
            except Exception:
                try:
                    await main_bot.send_message(chat_id=uid, text=text, reply_markup=kb, parse_mode="HTML")
                    sent_count += 1
                except Exception as e:
                    logger.debug(f"Could not notify requester {uid}: {e}")
        else:
            yr = f" ({req.get('prod_year')})" if req.get("prod_year") else ""
            title_display = f"{req.get('request_text', 'العمل المطلوب')}{yr}"
            watch_btn = InlineKeyboardButton(
                text="◈ شاهد الآن",
                web_app=WebAppInfo(url=webapp_url)
            ) if webapp_url.startswith("https://") else InlineKeyboardButton(
                text="◈ شاهد الآن",
                url=f"https://t.me/{main_user}"
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [watch_btn],
                [InlineKeyboardButton(text="◈ مراسلة الدعم", url=support_url)]
            ])
            text = (
                f"<b>تم نشر طلبك ✓</b>\n\n"
                f"◈ العمل: <b>{html.escape(title_display)}</b>\n"
                f"✦ صار متوفر هسة على تطبيق <b>فورتكس</b> وتكدر تفورتكسه وتحمله فوراً من الزر الجوة ▾"
            )
            try:
                await main_bot.send_message(chat_id=uid, text=text, reply_markup=kb, parse_mode="HTML")
                sent_count += 1
            except Exception as e:
                logger.debug(f"Could not notify requester {uid}: {e}")
    return sent_count


async def run_auto_sweep_pending_requests_and_notify() -> int:
    """يفحص كل الطلبات المعلقة ويكمل تلقائياً أي طلب متوفر عمله في المكتبة ويرسل إشعاراً فورياً للمستخدم."""
    try:
        pairs = sweep_and_fulfill_all_pending_requests()
        if not pairs:
            return 0
        total_sent = 0
        for req_d, m_item in pairs:
            total_sent += await notify_users_request_fulfilled([req_d], m_item)
        return total_sent
    except Exception as e:
        logger.debug(f"Auto sweep requests note: {e}")
        return 0


# =====================================================================
# النشر التلقائي الذكي في قناة العرض (أزرار الدقات + تحديث بدل التكرار + بديل النص)
# =====================================================================
def build_channel_post_caption_and_kb(media_item: Dict[str, Any]) -> Tuple[str, InlineKeyboardMarkup]:
    brand = get_brand_config()
    app_name_ar = brand["app_name_ar"]
    title = html.escape(str(media_item["title"]))
    orig_title = html.escape(str(media_item.get("original_title") or ""))
    year = html.escape(str(media_item.get("year") or ""))
    media_type = str(media_item.get("media_type") or "movie").lower()
    type_ar = "أنمي" if media_type == "anime" else ("فيلم" if media_type == "movie" else "مسلسل")
    type_icon = "✦" if media_type == "anime" else ("◈" if media_type == "movie" else "▣")
    genres = media_item.get("genres", [])
    overview_raw = str(media_item.get("overview", "") or "")
    rating = media_item.get("rating", 0.0)
    media_id = media_item["id"]
    files = media_item.get("files", [])

    qualities = sorted(list({normalize_quality_value(f["quality"]) for f in files}), key=quality_sort_key, reverse=True)
    qualities_str = " · ".join([format_quality_badge(q) for q in qualities]) if qualities else "720p"
    hashtags = " - ".join([f"#{g.replace(' ', '_')}" for g in genres]) if genres else f"#{app_name_ar} - #أفلام - #مسلسلات - #أنمي"

    header_lines = [f"{type_icon} <b>{type_ar}: {title} مترجم</b>"]
    if orig_title and orig_title.lower() != title.lower():
        header_lines.append(f"◈ <b>الاسم الأصلي:</b> {orig_title} ({year})")
    elif year:
        header_lines.append(f"◈ <b>سنة الإنتاج:</b> {year}")

    if rating:
        header_lines.append(f"★ <b>التقييم:</b> {rating}/10")

    col_info = media_item.get("collection_info")
    is_episodic = media_type in ("tv", "anime") and any(int(f.get("episode") or 0) > 0 for f in files)
    if is_episodic or media_type == "tv":
        seasons_set = {int(f.get("season") or 1) for f in files}
        eps_set = {(int(f.get("season") or 1), int(f.get("episode") or 0)) for f in files if int(f.get("episode") or 0) > 0}
        ep_count = len(eps_set) if eps_set else len(files)
        if len(seasons_set) > 1:
            header_lines.append(f"❖ <b>عدد المواسم:</b> {len(seasons_set)}  |  <b>عدد الحلقات:</b> {ep_count}")
        else:
            header_lines.append(f"❖ <b>عدد الحلقات المتوفرة:</b> {ep_count}")
        header_lines.append(f"◈ <b>الدقات المتوفرة:</b> {qualities_str}")
    else:
        header_lines.append(f"◈ <b>الدقات المتوفرة:</b> {qualities_str}")
        if col_info and col_info.get("parts_count", 0) >= 2:
            c_status = "متوفرة كاملة بالتطبيق" if col_info.get("is_active") else f"متوفر {col_info['parts_count']} أجزاء"
            header_lines.append(f"❖ <b>ضمن:</b> {html.escape(col_info['name'])} (الجزء {col_info['part_number']} — {c_status})")

    header_lines.append("")
    header_lines.append(hashtags)
    header_lines.append("")

    footer_text = f"\n\n<b>◈ للمشاهدة والتحميل عبر {html.escape(app_name_ar)} اختر من الأزرار أدناه ▾</b>"
    base_len = len("\n".join(header_lines)) + len(footer_text)
    max_overview_len = max(80, 950 - base_len)
    trimmed_overview = html.escape(overview_raw[:max_overview_len] + ("..." if len(overview_raw) > max_overview_len else ""))

    caption = "\n".join(header_lines) + trimmed_overview + footer_text
    if len(caption) > 1020:
        caption = caption[:1015] + "..."

    main_user = BOT_INFO["main_username"]
    sender_user = BOT_INFO["sender_username"]
    btn_mode = get_setting("channel_post_btn_mode", "webapp")

    kb_rows = [
        [ikb(
            text=f"🎬 شاهد وحمّل {type_ar} في تطبيق {app_name_ar} ▸",
            style="danger",
            url=f"https://t.me/{main_user}?start=watch_{media_id}"
        )]
    ]
    if col_info and col_info.get("is_active"):
        kb_rows.append([ikb(
            text=f"❖ {col_info['name']} ({col_info['parts_count']} أجزاء بالتطبيق)",
            style="primary",
            url=f"https://t.me/{main_user}?start=watch_{media_id}"
        )])

    pub_gen = get_setting("force_sub_channel_link", "").strip()
    if pub_gen and pub_gen.startswith("http"):
        kb_rows.append([
            ikb(text=f"◈ قناة {app_name_ar}", style="success", url=pub_gen),
            ikb(text="↺ تحديث الأزرار", style="success", callback_data=f"refresh_post_{media_id}")
        ])
    else:
        kb_rows.append([ikb(text="↺ تحديث الرابط والأزرار", style="success", callback_data=f"refresh_post_{media_id}")])
    return caption, InlineKeyboardMarkup(inline_keyboard=kb_rows)


async def _publish_or_edit_in_target_channel(
    raw_channel: str,
    prev_chat_id: str,
    prev_msg_id: int,
    poster: str,
    caption: str,
    kb: InlineKeyboardMarkup
) -> Tuple[bool, str, int, str]:
    chat_target = normalize_chat_target(raw_channel)
    if prev_msg_id > 0 and (not prev_chat_id or str(prev_chat_id) == str(raw_channel) or str(prev_chat_id) == str(chat_target)):
        for bot_inst in (main_bot, admin_bot):
            try:
                await bot_inst.edit_message_caption(
                    chat_id=chat_target,
                    message_id=prev_msg_id,
                    caption=caption,
                    reply_markup=kb,
                    parse_mode="HTML"
                )
                return True, f"تحديث (#{prev_msg_id})", prev_msg_id, str(chat_target)
            except Exception as e_cap:
                if "message is not modified" in str(e_cap).lower():
                    return True, f"محدث (#{prev_msg_id})", prev_msg_id, str(chat_target)
                try:
                    await bot_inst.edit_message_text(
                        chat_id=chat_target,
                        message_id=prev_msg_id,
                        text=caption,
                        reply_markup=kb,
                        parse_mode="HTML"
                    )
                    return True, f"تحديث (#{prev_msg_id})", prev_msg_id, str(chat_target)
                except Exception:
                    continue

    last_err = ""
    for bot_name, bot_inst in (("البوت الرئيسي", main_bot), ("بوت الإدارة", admin_bot)):
        if poster and poster.startswith("http"):
            try:
                sent = await bot_inst.send_photo(
                    chat_id=chat_target,
                    photo=poster,
                    caption=caption,
                    reply_markup=kb,
                    parse_mode="HTML"
                )
                return True, f"نُشر (#{sent.message_id})", sent.message_id, str(chat_target)
            except Exception as photo_err:
                last_err = f"{bot_name}: {photo_err}"
        try:
            sent = await bot_inst.send_message(
                chat_id=chat_target,
                text=caption,
                reply_markup=kb,
                parse_mode="HTML"
            )
            return True, f"نُشر نصياً (#{sent.message_id})", sent.message_id, str(chat_target)
        except Exception as txt_err:
            last_err = f"{bot_name}: {txt_err}"

    return False, f"فشل ({last_err[:80]})", 0, str(chat_target)


async def publish_to_updates_channel(media_id: int, new_episode_notice: Optional[int] = None) -> Tuple[bool, str]:
    media_item = get_media_by_id(media_id)
    if not media_item:
        return False, "العمل غير موجود بقاعدة البيانات"

    pub_ch = get_setting("publish_channel_id", "").strip()
    mtype = str(media_item.get("media_type") or "movie").lower()
    sec_pub_ch = get_section_publish_channel_for_type(mtype)

    if not pub_ch and not sec_pub_ch:
        return False, "قنوات النشر التلقائي غير مربوطة"

    caption, kb = build_channel_post_caption_and_kb(media_item)
    poster = (media_item.get("poster_url") or "").strip()

    reports = []
    any_ok = False
    new_main_chat = str(media_item.get("published_chat_id") or "")
    new_main_msg = int(media_item.get("published_msg_id") or 0)
    new_sec_chat = str(media_item.get("section_pub_chat_id") or "")
    new_sec_msg = int(media_item.get("section_pub_msg_id") or 0)

    if pub_ch:
        ok1, note1, msg1, chat1 = await _publish_or_edit_in_target_channel(
            pub_ch, new_main_chat, new_main_msg, poster, caption, kb
        )
        if ok1:
            any_ok = True
            new_main_chat, new_main_msg = chat1, msg1
        reports.append(f"العامة: {note1}")

    if sec_pub_ch and str(normalize_chat_target(sec_pub_ch)) != str(normalize_chat_target(pub_ch or "")):
        sec_label = "الأنمي" if mtype == "anime" else ("المسلسلات" if mtype == "tv" else "الأفلام")
        ok2, note2, msg2, chat2 = await _publish_or_edit_in_target_channel(
            sec_pub_ch, new_sec_chat, new_sec_msg, poster, caption, kb
        )
        if ok2:
            any_ok = True
            new_sec_chat, new_sec_msg = chat2, msg2
        reports.append(f"قناة {sec_label}: {note2}")

    if any_ok:
        set_media_published_post(media_id, new_main_chat, new_main_msg, new_sec_chat, new_sec_msg)

    return any_ok, " | ".join(reports)


@main_router.callback_query(F.data.startswith("refresh_post_"))
async def cb_refresh_channel_post(call: CallbackQuery):
    try:
        media_id = int(call.data.split("_")[2])
        media_item = get_media_by_id(media_id)
        if not media_item:
            await call.answer("◈ هذا العمل ما متوفر حالياً.", show_alert=True)
            return
        caption, kb = build_channel_post_caption_and_kb(media_item)
        try:
            await call.message.edit_caption(caption=caption, reply_markup=kb, parse_mode="HTML")
        except Exception:
            await call.message.edit_reply_markup(reply_markup=kb)
        await call.answer("✓ تحدث المنشور والأزرار بنجاح!", show_alert=True)
    except Exception:
        await call.answer("✓ الرابط شغال ومحدث! دوس على زر المشاهدة.", show_alert=True)


# =====================================================================
# 1. البوت الرئيسي (@Vortexee_Bot)
# =====================================================================
_NOTIFIED_PENDING_REF: set = set()


@main_router.message(CommandStart())
async def main_start_handler(message: Message, command: CommandObject):
    user = message.from_user
    args = (command.args or "").strip()

    referred_by = 0
    if args.startswith("ref_"):
        ref_part = args.replace("ref_", "").strip()
        if ref_part.isdigit():
            referred_by = int(ref_part)

    new_ref_attached = register_user(user.id, user.first_name or "", user.username or "", referred_by=referred_by, source="bot")
    asyncio.create_task(notify_admins_of_new_users())

    is_sub, ch_link, ch_title = await check_user_subscription(user.id)
    if not is_sub:
        # إذا دخل عن طريق دعوة ولم يشترك بالقناة بعد، نرسل تنبيهاً للداعي بأن النقاط معلقة لحين اشتراك صديقه بالقناة
        if new_ref_attached and (user.id not in _NOTIFIED_PENDING_REF):
            _NOTIFIED_PENDING_REF.add(user.id)
            try:
                await main_bot.send_message(
                    chat_id=int(new_ref_attached),
                    text=(
                        f"◷ <b>دخل صديقك «{html.escape(user.first_name or 'صديق جديد')}» من رابط دعوتك!</b>\n\n"
                        f"◈ <b>ملاحظة:</b> لا تُحتسب الدعوة ولا تُضاف النقاط إلا <b>بعد أن يشترك صديقك بقناة البوت الرسمية</b> ويضغط زر التحقق ✦"
                    ),
                    parse_mode="HTML"
                )
            except Exception:
                pass

        cb_data = f"check_sub_main:{args}"
        await message.answer(
            "<b>نورت بوت فورتكس ✦</b>\n\n"
            "لازم قبل ما تشغل البوت تشترك بقناتنا الرسمية أولاً وبعدها تدوس <b>«✓ اشتركت، تحقق هسة»</b> علمود يفتحلك التطبيق وتنحسب الدعوة ▾",
            reply_markup=build_force_sub_keyboard(ch_link, cb_data),
            parse_mode="HTML"
        )
        return

    await send_main_welcome(message, args)


async def send_main_welcome(message: Message, args: str = "", user_id: Optional[int] = None):
    uid = user_id or (message.from_user.id if message.from_user else 0)
    webapp_url = get_webapp_url()
    brand = get_brand_config()
    app_name = brand["app_name_ar"]

    if args == "support":
        sup_url = get_support_url()
        await message.answer(
            f"◈ <b>الدعم الفني لتطبيق {html.escape(app_name)}</b>\n\n"
            "✦ إذا واجهتك أي مشكلة بفيلم أو مسلسل أو أنمي تكدر تبلغ عنها مباشرة من زر البلاغ داخل صفحة العمل بالتطبيق، أو تراسل الدعم من الزر أدناه:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [ikb(text="◈ مراسلة الدعم الفني", style="success", url=sup_url)],
                [ikb(text=f"◈ فتح تطبيق {app_name}", style="danger", web_app=WebAppInfo(url=webapp_url))]
            ]),
            parse_mode="HTML"
        )
        return

    if args.startswith("watch_"):
        parts = args.split("_")
        media_id = parts[1] if len(parts) > 1 else ""
        media_item = get_media_by_id(int(media_id)) if media_id.isdigit() else None
        if not media_item:
            await message.answer("◈ عذراً، هذا العمل غير متوفر حالياً.")
            return

        mtype = str(media_item.get("media_type") or "movie").lower()
        requested_q = parts[2] if len(parts) > 2 and parts[2].isdigit() else ""
        target_url = f"{webapp_url}?media_id={media_id}&uid={uid}" + (f"&quality={requested_q}" if requested_q else "")
        title_str = html.escape(str(media_item["title"]))
        type_lbl = "أنمي" if mtype == "anime" else ("مسلسل" if mtype == "tv" else "فيلم")

        kb_rows = [
            [ikb(
                text=f"🎬 اضغط لمشاهدة وتحميل {type_lbl} في تطبيق {app_name} ▸",
                style="danger",
                web_app=WebAppInfo(url=target_url)
            )],
            [ikb(
                text=f"🏠 الصفحة الرئيسية لتطبيق {app_name}",
                style="primary",
                web_app=WebAppInfo(url=f"{webapp_url}?uid={uid}")
            )]
        ]
        caption_txt = (
            f"◈ <b>{type_lbl} {title_str} ({media_item.get('year', '')})</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"★ التقييم: <b>{media_item.get('rating', 0)}/10</b> · {html.escape(media_item.get('country', ''))}\n"
            f"✦ <b>اضغط على الزر بالأسفل لفتح العمل مباشرة داخل تطبيق {html.escape(app_name)} واختيار الدقة أو الحلقة ▾</b>"
        )
        poster = (media_item.get("poster_url") or "").strip()
        if poster and poster.startswith("http"):
            try:
                await message.answer_photo(photo=poster, caption=caption_txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="HTML")
                return
            except Exception:
                pass
        await message.answer(caption_txt, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="HTML")
        return

    ch_link = get_setting("force_sub_channel_link", "https://t.me/Vortexeo").strip() or "https://t.me/Vortexeo"
    sup_url = get_support_url()

    kb_rows = [
        [ikb(text=f"◈ فـتـح تـطـبـيـق {app_name} ({brand['app_name_en']})", style="danger", web_app=WebAppInfo(url=f"{webapp_url}?uid={uid}"))],
        [
            ikb(text="◈ طلب فيلم / مسلسل / أنمي", style="primary", web_app=WebAppInfo(url=f"{webapp_url}?open_requests=1&uid={uid}")),
            ikb(text="▣ طلباتي وحالتها", style="primary", callback_data="main_my_reqs")
        ],
        [
            ikb(text=f"★ اشتراك {brand['app_name_en']} PRO", style="primary", callback_data="main_vip_info"),
            ikb(text="❖ نقاطي ورابط الدعوة", style="primary", callback_data="main_my_points")
        ],
        [
            ikb(text=f"◈ قناة {app_name} العامة", style="success", url=ch_link),
            ikb(text="◈ الدعم الفني", style="success", url=sup_url)
        ]
    ]
    if uid and is_user_admin(uid):
        kb_rows.append([ikb(text=f"◈ لوحة إدارة {app_name} بالميني آب", style="danger", web_app=WebAppInfo(url=f"{webapp_url}?open_admin=1&uid={uid}"))])

    text = (
        f"◈ <b>أهلاً بك في عالم {html.escape(app_name)} ({html.escape(brand['app_name_en'])}) ✦</b>\n\n"
        f"◈ <b>الأقسام المتوفرة:</b> ◈ أفلام  |  ▣ مسلسلات  |  ✦ أنمي\n"
        f"✦ تصفح المكتبة الكاملة بواجهة سينمائية سريعة، اختر العمل والدقة اللي تناسبك ويوصلك الملف فوراً بالمحادثة!\n\n"
        f"▾ <b>اضغط على الزر الأحمر بالأسفل لفتح التطبيق:</b>"
    )
    await message.answer(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows),
        parse_mode="HTML"
    )
    try:
        await message.answer(
            "⌨ تم تفعيل أزرار الوصول السريع أسفل الشاشة:",
            reply_markup=build_main_bot_reply_keyboard(webapp_url, user=message.from_user)
        )
    except Exception:
        pass


@main_router.callback_query(F.data == "main_my_reqs")
async def cb_main_my_reqs(call: CallbackQuery):
    uid = call.from_user.id
    reqs = get_user_media_requests(uid, 10)
    webapp_url = get_webapp_url()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [ikb(text="◈ إرسال طلب جديد بالتطبيق", style="danger", web_app=WebAppInfo(url=f"{webapp_url}?open_requests=1"))]
    ])
    if not reqs:
        await call.message.answer(
            "▣ <b>قائمة طلباتك فارغة حالياً.</b>\n"
            "تگدر تطلب أي فيلم أو مسلسل أو أنمي من الزر الجوة ويوصلك إشعار أول ما ينزل!",
            reply_markup=kb,
            parse_mode="HTML"
        )
    else:
        lines = ["▣ <b>آخر طلباتك وحالتها:</b>\n"]
        for r in reqs:
            yr = f" ({r['prod_year']})" if r.get("prod_year") else ""
            lines.append(f"• <b>{html.escape(r['request_text'])}{yr}</b> — {r['status_ar']}")
        await call.message.answer("\n".join(lines), reply_markup=kb, parse_mode="HTML")
    await call.answer()


@main_router.callback_query(F.data == "main_my_points")
async def cb_main_my_points(call: CallbackQuery):
    uid = call.from_user.id
    prof = get_user_profile(uid)
    ref_pts = get_setting("referral_points", "20")
    vip_pts = get_setting("vip_points_price", "100")
    ref_link = f"https://t.me/{BOT_INFO['main_username']}?start=ref_{uid}"
    txt = (
        f"❖ <b>رصيد نقاطك ودعواتك في {html.escape(get_setting('app_name_ar', 'فورتكس'))}</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"◈ نقاطك الحالية: <b>{prof.get('points', 0)} نقطة</b>\n"
        f"◈ الأصدقاء المدعوين: <b>{prof.get('referrals_count', 0)} صديق</b>\n"
        f"✦ تربح <b>{ref_pts} نقطة</b> عن كل صديق يدخل البوت من رابطك ويشترك بالقناة!\n"
        f"✦ عند جمع <b>{vip_pts} نقطة</b> تگدر تستبدلها باشتراك <b>PRO مجاني لمدة 30 يوم</b>.\n\n"
        f"⟐ <b>رابط دعوتك الخاص:</b>\n<code>{ref_link}</code>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [ikb(text=f"↺ استبدال {vip_pts} نقطة باشتراك PRO", style="success", callback_data="main_redeem_pts")],
        [ikb(text="◈ فتح التطبيق", style="danger", web_app=WebAppInfo(url=get_webapp_url()))]
    ])
    await call.message.answer(txt, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@main_router.callback_query(F.data == "main_redeem_pts")
async def cb_main_redeem_pts(call: CallbackQuery):
    ok, msg = redeem_points_for_vip(call.from_user.id)
    await call.answer(msg, show_alert=True)


@main_router.callback_query(F.data == "main_vip_info")
async def cb_main_vip_info(call: CallbackQuery):
    uid = call.from_user.id
    prof = get_user_profile(uid)
    stars_price = int(get_setting("vip_stars_price", "50") or "50")
    pts_price = int(get_setting("vip_points_price", "100") or "100")
    status_line = f"فعّال ✓ (متبقي {prof.get('vip_days_left', 0)} يوم)" if prof.get("is_vip") else "غير مشترك حالياً"
    txt = (
        f"★ <b>اشتراك {html.escape(get_setting('app_name_en', 'VORTEX'))} PRO</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"◈ حالتك الحالية: <b>{status_line}</b>\n\n"
        f"✦ <b>مميزات اشتراك PRO:</b>\n"
        f"• استلام وتحميل جميع الأفلام والمسلسلات والأنمي فوراً بدون أي إعلانات أو مؤقت انتظار.\n"
        f"• أولوية قصوى في تلبية طلبات الأفلام والمسلسلات والأنمي.\n"
        f"• شارة PRO الذهبية في حسابك والتعليقات.\n\n"
        f" <b>طرق التفعيل الفوري (30 يوم):</b>\n"
        f"1. عبر نجوم تيليجرام: <b>{stars_price} نجمة ★</b>\n"
        f"2. عبر نقاط الدعوة المجانية: <b>{pts_price} نقطة ❖</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [ikb(text=f"★ التفعيل بـ {stars_price} نجمة تيليجرام", style="danger", callback_data="main_buy_stars")],
        [ikb(text=f"❖ التفعيل بـ {pts_price} نقطة مجانية", style="success", callback_data="main_redeem_pts")],
    ])
    await call.message.answer(txt, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@main_router.callback_query(F.data == "main_buy_stars")
async def cb_main_buy_stars(call: CallbackQuery):
    stars_price = int(get_setting("vip_stars_price", "50") or "50")
    app_en = get_setting("app_name_en", "VORTEX")
    try:
        invoice_link = await main_bot.create_invoice_link(
            title=f"اشتراك {app_en} PRO (30 يوم)",
            description="مشاهدة وتحميل كل الأفلام والمسلسلات والأنمي مباشرة بدون إعلانات أو انتظار لمدة 30 يوم.",
            payload=f"vip_stars_{call.from_user.id}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label=f"اشتراك {app_en} PRO", amount=stars_price)]
        )
        await call.message.answer(
            f"★ اضغط على الزر أدناه لإتمام الدفع وتفعيل <b>{html.escape(app_en)} PRO</b> فوراً:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [ikb(text=f" دفع {stars_price} نجمة وتفعيل PRO", style="danger", url=invoice_link)]
            ]),
            parse_mode="HTML"
        )
    except Exception as e:
        await call.answer(f"تعذر إنشاء الفاتورة: {e}", show_alert=True)
        return
    await call.answer()


@main_router.message(F.text.in_({"◈ طلب فيلم / مسلسل / أنمي", "▣ طلباتي وحالتها", "★ اشتراك PRO ونقاطي", "◈ قناتنا والدعم"}))
async def main_reply_kb_handler(message: Message):
    txt = (message.text or "").strip()
    webapp_url = get_webapp_url()
    if "طلب فيلم" in txt:
        await message.answer(
            "◈ اضغط على الزر أدناه لفتح صفحة الطلبات وإرسال اسم الفيلم أو المسلسل أو الأنمي:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [ikb(text="◈ فتح صفحة الطلبات", style="danger", web_app=WebAppInfo(url=f"{webapp_url}?open_requests=1"))]
            ])
        )
    elif "طلباتي" in txt:
        await main_requests_cmd(message)
    elif "PRO" in txt:
        uid = message.from_user.id
        prof = get_user_profile(uid)
        ref_link = f"https://t.me/{BOT_INFO['main_username']}?start=ref_{uid}"
        await message.answer(
            f"★ <b>حسابك في فورتكس</b>\n"
            f"◈ النقاط: <b>{prof.get('points', 0)}</b>  |  حالة PRO: <b>{'فعّال ✓' if prof.get('is_vip') else 'عادي'}</b>\n"
            f"⟐ رابط دعوتك: <code>{ref_link}</code>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [ikb(text="★ تفاصيل وتفعيل PRO", style="danger", callback_data="main_vip_info")],
                [ikb(text="❖ تفاصيل نقاطي والدعوة", style="primary", callback_data="main_my_points")]
            ]),
            parse_mode="HTML"
        )
    else:
        ch_link = get_setting("force_sub_channel_link", "https://t.me/Vortexeo").strip() or "https://t.me/Vortexeo"
        sup_url = get_support_url()
        await message.answer(
            "◈ <b>روابط فورتكس الرسمية والدعم الفني:</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [ikb(text="◈ قناة فورتكس العامة", style="success", url=ch_link)],
                [ikb(text="◈ مراسلة الدعم الفني", style="primary", url=sup_url)]
            ]),
            parse_mode="HTML"
        )


@main_router.message(Command("requests"))
async def main_requests_cmd(message: Message):
    uid = message.from_user.id
    reqs = get_user_media_requests(uid, 10)
    webapp_url = get_webapp_url()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❖ فتح صفحة الطلبات بالتطبيق", web_app=WebAppInfo(url=f"{webapp_url}?open_requests=1"))]
    ])
    if not reqs:
        await message.answer(
            "○ <b>قائمة طلباتك فارغة حالياً.</b>\n"
            "تكدر تطلب أي فيلم أو مسلسل من داخل التطبيق وراح يوصلك إشعار أول ما ينزل!",
            reply_markup=kb,
            parse_mode="HTML"
        )
        return
    lines = ["▣ <b>آخر طلباتك وحالتها:</b>\n"]
    for r in reqs:
        yr = f" ({r['prod_year']})" if r.get("prod_year") else ""
        lines.append(f"✦ <b>{html.escape(r['request_text'])}{yr}</b> — {r['status_ar']}")
    await message.answer("\n".join(lines), reply_markup=kb, parse_mode="HTML")


@main_router.callback_query(F.data.startswith("check_sub_main:"))
async def cb_check_sub_main(call: CallbackQuery):
    args = call.data.split(":", 1)[1] if ":" in call.data else ""
    is_sub, _, _ = await check_user_subscription(call.from_user.id, force_refresh=True)
    if not is_sub:
        await call.answer("◈ بعدك ما مشترك بالقناة! دوس على «اشترك الان» وانضم للقناة وبعدين دوس تحقق.", show_alert=True)
        return
    await call.answer("✓ عاشت إيدك! تم التحقق من اشتراكك.")
    try:
        await call.message.delete()
    except Exception:
        pass
    await send_main_welcome(call.message, args, user_id=call.from_user.id)




def _group_series_episodes_for_bot(files: List[Dict[str, Any]]) -> Dict[int, Dict[int, List[Dict[str, Any]]]]:
    by_s: Dict[int, Dict[int, List[Dict[str, Any]]]] = {}
    for f in (files or []):
        s_num = int(f.get("season") or 1)
        e_num = int(f.get("episode") or 1)
        by_s.setdefault(s_num, {}).setdefault(e_num, []).append(f)
    for s_num in by_s:
        for e_num in by_s[s_num]:
            by_s[s_num][e_num].sort(key=lambda x: quality_sort_key(x.get("quality", "720")), reverse=True)
    return by_s


def build_bot_series_episodes_view(media_item: Dict[str, Any], season: int = 1, page: int = 0) -> Tuple[str, InlineKeyboardMarkup]:
    mid = media_item["id"]
    files = media_item.get("files") or []
    by_s = _group_series_episodes_for_bot(files)
    seasons = sorted(by_s.keys()) or [1]
    if season not in by_s and seasons:
        season = seasons[0]

    eps_map = by_s.get(season, {})
    ep_nums = sorted(eps_map.keys())
    total_eps = len(ep_nums)

    per_page = 24
    total_pages = max(1, (total_eps + per_page - 1) // per_page)
    page = max(0, min(page, total_pages - 1))
    page_eps = ep_nums[page * per_page : (page + 1) * per_page]

    all_qs = sorted({normalize_quality_value(f["quality"]) for f in files}, key=quality_sort_key, reverse=True)
    qs_str = " · ".join(format_quality_badge(q) for q in all_qs) or "720p"

    text = (
        f"▣ <b>مسلسل {html.escape(str(media_item['title']))} ({media_item.get('year', '')})</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"❖ <b>الموسم الحالي:</b> الموسم {season}  |  <b>عدد الحلقات:</b> {total_eps} حلقة\n"
        f"◈ <b>الدقات المتوفرة:</b> <code>{qs_str}</code>\n"
        f"━━━━━━━━━━━━━━\n"
        f"◈ <b>اضغط على رقم الحلقة بالأسفل لاختيار الجودة واستلامها فوراً ▾</b>"
    )

    kb_rows: List[List[InlineKeyboardButton]] = []

    # شريط المواسم إذا كان هناك أكثر من موسم
    if len(seasons) > 1:
        s_row = []
        for s_n in seasons:
            mark = "✓ " if s_n == season else ""
            s_row.append(InlineKeyboardButton(
                text=f"{mark}الموسم {s_n}",
                callback_data=f"bot_tv_s:{mid}:{s_n}:0"
            ))
            if len(s_row) == 3:
                kb_rows.append(s_row)
                s_row = []
        if s_row:
            kb_rows.append(s_row)

    # أزرار الحلقات (3 أزرار في كل صف)
    ep_row = []
    for e_n in page_eps:
        ep_files = eps_map[e_n]
        qs_count = len({normalize_quality_value(x["quality"]) for x in ep_files})
        q_hint = f" ({qs_count} دقات)" if qs_count > 1 else f" ({format_quality_badge(ep_files[0]['quality'])})"
        ep_row.append(InlineKeyboardButton(
            text=f"◈ حلقة {e_n}{q_hint}",
            callback_data=f"bot_tv_ep:{mid}:{season}:{e_n}"
        ))
        if len(ep_row) == 2:
            kb_rows.append(ep_row)
            ep_row = []
    if ep_row:
        kb_rows.append(ep_row)

    # أزرار التنقل بين صفحات الحلقات للمواسم الطويلة
    if total_pages > 1:
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton(text=" Right ◂ السابق", callback_data=f"bot_tv_s:{mid}:{season}:{page - 1}".replace(" Right ", "")))
        nav_row.append(InlineKeyboardButton(text=f"صفحة {page + 1}/{total_pages}", callback_data="noop"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton(text="التالي ▸", callback_data=f"bot_tv_s:{mid}:{season}:{page + 1}"))
        kb_rows.append(nav_row)

    webapp_url = get_webapp_url()
    if webapp_url.startswith("https://"):
        kb_rows.append([InlineKeyboardButton(
            text="◈ فتح المسلسل بالميني آب (مشغل VIP) ▸",
            web_app=WebAppInfo(url=f"{webapp_url}?media_id={mid}")
        )])

    return text, InlineKeyboardMarkup(inline_keyboard=kb_rows)


def build_bot_episode_qualities_view(media_item: Dict[str, Any], season: int, episode: int) -> Tuple[str, InlineKeyboardMarkup]:
    mid = media_item["id"]
    files = media_item.get("files") or []
    by_s = _group_series_episodes_for_bot(files)
    ep_files = (by_s.get(season) or {}).get(episode) or []

    ep_title = ""
    dur_mins = 0
    for f in ep_files:
        raw_t = (f.get("episode_title") or "").strip()
        if raw_t and not re.fullmatch(r"(?:الحلقة|حلقة|episode)\s*\d+", raw_t, re.I):
            ep_title = raw_t
        if f.get("duration_mins") and not dur_mins:
            dur_mins = int(f["duration_mins"])

    ep_title_line = f"\n✦ <b>عنوان الحلقة:</b> {html.escape(ep_title)}" if ep_title else ""
    dur_line = f"  |  ◷ <b>المدة:</b> {dur_mins} دقيقة" if dur_mins else ""

    text = (
        f"◈ <b>مسلسل {html.escape(str(media_item['title']))} ({media_item.get('year', '')})</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"❖ <b>الموسم {season} — الحلقة {episode}</b>{dur_line}{ep_title_line}\n"
        f"━━━━━━━━━━━━━━\n"
        f"▸ <b>اضغط على الجودة اللي تناسبك حتى تنرسلك الحلقة فوراً ▾</b>"
    )

    kb_rows: List[List[InlineKeyboardButton]] = []
    seen_q = set()
    q_row = []
    for f in ep_files:
        q = normalize_quality_value(f["quality"])
        if q in seen_q:
            continue
        seen_q.add(q)
        sz_mb = float(f.get("size_mb") or 0)
        sz_txt = f" · {round(sz_mb)}MB" if sz_mb > 0 else ""
        q_row.append(InlineKeyboardButton(
            text=f"▸ دقة {format_quality_badge(q)}{sz_txt}",
            callback_data=f"bot_send_file:{f['id']}"
        ))
        if len(q_row) == 2:
            kb_rows.append(q_row)
            q_row = []
    if q_row:
        kb_rows.append(q_row)

    # أزرار الحلقة السابقة والتالية للتنقل السريع
    season_eps = sorted((by_s.get(season) or {}).keys())
    if episode in season_eps:
        idx = season_eps.index(episode)
        adj_row = []
        if idx > 0:
            prev_e = season_eps[idx - 1]
            adj_row.append(InlineKeyboardButton(text=f"◂ الحلقة {prev_e}", callback_data=f"bot_tv_ep:{mid}:{season}:{prev_e}"))
        if idx < len(season_eps) - 1:
            next_e = season_eps[idx + 1]
            adj_row.append(InlineKeyboardButton(text=f"الحلقة {next_e} ▸", callback_data=f"bot_tv_ep:{mid}:{season}:{next_e}"))
        if adj_row:
            kb_rows.append(adj_row)

    kb_rows.append([InlineKeyboardButton(
        text=f"▣ رجوع لقائمة حلقات الموسم {season}",
        callback_data=f"bot_tv_s:{mid}:{season}:0"
    )])

    webapp_url = get_webapp_url()
    if webapp_url.startswith("https://"):
        kb_rows.append([InlineKeyboardButton(
            text="◈ تشغيل الحلقة بالميني آب ▸",
            web_app=WebAppInfo(url=f"{webapp_url}?media_id={mid}")
        )])

    return text, InlineKeyboardMarkup(inline_keyboard=kb_rows)


@main_router.callback_query(F.data == "noop")
async def cb_noop(call: CallbackQuery):
    await call.answer()


@main_router.callback_query(F.data.startswith("bot_tv_s:"))
async def cb_bot_tv_season(call: CallbackQuery):
    parts = call.data.split(":")
    mid = int(parts[1])
    s_num = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 else 0
    media_item = get_media_by_id(mid)
    if not media_item:
        await call.answer("العمل غير متوفر", show_alert=True)
        return
    txt, kb = build_bot_series_episodes_view(media_item, season=s_num, page=page)
    try:
        if call.message.photo:
            await call.message.edit_caption(caption=txt, reply_markup=kb, parse_mode="HTML")
        else:
            await call.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await call.message.answer(txt, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@main_router.callback_query(F.data.startswith("bot_tv_ep:"))
async def cb_bot_tv_episode(call: CallbackQuery):
    parts = call.data.split(":")
    mid = int(parts[1])
    s_num = int(parts[2])
    e_num = int(parts[3])
    media_item = get_media_by_id(mid)
    if not media_item:
        await call.answer("العمل غير متوفر", show_alert=True)
        return
    txt, kb = build_bot_episode_qualities_view(media_item, season=s_num, episode=e_num)
    try:
        if call.message.photo:
            await call.message.edit_caption(caption=txt, reply_markup=kb, parse_mode="HTML")
        else:
            await call.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await call.message.answer(txt, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@main_router.callback_query(F.data.startswith("bot_send_file:"))
async def cb_bot_send_file_direct(call: CallbackQuery):
    uid = call.from_user.id
    file_db_id = int(call.data.split(":")[1])
    file_record = get_file_by_id(file_db_id)
    if not file_record:
        await call.answer("◈ عذراً، هذا الملف غير متوفر حالياً.", show_alert=True)
        return

    is_sub, ch_link, ch_title = await check_user_subscription(uid, force_refresh=True)
    ad_eval = await evaluate_user_ad_tasks(uid)
    if not is_sub or ad_eval["pending_channels"]:
        await call.answer("يرجى الاشتراك بالقنوات المطلوبة أولاً لفتح الفيديو!", show_alert=True)
        await call.message.answer(
            "◈ <b>لفتح واستلام فيديو الفيلم أو الحلقة، اشترك في القنوات التالية أولاً ثم اضغط زر التحقق:</b>",
            reply_markup=build_ad_channels_wall_keyboard(
                ad_eval["pending_channels"],
                f"bot_send_file:{file_db_id}",
                main_force_link=(ch_link if not is_sub else ""),
                main_force_title=(ch_title if not is_sub else "")
            ),
            parse_mode="HTML"
        )
        return

    quality = normalize_quality_value(file_record["quality"])
    sender_user = BOT_INFO.get("sender_username") or "VortexSenderBot"
    deep_link = f"https://t.me/{sender_user}?start=file_{file_db_id}"
    await call.answer("تم تجهيز الملف في بوت الإرسال ✓")
    await call.message.answer(
        f"◈ <b>تم التحقق بنجاح! الفيديو جاهز للاستلام في بوت الإرسال:</b>\n"
        f"✦ اضغط على الزر أدناه لاستلام الفيديو بدقة <b>{format_quality_badge(quality)}</b> فوراً ▾",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [ikb(text=f"▸ استلام الفيديو ({format_quality_badge(quality)}) من بوت الإرسال", style="danger", url=deep_link)]
        ]),
        parse_mode="HTML"
    )


@main_router.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout_query: PreCheckoutQuery):
    await main_bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)


@main_router.message(F.successful_payment)
async def process_successful_stars_payment(message: Message):
    user_id = message.from_user.id
    grant_vip_days(user_id, 30)
    await message.answer(
        "✦ <b>ألف مبروك! تفعل عندك اشتراك فورتكس PRO لمدة 30 يوم!</b>\n\n"
        "✦ هسة تكدر تفورتكس وتحمل كل الأفلام والمسلسلات قبل بدون أي إعلانات أو تذاكر انتظار ✦",
        parse_mode="HTML"
    )
    for aid in get_all_admins():
        try:
            await admin_bot.send_message(
                aid,
                f"✦ <b>عملية شراء VIP جديدة بالنجوم!</b>\n"
                f"◈ المستخدم: {html.escape(message.from_user.first_name or '')} (<code>{user_id}</code>)\n"
                f"★ المبلغ: {message.successful_payment.total_amount} نجمة",
                parse_mode="HTML"
            )
        except Exception:
            pass


# =====================================================================
# 2. بوت الإرسال الفرعي (@VortexSenderBot)
# =====================================================================
@sender_router.message(CommandStart())
async def sender_start_handler(message: Message, command: CommandObject):
    user = message.from_user
    register_user(user.id, user.first_name or "", user.username or "", source="sender")
    asyncio.create_task(notify_admins_of_new_users())
    args = command.args or ""

    is_sub, ch_link, ch_title = await check_user_subscription(user.id, force_refresh=True)
    ad_eval = await evaluate_user_ad_tasks(user.id)
    if not is_sub or ad_eval["pending_channels"]:
        cb_data = f"check_sub_sender:{args}"
        await message.answer(
            "◈ <b>بوابة استلام الفيديو | إرسال فورتكس ✦</b>\n"
            "━━━━━━━━━━━━━━\n"
            "لفتح واستلام فيديو الفيلم أو الحلقة مجاناً، يرجى الاشتراك في القنوات التالية أولاً ثم الضغط على زر <b>«✓ اشتركت بالقنوات — أرسل الفيديو الآن»</b> ▾",
            reply_markup=build_ad_channels_wall_keyboard(
                ad_eval["pending_channels"],
                cb_data,
                main_force_link=(ch_link if not is_sub else ""),
                main_force_title=(ch_title if not is_sub else "")
            ),
            parse_mode="HTML"
        )
        return

    await deliver_file_to_user(message.chat.id, user.id, args)


@sender_router.callback_query(F.data.startswith("check_sub_sender:"))
async def cb_check_sub_sender(call: CallbackQuery):
    args = call.data.split(":", 1)[1] if ":" in call.data else ""
    uid = call.from_user.id
    is_sub, ch_link, ch_title = await check_user_subscription(uid, force_refresh=True)
    ad_eval = await evaluate_user_ad_tasks(uid)
    if not is_sub or ad_eval["pending_channels"]:
        await call.answer("◈ بعدك ما مشترك بجميع القنوات المطلوبة! اشترك أولاً ثم اضغط تحقق.", show_alert=True)
        return
    await call.answer("✓ تم التحقق من اشتراكك! جاري إرسال الفيديو...")
    try:
        await call.message.delete()
    except Exception:
        pass
    await deliver_file_to_user(call.message.chat.id, uid, args)


AUTO_DELETE_HOURS = 6


async def _execute_single_scheduled_deletion(item: Dict[str, Any]):
    """يحذف رسالة الفيديو بعد مرور 6 ساعات ويرسل رسالة سبب الحذف مع زر مباشر يرجع المستخدم لنفس العمل."""
    del_id = int(item["id"])
    chat_id = int(item["chat_id"])
    msg_id = int(item["message_id"])
    media_id = int(item.get("media_id") or 0)
    file_id = int(item.get("file_id") or 0)
    title = str(item.get("media_title") or "العمل")
    media_type = str(item.get("media_type") or "movie")
    quality = str(item.get("quality") or "720")
    season = int(item.get("season") or 1)
    episode = int(item.get("episode") or 0)

    remove_scheduled_deletion(del_id)

    deleted_ok = False
    try:
        await sender_bot.delete_message(chat_id=chat_id, message_id=msg_id)
        deleted_ok = True
    except Exception as e:
        logger.debug(f"Auto-delete message {msg_id} in {chat_id} note: {e}")

    if not deleted_ok:
        return

    type_lbl = "مسلسل" if media_type == "tv" else "فيلم"
    ep_text = f" — الموسم {season} الحلقة {episode}" if episode and episode > 0 else ""
    webapp_url = get_webapp_url()
    main_user = BOT_INFO.get("main_username") or "Vortexee_Bot"
    sender_user = BOT_INFO.get("sender_username") or "VortexSenderBot"

    kb_rows = []
    if webapp_url.startswith("https://") and media_id > 0:
        kb_rows.append([InlineKeyboardButton(
            text=f"◈ فتح {type_lbl} {title[:22]} بالتطبيق ▸",
            web_app=WebAppInfo(url=f"{webapp_url}?media_id={media_id}&quality={quality}")
        )])
    elif media_id > 0:
        kb_rows.append([InlineKeyboardButton(
            text=f"◈ فتح {type_lbl} {title[:22]} بالتطبيق ▸",
            url=f"https://t.me/{main_user}?start=watch_{media_id}_{quality}"
        )])

    if file_id > 0:
        kb_rows.append([InlineKeyboardButton(
            text=f"↻ إعادة استلام الملف ({quality}p)",
            url=f"https://t.me/{sender_user}?start=file_{file_id}"
        )])

    notice_text = (
        f"◷ <b>تم حذف الفيديو تلقائياً بعد مرور 6 ساعات</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"◈ <b>العمل المحذوف:</b> {type_lbl} <b>{html.escape(title)}</b>{ep_text} (<code>{quality}p</code>)\n"
        f"✦ <b>سبب الحذف:</b> يتم حذف الملفات المرسلة تلقائياً كل 6 ساعات لتخفيف الحمل على السيرفر وحماية البوت من بلاغات حقوق الطبع والنشر.\n\n"
        f"◈ <b>العمل محفوظ وما يضيع منك!</b> تگدر ترجع لنفس العمل بأي وقت من الزر الجوة، أو تشترك بـ <b>VIP PRO</b> للمشاهدة بالبث المباشر داخل التطبيق بدون حذف ▾"
    )
    try:
        await sender_bot.send_message(
            chat_id=chat_id,
            text=notice_text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows) if kb_rows else None,
            parse_mode="HTML"
        )
    except Exception:
        pass


def clean_old_server_temp_files(max_age_hours: float = 6.0) -> int:
    """ينظف الملفات المؤقتة القديمة على قرص السيرفر حتى ما يبقى أي حمل على سيرفر Wispbyte."""
    removed = 0
    now = time.time()
    cutoff = now - (max_age_hours * 3600)
    prefixes = ("vortex_temp_", "temp_video_", "temp_cover_")
    try:
        for fname in os.listdir(TEMP_DIR):
            if fname.startswith(prefixes):
                fpath = os.path.join(TEMP_DIR, fname)
                try:
                    if os.path.getmtime(fpath) < cutoff:
                        os.remove(fpath)
                        removed += 1
                except Exception:
                    pass
    except Exception:
        pass
    try:
        db_checkpoint()
    except Exception:
        pass
    return removed


async def _mandatory_6h_deletion_worker():
    """عامل خلفي دائم يفحص مواعيد الحذف الإجباري (6 ساعات) وينظف قرص السيرفر دورياً."""
    await asyncio.sleep(5)
    last_temp_clean = 0.0
    while True:
        try:
            due_items = get_due_scheduled_deletions()
            for item in due_items:
                await _execute_single_scheduled_deletion(item)
                await asyncio.sleep(0.15)

            now = time.time()
            if now - last_temp_clean >= 1800:  # كل نصف ساعة نفحص وننظف الملفات المؤقتة الأقدم من 6 ساعات
                last_temp_clean = now
                clean_old_server_temp_files(6.0)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Mandatory 6h deletion worker warning: {e}")
        await asyncio.sleep(20)


async def deliver_file_to_user(chat_id: int, user_id: int, args: str):
    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="فتح فورتكس ◈", url=f"https://t.me/{BOT_INFO['main_username']}")]
    ])

    if not args or not args.startswith("file_"):
        await sender_bot.send_message(
            chat_id=chat_id,
            text="هلا بيك بسيرفر التحميل المباشر لبوت فورتكس ▸\nاختار الفيلم أو المسلسل من داخل التطبيق حتى يوصلك الملف هنا فوراً.",
            reply_markup=back_kb
        )
        return

    try:
        file_db_id = int(args.split("_")[1])
    except Exception:
        await sender_bot.send_message(chat_id=chat_id, text="◈ رابط الملف مو صالح.", reply_markup=back_kb)
        return

    ad_eval = await evaluate_user_ad_tasks(user_id)
    if ad_eval["pending_channels"]:
        await sender_bot.send_message(
            chat_id=chat_id,
            text="◈ <b>لإتمام استلام الفيلم أو الحلقة مجاناً، يرجى الاشتراك في القنوات الإجبارية التالية أولاً ثم الضغط على زر التحقق:</b>",
            reply_markup=build_ad_channels_wall_keyboard(ad_eval["pending_channels"], f"check_sub_sender:{args}"),
            parse_mode="HTML"
        )
        return

    file_record = get_file_by_id(file_db_id)
    if not file_record:
        await sender_bot.send_message(chat_id=chat_id, text="◈ عذراً، هذا الملف ما متوفر حالياً.", reply_markup=back_kb)
        return

    storage_chat_id = file_record["storage_chat_id"]
    storage_message_id = file_record["storage_message_id"]
    media_id = int(file_record["media_id"])
    title = file_record["title"]
    year = file_record["year"]
    quality = file_record["quality"]
    episode = int(file_record.get("episode") or 0)
    season = int(file_record.get("season") or 1)
    media_type = str(file_record.get("media_type") or "movie")

    ep_t_raw = str(file_record.get("episode_title") or "").strip()
    ep_t_clean = f" ({ep_t_raw})" if (ep_t_raw and not re.fullmatch(r"(?:الحلقة|حلقة|episode)\s*\d+", ep_t_raw, re.I)) else ""
    ep_text = f" - الموسم {season} الحلقة {episode}{ep_t_clean}" if episode and episode > 0 else ""
    u_prof_delivery = get_user_profile(user_id) if user_id else {}
    is_vip_delivery = bool(u_prof_delivery.get("is_vip") or is_user_admin(user_id))
    if is_vip_delivery:
        del_notice = (
            "\n\n👑 <b>ميزة مشترك PRO المفعّلة:</b>\n"
            "هذا الفيديو محفوظ في محادثتك دائماً ولن يُحذف بعد 6 ساعات ✦"
        )
    else:
        del_notice = (
            "\n\n◷ <b>تنبيه الحذف التلقائي (6 ساعات):</b>\n"
            "سيتم حذف هذا الفيديو تلقائياً بعد مرور <b>6 ساعات</b> (اشترك في PRO أو اجمع 100 نقطة لإلغاء الحذف التلقائي) ✦"
        )

    type_lbl = "أنمي" if media_type == "anime" else ("مسلسل" if media_type == "tv" else "فيلم")
    caption = (
        f"◈ <b>{type_lbl} {html.escape(str(title))} ({year}){ep_text} — الدقة {quality}p</b>"
        f"{del_notice}"
    )

    webapp_url = get_webapp_url()
    file_kb_rows = []
    if webapp_url.startswith("https://"):
        file_kb_rows.append([ikb(
            text="◈ فتح صفحة العمل في تطبيق فورتكس ▸",
            style="danger",
            web_app=WebAppInfo(url=f"{webapp_url}?media_id={media_id}&quality={quality}&uid={user_id}")
        )])
    reply_kb = InlineKeyboardMarkup(inline_keyboard=file_kb_rows) if file_kb_rows else None
    src_chat_norm = int(storage_chat_id) if re.match(r"^-?\d+$", str(storage_chat_id)) else storage_chat_id
    sent_msg = None
    delivered_bot = sender_bot

    # 1) المحاولة الأساسية عبر بوت الإرسال (copy_message من قناة الخزن)
    if src_chat_norm and int(storage_message_id or 0) > 0:
        try:
            sent_msg = await sender_bot.copy_message(
                chat_id=chat_id,
                from_chat_id=src_chat_norm,
                message_id=int(storage_message_id),
                caption=caption,
                reply_markup=reply_kb,
                parse_mode="HTML"
            )
        except Exception as e1:
            logger.debug(f"Sender direct copy_message fallback triggered: {e1}")

    # 2) إذا كانت القناة مصدر سحب خارجي وبوت الإدارة أو الرئيسي مشرف فيها، ننسخ الرسالة لقناة الأرشيف السري ثم نرسلها
    if sent_msg is None and src_chat_norm and int(storage_message_id or 0) > 0:
        relay_targets = [SECRET_ARCHIVE_CHANNEL_ID]
        st_alt = get_storage_channel_for_type(media_type)
        if st_alt:
            relay_targets.append(normalize_chat_target(st_alt))
        for helper_bot in (admin_bot, main_bot):
            if sent_msg is not None:
                break
            for rt in relay_targets:
                if not rt:
                    continue
                try:
                    relayed = await helper_bot.copy_message(
                        chat_id=rt,
                        from_chat_id=src_chat_norm,
                        message_id=int(storage_message_id)
                    )
                    if relayed and relayed.message_id:
                        update_media_file_record(file_db_id, storage_chat_id=str(rt), storage_message_id=int(relayed.message_id))
                        sent_msg = await sender_bot.copy_message(
                            chat_id=chat_id,
                            from_chat_id=rt,
                            message_id=int(relayed.message_id),
                            caption=caption,
                            reply_markup=reply_kb,
                            parse_mode="HTML"
                        )
                        break
                except Exception:
                    continue

    # 3) محاولة بديلة أخيرة: إرسال مباشر للمستخدم عبر البوت الرئيسي أو بوت الإدارة إذا تعذر وصول بوت الإرسال للقناة
    if sent_msg is None and src_chat_norm and int(storage_message_id or 0) > 0:
        for fallback_bot in (main_bot, admin_bot):
            try:
                sent_msg = await fallback_bot.copy_message(
                    chat_id=user_id,
                    from_chat_id=src_chat_norm,
                    message_id=int(storage_message_id),
                    caption=caption,
                    reply_markup=reply_kb,
                    parse_mode="HTML"
                )
                delivered_bot = fallback_bot
                if fallback_bot is not sender_bot:
                    await sender_bot.send_message(
                        chat_id=chat_id,
                        text="✓ <b>تم إرسال الفيديو لك مباشرة في محادثة البوت الرئيسي!</b>",
                        parse_mode="HTML"
                    )
                break
            except Exception:
                continue

    if sent_msg is not None:
        toggle_user_list(user_id, media_id, "watched")
        log_content_pull(user_id, media_id, file_db_id, pull_mode="bot")
        if delivered_bot is sender_bot and not is_vip_delivery:
            schedule_mandatory_deletion(
                chat_id=chat_id,
                user_id=user_id,
                message_id=sent_msg.message_id,
                media_id=media_id,
                file_id=file_db_id,
                media_title=str(title),
                media_type=media_type,
                quality=str(quality),
                season=season,
                episode=episode,
                hours=AUTO_DELETE_HOURS
            )
    else:
        sender_u = BOT_INFO.get("sender_username") or "Vortexeserver_Bot"
        await sender_bot.send_message(
            chat_id=chat_id,
            text=f"◈ تعذر إرسال الملف حالياً. تأكد من رفع البوتات (@{sender_u}) مشرفين في قناة الخزن.",
            reply_markup=back_kb
        )


# =====================================================================
# 3. بوت الإدارة المنظّم (@VortexAdminBot) + فحص القنوات + /edit + /check_files
# =====================================================================
class AdminStates(StatesGroup):
    choosing_type = State()
    entering_title = State()
    choosing_tmdb_result = State()
    uploading_files = State()
    deleting_search = State()
    setting_movies_channel = State()
    setting_series_channel = State()
    setting_anime_channel = State()
    setting_force_sub_channel = State()
    setting_force_sub_link = State()
    setting_publish_channel = State()
    setting_movies_pub_channel = State()
    setting_series_pub_channel = State()
    setting_anime_pub_channel = State()
    setting_tmdb_key = State()
    setting_webapp_url = State()
    setting_support_username = State()
    adding_new_admin = State()
    broadcasting_message = State()
    setting_adsgram_block_id = State()
    setting_vip_stars_price = State()
    setting_vip_points_price = State()
    setting_referral_points = State()
    granting_vip_user = State()
    adding_points_user = State()
    setting_req_quota_num = State()
    setting_video_cover = State()
    editing_search = State()
    editing_field_value = State()
    replacing_file_upload = State()
    editing_file_meta = State()
    adding_custom_section_title = State()
    adding_custom_section_country = State()
    adding_auto_source_channel = State()
    importing_json_backup = State()


def build_admin_home_view() -> Tuple[str, InlineKeyboardMarkup]:
    s = get_stats()
    brand = get_brand_config()
    app_name = brand["app_name_ar"]

    movies_ch = "✓" if get_setting("movies_channel_id") else "✕"
    series_ch = "✓" if get_setting("series_channel_id") else "✕"
    anime_ch = "✓" if get_setting("anime_channel_id") else "✕"

    pub_ch = "✓" if get_setting("publish_channel_id") else "✕"
    mpub_ch = "✓" if get_setting("movies_publish_channel_id") else "✕"
    spub_ch = "✓" if get_setting("series_publish_channel_id") else "✕"
    apub_ch = "✓" if get_setting("anime_publish_channel_id") else "✕"

    force_ch = get_setting("force_sub_channel_id", "")
    force_st = f"مفعّل ✓ ({force_ch})" if force_ch else "معطّل ✕"
    webapp_url = get_webapp_url()

    text = (
        f"◈ <b>لوحة التحكم الرئيسية | إدارة {html.escape(app_name)} ({html.escape(brand['app_name_en'])})</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"◈ المستخدمين: <b>{s['users']}</b> (اليوم: <b>+{s.get('new_users_today', 0)}</b>)  |  ★ PRO: <b>{s['vips']}</b>\n"
        f"◈ الأفلام: <b>{s['movies']}</b>  |  ▣ المسلسلات: <b>{s['series']}</b>  |  ✦ الأنمي: <b>{s.get('anime', 0)}</b>\n"
        f"◈ إجمالي الملفات: <b>{s['files']}</b>  |  ▸ السحب الكلي: <b>{s['downloads']}</b> (اليوم: <b>{s.get('pulls_today', 0)}</b>)\n"
        f"◈ طلبات معلقة: <b>{s['requests_pending']}</b>  |  ❖ بلاغات: <b>{s['reports_pending']}</b>  |  ◈ المشرفين: <b>{s.get('admins', 1)}</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"▣ <b>قنوات الخزن (3):</b> أفلام ({movies_ch}) · مسلسلات ({series_ch}) · أنمي ({anime_ch})\n"
        f"◈ <b>قنوات النشر (4):</b> العامة ({pub_ch}) · أفلام ({mpub_ch}) · مسلسلات ({spub_ch}) · أنمي ({apub_ch})\n"
        f"◈ الاشتراك الإجباري: <b>{force_st}</b>\n"
        f"⟐ سحابة Firebase: <b>{firebase_status_line()}</b>\n\n"
        "▾ <b>الأزرار مرتبة وملونة حسب الأهمية (أحمر للمهم · أزرق للمرتب · أخضر للإعدادات):</b>"
    )

    kb_rows = [
        # المجموعة الحمراء (danger): الأزرار المهمة والأساسية والحساسة
        [
            ikb(text="⚡ قسم الإضافة التلقائية وسحب القنوات", style="danger", callback_data="auto_ingest_hub"),
        ],
        [
            ikb(text="＋ إضافة عمل (فيلم / مسلسل / أنمي)", style="danger", callback_data="quick_add"),
        ],
        [
            ikb(text="＋ إضافة حلقات / دقات", style="danger", callback_data="quick_append"),
            ikb(text=f"◈ الطلبات ({s['requests_pending']}) والبلاغات ({s['reports_pending']})", style="danger", callback_data="menu_requests"),
        ],
        [
            ikb(text="✕ حذف عمل أو دقة محددة", style="danger", callback_data="quick_del"),
        ],
        # المجموعة الزرقاء (primary): الأزرار المرتبة والاعتيادية
        [
            ikb(text="✎ تعديل عمل أو دمج", style="primary", callback_data="quick_edit"),
            ikb(text="✦ تحديد اختيار الليلة", style="primary", callback_data="quick_tonight"),
        ],
        [
            ikb(text="▣ الأقسام الذكية", style="primary", callback_data="quick_sections"),
            ikb(text="❖ سلاسل الأفلام", style="primary", callback_data="quick_collections"),
        ],
        [
            ikb(text=f"◈ المستخدمين (+{s.get('new_users_today', 0)})", style="primary", callback_data="menu_users"),
            ikb(text="▣ آخر الأعمال المنشورة", style="primary", callback_data="quick_list"),
        ],
        [
            ikb(text="◈ إذاعة للمستخدمين", style="primary", callback_data="quick_broadcast"),
            ikb(text="▣ إدارة المحتوى والقنوات", style="primary", callback_data="menu_content"),
        ],
        # المجموعة الخضراء (success): نادرة الاستخدام لكن مهمة
        [
            ikb(text="❖ الإعدادات والهوية والإعلانات", style="success", callback_data="menu_settings"),
            ikb(text="⟐ ضبط وفحص القنوات الـ 7", style="success", callback_data="quick_channels"),
        ],
        [
            ikb(text="▣ الغلاف الموحد للفيديوهات", style="success", callback_data="set_video_cover"),
            ikb(text=f"◈ المشرفين ({s.get('admins', 1)})", style="success", callback_data="menu_admins"),
        ],
        [
            ikb(text="⟐ مزامنة Firebase", style="success", callback_data="fb_menu_open"),
            ikb(text="▣ نسخة احتياطية", style="success", callback_data="quick_backup"),
            ikb(text="◈ فحص الملفات", style="success", callback_data="quick_check_files"),
        ],
    ]
    if is_valid_http_url(webapp_url) and webapp_url.startswith("https://"):
        kb_rows.append([
            ikb(text=f"◈ فتح لوحة إدارة {app_name} وتخصيص الميني آب", style="danger", web_app=WebAppInfo(url=f"{webapp_url}?open_admin=1"))
        ])
    return text, InlineKeyboardMarkup(inline_keyboard=kb_rows)


def build_content_section_view() -> Tuple[str, InlineKeyboardMarkup]:
    movies_ch = get_setting("movies_channel_id", "") or "غير مربوطة ✕"
    series_ch = get_setting("series_channel_id", "") or "غير مربوطة ✕"
    anime_ch = get_setting("anime_channel_id", "") or "غير مربوطة ✕"

    pub_ch = get_setting("publish_channel_id", "") or "غير مربوطة ✕"
    mpub_ch = get_setting("movies_publish_channel_id", "") or "غير مربوطة ✕"
    spub_ch = get_setting("series_publish_channel_id", "") or "غير مربوطة ✕"
    apub_ch = get_setting("anime_publish_channel_id", "") or "غير مربوطة ✕"

    btn_mode = get_setting("channel_post_btn_mode", "webapp")
    btn_mode_ar = "يفتح التطبيق (يمر بالإعلان/VIP)" if btn_mode == "webapp" else "إرسال مباشر من بوت السيرفر"
    tmdb_key = get_setting("tmdb_api_key", "")
    tmdb_status = f"مربوط ✓ (<code>{tmdb_key[:6]}...</code>)" if tmdb_key else "غير مضاف ✕"

    cov_enabled = get_setting("cover_enabled", "1") == "1"
    cov_fid = get_setting("video_cover_file_id", "").strip()
    if not cov_enabled:
        cover_st = "متوقف مؤقتاً ✕"
    elif cov_fid:
        cover_st = "مفعّل ومعتمد لكل الفيديوهات ✓ (عبر تيليجرام بدون استهلاك رام)"
    else:
        cover_st = "غير مضاف بعد (اضغط زر الغلاف بالأسفل لإرسال صورة الغلاف)"

    text = (
        "▣ <b>قسم المحتوى والقنوات الـ 7 والنشر التلقائي</b>\n"
        "━━━━━━━━━━━━━━\n"
        "<b>1. قنوات الخزن السرية (Storage Channels):</b>\n"
        f"• ◈ خزن الأفلام: <code>{html.escape(movies_ch)}</code>\n"
        f"• ▣ خزن المسلسلات: <code>{html.escape(series_ch)}</code>\n"
        f"• ✦ خزن الأنمي: <code>{html.escape(anime_ch)}</code>\n\n"
        "<b>2. قنوات النشر العام التلقائي (Publish Channels):</b>\n"
        f"• ⟐ القناة العامة (الجديد): <code>{html.escape(pub_ch)}</code>\n"
        f"• ◈ قناة نشر الأفلام: <code>{html.escape(mpub_ch)}</code>\n"
        f"• ▣ قناة نشر المسلسلات: <code>{html.escape(spub_ch)}</code>\n"
        f"• ✦ قناة نشر الأنمي: <code>{html.escape(apub_ch)}</code>\n"
        "━━━━━━━━━━━━━━\n"
        f"◈ <b>أزرار الدقات بالمنشور:</b> <b>{btn_mode_ar}</b>\n"
        f"❖ <b>مفتاح البحث التلقائي TMDB:</b> {tmdb_status}\n"
        f"▣ <b>الغلاف الموحد للفيديوهات:</b> {cover_st}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        # أحمر للمهم
        [
            ikb(text="⚡ قسم الإضافة التلقائية وسحب القنوات", style="danger", callback_data="auto_ingest_hub")
        ],
        [
            ikb(text="＋ إضافة عمل جديد", style="danger", callback_data="quick_add"),
            ikb(text="＋ إضافة حلقات / دقات", style="danger", callback_data="quick_append")
        ],
        [
            ikb(text="✎ تعديل عمل أو دمج", style="primary", callback_data="quick_edit"),
            ikb(text="✕ حذف عمل أو دقة", style="danger", callback_data="quick_del")
        ],
        # أزرق لقنوات الخزن الثلاث
        [
            ikb(text="▣ خزن الأفلام", style="primary", callback_data="set_movies_ch"),
            ikb(text="▣ خزن المسلسلات", style="primary", callback_data="set_series_ch"),
            ikb(text="▣ خزن الأنمي", style="primary", callback_data="set_anime_ch")
        ],
        # أزرق لقنوات النشر الأربع
        [
            ikb(text="⟐ قناة النشر العامة (الجديد)", style="primary", callback_data="set_publish_ch"),
            ikb(text="◈ قناة الاشتراك الإجباري", style="primary", callback_data="set_force_ch")
        ],
        [
            ikb(text="◈ نشر الأفلام", style="primary", callback_data="set_movies_pub_ch"),
            ikb(text="▣ نشر المسلسلات", style="primary", callback_data="set_series_pub_ch"),
            ikb(text="✦ نشر الأنمي", style="primary", callback_data="set_anime_pub_ch")
        ],
        # أخضر للإعدادات المساندة والفحص
        [
            ikb(text="⟐ فحص القنوات الـ 7", style="success", callback_data="quick_channels"),
            ikb(text="◈ فحص سلامة الملفات", style="success", callback_data="quick_check_files")
        ],
        [
            ikb(text="📋 كليشة المسلسلات والأفلام (روابط)", style="danger", callback_data="open_template_menu"),
            ikb(text="🎭 مزامنة الممثلين للأعمال", style="primary", callback_data="sync_missing_cast")
        ],
        [
            ikb(text="▣ الغلاف الموحد للفيديوهات", style="success", callback_data="set_video_cover"),
            ikb(text="❖ مفتاح TMDB API", style="success", callback_data="set_tmdb_key")
        ],
        [
            ikb(text=f"أزرار المنشور: {'التطبيق' if btn_mode == 'webapp' else 'مباشر'}", style="success", callback_data="toggle_post_btn_mode"),
            ikb(text="↺ تنظيف مؤقتات السيرفر", style="success", callback_data="toggle_autodel")
        ],
        [
            ikb(text="◂ رجوع للوحة الرئيسية", style="primary", callback_data="menu_home")
        ]
    ])
    return text, kb


def _parse_mybid_id(raw_val: str, attr_name: str = "data-admpid") -> str:
    s = str(raw_val or "").strip()
    if not s:
        return ""
    m = re.search(rf'{attr_name}\s*=\s*["\']?(\d+)["\']?', s, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    m_num = re.search(r"#?(\d{3,12})", s)
    if m_num:
        return m_num.group(1)
    return s


def build_settings_section_view() -> Tuple[str, InlineKeyboardMarkup]:
    brand = get_brand_config()
    force_ch = get_setting("force_sub_channel_id", "")
    force_link = get_setting("force_sub_channel_link", "")
    force_title = get_setting("force_sub_channel_title", "")
    force_status = f"مفعّل ✓ ({html.escape(force_title)} | <code>{force_ch}</code>)" if force_ch else "معطّل ○"

    ads_enabled = get_setting("ads_enabled", "1") == "1"
    all_ad_tasks = get_ad_tasks(only_enabled=False)
    active_ad_tasks = [t for t in all_ad_tasks if t.get("is_enabled")]
    ch_tasks_cnt = sum(1 for t in active_ad_tasks if t.get("task_type") == "channel")
    lnk_tasks_cnt = sum(1 for t in active_ad_tasks if t.get("task_type") == "link")
    total_clicks = sum(int(t.get("clicks_count") or 0) for t in all_ad_tasks)
    timer_sec = get_setting("ads_timer_seconds", "15").strip() or "15"
    fallback_timer = get_setting("ads_fallback_timer", "1") == "1"

    ads_status = (
        f"مفعّل ✓ ({len(active_ad_tasks)} مهام نشطة: {ch_tasks_cnt} قنوات · {lnk_tasks_cnt} روابط)"
        if ads_enabled else "معطّل ✕"
    )
    banners_str = f"إجمالي الاشتراكات/الزيارات المحققة: <b>{total_clicks}</b>"
    timer_status = f"مفعّل ✓ ({timer_sec} ثانية)" if fallback_timer else "معطّل ✕"

    stars_price = get_setting("vip_stars_price", "50")
    points_price = get_setting("vip_points_price", "100")
    ref_pts = get_setting("referral_points", "20")
    sup_user = get_setting("support_username", "") or "Vortexeo"
    webapp_url = get_webapp_url()

    text = (
        f"❖ <b>قسم الإعدادات وهوية {html.escape(brand['app_name_ar'])} والإعلانات</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"◈ <b>اسم التطبيق:</b> <b>{html.escape(brand['app_name_ar'])} ({html.escape(brand['app_name_en'])})</b>\n"
        f"✦ <b>اللون الرئيسي للواجهة والغلاف:</b> <code>{brand['app_primary_color']}</code>\n"
        f"▣ <b>حالة الغلاف الموحد:</b> {'مفعّل ✓' if brand.get('cover_enabled') else 'معطّل ✕'}\n"
        "━━━━━━━━━━━━━━\n"
        f"◈ <b>قناة الاشتراك الإجباري:</b> {force_status}\n"
        f"⟐ <b>رابط قناة الاشتراك:</b> <code>{force_link or 'تلقائي'}</code>\n"
        f"◈ <b>نظام إعلانات فورتكس الخاص:</b> {ads_status}\n"
        f"▣ <b>إحصائيات مهام الإعلانات:</b> {banners_str}\n"
        f"◷ <b>مؤقت التذكرة الداخلي:</b> {timer_status}\n"
        f"◈ <b>حساب الدعم الفني:</b> <code>@{sup_user.lstrip('@')}</code>\n"
        "━━━━━━━━━━━━━━\n"
        f"★ <b>سعر PRO بالنجوم:</b> {stars_price} نجمة  |  ❖ <b>بالنقاط:</b> {points_price} نقطة\n"
        f"⟐ <b>نقاط كل دعوة:</b> {ref_pts} نقطة\n"
        f"⟐ <b>رابط الميني آب:</b> <code>{webapp_url}</code>\n\n"
        "✦ <i>لتعديل اسم التطبيق والألوان والشعار والواجهة بالكامل افتح «لوحة الإدارة بالميني آب» من الزر الأحمر بالأسفل!</i>"
    )
    ads_toggle_text = "○ إيقاف نظام الإعلانات" if ads_enabled else "▸ تشغيل نظام الإعلانات"
    timer_toggle_text = f"◷ مؤقت التذكرة ({timer_sec}ث): مفعّل ✓" if fallback_timer else "◷ مؤقت التذكرة: معطّل ✕"

    kb_rows = [
        [
            ikb(text="◈ تعيين قناة الاشتراك", style="primary", callback_data="set_force_ch"),
            ikb(text="○ إيقاف الاشتراك الإجباري", style="danger", callback_data="disable_force_ch")
        ],
        [
            ikb(text="❖ إدارة مهام الإعلانات والرعاة", style="primary", callback_data="set_ads_block"),
            ikb(text=ads_toggle_text, style="success", callback_data="toggle_ads_status")
        ],
        [
            ikb(text=timer_toggle_text, style="success", callback_data="toggle_fallback_timer"),
            ikb(text="◈ يوزر الدعم الفني", style="primary", callback_data="set_support_user")
        ],
        [
            ikb(text=f"★ سعر النجوم ({stars_price})", style="primary", callback_data="set_stars_price"),
            ikb(text=f"❖ سعر النقاط ({points_price})", style="primary", callback_data="set_points_price"),
            ikb(text=f"⟐ نقاط الدعوة ({ref_pts})", style="primary", callback_data="set_ref_pts")
        ],
        [
            ikb(text="★ منح/سحب PRO لمستخدم", style="danger", callback_data="admin_grant_vip"),
            ikb(text="＋ إضافة نقاط لمستخدم", style="primary", callback_data="admin_add_pts")
        ],
        [
            ikb(
                text=("🛠️ إيقاف صيانة التطبيق (فتح للزوار)" if get_setting("app_maintenance", "0") == "1" else "🛠️ تفعيل وضع صيانة الميني آب"),
                style=("danger" if get_setting("app_maintenance", "0") == "1" else "primary"),
                callback_data="toggle_app_maint_bot"
            )
        ],
        [
            ikb(text="⟐ تعديل رابط الميني آب", style="success", callback_data="set_webapp_url"),
            ikb(text="◈ إضافة مشرف جديد", style="success", callback_data="add_new_admin")
        ],
    ]
    if webapp_url.startswith("https://"):
        kb_rows.append([
            ikb(text="✦ فتح إعدادات الهوية والألوان بالميني آب", style="danger", web_app=WebAppInfo(url=f"{webapp_url}?open_admin=1"))
        ])
    kb_rows.append([ikb(text="◂ رجوع للوحة الرئيسية", style="primary", callback_data="menu_home")])
    return text, InlineKeyboardMarkup(inline_keyboard=kb_rows)


def build_requests_section_view() -> Tuple[str, InlineKeyboardMarkup]:
    req_enabled = get_setting("requests_enabled", "1") == "1"
    cycle = get_setting("requests_cycle", "weekly")
    quota_max = get_setting("requests_quota_max", "5")

    cycle_names = {"daily": "كل 24 ساعة (يومي)", "weekly": "كل 7 أيام (أسبوعي)", "monthly": "كل 30 يوم (شهري)"}
    cycle_ar = cycle_names.get(cycle, "أسبوعي")
    st_text = "مفتوح ويستقبل ✓" if req_enabled else "مغلق ✕"

    pending_list = get_pending_media_requests(25)
    pending_count = len(pending_list)
    reports_list = get_pending_media_reports(25)
    reports_count = len(reports_list)

    text = (
        "◈ <b>قسم إدارة الطلبات والبلاغات</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"▸ <b>حالة استقبال الطلبات:</b> {st_text}\n"
        f"⟐ <b>دورة تجديد الطلبات للمستخدم:</b> <b>{cycle_ar}</b>\n"
        f"◈ <b>عدد الطلبات المسموح بها بالدورة:</b> <b>{quota_max} طلب</b>\n"
        f"▣ <b>الطلبات المعلقة الحالية:</b> <b>{pending_count} طلب</b>\n"
        f"◈ <b>البلاغات المعلقة الحالية:</b> <b>{reports_count} بلاغ</b>\n"
        "━━━━━━━━━━━━━━\n"
        "◈ <i>عند إضافة عمل يطابق طلب، أو الضغط على تلبية الطلب، يوصل للمستخدم إشعار «تم نشر طلبك ✓» مع زر المشاهدة تلقائياً!</i>"
    )

    toggle_btn_text = "○ إيقاف استقبال الطلبات" if req_enabled else "▸ فتح استقبال الطلبات"
    kb_rows = [
        [
            ikb(text=f"▣ تصفح وتلبية الطلبات ({pending_count})", style="danger", callback_data="view_pending_reqs"),
            ikb(text=f"❖ تصفح وحل البلاغات ({reports_count})", style="danger", callback_data="view_pending_reports")
        ],
        [
            ikb(text=toggle_btn_text, style="danger" if req_enabled else "success", callback_data="toggle_req_status"),
            ikb(text=f"تحديد العدد ({quota_max})", style="primary", callback_data="set_req_quota_btn")
        ],
        [
            ikb(text="يومي (24س)", style="success", callback_data="set_cycle_daily"),
            ikb(text="أسبوعي (7أيام)", style="success", callback_data="set_cycle_weekly"),
            ikb(text="شهري (30يوم)", style="success", callback_data="set_cycle_monthly")
        ],
        [
            ikb(text="◂ رجوع للوحة الرئيسية", style="primary", callback_data="menu_home")
        ]
    ]
    return text, InlineKeyboardMarkup(inline_keyboard=kb_rows)


async def verify_bot_in_channel(bot_inst: Bot, bot_label: str, chat_target, need_post: bool = True) -> str:
    try:
        me = await bot_inst.get_me()
        member = await bot_inst.get_chat_member(chat_id=chat_target, user_id=me.id)
        status_val = getattr(member.status, "value", str(member.status)).lower()
        if status_val == "creator":
            return f"✓ {bot_label}: مالك القناة"
        if status_val == "administrator":
            can_post = getattr(member, "can_post_messages", True)
            if need_post and can_post is False:
                return f"◈ {bot_label}: مشرف لكن <b>بدون صلاحية نشر الرسائل</b>!"
            return f"✓ {bot_label}: مشرف وجاهز"
        return f"✕ {bot_label}: مو مشرف بالقناة ({status_val})"
    except Exception as e:
        return f"✕ {bot_label}: ما يقدر يوصل للقناة — <code>{html.escape(str(e)[:60])}</code>"


async def test_channel_post_and_delete(bot_inst: Bot, chat_target) -> Tuple[bool, str]:
    try:
        msg = await bot_inst.send_message(
            chat_id=chat_target,
            text="✓ <b>رسالة فحص تلقائية من نظام فورتكس VORTEX</b> (سيتم حذفها فوراً...)",
            parse_mode="HTML"
        )
        try:
            await bot_inst.delete_message(chat_id=chat_target, message_id=msg.message_id)
        except Exception:
            pass
        return True, "تم إرسال وحذف رسالة الفحص بنجاح ✓"
    except Exception as e:
        return False, f"فشل إرسال رسالة تجريبية: {e}"


async def build_channels_diagnostic_report() -> str:
    channels_cfg = [
        ("◈ قناة خزن الأفلام (Storage)", "movies_channel_id", [(admin_bot, "بوت الإدارة", True), (sender_bot, "بوت الإرسال", False)]),
        ("▣ قناة خزن المسلسلات (Storage)", "series_channel_id", [(admin_bot, "بوت الإدارة", True), (sender_bot, "بوت الإرسال", False)]),
        ("✦ قناة خزن الأنمي (Storage)", "anime_channel_id", [(admin_bot, "بوت الإدارة", True), (sender_bot, "بوت الإرسال", False)]),
        ("◈ القناة العامة / الاشتراك الإجباري", "force_sub_channel_id", [(main_bot, "البوت الرئيسي", False), (admin_bot, "بوت الإدارة", False)]),
        ("⟐ قناة النشر العامة (الجديد)", "publish_channel_id", [(main_bot, "البوت الرئيسي", True), (admin_bot, "بوت الإدارة", True)]),
        ("◈ قناة نشر الأفلام", "movies_publish_channel_id", [(main_bot, "البوت الرئيسي", True), (admin_bot, "بوت الإدارة", True)]),
        ("▣ قناة نشر المسلسلات", "series_publish_channel_id", [(main_bot, "البوت الرئيسي", True), (admin_bot, "بوت الإدارة", True)]),
        ("✦ قناة نشر الأنمي", "anime_publish_channel_id", [(main_bot, "البوت الرئيسي", True), (admin_bot, "بوت الإدارة", True)]),
    ]
    lines = ["⟐ <b>تقرير فحص القنوات الـ 7 وصلاحيات البوتات</b>\n━━━━━━━━━━━━━━"]
    for label, key, bots_to_check in channels_cfg:
        raw_val = get_setting(key, "").strip()
        if not raw_val:
            lines.append(f"\n<b>{label}:</b>\n  ○ غير مربوطة حالياً")
            continue
        chat_target = normalize_chat_target(raw_val)
        title_str = ""
        for b_inst, _, _ in bots_to_check:
            try:
                c_obj = await b_inst.get_chat(chat_target)
                title_str = f" ({html.escape(c_obj.title or '')})"
                break
            except Exception:
                continue
        lines.append(f"\n<b>{label}:</b> <code>{html.escape(str(raw_val))}</code>{title_str}")
        for b_inst, b_name, need_post in bots_to_check:
            st_line = await verify_bot_in_channel(b_inst, b_name, chat_target, need_post=need_post)
            lines.append(f"  • {st_line}")
    lines.append("\n━━━━━━━━━━━━━━\n✦ <i>لتعيين أو تغيير أي قناة استخدم الأزرار أدناه أو من إعدادات الميني آب:</i>")
    return "\n".join(lines)


@admin_router.message(Command("channels"))
async def admin_channels_cmd(message: Message):
    if not is_user_admin(message.from_user.id):
        return
    wait = await message.answer("◷ جاري فحص القنوات وصلاحيات البوتات...")
    report_txt = await build_channels_diagnostic_report()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            ikb(text="▣ خزن الأفلام", style="primary", callback_data="set_movies_ch"),
            ikb(text="▣ خزن المسلسلات", style="primary", callback_data="set_series_ch"),
            ikb(text="▣ خزن الأنمي", style="primary", callback_data="set_anime_ch")
        ],
        [
            ikb(text="⟐ النشر العامة", style="primary", callback_data="set_publish_ch"),
            ikb(text="◈ الاشتراك الإجباري", style="primary", callback_data="set_force_ch")
        ],
        [
            ikb(text="◈ نشر الأفلام", style="primary", callback_data="set_movies_pub_ch"),
            ikb(text="▣ نشر المسلسلات", style="primary", callback_data="set_series_pub_ch"),
            ikb(text="✦ نشر الأنمي", style="primary", callback_data="set_anime_pub_ch")
        ],
        [ikb(text="↺ إعادة فحص القنوات", style="success", callback_data="quick_channels")],
        [ikb(text="◂ رجوع للوحة الرئيسية", style="primary", callback_data="menu_home")]
    ])
    await wait.edit_text(report_txt, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "quick_channels")
async def cb_quick_channels(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    await call.answer("◷ جاري فحص القنوات...")
    report_txt = await build_channels_diagnostic_report()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            ikb(text="▣ خزن الأفلام", style="primary", callback_data="set_movies_ch"),
            ikb(text="▣ خزن المسلسلات", style="primary", callback_data="set_series_ch"),
            ikb(text="▣ خزن الأنمي", style="primary", callback_data="set_anime_ch")
        ],
        [
            ikb(text="⟐ النشر العامة", style="primary", callback_data="set_publish_ch"),
            ikb(text="◈ الاشتراك الإجباري", style="primary", callback_data="set_force_ch")
        ],
        [
            ikb(text="◈ نشر الأفلام", style="primary", callback_data="set_movies_pub_ch"),
            ikb(text="▣ نشر المسلسلات", style="primary", callback_data="set_series_pub_ch"),
            ikb(text="✦ نشر الأنمي", style="primary", callback_data="set_anime_pub_ch")
        ],
        [ikb(text="↺ إعادة فحص القنوات", style="success", callback_data="quick_channels")],
        [ikb(text="◂ رجوع للوحة الرئيسية", style="primary", callback_data="menu_home")]
    ])
    try:
        await call.message.edit_text(report_txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await call.message.answer(report_txt, reply_markup=kb, parse_mode="HTML")


# =====================================================================
# إدارة سلاسل الأفلام الكاملة ببوت الإدارة (/collections)
# =====================================================================
async def show_collections_admin_view(target_msg: Message, edit: bool = False):
    auto_group_similar_movies()
    cols = get_all_collections(only_active=False)
    active_cnt = sum(1 for c in cols if c["is_active"])
    lines = [
        "❖ <b>نظام سلاسل الأفلام الكاملة (/collections)</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"◈ مجموع السلاسل المكتشفة: <b>{len(cols)}</b>  |  المفعّلة بالواجهة: <b>{active_cnt}</b>\n"
        "✦ <i>أي سلسلة توصل 3 أفلام فأكثر (أو تكتمل أجزاؤها) تتفعّل وتنزل بالواجهة تلقائياً!</i>\n"
    ]
    kb_rows = []
    for c in cols[:15]:
        st_lbl = "✓ مفعّلة" if c["is_active"] else f"◷ بانتظار الاكتمال ({c['parts_count']}/3)"
        lines.append(f"• <b>{html.escape(c['name'])}</b> — {c['parts_count']} أجزاء ({c['years_range']}) [{st_lbl}]")
        first_mid = c["parts"][0]["id"] if c.get("parts") else 0
        if first_mid:
            toggle_lbl = f"○ إخفاء «{c['name'][:16]}»" if (c["forced"] and c["parts_count"] < 3) else (f"✓ تفعيل «{c['name'][:16]}» يدوياً" if not c["is_active"] else f"❖ {c['name'][:20]} ({c['parts_count']} أجزاء)")
            kb_rows.append([InlineKeyboardButton(text=toggle_lbl, callback_data=f"coltoggle_{first_mid}")])
    kb_rows.append([InlineKeyboardButton(text="⟐ فحص وتحديث السلاسل من TMDb تلقائياً", callback_data="col_autoscan")])
    kb_rows.append([InlineKeyboardButton(text="◂ رجوع لقسم المحتوى", callback_data="menu_content")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    text = "\n".join(lines)
    if edit:
        try:
            await target_msg.edit_text(text, reply_markup=kb, parse_mode="HTML")
            return
        except Exception:
            pass
    await target_msg.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.message(Command("collections"))
async def admin_collections_cmd(message: Message):
    if not is_user_admin(message.from_user.id):
        return
    await show_collections_admin_view(message, edit=False)


@admin_router.callback_query(F.data == "quick_collections")
async def cb_quick_collections(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    await call.answer()
    await show_collections_admin_view(call.message, edit=True)


@admin_router.callback_query(F.data.startswith("coltoggle_"))
async def cb_col_toggle(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    mid = int(call.data.split("_")[1])
    media = get_media_by_id(mid)
    if media and media.get("collection_info"):
        cinfo = media["collection_info"]
        part_ids = [p["id"] for p in cinfo.get("parts", [])]
        new_forced = not bool(media.get("collection_forced"))
        set_movies_collection(part_ids, cinfo["name"], force_active=new_forced)
        await call.answer("✓ تم تحديث حالة السلسلة!")
    await show_collections_admin_view(call.message, edit=True)


@admin_router.callback_query(F.data == "col_autoscan")
async def cb_col_autoscan(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    await call.answer("◷ جاري فحص الأفلام وربط السلاسل...")
    auto_group_similar_movies()
    items = get_all_media()
    for it in items:
        if it.get("media_type") == "movie" and int(it.get("tmdb_id") or 0) > 0 and not it.get("collection_name"):
            det = await get_tmdb_details(int(it["tmdb_id"]), "movie")
            if det and (det.get("collection_id") or det.get("collection_name")):
                update_media_metadata(
                    it["id"],
                    collection_id=det.get("collection_id", 0),
                    collection_name=det.get("collection_name", ""),
                    collection_poster=det.get("collection_poster", ""),
                    collection_backdrop=det.get("collection_backdrop", ""),
                    collection_total_parts=det.get("collection_total_parts", 0)
                )
    auto_group_similar_movies()
    await show_collections_admin_view(call.message, edit=True)



# =====================================================================
# إدارة الأقسام الذكية ومربعات البوسترات المتقلبة (/sections)
# =====================================================================
def _infer_section_rules_from_title(title: str) -> Tuple[str, str, str]:
    t = title.strip()
    mtype = "all"
    if "مسلسل" in t or "دراما" in t:
        mtype = "tv"
    elif "أفلام" in t or "افلام" in t or "فيلم" in t:
        mtype = "movie"

    country = ""
    cat = ""
    if "عراق" in t:
        country = "العراق"
        cat = "مسلسل عراقي" if mtype == "tv" else ("فيلم عراقي" if mtype == "movie" else "مسلسل عراقي,فيلم عراقي")
    elif "ترك" in t:
        country = "تركيا"
        cat = "مسلسل تركي" if mtype == "tv" else ("فيلم تركي" if mtype == "movie" else "مسلسل تركي,فيلم تركي")
    elif "هند" in t or "بوليوود" in t:
        country = "الهند"
        cat = "مسلسل هندي" if mtype == "tv" else ("فيلم هندي" if mtype == "movie" else "مسلسل هندي,فيلم هندي")
    elif "كور" in t or "آسيو" in t or "اسيو" in t:
        country = "كوريا الجنوبية,اليابان,الصين"
        cat = "مسلسل آسيوي" if mtype == "tv" else ("فيلم آسيوي" if mtype == "movie" else "مسلسل آسيوي,فيلم آسيوي")
    elif "عرب" in t or "مصر" in t or "شام" in t or "سور" in t or "خليج" in t:
        country = "مصر,سوريا,لبنان,الكويت,السعودية,الإمارات,الأردن"
        cat = "مسلسل عربي" if mtype == "tv" else ("فيلم عربي" if mtype == "movie" else "مسلسل عربي,فيلم عربي")
    elif "أنمي" in t or "انمي" in t:
        cat = "أنمي"
    elif "أجنب" in t or "اجنب" in t or "أمر" in t or "هوليوود" in t:
        country = "الولايات المتحدة الأمريكية,المملكة المتحدة,إسبانيا,ألمانيا,فرنسا,كندا"
        cat = "مسلسل أجنبي" if mtype == "tv" else ("فيلم أجنبي" if mtype == "movie" else "مسلسل أجنبي,فيلم أجنبي")
    return mtype, country, cat


async def show_sections_admin_view(target_msg: Message, edit: bool = False):
    secs = get_smart_sections(only_non_empty=False)
    active_cnt = sum(1 for s in secs if s["is_enabled"] and s["items_count"] > 0)

    lines = [
        f"▣ <b>إدارة الأقسام الذكية ومربعات الرئيسية ({active_cnt} قسم ظاهر حالياً)</b>",
        "━━━━━━━━━━━━━━",
        "✦ <i>ملاحظة ذكية: أي قسم بيه (0 أعمال) يظل مخفي تلقائياً عن الزوار وما يطلع فارغ أبداً، وأول ما تضيف عمل إله يظهر مربعه المتقلب بالرئيسية تلقائياً!</i>\n"
    ]
    kb_rows = [
        [InlineKeyboardButton(text="➕ إضافة قسم جديد (ينربط تلقائياً)", callback_data="sec_new_start")]
    ]
    for s in secs:
        cnt = s["items_count"]
        if not s["is_enabled"]:
            st = "معطّل يدوياً"
        elif cnt > 0:
            st = f"ظاهر بالرئيسية ({cnt} عمل)"
        else:
            st = "⚪ بالانتظار (0 أعمال — مخفي تلقائياً)"
        lines.append(f"• <b>{html.escape(s['title'])}</b> — {st}")
        toggle_lbl = f"{'○ إخفاء' if s['is_enabled'] else '○ تفعيل'}: {s['title']} ({cnt})"
        row_btns = [InlineKeyboardButton(text=toggle_lbl[:42], callback_data=f"sectoggle_{s['section_key']}")]
        if str(s["section_key"]).startswith("custom_"):
            row_btns.append(InlineKeyboardButton(text="✕ حذف", callback_data=f"secdel_{s['section_key']}"))
        kb_rows.append(row_btns)

    kb_rows.append([InlineKeyboardButton(text="◂ رجوع لقسم المحتوى", callback_data="menu_content")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
    text = "\n".join(lines)
    if edit:
        try:
            await target_msg.edit_text(text, reply_markup=kb, parse_mode="HTML")
            return
        except Exception:
            pass
    await target_msg.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.message(Command("sections"))
async def admin_sections_cmd(message: Message):
    if not is_user_admin(message.from_user.id):
        return
    await show_sections_admin_view(message, edit=False)


@admin_router.callback_query(F.data == "quick_sections")
async def cb_quick_sections(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    await call.answer()
    await show_sections_admin_view(call.message, edit=True)


@admin_router.callback_query(F.data.startswith("sectoggle_"))
async def cb_sec_toggle(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    skey = call.data.split("sectoggle_", 1)[1]
    new_st = toggle_custom_section(skey)
    await call.answer("✓ تم تفعيل القسم!" if new_st else "○ تم إخفاء القسم عن الرئيسية!")
    await show_sections_admin_view(call.message, edit=True)


@admin_router.callback_query(F.data.startswith("secdel_"))
async def cb_sec_delete(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    skey = call.data.split("secdel_", 1)[1]
    delete_custom_section(skey)
    await call.answer("✕ تم حذف القسم المخصص!")
    await show_sections_admin_view(call.message, edit=True)


@admin_router.callback_query(F.data == "sec_new_start")
async def cb_sec_new_start(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    await state.set_state(AdminStates.adding_custom_section_title)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="إلغاء", callback_data="quick_sections")]
    ])
    await call.message.answer(
        "▣ <b>إضافة قسم جديد للميني آب</b>\n\n"
        "✦ اكتب هسة <b>اسم القسم الجديد</b> (مثلاً: <code>مسلسلات تركية</code> أو <code>مسلسلات عراقية</code> أو <code>مسلسلات رمضانية</code> أو <code>أفلام هندية</code>):\n"
        "✦ النظام راح يتعرف تلقائياً على الدولة والنوع ويربط الأعمال بيه، وما يظهر المربع للزوار إلا من ينزل بيه أول عمل!",
        reply_markup=kb,
        parse_mode="HTML"
    )
    await call.answer()


@admin_router.message(AdminStates.adding_custom_section_title)
async def process_new_section_title(message: Message, state: FSMContext):
    if not message.text:
        return
    title = message.text.strip()
    mtype, auto_country, auto_cat = _infer_section_rules_from_title(title)
    sec = upsert_custom_section(
        title=title,
        subtitle=f"أحدث {title} المختارة",
        media_type_filter=mtype,
        auto_country=auto_country,
        auto_category=auto_cat
    )
    await state.clear()
    rule_note = f"الربط التلقائي: الدولة (<code>{auto_country or 'يدوي/عام'}</code>)"
    await message.answer(
        f"✓ <b>تم إنشاء القسم «{html.escape(title)}» بنجاح!</b>\n"
        f"✦ {rule_note}\n"
        "✦ راح يظهر المربع المتقلب مالته بالرئيسية تلقائياً أول ما تضيف بيه عمل.",
        parse_mode="HTML"
    )
    await show_sections_admin_view(message, edit=False)


# =====================================================================
# فحص سلامة الملفات بقنوات الخزن (/check_files)
# =====================================================================
@admin_router.message(Command("check_files"))
async def admin_check_files_cmd(message: Message):
    if not is_user_admin(message.from_user.id):
        return
    await run_files_integrity_check(message)


@admin_router.callback_query(F.data == "quick_check_files")
async def cb_quick_check_files(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    await call.answer("◷ جاري فحص الملفات...")
    await run_files_integrity_check(call.message)


async def run_files_integrity_check(target_msg: Message):
    all_files = get_all_media_files_with_titles()
    if not all_files:
        await target_msg.answer("○ ماكو أي ملفات مسجلة بقاعدة البيانات حالياً.")
        return
    wait = await target_msg.answer(f"◷ جاري فحص {len(all_files)} ملف بقنوات الخزن...")
    ok_cnt = 0
    missing = []
    for f in all_files[:60]:
        st_chat = f.get("storage_chat_id")
        st_msg = int(f.get("storage_message_id") or 0)
        if not st_chat or not st_msg:
            missing.append(f)
            continue
        chat_target = normalize_chat_target(st_chat)
        # نفحص وجود الرسالة عبر محاولة تعديل الكابشن لنفس الكابشن أو قراءة الرسالة
        cap = f.get("storage_caption") or f"#id{f['media_id']} #file{f['id']} · {f.get('title','')} ({f.get('year','')}) · {f.get('quality','720')}p" + (f" · S{int(f.get('season') or 1):02d}E{int(f.get('episode') or 0):02d}" if int(f.get("episode") or 0) > 0 else "")
        try:
            await admin_bot.edit_message_caption(
                chat_id=chat_target,
                message_id=st_msg,
                caption=cap[:1020]
            )
            if not f.get("storage_caption"):
                update_media_file_record(f["id"], storage_caption=cap[:1020])
            ok_cnt += 1
        except Exception as e:
            err_s = str(e).lower()
            if "message is not modified" in err_s or "message can't be edited" in err_s:
                ok_cnt += 1
            elif "message to edit not found" in err_s or "message_id_invalid" in err_s:
                missing.append(f)
            else:
                ok_cnt += 1
        await asyncio.sleep(0.05)

    lines = [
        "◈ <b>نتيجة فحص الملفات وتسمية الكابشن بقناة الخزن</b>\n━━━━━━━━━━━━━━",
        f"✓ ملفات شغالة وموجودة بالقناة: <b>{ok_cnt}</b>",
        f"◈ ملفات مفقودة أو محذوفة من القناة: <b>{len(missing)}</b>"
    ]
    kb_rows = []
    if missing:
        lines.append("\n<b>الملفات المفقودة:</b>")
        for mf in missing[:10]:
            ep_s = f" ح{mf['episode']}" if mf.get("episode") else ""
            lines.append(f"• #file{mf['id']} — {html.escape(str(mf.get('title','')))} ({mf['quality']}p{ep_s})")
            kb_rows.append([InlineKeyboardButton(
                text=f"✕ حذف الملف المفقود #file{mf['id']} ({mf.get('title','')[:15]} {mf['quality']}p)",
                callback_data=f"delfile_{mf['media_id']}_{mf['id']}"
            )])
    kb_rows.append([InlineKeyboardButton(text="◂ رجوع لقسم المحتوى", callback_data="menu_content")])
    await wait.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="HTML")


async def cleanup_temp_files(files_list):
    for f in files_list:
        try:
            await admin_bot.delete_message(chat_id=int(f["storage_chat_id"]), message_id=int(f["storage_message_id"]))
        except Exception:
            pass


@admin_router.message(CommandStart())
async def admin_start(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        await message.answer("✕ هذا البوت مخصص لإدارة تطبيق فورتكس فقط.")
        return
    await state.clear()
    text, kb = build_admin_home_view()
    await message.answer(
        "◈ <b>تم تفعيل لوحة أزرار فورتكس الملونة (بدون حاجة لكتابة أوامر):</b>",
        reply_markup=build_admin_reply_keyboard(),
        parse_mode="HTML"
    )
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.message(F.text.in_({
    "＋ إضافة عمل جديد",
    "＋ إضافة حلقات / دقات",
    "⚡ الإضافة التلقائية والسحب",
    "◈ اللوحة الرئيسية",
    "◈ الطلبات والبلاغات",
    "✎ تعديل أو حذف",
    "❖ الإعدادات والقنوات",
    "▣ الغلاف والنسخ الاحتياطي",
}))
async def admin_reply_kb_router(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    txt = (message.text or "").strip()
    await state.clear()
    if "الإضافة التلقائية" in txt:
        t, kb = build_auto_ingest_hub_view()
        await message.answer(t, reply_markup=kb, parse_mode="HTML")
    elif "إضافة عمل جديد" in txt:
        await start_add_flow(message, state)
    elif "إضافة حلقات" in txt:
        await show_append_list(message)
    elif "الطلبات والبلاغات" in txt:
        t, kb = build_requests_section_view()
        await message.answer(t, reply_markup=kb, parse_mode="HTML")
    elif "تعديل أو حذف" in txt:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [ikb(text="✎ تعديل عمل أو استبدال ملف أو دمج", style="primary", callback_data="quick_edit")],
            [ikb(text="✕ حذف عمل أو دقة محددة", style="danger", callback_data="quick_del")],
            [ikb(text="✦ تحديد اختيار الليلة", style="primary", callback_data="quick_tonight")],
            [ikb(text="◂ اللوحة الرئيسية", style="primary", callback_data="menu_home")]
        ])
        await message.answer("اختار العملية المطلوبة:", reply_markup=kb)
    elif "الإعدادات والقنوات" in txt:
        t, kb = build_content_section_view()
        await message.answer(t, reply_markup=kb, parse_mode="HTML")
    elif "الغلاف والنسخ الاحتياطي" in txt:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [ikb(text="▣ إدارة الغلاف الموحد للفيديوهات", style="success", callback_data="set_video_cover")],
            [ikb(text="⟐ حالة ومزامنة Firebase", style="success", callback_data="fb_menu_open")],
            [ikb(text="▣ سحب نسخة احتياطية (vortex.db)", style="success", callback_data="quick_backup")],
            [ikb(text="◂ اللوحة الرئيسية", style="primary", callback_data="menu_home")]
        ])
        await message.answer("▣ <b>إدارة الغلاف الموحد والنسخ الاحتياطي:</b>", reply_markup=kb, parse_mode="HTML")
    else:
        t, kb = build_admin_home_view()
        await message.answer(t, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "fb_menu_open")
async def cb_fb_menu_open(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            ikb(text="⟐ رفع البيانات المحلية لـ Firebase", style="success", callback_data="fb_sync_confirm"),
            ikb(text="⟐ استعادة من Firebase", style="primary", callback_data="fb_restore_confirm")
        ],
        [ikb(text="◂ رجوع للوحة الرئيسية", style="primary", callback_data="menu_home")]
    ])
    await call.message.answer(
        f"⟐ <b>إدارة سحابة Firebase Realtime Database</b>\n"
        f"◈ الحالة الحالية: <b>{firebase_status_line()}</b>",
        reply_markup=kb,
        parse_mode="HTML"
    )
    await call.answer()


@admin_router.message(Command("content"))
async def admin_content_cmd(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    await state.clear()
    text, kb = build_content_section_view()
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.message(Command("settings"))
async def admin_settings_cmd(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    await state.clear()
    text, kb = build_settings_section_view()
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.message(Command("requests"))
async def admin_requests_cmd(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    await state.clear()
    await run_auto_sweep_pending_requests_and_notify()
    text, kb = build_requests_section_view()
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "menu_home")
async def cb_menu_home(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    await state.clear()
    text, kb = build_admin_home_view()
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@admin_router.callback_query(F.data == "menu_content")
async def cb_menu_content(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    await state.clear()
    text, kb = build_content_section_view()
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@admin_router.callback_query(F.data == "menu_settings")
async def cb_menu_settings(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    await state.clear()
    text, kb = build_settings_section_view()
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@admin_router.callback_query(F.data == "menu_requests")
async def cb_menu_requests(call: CallbackQuery, state: FSMContext):
    if not has_admin_perm(call.from_user.id, "can_requests"):
        await call.answer("✕ ليست لديك صلاحية إدارة الطلبات والبلاغات", show_alert=True)
        return
    await state.clear()
    text, kb = build_requests_section_view()
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


# =====================================================================
# قسم المستخدمين والإحصائيات اليومية الكامل (@VortexAdminBot)
# =====================================================================
def build_users_section_view() -> Tuple[str, InlineKeyboardMarkup]:
    dash = get_users_dashboard_data(limit_new=15, limit_top=10)
    sm = dash["summary"]
    notify_on = sm["notify_new_users"]
    notify_lbl = "◈ إشعار دخول مستخدم جديد: مفعّل ✓" if notify_on else " إشعار دخول مستخدم جديد: متوقف ✕"

    text = (
        "◈ <b>قسم المستخدمين والإحصائيات اليومية</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"◈ <b>إجمالي المستخدمين:</b> <b>{sm['total_users']}</b> مستخدم\n"
        f"✦ <b>المستخدمون الجدد اليوم:</b> <b>+{sm['new_users_today']}</b> مستخدم\n"
        f"▸ <b>سحب المحتوى اليوم:</b> <b>{sm['pulls_today']}</b> عملية سحب/مشاهدة\n"
        f"▣ <b>إجمالي سحب المحتوى الكلي:</b> <b>{sm['total_pulls']}</b> عملية\n"
        f"❖ <b>الدعوات الناجحة اليوم:</b> <b>{sm['referrals_today']}</b>  (الإجمالي: <b>{sm['total_referrals']}</b>)\n"
        f"◈ <b>مشاركات الأعمال اليوم:</b> <b>{sm['shares_today']}</b>  (الإجمالي: <b>{sm['total_shares']}</b>)\n"
        f"◈ <b>إشعار الدخول الجديد بالبوت:</b> <b>{'شغّال ✓' if notify_on else 'متوقف ✕'}</b>\n"
        "━━━━━━━━━━━━━━\n"
        "▾ <b>اختار الكشف أو الإجراء المطلوب:</b>"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f" عرض المستخدمين الجدد (+{sm['new_users_today']} اليوم)", callback_data="usr_view_new"),
            InlineKeyboardButton(text=" أعلى من سحب محتوى + محتواهم", callback_data="usr_view_top_pullers")
        ],
        [
            InlineKeyboardButton(text=f"▸ سحب المحتوى اليوم ({sm['pulls_today']})", callback_data="usr_view_today_pulls"),
            InlineKeyboardButton(text=f"⟐ الدعوات والمشاركات اليوم ({sm['referrals_today'] + sm['shares_today']})", callback_data="usr_view_today_refs")
        ],
        [
            InlineKeyboardButton(text=notify_lbl, callback_data="toggle_new_user_notify")
        ],
        [
            InlineKeyboardButton(text="✦ منح/سحب VIP لمستخدم", callback_data="admin_grant_vip"),
            InlineKeyboardButton(text="＋ إضافة نقاط لمستخدم", callback_data="admin_add_pts")
        ],
        [
            InlineKeyboardButton(text="◂ رجوع للوحة الرئيسية", callback_data="menu_home")
        ]
    ])
    return text, kb


@admin_router.message(Command("users"))
async def admin_users_cmd(message: Message, state: FSMContext):
    if not has_admin_perm(message.from_user.id, "can_users"):
        await message.answer("✕ ليست لديك صلاحية الدخول لقسم المستخدمين.")
        return
    await state.clear()
    text, kb = build_users_section_view()
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "menu_users")
async def cb_menu_users(call: CallbackQuery, state: FSMContext):
    if not has_admin_perm(call.from_user.id, "can_users"):
        await call.answer("✕ ليست لديك صلاحية إدارة المستخدمين", show_alert=True)
        return
    await state.clear()
    text, kb = build_users_section_view()
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@admin_router.callback_query(F.data == "toggle_new_user_notify")
async def cb_toggle_new_user_notify(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    curr = get_setting("notify_new_users", "1")
    new_v = "0" if curr == "1" else "1"
    set_setting("notify_new_users", new_v)
    status_str = "◈ تم تشغيل إشعار دخول المستخدمين الجدد!" if new_v == "1" else " تم إيقاف إشعار دخول المستخدمين الجدد."
    await call.answer(status_str, show_alert=True)
    text, kb = build_users_section_view()
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass


@admin_router.callback_query(F.data == "usr_view_new")
async def cb_usr_view_new(call: CallbackQuery):
    if not has_admin_perm(call.from_user.id, "can_users"):
        await call.answer("✕ غير مصرح", show_alert=True)
        return
    dash = get_users_dashboard_data(limit_new=20, limit_top=5)
    new_users = dash["new_users"]
    lines = [
        f" <b>أحدث المستخدمين المسجلين بالبوت (الجدد اليوم: {dash['summary']['new_users_today']})</b>\n"
        "━━━━━━━━━━━━━━"
    ]
    kb_rows = []
    if not new_users:
        lines.append("○ لا يوجد مستخدمون بعد.")
    else:
        for idx, u in enumerate(new_users[:18], 1):
            uname = f" (@{html.escape(u['username'])})" if u.get("username") else ""
            vip_tag = " · ✦VIP" if u.get("is_vip") else ""
            ref_tag = f" · دعوة من <code>{u['referred_by']}</code>" if u.get("referred_by") else ""
            lines.append(
                f"{idx}. <b>{html.escape(u['first_name'])}</b>{uname} — <code>{u['user_id']}</code>{vip_tag}\n"
                f"   ◷ {u.get('joined_at', '')[:16]} · سحباته: <b>{u['pulls_count']}</b>{ref_tag}"
            )
            if u["pulls_count"] > 0 and len(kb_rows) < 6:
                kb_rows.append([InlineKeyboardButton(
                    text=f"▣ محتوى {u['first_name'][:18]} ({u['pulls_count']} سحبة)",
                    callback_data=f"usr_pulled_{u['user_id']}"
                )])
    kb_rows.append([InlineKeyboardButton(text="◂ رجوع لقسم المستخدمين", callback_data="menu_users")])
    await call.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="HTML")
    await call.answer()


@admin_router.callback_query(F.data == "usr_view_top_pullers")
async def cb_usr_view_top_pullers(call: CallbackQuery):
    if not has_admin_perm(call.from_user.id, "can_users"):
        await call.answer("✕ غير مصرح", show_alert=True)
        return
    dash = get_users_dashboard_data(limit_new=5, limit_top=15)
    top_list = dash["top_pullers"]
    lines = [
        " <b>أعلى المستخدمين سحباً للمحتوى + المحتوى الخاص بهم</b>\n"
        "━━━━━━━━━━━━━━"
    ]
    kb_rows = []
    if not top_list:
        lines.append("○ لا توجد عمليات سحب مسجلة بعد.")
    else:
        for idx, u in enumerate(top_list[:12], 1):
            uname = f" (@{html.escape(u['username'])})" if u.get("username") else ""
            vip_b = " ✦VIP" if u.get("is_vip") else ""
            sample_titles = []
            for it in (u.get("pulled_items") or [])[:3]:
                ep_s = f" ح{it['episode']}" if int(it.get("episode") or 0) > 0 else ""
                sample_titles.append(f"{html.escape(str(it.get('media_title') or ''))}{ep_s} ({it.get('quality','720')}p)")
            sample_str = " ، ".join(sample_titles) if sample_titles else "—"
            lines.append(
                f"{idx}. <b>{html.escape(u['first_name'])}</b>{uname}{vip_b} (<code>{u['user_id']}</code>)\n"
                f"   ▸ السحبات: <b>{u['pulls_count']}</b> · آخر ما سحب: <i>{sample_str}</i>"
            )
            kb_rows.append([InlineKeyboardButton(
                text=f"▣ كل المحتوى المسحوب لـ {u['first_name'][:16]} ({u['pulls_count']})",
                callback_data=f"usr_pulled_{u['user_id']}"
            )])
    kb_rows.append([InlineKeyboardButton(text="◂ رجوع لقسم المستخدمين", callback_data="menu_users")])
    await call.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="HTML")
    await call.answer()


@admin_router.callback_query(F.data.startswith("usr_pulled_"))
async def cb_usr_pulled_detail(call: CallbackQuery):
    if not has_admin_perm(call.from_user.id, "can_users"):
        await call.answer("✕ غير مصرح", show_alert=True)
        return
    uid = int(call.data.split("_")[2])
    info = get_user_pulled_content(uid, limit=30)
    uname = f" (@{html.escape(info['username'])})" if info.get("username") else ""
    lines = [
        f"▣ <b>المحتوى الخاص بالمستخدم: {html.escape(info['first_name'])}{uname}</b>\n"
        f"◈ الأيدي: <code>{uid}</code>  |  إجمالي السحب: <b>{info['pulls_count']}</b>  |  النقاط: <b>{info['points']}</b>\n"
        "━━━━━━━━━━━━━━"
    ]
    items = info.get("pulled_items") or []
    if not items:
        lines.append("○ لم يقم بسحب أي محتوى بعد.")
    else:
        for idx, it in enumerate(items[:25], 1):
            ep_s = f" · S{int(it.get('season') or 1):02d}E{int(it.get('episode') or 0):02d}" if int(it.get("episode") or 0) > 0 else ""
            mode_s = "بث VIP ▸" if it.get("pull_mode") == "vip_stream" else "بوت السيرفر ▸"
            lines.append(
                f"{idx}. <b>{html.escape(str(it.get('media_title') or ''))}</b>{ep_s} — <code>{it.get('quality','720')}p</code> [{mode_s}] "
                f"({str(it.get('created_at') or '')[:16]})"
            )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=" رجوع لأعلى الساحبين", callback_data="usr_view_top_pullers")],
        [InlineKeyboardButton(text="◂ رجوع لقسم المستخدمين", callback_data="menu_users")]
    ])
    await call.message.edit_text("\n".join(lines), reply_markup=kb, parse_mode="HTML")
    await call.answer()


@admin_router.callback_query(F.data == "usr_view_today_pulls")
async def cb_usr_view_today_pulls(call: CallbackQuery):
    if not has_admin_perm(call.from_user.id, "can_users"):
        await call.answer("✕ غير مصرح", show_alert=True)
        return
    dash = get_users_dashboard_data(limit_new=5, limit_top=5)
    pulls = dash["today_pulls_list"]
    lines = [
        f"▸ <b>سجل سحب المحتوى اليوم ({dash['summary']['pulls_today']} عملية اليوم)</b>\n"
        "━━━━━━━━━━━━━━"
    ]
    if not pulls:
        lines.append("○ لا توجد عمليات سحب مسجلة اليوم حتى الآن.")
    else:
        for idx, p in enumerate(pulls[:22], 1):
            ep_s = f" ح{p['episode']}" if int(p.get("episode") or 0) > 0 else ""
            mode_s = "VIP▸" if p.get("pull_mode") == "vip_stream" else "بوت▸"
            lines.append(
                f"{idx}. <b>{html.escape(str(p.get('user_name') or 'مستخدم'))}</b> (<code>{p['user_id']}</code>) ◂ "
                f"<b>{html.escape(str(p.get('media_title') or ''))}{ep_s}</b> [{p.get('quality','720')}p · {mode_s}]"
            )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↻ تحديث", callback_data="usr_view_today_pulls")],
        [InlineKeyboardButton(text="◂ رجوع لقسم المستخدمين", callback_data="menu_users")]
    ])
    await call.message.edit_text("\n".join(lines), reply_markup=kb, parse_mode="HTML")
    await call.answer()


@admin_router.callback_query(F.data == "usr_view_today_refs")
async def cb_usr_view_today_refs(call: CallbackQuery):
    if not has_admin_perm(call.from_user.id, "can_users"):
        await call.answer("✕ غير مصرح", show_alert=True)
        return
    dash = get_users_dashboard_data(limit_new=5, limit_top=5)
    refs = dash["today_referrals_list"]
    shares = dash["today_shares_list"]
    sm = dash["summary"]
    lines = [
        f"⟐ <b>الدعوات والمشاركات اليوم</b>\n"
        f"✦ دعوات اليوم: <b>{sm['referrals_today']}</b> (الإجمالي: <b>{sm['total_referrals']}</b>)\n"
        f"◈ مشاركات اليوم: <b>{sm['shares_today']}</b> (الإجمالي: <b>{sm['total_shares']}</b>)\n"
        "━━━━━━━━━━━━━━\n"
        "<b>أحدث الدعوات اليوم:</b>"
    ]
    if not refs:
        lines.append("• لا توجد دعوات مسجلة اليوم بعد.")
    else:
        for r in refs[:12]:
            lines.append(
                f"• <b>{html.escape(str(r.get('referrer_name') or ''))}</b> (<code>{r['referrer_id']}</code>) دعا ◂ "
                f"<b>{html.escape(str(r.get('invited_name') or ''))}</b> (<code>{r['invited_id']}</code>)"
            )
    lines.append("\n<b>أحدث مشاركات الأعمال اليوم:</b>")
    if not shares:
        lines.append("• لا توجد مشاركات مسجلة اليوم بعد.")
    else:
        for s in shares[:12]:
            lines.append(
                f"• <b>{html.escape(str(s.get('user_name') or ''))}</b> (<code>{s['user_id']}</code>) شارك ◂ "
                f"<b>{html.escape(str(s.get('media_title') or 'التطبيق'))}</b>"
            )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="↻ تحديث", callback_data="usr_view_today_refs")],
        [InlineKeyboardButton(text="◂ رجوع لقسم المستخدمين", callback_data="menu_users")]
    ])
    await call.message.edit_text("\n".join(lines), reply_markup=kb, parse_mode="HTML")
    await call.answer()


# =====================================================================
# قسم المشرفين والصلاحيات المفصلة وحماية البوت (@VortexAdminBot)
# =====================================================================
def build_admins_section_view(viewer_id: int) -> Tuple[str, InlineKeyboardMarkup]:
    admins_list = get_all_admins_detailed()
    is_own = is_owner_id(viewer_id)
    lines = [
        "◈ <b>قسم المشرفين والصلاحيات وحماية البوت</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"★ <b>الملاك الأساسيون (ADMIN_IDS):</b> <code>{', '.join(str(x) for x in ADMIN_IDS)}</code>\n"
        f"◈ <b>إجمالي المشرفين المضافين:</b> <b>{len(admins_list)}</b>\n"
        "◈ <b>حماية البوت الذكية (Anti-Sabotage):</b> <b>مفعّلة ✓</b>\n"
        "   • تمنع عزل المالك الأساسي أو تعديل صلاحياته.\n"
        "   • تجمد أي مشرف تلقائياً إذا حاول حذف أكثر من 3 أعمال خلال 10 دقائق وترسل إنذاراً للمالك.\n"
        "   • تقفل إعدادات القنوات و Firebase وإدارة المشرفين بيد المالك الأساسي فقط.\n"
        "━━━━━━━━━━━━━━\n"
        "▾ <b>قائمة المشرفين الحاليين (اضغط على أي مشرف لعرض معلوماته وتعديل صلاحياته أو عزله):</b>"
    ]
    kb_rows = []
    for a in admins_list:
        uid = a["user_id"]
        uname = f" (@{a['username']})" if a.get("username") else ""
        st_badge = "★ مالك" if a["is_owner"] else ("○ مجمد" if a["is_frozen"] else "✓ مشرف")
        p_count = sum(1 for v in a["permissions"].values() if v)
        lines.append(
            f"• <b>{html.escape(a['first_name'])}</b>{html.escape(uname)} — <code>{uid}</code> [{st_badge} · {p_count}/6 صلاحيات]"
        )
        kb_rows.append([InlineKeyboardButton(
            text=f"{st_badge} · {a['first_name'][:18]} ({uid}) [{p_count}/6]",
            callback_data=f"adm_manage_{uid}"
        )])

    if is_own:
        kb_rows.append([InlineKeyboardButton(text="＋ إضافة مشرف جديد وتحديد صلاحياته", callback_data="add_new_admin")])
    kb_rows.append([InlineKeyboardButton(text="◂ رجوع للوحة الرئيسية", callback_data="menu_home")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=kb_rows)


def build_single_admin_card_view(target_uid: int, viewer_id: int) -> Tuple[str, InlineKeyboardMarkup]:
    rec = get_admin_record(target_uid)
    if not rec:
        return "◈ المشرف غير موجود أو تم عزله.", InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◂ رجوع لقسم المشرفين", callback_data="menu_admins")]
        ])

    is_own_target = rec["is_owner"]
    viewer_is_owner = is_owner_id(viewer_id)
    uname = f"@{rec['username']}" if rec.get("username") else "بدون يوزر"
    status_ar = "★ المالك الأساسي (صلاحيات كاملة محمية)" if is_own_target else ("○ مجمد أمنياً (موقوف عن العمل)" if rec["is_frozen"] else "✓ مشرف نشط")

    perms = rec["permissions"]
    perm_lines = []
    for k in PERM_KEYS:
        mark = "✓ مسموح" if perms.get(k) else "✕ ممنوع"
        perm_lines.append(f"• {PERM_LABELS_AR[k]}: <b>{mark}</b>")

    text = (
        f"◈ <b>بطاقة معلومات وصلاحيات المشرف</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"◈ <b>الاسم:</b> {html.escape(rec['first_name'])}\n"
        f"◈ <b>اليوزر:</b> {html.escape(uname)}\n"
        f"◈ <b>الأيدي:</b> <code>{rec['user_id']}</code>\n"
        f"◈ <b>الحالة:</b> <b>{status_ar}</b>\n"
        f"◈ <b>تاريخ الإضافة:</b> <code>{rec.get('added_at') or '—'}</code>\n"
        f"◈ <b>عدد العمليات المسجلة:</b> <b>{rec.get('actions_count', 0)}</b>\n"
        "━━━━━━━━━━━━━━\n"
        "<b>الصلاحيات الممنوحة:</b>\n" + "\n".join(perm_lines)
    )

    kb_rows = []
    if viewer_is_owner and not is_own_target:
        for k in PERM_KEYS:
            cur_v = perms.get(k, False)
            btn_lbl = f"{'✓' if cur_v else '✕'} {PERM_LABELS_AR[k]}"
            kb_rows.append([InlineKeyboardButton(text=btn_lbl, callback_data=f"adm_tperm_{target_uid}_{k}")])
        freeze_btn = (
            InlineKeyboardButton(text="✓ فك التجميد عن المشرف", callback_data=f"adm_unfreeze_{target_uid}")
            if rec["is_frozen"] else
            InlineKeyboardButton(text="○ تجميد صلاحيات المشرف مؤقتاً", callback_data=f"adm_freeze_{target_uid}")
        )
        kb_rows.append([freeze_btn])
        kb_rows.append([InlineKeyboardButton(text="✕ عزل هذا المشرف نهائياً من البوت", callback_data=f"adm_remove_{target_uid}")])

    kb_rows.append([InlineKeyboardButton(text="◂ رجوع لقسم المشرفين", callback_data="menu_admins")])
    return text, InlineKeyboardMarkup(inline_keyboard=kb_rows)


@admin_router.message(Command("admins"))
async def admin_admins_cmd(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    await state.clear()
    text, kb = build_admins_section_view(message.from_user.id)
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "menu_admins")
async def cb_menu_admins(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    await state.clear()
    text, kb = build_admins_section_view(call.from_user.id)
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@admin_router.callback_query(F.data.startswith("adm_manage_"))
async def cb_adm_manage(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    target_uid = int(call.data.split("_")[2])
    text, kb = build_single_admin_card_view(target_uid, call.from_user.id)
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@admin_router.callback_query(F.data.startswith("adm_tperm_"))
async def cb_adm_toggle_perm(call: CallbackQuery):
    if not is_owner_id(call.from_user.id):
        await call.answer("★ تعديل صلاحيات المشرفين متاح للمالك الأساسي فقط!", show_alert=True)
        return
    parts = call.data.split("_", 3)
    target_uid = int(parts[2])
    perm_key = parts[3]
    if is_owner_id(target_uid):
        await call.answer("★ لا يمكن تقليص صلاحيات المالك الأساسي!", show_alert=True)
        return
    toggle_admin_permission(target_uid, perm_key)
    text, kb = build_single_admin_card_view(target_uid, call.from_user.id)
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer("✓ تم تحديث الصلاحية ومزامنتها مع Firebase")


@admin_router.callback_query(F.data.startswith("adm_freeze_"))
async def cb_adm_freeze(call: CallbackQuery):
    if not is_owner_id(call.from_user.id):
        await call.answer("★ متاح للمالك الأساسي فقط!", show_alert=True)
        return
    target_uid = int(call.data.split("_")[2])
    set_admin_frozen(target_uid, True)
    text, kb = build_single_admin_card_view(target_uid, call.from_user.id)
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer("○ تم تجميد صلاحيات المشرف فوراً", show_alert=True)


@admin_router.callback_query(F.data.startswith("adm_unfreeze_"))
async def cb_adm_unfreeze(call: CallbackQuery):
    if not is_owner_id(call.from_user.id):
        await call.answer("★ متاح للمالك الأساسي فقط!", show_alert=True)
        return
    target_uid = int(call.data.split("_")[2])
    set_admin_frozen(target_uid, False)
    text, kb = build_single_admin_card_view(target_uid, call.from_user.id)
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer("✓ تم فك التجميد عن المشرف", show_alert=True)


@admin_router.callback_query(F.data.startswith("adm_remove_"))
async def cb_adm_remove(call: CallbackQuery):
    if not is_owner_id(call.from_user.id):
        await call.answer("★ عزل المشرفين متاح للمالك الأساسي فقط!", show_alert=True)
        return
    target_uid = int(call.data.split("_")[2])
    if is_owner_id(target_uid):
        await call.answer("★ لا يمكن عزل المالك الأساسي!", show_alert=True)
        return
    ok = remove_admin_id(target_uid)
    await call.answer("✕ تم عزل المشرف وسحب جميع صلاحياته بنجاح!" if ok else "المشرف غير موجود", show_alert=True)
    text, kb = build_admins_section_view(call.from_user.id)
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "toggle_post_btn_mode")
async def cb_toggle_post_btn_mode(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    curr = get_setting("channel_post_btn_mode", "webapp")
    new_v = "direct" if curr == "webapp" else "webapp"
    set_setting("channel_post_btn_mode", new_v)
    text, kb = build_content_section_view()
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer("✓ تم تبديل نظام أزرار منشور القناة!")


# ---- إدارة الطلبات والبلاغات في بوت الإدارة ----
@admin_router.callback_query(F.data == "toggle_req_status")
async def cb_toggle_req_status(call: CallbackQuery):
    curr = get_setting("requests_enabled", "1")
    new_v = "0" if curr == "1" else "1"
    set_setting("requests_enabled", new_v)
    text, kb = build_requests_section_view()
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer("تم تبديل حالة استقبال الطلبات")


@admin_router.callback_query(F.data == "set_cycle_daily")
async def cb_set_cycle_daily(call: CallbackQuery):
    set_setting("requests_cycle", "daily")
    text, kb = build_requests_section_view()
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer("تم تعيين التجديد: كل يوم (24 ساعة)")


@admin_router.callback_query(F.data == "set_cycle_weekly")
async def cb_set_cycle_weekly(call: CallbackQuery):
    set_setting("requests_cycle", "weekly")
    text, kb = build_requests_section_view()
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer("تم تعيين التجديد: كل أسبوع (7 أيام)")


@admin_router.callback_query(F.data == "set_cycle_monthly")
async def cb_set_cycle_monthly(call: CallbackQuery):
    set_setting("requests_cycle", "monthly")
    text, kb = build_requests_section_view()
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer("تم تعيين التجديد: كل شهر (30 يوماً)")


@admin_router.callback_query(F.data == "set_req_quota_btn")
async def cb_set_req_quota_btn(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.setting_req_quota_num)
    await call.message.answer("◈ أرسل عدد الطلبات المسموح بها للمستخدم في كل دورة (مثال: <code>1</code> أو <code>3</code>):", parse_mode="HTML")
    await call.answer()


@admin_router.message(AdminStates.setting_req_quota_num)
async def save_req_quota_num(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().isdigit():
        await message.answer("◈ أرسل رقماً صحيحاً فقط (مثال: 1 أو 3).")
        return
    num = str(int(message.text.strip()))
    set_setting("requests_quota_max", num)
    await state.clear()
    text, kb = build_requests_section_view()
    await message.answer(f"✓ تم تحديد حصة الطلبات بـ {num} طلب لكل دورة!")
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "view_pending_reqs")
async def cb_view_pending_reqs(call: CallbackQuery):
    reqs = get_pending_media_requests(15)
    if not reqs:
        await call.answer("ماكو أي طلبات معلقة هسة!", show_alert=True)
        text, kb = build_requests_section_view()
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            pass
        return
    lines = ["▣ <b>طلبات المستخدمين المعلقة:</b>\n"]
    buttons = []
    for r in reqs:
        year_s = f" ({r.get('prod_year')})" if r.get("prod_year") else ""
        cnt_s = f" · ◈ مطلوب من <b>{r['requesters_count']}</b>" if r.get("requesters_count", 1) > 1 else ""
        lines.append(
            f"✦ #{r['id']} · 📋 <code>{html.escape(r['request_text'])}</code><b>{year_s}</b>{cnt_s}\n"
            f"   ◈ من: {html.escape(r['user_name'] or 'مستخدم')} (<code>{r['user_id']}</code>)"
        )
        buttons.append([
            InlineKeyboardButton(text=f"✓ تلبية وإشعار #{r['id']} ({r['request_text'][:14]})", callback_data=f"fulfill_req_{r['id']}"),
            InlineKeyboardButton(text=f"✕ رفض #{r['id']}", callback_data=f"reject_req_{r['id']}")
        ])
    buttons.append([InlineKeyboardButton(text="◂ رجوع", callback_data="menu_requests")])
    await call.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@admin_router.callback_query(F.data.startswith("fulfill_req_"))
async def cb_fulfill_req(call: CallbackQuery):
    req_id = int(call.data.split("_")[2])
    fulfilled_list = mark_media_request_fulfilled(req_id)
    sent_cnt = await notify_users_request_fulfilled(fulfilled_list)
    await call.answer(f"✓ تم تحديد الطلب كمكتمل وإرسال إشعار لـ {sent_cnt} مستخدم!", show_alert=True)
    await cb_view_pending_reqs(call)


@admin_router.callback_query(F.data.startswith("reject_req_"))
async def cb_reject_req(call: CallbackQuery):
    req_id = int(call.data.split("_")[2])
    mark_media_request_rejected(req_id)
    await call.answer("✕ تم رفض الطلب.")
    await cb_view_pending_reqs(call)


@admin_router.callback_query(F.data == "view_pending_reports")
async def cb_view_pending_reports(call: CallbackQuery):
    reps = get_pending_media_reports(15)
    if not reps:
        await call.answer("ماكو أي بلاغات معلقة هسة! ✓", show_alert=True)
        text, kb = build_requests_section_view()
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            pass
        return
    lines = ["◈ <b>البلاغات المعلقة عن الأعمال:</b>\n"]
    buttons = []
    for rp in reps:
        lines.append(
            f"✦ بلاغ #{rp['id']} على: <b>{html.escape(rp['media_title'] or '')}</b> (#id{rp['media_id']})\n"
            f"   ◈ السبب: {html.escape(rp['reason_type'] or '')}\n"
            f"   ✎ التفاصيل: {html.escape(rp['details'] or '—')}\n"
            f"   ◈ من: {html.escape(rp['user_name'] or '')} (<code>{rp['user_id']}</code>)"
        )
        buttons.append([
            InlineKeyboardButton(text=f"✓ تم الحل #{rp['id']}", callback_data=f"resolve_rep_{rp['id']}"),
            InlineKeyboardButton(text=f"✎ تعديل العمل #{rp['media_id']}", callback_data=f"edit_open_{rp['media_id']}")
        ])
    buttons.append([InlineKeyboardButton(text="◂ رجوع", callback_data="menu_requests")])
    await call.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer()


@admin_router.callback_query(F.data.startswith("resolve_rep_"))
async def cb_resolve_rep(call: CallbackQuery):
    rep_id = int(call.data.split("_")[2])
    resolve_media_report(rep_id)
    await call.answer("✓ تم إغلاق البلاغ!")
    await cb_view_pending_reports(call)


@admin_router.message(Command("cancel"))
async def admin_cancel(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    data = await state.get_data()
    temp_files = data.get("uploaded_files", [])
    if temp_files:
        await cleanup_temp_files(temp_files)
    await state.clear()
    text, kb = build_admin_home_view()
    await message.answer("تم الإلغاء ✕\n✦ تم إلغاء العملية وحذف الملفات المؤقتة.")
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "cancel_op")
async def admin_cancel_cb(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    data = await state.get_data()
    temp_files = data.get("uploaded_files", [])
    if temp_files:
        await cleanup_temp_files(temp_files)
    await state.clear()
    text, kb = build_admin_home_view()
    await call.message.answer("تم الإلغاء ✕\n✦ تم إلغاء العملية وحذف الملفات المؤقتة.")
    await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


# ----------------- النسخ الاحتياطي واستعادته (/backup) -----------------
@admin_router.message(Command("backup"))
async def admin_backup_cmd(message: Message):
    if not is_user_admin(message.from_user.id):
        return
    if not os.path.exists(DB_PATH):
        await message.answer("◈ قاعدة البيانات فارغة حالياً.")
        return
    db_checkpoint()
    doc = FSInputFile(DB_PATH, filename="vortex_backup.db")
    await message.answer_document(
        document=doc,
        caption="▣ <b>النسخة الاحتياطية لقاعدة بيانات فورتكس</b>\n✦ البيانات محفوظة بسحابة Firebase تلقائياً أيضاً.",
        parse_mode="HTML"
    )


@admin_router.callback_query(F.data == "quick_backup")
async def cb_quick_backup(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    if os.path.exists(DB_PATH):
        db_checkpoint()
        doc = FSInputFile(DB_PATH, filename="vortex_backup.db")
        await call.message.answer_document(
            document=doc,
            caption="▣ <b>النسخة الاحتياطية لقاعدة بيانات فورتكس</b>\n✦ البيانات محفوظة بسحابة Firebase تلقائياً أيضاً.",
            parse_mode="HTML"
        )
    await call.answer()


@admin_router.message(F.document.file_name.endswith(".db"))
async def restore_db_backup(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    cur_state = await state.get_state()
    if cur_state in (AdminStates.uploading_files.state, AdminStates.replacing_file_upload.state):
        return
    try:
        file_info = await admin_bot.get_file(message.document.file_id)
        db_remove_sidecars()
        await admin_bot.download_file(file_info.file_path, destination=DB_PATH)
        init_db()
        await message.answer(
            "✓ <b>تمت استعادة النسخة الاحتياطية لقاعدة البيانات بنجاح!</b>\n\n"
            "◈ جاري مزامنتها مع سحابة Firebase...",
            parse_mode="HTML"
        )
        await push_all_local_to_firebase()
    except Exception as e:
        await message.answer(f"◈ فشل استعادة النسخة الاحتياطية: {e}")


# ----------------- الإذاعة (/broadcast) -----------------
@admin_router.message(Command("broadcast"))
async def admin_broadcast_cmd(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    await state.set_state(AdminStates.broadcasting_message)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")]])
    await message.answer(
        "◈ <b>الإذاعة الجماعية للمستخدمين</b>\n\n"
        "✦ دز هسة أي رسالة (نص، صورة، فيديو) حتى تنرسل لكل مستخدمين البوت الرئيسي.",
        reply_markup=kb,
        parse_mode="HTML"
    )


@admin_router.callback_query(F.data == "quick_broadcast")
async def cb_quick_broadcast(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    await state.set_state(AdminStates.broadcasting_message)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")]])
    await call.message.answer(
        "◈ <b>الإذاعة الجماعية للمستخدمين</b>\n\n"
        "✦ دز هسة أي رسالة (نص، صورة، فيديو) حتى تنرسل لكل مستخدمين البوت الرئيسي.",
        reply_markup=kb,
        parse_mode="HTML"
    )
    await call.answer()


@admin_router.message(AdminStates.broadcasting_message)
async def execute_broadcast(message: Message, state: FSMContext):
    await state.clear()
    user_ids = get_all_user_ids()
    if not user_ids:
        await message.answer("◈ ماكو مستخدمين مسجلين لحد هسة.")
        return

    status_msg = await message.answer(f"◷ جاري الإرسال إلى {len(user_ids)} مستخدم...")
    sent = 0
    failed = 0
    for uid in user_ids:
        try:
            if message.photo:
                await main_bot.send_photo(uid, message.photo[-1].file_id, caption=message.caption or "")
            elif message.video:
                await main_bot.send_video(uid, message.video.file_id, caption=message.caption or "")
            elif message.text:
                await main_bot.send_message(uid, message.text)
            sent += 1
            await asyncio.sleep(0.04)
        except Exception:
            failed += 1

    await status_msg.edit_text(
        f"✓ <b>كملت الإذاعة بنجاح!</b>\n\n"
        f"✦ وصلت لـ: {sent} مستخدم\n"
        f"✦ فشل (حاضرين البوت): {failed}",
        parse_mode="HTML"
    )


# ---- إدارة نظام إعلانات فورتكس الخاص (قنوات إجبارية + روابط مواقع) ----
def build_ad_tasks_bot_view() -> Tuple[str, InlineKeyboardMarkup]:
    tasks = get_ad_tasks(only_enabled=False)
    lines = [
        "❖ <b>نظام إعلانات فورتكس الخاص (اشتراكات القنوات + زيارات المواقع)</b>",
        "━━━━━━━━━━━━━━",
        "• <b>قنوات/كروبات إجبارية:</b> يتم التحقق منها برمجياً في كل مرة يطلب فيها المستخدم فيلماً أو حلقة.",
        "• <b>روابط مواقع (زيارات):</b> يفتح المستخدم الرابط وينتظر المؤقت ليحصل على الفيلم.",
        "━━━━━━━━━━━━━━"
    ]
    kb_rows = [
        [
            ikb(text="＋ إضافة قناة/كروب إعلاني", style="danger", callback_data="bot_add_ad_channel"),
            ikb(text="＋ إضافة رابط موقع (زيارات)", style="primary", callback_data="bot_add_ad_link")
        ]
    ]
    if not tasks:
        lines.append("○ لا توجد مهام إعلانية مضافة حالياً.")
    else:
        for t in tasks[:15]:
            st_sym = "✓" if t.get("is_enabled") else "✕"
            ttype_ar = "قناة/كروب" if t.get("task_type") == "channel" else f"رابط ({t.get('timer_seconds', 10)}ث)"
            max_str = f"/{t['max_completions']}" if int(t.get("max_completions") or 0) > 0 else ""
            lines.append(
                f"{st_sym} <b>#{t['id']} [{ttype_ar}] {html.escape(t.get('title', ''))}</b> — الإنجاز: <code>{t.get('clicks_count', 0)}{max_str}</code>"
            )
            kb_rows.append([
                ikb(
                    text=f"{'إيقاف' if t.get('is_enabled') else 'تفعيل'} #{t['id']} ({t.get('title', '')[:14]})",
                    style="success" if not t.get("is_enabled") else "primary",
                    callback_data=f"bot_ad_toggle:{t['id']}"
                ),
                ikb(text=f"✕ حذف #{t['id']}", style="danger", callback_data=f"bot_ad_del:{t['id']}")
            ])
    kb_rows.append([ikb(text="◂ رجوع للإعدادات", style="primary", callback_data="menu_settings")])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=kb_rows)


@admin_router.callback_query(F.data == "set_ads_block")
async def cb_set_ads_block(call: CallbackQuery, state: FSMContext):
    await state.clear()
    txt, kb = build_ad_tasks_bot_view()
    try:
        await call.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await call.message.answer(txt, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@admin_router.callback_query(F.data == "bot_add_ad_channel")
async def cb_bot_add_ad_channel(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.setting_adsgram_block_id)
    await state.update_data(ad_new_type="channel")
    kb = InlineKeyboardMarkup(inline_keyboard=[[ikb(text="إلغاء", style="danger", callback_data="set_ads_block")]])
    await call.message.answer(
        "◈ <b>إضافة قناة أو كروب إعلاني إجباري</b>\n\n"
        "أرسل البيانات بسطر واحد أو عدة أسطر بالصيغة التالية:\n"
        "<code>اسم القناة | معرف القناة (@user أو -100...) | رابط القناة | العدد المطلوب (اختياري)</code>\n\n"
        "مثال سريع:\n"
        "<code>قناة الراعي | @Vortexeo | https://t.me/Vortexeo | 500</code>\n"
        "<i>ملاحظة: ارفع البوت الرئيسي مشرفاً في القناة/الكروب ليتمكن من فحص اشتراك الأعضاء بالوقت الفعلي.</i>",
        reply_markup=kb, parse_mode="HTML"
    )
    await call.answer()


@admin_router.callback_query(F.data == "bot_add_ad_link")
async def cb_bot_add_ad_link(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.setting_adsgram_block_id)
    await state.update_data(ad_new_type="link")
    kb = InlineKeyboardMarkup(inline_keyboard=[[ikb(text="إلغاء", style="danger", callback_data="set_ads_block")]])
    await call.message.answer(
        "◈ <b>إضافة رابط موقع إعلاني (يعتمد على الزيارات)</b>\n\n"
        "أرسل البيانات بالصيغة التالية:\n"
        "<code>عنوان الإعلان | رابط الموقع | مدة الانتظار بالثواني | العدد المطلوب (اختياري)</code>\n\n"
        "مثال سريع:\n"
        "<code>زيارة موقع الراعي الرسمي | https://example.com | 12 | 1000</code>",
        reply_markup=kb, parse_mode="HTML"
    )
    await call.answer()


@admin_router.callback_query(F.data.startswith("bot_ad_toggle:"))
async def cb_bot_ad_toggle(call: CallbackQuery):
    tid = int(call.data.split(":")[1])
    toggle_ad_task(tid)
    txt, kb = build_ad_tasks_bot_view()
    await call.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    await call.answer("✓ تم تحديث حالة المهمة الإعلانية")


@admin_router.callback_query(F.data.startswith("bot_ad_del:"))
async def cb_bot_ad_del(call: CallbackQuery):
    tid = int(call.data.split(":")[1])
    delete_ad_task(tid)
    txt, kb = build_ad_tasks_bot_view()
    await call.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    await call.answer("✕ تم حذف المهمة الإعلانية")


@admin_router.message(AdminStates.setting_adsgram_block_id)
async def save_ads_block_id(message: Message, state: FSMContext):
    if not message.text:
        return
    data = await state.get_data()
    ttype = data.get("ad_new_type", "channel")
    parts = [p.strip() for p in message.text.strip().split("|")]
    if ttype == "channel":
        title = parts[0] if len(parts) >= 1 and parts[0] else "اشترك في قناة الراعي"
        ch_id = parts[1] if len(parts) >= 2 else parts[0]
        url = parts[2] if len(parts) >= 3 else (f"https://t.me/{ch_id.lstrip('@')}" if ch_id.startswith("@") else "")
        max_c = int(parts[3]) if len(parts) >= 4 and parts[3].isdigit() else 0
        add_ad_task("channel", title=title, target_url=url, target_channel_id=ch_id, timer_seconds=0, max_completions=max_c)
    else:
        title = parts[0] if len(parts) >= 1 and parts[0] else "زيارة موقع الراعي"
        url = parts[1] if len(parts) >= 2 else parts[0]
        timer_s = int(parts[2]) if len(parts) >= 3 and parts[2].isdigit() else 10
        max_c = int(parts[3]) if len(parts) >= 4 and parts[3].isdigit() else 0
        add_ad_task("link", title=title, target_url=url, target_channel_id="", timer_seconds=timer_s, max_completions=max_c)

    set_setting("ads_enabled", "1")
    await state.clear()
    txt, kb = build_ad_tasks_bot_view()
    await message.answer("✓ تمت إضافة المهمة الإعلانية وتفعيلها بالتطبيق والبوت بنجاح!", parse_mode="HTML")
    await message.answer(txt, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "toggle_ads_status")
async def cb_toggle_ads_status(call: CallbackQuery):
    curr = get_setting("ads_enabled", "1")
    new_val = "0" if curr == "1" else "1"
    set_setting("ads_enabled", new_val)
    text, kb = build_settings_section_view()
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    status_msg = "▸ اشتغلت تذاكر الإعلانات" if new_val == "1" else "○ طفت الإعلانات"
    await call.answer(status_msg, show_alert=True)


@admin_router.callback_query(F.data == "toggle_fallback_timer")
async def cb_toggle_fallback_timer(call: CallbackQuery):
    curr = get_setting("ads_fallback_timer", "1")
    new_val = "0" if curr == "1" else "1"
    set_setting("ads_fallback_timer", new_val)
    text, kb = build_settings_section_view()
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer("✓ تحدث إعداد المؤقت الذكي الاحتياطي")


@admin_router.callback_query(F.data == "set_support_user")
async def cb_set_support_user(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.setting_support_username)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")]])
    await call.message.answer(
        "◈ <b>تحديد حساب أو رابط الدعم الفني</b>\n\n"
        "✦ دز يوزر حساب الدعم أو البوت اللي يفتحه زر «مراسلة الدعم» بإشعار الطلبات (مثال: <code>@VortexSupport</code>):",
        reply_markup=kb, parse_mode="HTML"
    )
    await call.answer()


@admin_router.message(AdminStates.setting_support_username)
async def save_support_username(message: Message, state: FSMContext):
    if not message.text:
        return
    set_setting("support_username", message.text.strip())
    await state.clear()
    text, kb = build_settings_section_view()
    await message.answer(f"✓ انحفظ حساب الدعم الفني: <code>{html.escape(message.text.strip())}</code>", parse_mode="HTML")
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "set_stars_price")
async def cb_set_stars_price(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.setting_vip_stars_price)
    await call.message.answer("★ دز هسة <b>سعر اشتراك PRO الشهري بالنجوم</b> (مثال: <code>50</code>):", parse_mode="HTML")
    await call.answer()


@admin_router.message(AdminStates.setting_vip_stars_price)
async def save_stars_price(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().isdigit():
        await message.answer("◈ دز رقم صحيح فقط (مثال: 50).")
        return
    set_setting("vip_stars_price", message.text.strip())
    await state.clear()
    text, kb = build_settings_section_view()
    await message.answer(f"✓ تحدد سعر اشتراك PRO بـ {message.text.strip()} نجمة!")
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "set_points_price")
async def cb_set_points_price(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.setting_vip_points_price)
    await call.message.answer("❖ دز هسة <b>عدد النقاط المطلوبة لتفعيل شهر PRO مجاني</b> (مثال: <code>100</code>):", parse_mode="HTML")
    await call.answer()


@admin_router.message(AdminStates.setting_vip_points_price)
async def save_points_price(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().isdigit():
        await message.answer("◈ دز رقم صحيح فقط (مثال: 100).")
        return
    set_setting("vip_points_price", message.text.strip())
    await state.clear()
    text, kb = build_settings_section_view()
    await message.answer(f"✓ تحدد سعر اشتراك PRO بـ {message.text.strip()} نقطة!")
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "set_ref_pts")
async def cb_set_ref_pts(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.setting_referral_points)
    await call.message.answer("✦ دز هسة <b>عدد النقاط اللي يحصلها المستخدم عن كل دعوة صديق</b> (مثال: <code>20</code>):", parse_mode="HTML")
    await call.answer()


@admin_router.message(AdminStates.setting_referral_points)
async def save_ref_pts(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().isdigit():
        await message.answer("◈ دز رقم صحيح فقط (مثال: 20).")
        return
    set_setting("referral_points", message.text.strip())
    await state.clear()
    text, kb = build_settings_section_view()
    await message.answer(f"✓ تحددت مكافأة كل دعوة بـ {message.text.strip()} نقطة!")
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "admin_grant_vip")
async def cb_admin_grant_vip(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.granting_vip_user)
    await call.message.answer(
        "✦ <b>إعطاء أو سحب اشتراك PRO (VIP) يدوياً</b>\n\n"
        "✦ دز أيدي المستخدم وراه عدد الأيام (مثال لإعطاء 30 يوم: <code>123456789 30</code>)\n"
        "✦ أو دز الأيدي وراه <code>0</code> حتى تلغي الـ VIP منه:",
        parse_mode="HTML"
    )
    await call.answer()


@admin_router.message(AdminStates.granting_vip_user)
async def save_admin_grant_vip(message: Message, state: FSMContext):
    if not message.text:
        return
    parts = message.text.strip().split()
    if not parts or not parts[0].isdigit():
        await message.answer("◈ الصيغة مو صحيحة. دز: `أيدي_المستخدم عدد_الأيام`")
        return
    uid = int(parts[0])
    days = int(parts[1]) if len(parts) > 1 and parts[1].lstrip("-").isdigit() else 30
    grant_vip_days(uid, days)
    await state.clear()
    if days > 0:
        await message.answer(f"✓ انطيت للمستخدم <code>{uid}</code> اشتراك PRO لمدة {days} يوم!", parse_mode="HTML")
        try:
            await main_bot.send_message(uid, f"✦ <b>مبروك! إدارة فورتكس انطتك اشتراك PRO مجاني لمدة {days} يوم بدون إعلانات!</b>", parse_mode="HTML")
        except Exception:
            pass
    else:
        await message.answer(f"✓ انلغى اشتراك PRO عن المستخدم <code>{uid}</code>.", parse_mode="HTML")


@admin_router.callback_query(F.data == "admin_add_pts")
async def cb_admin_add_pts(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.adding_points_user)
    await call.message.answer(
        "＋ <b>إضافة أو خصم نقاط لمستخدم</b>\n\n"
        "✦ دز أيدي المستخدم وراه عدد النقاط (مثال: <code>123456789 100</code>):",
        parse_mode="HTML"
    )
    await call.answer()


@admin_router.message(AdminStates.adding_points_user)
async def save_admin_add_pts(message: Message, state: FSMContext):
    if not message.text:
        return
    parts = message.text.strip().split()
    if len(parts) < 2 or not parts[0].isdigit() or not parts[1].lstrip("-").isdigit():
        await message.answer("◈ دز: `أيدي_المستخدم عدد_النقاط`")
        return
    uid = int(parts[0])
    pts = int(parts[1])
    add_user_points(uid, pts)
    await state.clear()
    await message.answer(f"✓ تعدل رصيد المستخدم <code>{uid}</code> بـ ({pts:+} نقطة)!", parse_mode="HTML")


# ---- إعدادات القنوات والاشتراك الإجباري (مع فحص الرسالة التجريبية والـ ID الرقمي) ----
async def extract_channel_info_from_msg(message: Message) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    ch_id, link, title, _ = await extract_channel_info_with_msg_id(message)
    return ch_id, link, title


async def extract_channel_info_with_msg_id(message: Message) -> Tuple[Optional[str], Optional[str], Optional[str], int]:
    """
    يستخرج (channel_id_str, invite_or_public_link, channel_title, forwarded_msg_id)
    من أي رسالة في تيليجرام:
    - يدعم التحديث الجديد (message.forward_origin: MessageOriginChannel / MessageOriginChat)
    - يدعم message.forward_from_chat و message.sender_chat
    - يدعم النصوص والكابتشن (للفيديوهات والصور المحولة) والروابط المخفية داخل entities
    - يدعم الآيديات الرقمية مع أو بدون -100 وروابط t.me/c/...
    """
    raw_ch_target = ""
    title = ""
    uname = ""
    fwd_msg_id = 0

    # 1. فحص forward_origin الحديث في Bot API 7.0+ / Aiogram 3.13
    origin = getattr(message, "forward_origin", None)
    if origin is not None:
        orig_chat = getattr(origin, "chat", None) or getattr(origin, "sender_chat", None)
        orig_mid = int(getattr(origin, "message_id", 0) or 0)
        if orig_mid > 0:
            fwd_msg_id = orig_mid
        if orig_chat is not None and getattr(orig_chat, "id", None):
            raw_ch_target = str(orig_chat.id)
            title = getattr(orig_chat, "title", "") or ""
            uname = getattr(orig_chat, "username", "") or ""

    # 2. فحص forward_from_chat الكلاسيكي
    if not raw_ch_target and getattr(message, "forward_from_chat", None):
        ch = message.forward_from_chat
        raw_ch_target = str(ch.id)
        title = ch.title or title
        uname = ch.username or uname
        if not fwd_msg_id:
            fwd_msg_id = int(getattr(message, "forward_from_message_id", 0) or 0)

    # 3. فحص sender_chat إذا كانت الرسالة مرسلة باسم قناة
    if not raw_ch_target and getattr(message, "sender_chat", None):
        sch = message.sender_chat
        if str(getattr(sch, "type", "")).lower() == "channel" or str(sch.id).startswith("-100"):
            raw_ch_target = str(sch.id)
            title = sch.title or title
            uname = sch.username or uname

    # 4. فحص الروابط داخل entities و caption_entities
    if not raw_ch_target:
        for ent_list in (getattr(message, "entities", None) or [], getattr(message, "caption_entities", None) or []):
            for ent in ent_list:
                ent_url = getattr(ent, "url", None)
                if ent_url and "t.me/" in ent_url:
                    cand_ch, cand_mid = clean_and_normalize_channel_input(ent_url)
                    if cand_ch:
                        raw_ch_target = cand_ch
                        if cand_mid > 0 and not fwd_msg_id:
                            fwd_msg_id = cand_mid
                        break
            if raw_ch_target:
                break

    # 5. فحص النص أو الكابتشن (message.text أو message.caption)
    if not raw_ch_target:
        raw_txt = (getattr(message, "text", None) or getattr(message, "caption", None) or "").strip()
        if raw_txt and not raw_txt.startswith("/"):
            cand_ch, cand_mid = clean_and_normalize_channel_input(raw_txt)
            if cand_ch:
                raw_ch_target = cand_ch
                if cand_mid > 0 and not fwd_msg_id:
                    fwd_msg_id = cand_mid

    if not raw_ch_target:
        return None, None, None, 0

    chat_target = normalize_chat_target(raw_ch_target)
    resolved_id = str(raw_ch_target)
    link = f"https://t.me/{uname.lstrip('@')}" if uname else (f"https://t.me/{raw_ch_target.lstrip('@')}" if str(raw_ch_target).startswith("@") else "")

    # محاولة جلب معلومات القناة عبر البوتات الثلاثة لتأكيد الآيدي الرقمي والاسم والرابط
    for b in (admin_bot, main_bot, sender_bot):
        try:
            chat_obj = await asyncio.wait_for(b.get_chat(chat_target), timeout=4.0)
            if chat_obj and getattr(chat_obj, "id", None):
                resolved_id = str(chat_obj.id)
                title = getattr(chat_obj, "title", None) or title
                if getattr(chat_obj, "username", None):
                    link = f"https://t.me/{chat_obj.username}"
                elif getattr(chat_obj, "invite_link", None):
                    link = chat_obj.invite_link
                if not link:
                    try:
                        created = await asyncio.wait_for(b.create_chat_invite_link(chat_obj.id, name="Vortex Link"), timeout=3.5)
                        if created and created.invite_link:
                            link = created.invite_link
                    except Exception:
                        pass
                break
        except Exception:
            continue

    if not title:
        title = f"قناة {resolved_id}"

    return resolved_id, link, title, fwd_msg_id


@admin_router.callback_query(F.data == "set_movies_ch")
async def cb_set_movies_ch(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.setting_movies_channel)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")]])
    await call.message.answer(
        "◈ <b>ربط قناة خزن الأفلام</b>\n\n"
        "1. ارفع <b>بوت الإدارة</b> و<b>بوت الإرسال (@VortexSenderBot)</b> مشرفين بالقناة.\n"
        "2. حوّل (Forward) أي رسالة من القناة لهنا، أو دز الـ ID أو اليوزر مالتها:",
        reply_markup=kb, parse_mode="HTML"
    )
    await call.answer()


@admin_router.message(AdminStates.setting_movies_channel)
async def save_movies_channel(message: Message, state: FSMContext):
    ch_id, _, title = await extract_channel_info_from_msg(message)
    if not ch_id:
        await message.answer("◈ ما كدرت أقرا القناة. حوّل أي منشور من القناة لهنا أو دز الآيدي (مثل `-100...`) أو رابطها.")
        return
    chat_target = normalize_chat_target(ch_id)
    ok_test, test_msg = await test_channel_post_and_delete(admin_bot, chat_target)
    sender_st = await verify_bot_in_channel(sender_bot, "بوت الإرسال", chat_target, need_post=False)
    set_setting("movies_channel_id", str(ch_id))
    if not get_setting("series_channel_id"):
        set_setting("series_channel_id", str(ch_id))
    await state.clear()
    text, kb = build_content_section_view()
    perm_note = test_msg if ok_test else f"⚠️ تم حفظ القناة بنجاح، لكن تأكد من رفع بوت الإدارة مشرفاً فيها ({test_msg[:80]})"
    await message.answer(
        f"✓ <b>تم حفظ وربط قناة خزن الأفلام بنجاح!</b>\n"
        f"✦ القناة: <b>{html.escape(title or '')}</b> (<code>{ch_id}</code>)\n"
        f"✦ {html.escape(perm_note)}\n"
        f"✦ {sender_st}",
        parse_mode="HTML"
    )
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "set_series_ch")
async def cb_set_series_ch(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.setting_series_channel)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")]])
    await call.message.answer(
        "❖ <b>ربط قناة خزن المسلسلات</b>\n\n"
        "1. ارفع <b>بوت الإدارة</b> و<b>بوت الإرسال (@VortexSenderBot)</b> مشرفين بالقناة.\n"
        "2. حوّل (Forward) أي رسالة من قناة المسلسلات لهنا، أو دز الـ ID أو اليوزر مالتها:",
        reply_markup=kb, parse_mode="HTML"
    )
    await call.answer()


@admin_router.message(AdminStates.setting_series_channel)
async def save_series_channel(message: Message, state: FSMContext):
    ch_id, _, title = await extract_channel_info_from_msg(message)
    if not ch_id:
        await message.answer("◈ ما كدرت أقرا القناة. حوّل أي منشور من القناة لهنا أو دز الآيدي (مثل `-100...`) أو رابطها.")
        return
    chat_target = normalize_chat_target(ch_id)
    ok_test, test_msg = await test_channel_post_and_delete(admin_bot, chat_target)
    sender_st = await verify_bot_in_channel(sender_bot, "بوت الإرسال", chat_target, need_post=False)
    set_setting("series_channel_id", str(ch_id))
    await state.clear()
    text, kb = build_content_section_view()
    perm_note = test_msg if ok_test else f"⚠️ تم حفظ القناة بنجاح، لكن تأكد من رفع بوت الإدارة مشرفاً فيها ({test_msg[:80]})"
    await message.answer(
        f"✓ <b>تم حفظ وربط قناة خزن المسلسلات بنجاح!</b>\n"
        f"✦ القناة: <b>{html.escape(title or '')}</b> (<code>{ch_id}</code>)\n"
        f"✦ {html.escape(perm_note)}\n"
        f"✦ {sender_st}",
        parse_mode="HTML"
    )
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "set_force_ch")
async def cb_set_force_ch(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.setting_force_sub_channel)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")]])
    await call.message.answer(
        "⬡ <b>ربط قناة الاشتراك الإجباري</b>\n\n"
        "1. صعّد <b>البوت الرئيسي (@Vortexee_Bot)</b> وبوت الإدارة كمشرفين (Admins) بالقناة.\n"
        "2. هسة <b>حوّل أي رسالة من القناة</b> لهنا، أو دز <b>يوزرها</b> أو <b>الأيدي</b>:",
        reply_markup=kb, parse_mode="HTML"
    )
    await call.answer()


@admin_router.message(AdminStates.setting_force_sub_channel)
async def save_force_sub_channel(message: Message, state: FSMContext):
    ch_id, link, title = await extract_channel_info_from_msg(message)
    if not ch_id:
        await message.answer("◈ يرجى تحويل رسالة من القناة، أو إرسال يوزرها `@...` أو الأيدي `-100...`")
        return

    chat_target = normalize_chat_target(ch_id)
    main_st = await verify_bot_in_channel(main_bot, "البوت الرئيسي", chat_target, need_post=False)

    set_setting("force_sub_channel_id", str(ch_id))
    set_setting("force_sub_channel_title", title or "قناة فورتكس")

    if link:
        set_setting("force_sub_channel_link", link)
        await state.clear()
        text, kb = build_settings_section_view()
        await message.answer(
            f"✓ <b>تفعل الاشتراك الإجباري بنجاح!</b>\n\n"
            f"✦ القناة: <b>{html.escape(title or '')}</b> (<code>{ch_id}</code>)\n"
            f"✦ رابط الانضمام: {link}\n"
            f"✦ {main_st}",
            parse_mode="HTML"
        )
        await message.answer(text, reply_markup=kb, parse_mode="HTML")
    else:
        await state.set_state(AdminStates.setting_force_sub_link)
        await message.answer(
            "⟐ دزلي هسة <b>رابط الدعوة (Invite Link)</b> الخاص بالقناة (مثال: <code>https://t.me/+AbCdEfG...</code>):",
            parse_mode="HTML"
        )


@admin_router.message(AdminStates.setting_force_sub_link)
async def save_force_sub_link(message: Message, state: FSMContext):
    if not message.text or "t.me/" not in message.text:
        await message.answer("◈ دز رابط تيليجرام صحيح يبدأ بـ `https://t.me/`")
        return
    set_setting("force_sub_channel_link", message.text.strip())
    await state.clear()
    text, kb = build_settings_section_view()
    await message.answer("✓ انحفظ رابط قناة الاشتراك الإجباري بنجاح!")
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "disable_force_ch")
async def cb_disable_force_ch(call: CallbackQuery):
    set_setting("force_sub_channel_id", "")
    set_setting("force_sub_channel_link", "")
    set_setting("force_sub_channel_title", "")
    _SUB_VERIFIED_CACHE.clear()
    text, kb = build_settings_section_view()
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer("✓ تمت إزالة وإيقاف قناة الاشتراك الإجباري بنجاح", show_alert=True)


@admin_router.callback_query(F.data == "set_publish_ch")
async def cb_set_publish_ch(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.setting_publish_channel)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")]])
    await call.message.answer(
        "◈ <b>ربط قناة نشر الإضافات الجديدة (قناة العرض)</b>\n\n"
        "✦ صعّد <b>البوت الرئيسي (@Vortexee_Bot)</b> (ويفضل بوت الإدارة أيضاً) كمشرف بالقناة مع صلاحية <b>نشر الرسائل</b>.\n"
        "✦ هسة حوّل رسالة منها أو دز يوزرها أو الأيدي حتى نفحص النشر فوراً:",
        reply_markup=kb, parse_mode="HTML"
    )
    await call.answer()


@admin_router.message(AdminStates.setting_publish_channel)
async def save_publish_channel(message: Message, state: FSMContext):
    ch_id, _, title = await extract_channel_info_from_msg(message)
    if not ch_id:
        await message.answer("◈ ما كدرت أقرا القناة. حوّل رسالة منها أو دز يوزرها أو الأيدي.")
        return

    chat_target = normalize_chat_target(ch_id)
    ok_main, msg_main = await test_channel_post_and_delete(main_bot, chat_target)
    ok_adm, msg_adm = (False, "")
    if not ok_main:
        ok_adm, msg_adm = await test_channel_post_and_delete(admin_bot, chat_target)

    if not ok_main and not ok_adm:
        await message.answer(
            f"✕ <b>ما كدرت أنشر رسالة تجريبية بالقناة (<code>{ch_id}</code>)!</b>\n\n"
            f"✦ السبب من البوت الرئيسي: <code>{html.escape(msg_main[:140])}</code>\n\n"
            f"◈ الحل: ارفع <b>البوت الرئيسي (@{BOT_INFO['main_username']})</b> مشرف (Admin) بالقناة وفعل له صلاحية <b>نشر الرسائل</b>، وبعدها دز القناة مرة ثانية.",
            parse_mode="HTML"
        )
        return

    set_setting("publish_channel_id", str(ch_id))
    await state.clear()
    text, kb = build_content_section_view()
    used_bot_note = "البوت الرئيسي جاهز للنشر ✓" if ok_main else "بوت الإدارة جاهز للنشر ✓ (يفضل رفع البوت الرئيسي أيضاً)"
    await message.answer(
        f"✓ <b>انربطت قناة العرض وتم اختبار النشر بنجاح!</b>\n"
        f"✦ القناة: <b>{html.escape(title or '')}</b> (<code>{ch_id}</code>)\n"
        f"✦ الفحص: {used_bot_note}",
        parse_mode="HTML"
    )
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "disable_publish_ch")
async def cb_disable_publish_ch(call: CallbackQuery):
    set_setting("publish_channel_id", "")
    text, kb = build_content_section_view()
    await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    await call.answer("○ طفى النشر التلقائي")


@admin_router.callback_query(F.data == "toggle_autodel")
async def cb_toggle_autodel(call: CallbackQuery):
    removed = clean_old_server_temp_files(0.5)
    await call.answer(f"✓ الحذف الإجباري (6 ساعات) فعّال دائماً، وتم تنظيف {removed} ملف مؤقت من السيرفر!", show_alert=True)


# ----------------- ربط قناة خزن الأنمي وقنوات النشر الثلاث المخصصة -----------------
@admin_router.callback_query(F.data == "set_anime_ch")
async def cb_set_anime_ch(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.setting_anime_channel)
    kb = InlineKeyboardMarkup(inline_keyboard=[[ikb(text="إلغاء", style="danger", callback_data="cancel_op")]])
    await call.message.answer(
        "✦ <b>ربط قناة خزن الأنمي (Storage)</b>\n\n"
        "1. ارفع <b>بوت الإدارة</b> و<b>بوت الإرسال</b> كمشرفين بالقناة.\n"
        "2. حوّل أي رسالة من القناة لهنا، أو دز يوزرها أو الأيدي (<code>-100...</code>):",
        reply_markup=kb, parse_mode="HTML"
    )
    await call.answer()


@admin_router.message(AdminStates.setting_anime_channel)
async def save_anime_channel(message: Message, state: FSMContext):
    ch_id, _, title = await extract_channel_info_from_msg(message)
    if not ch_id:
        await message.answer("◈ يرجى تحويل رسالة من القناة أو إرسال الأيدي `-100...`")
        return
    chat_target = normalize_chat_target(ch_id)
    ok_test, test_msg = await test_channel_post_and_delete(admin_bot, chat_target)
    set_setting("anime_channel_id", str(ch_id))
    await state.clear()
    text, kb = build_content_section_view()
    note_str = "" if ok_test else f"\n⚠️ تنبيه: ارفع بوت الإدارة مشرفاً في القناة ليتمكن من الخزن ({html.escape(test_msg[:70])})"
    await message.answer(f"✓ <b>تم حفظ وربط قناة خزن الأنمي بنجاح:</b> {html.escape(title or '')} (<code>{ch_id}</code>){note_str}", parse_mode="HTML")
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data.in_({"set_movies_pub_ch", "set_series_pub_ch", "set_anime_pub_ch"}))
async def cb_set_section_pub_ch(call: CallbackQuery, state: FSMContext):
    mapping = {
        "set_movies_pub_ch": (AdminStates.setting_movies_pub_channel, "◈ قناة نشر الأفلام العامة"),
        "set_series_pub_ch": (AdminStates.setting_series_pub_channel, "▣ قناة نشر المسلسلات العامة"),
        "set_anime_pub_ch": (AdminStates.setting_anime_pub_channel, "✦ قناة نشر الأنمي العامة"),
    }
    st_obj, label = mapping[call.data]
    await state.set_state(st_obj)
    kb = InlineKeyboardMarkup(inline_keyboard=[[ikb(text="إلغاء", style="danger", callback_data="cancel_op")]])
    await call.message.answer(
        f"◈ <b>ربط {label}</b>\n\n"
        f"✦ ارفع <b>البوت الرئيسي (@{BOT_INFO['main_username']})</b> وبوت الإدارة كمشرفين بالقناة مع صلاحية النشر.\n"
        "✦ حوّل رسالة من القناة لهنا، أو دز يوزرها `@...` أو الأيدي `-100...` (أو أرسل `0` لإلغاء ربطها):",
        reply_markup=kb, parse_mode="HTML"
    )
    await call.answer()


@admin_router.message(AdminStates.setting_movies_pub_channel)
@admin_router.message(AdminStates.setting_series_pub_channel)
@admin_router.message(AdminStates.setting_anime_pub_channel)
async def save_section_pub_channel(message: Message, state: FSMContext):
    cur_st = await state.get_state()
    key_map = {
        AdminStates.setting_movies_pub_channel.state: ("movies_publish_channel_id", "قناة نشر الأفلام"),
        AdminStates.setting_series_pub_channel.state: ("series_publish_channel_id", "قناة نشر المسلسلات"),
        AdminStates.setting_anime_pub_channel.state: ("anime_publish_channel_id", "قناة نشر الأنمي"),
    }
    setting_key, label = key_map.get(cur_st, ("movies_publish_channel_id", "قناة النشر"))

    if message.text and message.text.strip() in ("0", "حذف", "الغاء"):
        set_setting(setting_key, "")
        await state.clear()
        text, kb = build_content_section_view()
        await message.answer(f"○ تم إلغاء ربط {label}.")
        await message.answer(text, reply_markup=kb, parse_mode="HTML")
        return

    ch_id, _, title = await extract_channel_info_from_msg(message)
    if not ch_id:
        await message.answer("◈ ما كدرت أقرا القناة. حوّل رسالة منها أو دز يوزرها `@...` أو الأيدي `-100...`")
        return
    chat_target = normalize_chat_target(ch_id)
    ok_main, msg_main = await test_channel_post_and_delete(main_bot, chat_target)
    ok_adm, _ = (False, "")
    if not ok_main:
        ok_adm, _ = await test_channel_post_and_delete(admin_bot, chat_target)
    if not ok_main and not ok_adm:
        await message.answer(
            f"✕ <b>تعذر النشر التجريبي في {label} (<code>{ch_id}</code>)!</b>\n"
            f"تأكد من رفع البوت الرئيسي وبوت الإدارة كمشرفين بالقناة.",
            parse_mode="HTML"
        )
        return
    set_setting(setting_key, str(ch_id))
    await state.clear()
    text, kb = build_content_section_view()
    await message.answer(f"✓ <b>تم ربط {label} بنجاح:</b> {html.escape(title or '')} (<code>{ch_id}</code>)", parse_mode="HTML")
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


# ----------------- نظام الغلاف الخاص الموحد عبر تيليجرام (/cover) — بدون استهلاك رام -----------------
async def start_cover_flow(target_msg: Message, state: FSMContext):
    await state.set_state(AdminStates.setting_video_cover)
    enabled = get_setting("cover_enabled", "1") == "1"
    custom_fid = get_setting("video_cover_file_id", "").strip()

    rows = []
    if custom_fid:
        rows.append([ikb(text="○ معاينة الغلاف المعتمد حالياً", style="primary", callback_data="preview_static_cover")])
    rows.append([
        ikb(
            text="الغلاف الموحد: مفعّل ✓" if enabled else "الغلاف الموحد: متوقف ✕",
            style="success" if enabled else "danger",
            callback_data="toggle_cover_enabled"
        )
    ])
    if custom_fid:
        rows.append([ikb(text="✕ حذف الغلاف الحالي (نشر بدون غلاف)", style="danger", callback_data="remove_video_cover")])
    rows.append([ikb(text="◂ رجوع للوحة الرئيسية", style="primary", callback_data="menu_home")])

    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    cov_status_txt = (
        "مضاف ومعتمد لجميع الفيديوهات ✓ (مخزون على تيليجرام)"
        if custom_fid else
        "غير مضاف بعد ✕ (أرسل صورة الآن لاعتمادها)"
    )
    text = (
        "▣ <b>إدارة الغلاف الموحد للفيديوهات (خفيف بدون استهلاك رام)</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"◈ <b>حالة التركيب:</b> <b>{'مفعّل ✓' if enabled else 'متوقف مؤقتاً ✕'}</b>\n"
        f"◈ <b>صورة الغلاف:</b> <b>{cov_status_txt}</b>\n"
        "━━━━━━━━━━━━━━\n"
        "✦ <b>طريقة الإضافة أو التغيير:</b>\n"
        "فقط <b>أرسل صورة الغلاف هنا الآن</b> في هذه المحادثة، وسيقوم البوت بحفظها على سيرفرات تيليجرام وتركيبها تلقائياً على جميع الأفلام والمسلسلات والأنمي بدون استهلاك أي ذاكرة (RAM) من الاستضافة."
    )
    await target_msg.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.message(Command("cover"))
async def admin_cover_cmd(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    await start_cover_flow(message, state)


@admin_router.callback_query(F.data == "set_video_cover")
async def cb_set_video_cover(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    await call.answer()
    await start_cover_flow(call.message, state)


@admin_router.callback_query(F.data == "toggle_cover_enabled")
async def cb_toggle_cover_enabled(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    cur = get_setting("cover_enabled", "1") == "1"
    set_setting("cover_enabled", "0" if cur else "1")
    await call.answer("تم تحديث حالة الغلاف الموحد ✓")
    await start_cover_flow(call.message, state)


@admin_router.callback_query(F.data == "preview_static_cover")
async def cb_preview_static_cover(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    fid = get_setting("video_cover_file_id", "").strip()
    if not fid:
        await call.answer("لا يوجد غلاف مضاف حالياً! أرسل صورة هنا لإضافته.", show_alert=True)
        return
    await call.answer("جاري عرض الغلاف المعتمد...")
    try:
        await call.message.answer_photo(
            photo=fid,
            caption="✦ <b>هذا هو الغلاف الخاص المعتمد حالياً لجميع الفيديوهات ✓</b>\n(لتغييره أرسل صورة جديدة هنا مباشرة)",
            parse_mode="HTML"
        )
    except Exception as e:
        await call.message.answer(f"◈ تعذّر عرض الغلاف القديم ({html.escape(str(e)[:80])})، أرسل صورة جديدة لتحديثه.", parse_mode="HTML")


@admin_router.callback_query(F.data == "remove_video_cover")
async def cb_remove_video_cover(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    set_setting("video_cover_file_id", "")
    await call.answer("تم حذف الغلاف المعتمد ✓")
    await call.message.answer("✓ تم حذف الغلاف المعتمد، ستُنشر الفيديوهات القادمة بدون غلاف إلى أن ترسل صورة غلاف جديدة.")
    await start_cover_flow(call.message, state)


@admin_router.message(AdminStates.setting_video_cover, F.photo)
async def save_video_cover_photo(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    new_fid = message.photo[-1].file_id
    set_setting("video_cover_file_id", new_fid)
    set_setting("cover_enabled", "1")
    await state.clear()
    text, kb = build_content_section_view()
    await message.answer("✓ <b>تم حفظ واعتماد صورتك كغلاف موحد لجميع الفيديوهات بنجاح!</b>\n(مخزونة على تيليجرام وبدون أي استهلاك لرام السيرفر)", parse_mode="HTML")
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.message(AdminStates.setting_video_cover)
async def save_video_cover_invalid(message: Message, state: FSMContext):
    if message.text and message.text.startswith("/"):
        return
    await message.answer("◈ أرسل <b>صورة عادية</b> هنا في المحادثة لاعتمادها كغلاف موحد لجميع الفيديوهات.", parse_mode="HTML")


@admin_router.callback_query(F.data == "set_tmdb_key")
async def cb_set_tmdb_key(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.setting_tmdb_key)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")]])
    await call.message.answer(
        "❖ <b>إضافة مفتاح TMDB API</b>\n\n"
        "✦ دز هسة مفتاح الـ API الخاص بموقع TheMovieDB:",
        reply_markup=kb, parse_mode="HTML"
    )
    await call.answer()


@admin_router.message(AdminStates.setting_tmdb_key)
async def save_tmdb_key(message: Message, state: FSMContext):
    if not message.text:
        return
    set_setting("tmdb_api_key", message.text.strip())
    await state.clear()
    text, kb = build_content_section_view()
    await message.answer("✓ انحفظ مفتاح TMDB بنجاح!")
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "toggle_app_maint_bot")
async def cb_toggle_app_maint_bot(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        await call.answer("غير مصرح", show_alert=True)
        return
    cur_m = get_setting("app_maintenance", "0") == "1"
    set_setting("app_maintenance", "0" if cur_m else "1")
    state_txt = "تم إيقاف وضع الصيانة وفتح التطبيق للزوار ✓" if cur_m else "🛠️ تم تفعيل شاشة الصيانة في الميني آب!"
    await call.answer(state_txt, show_alert=True)
    text, kb = build_settings_section_view()
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "set_webapp_url")
async def cb_set_webapp_url(call: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.setting_webapp_url)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")]])
    await call.message.answer(
        "◈ <b>تعديل رابط واجهة التطبيق (Mini App URL)</b>\n\n"
        "✦ دز رابط الـ HTTPS مال سيرفرك (Railway / Render / أي استضافة):",
        reply_markup=kb, parse_mode="HTML"
    )
    await call.answer()


@admin_router.message(AdminStates.setting_webapp_url)
async def save_webapp_url(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().startswith("https://"):
        await message.answer("◈ لازم يبدأ الرابط بـ `https://`")
        return
    url = message.text.strip().rstrip("/")
    set_setting("webapp_url", url)
    try:
        await main_bot.set_chat_menu_button(
            menu_button=MenuButtonWebApp(text="VORTEX", web_app=WebAppInfo(url=url))
        )
    except Exception as e:
        logger.warning(f"Could not update menu button: {e}")
    await state.clear()
    text, kb = build_settings_section_view()
    await message.answer(f"✓ تحدث رابط الواجهة وتفعل بالبوت الرئيسي:\n<code>{url}</code>", parse_mode="HTML")
    await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "add_new_admin")
async def cb_add_new_admin(call: CallbackQuery, state: FSMContext):
    if not is_owner_id(call.from_user.id):
        await call.answer("★ إضافة المشرفين متاحة للمالك الأساسي فقط لحماية البوت!", show_alert=True)
        return
    await state.set_state(AdminStates.adding_new_admin)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="إلغاء", callback_data="menu_admins")]])
    await call.message.answer(
        "◈ <b>إضافة مشرف جديد وتحديد صلاحياته</b>\n\n"
        "✦ دز <b>الـ ID</b> مال حساب المشرف الجديد (أو حوّل رسالة منه لهنا):\n"
        "✦ بعدها راح تنفتحلك بطاقة تحديد صلاحياته مباشرة (إضافة، تعديل، حذف، طلبات، مستخدمين، إذاعة).",
        reply_markup=kb, parse_mode="HTML"
    )
    await call.answer()


@admin_router.message(AdminStates.adding_new_admin)
async def save_new_admin(message: Message, state: FSMContext):
    if not is_owner_id(message.from_user.id):
        await state.clear()
        return
    new_id = 0
    f_name = ""
    u_name = ""
    if message.forward_from:
        new_id = message.forward_from.id
        f_name = message.forward_from.first_name or ""
        u_name = message.forward_from.username or ""
    elif message.text and message.text.strip().isdigit():
        new_id = int(message.text.strip())
    else:
        await message.answer("◈ دز رقم ID صحيح أو حوّل رسالة من الشخص اللي تريد تصعده مشرف.")
        return

    if not f_name:
        for b in (main_bot, admin_bot):
            try:
                chat_info = await b.get_chat(new_id)
                f_name = chat_info.first_name or chat_info.title or ""
                u_name = chat_info.username or ""
                if f_name:
                    break
            except Exception:
                pass

    add_admin_id(new_id, added_by=message.from_user.id, first_name=f_name, username=u_name)
    await state.clear()
    text, kb = build_single_admin_card_view(new_id, message.from_user.id)
    await message.answer(
        f"✓ <b>تمت إضافة المشرف <code>{new_id}</code> بنجاح!</b>\n"
        "تگدر هسة تحدد صلاحياته بالضبط من الأزرار الجوة (افتراضياً صلاحية الحذف والإذاعة مقفولة للأمان):",
        reply_markup=kb,
        parse_mode="HTML"
    )


# =====================================================================
# إضافة عمل، دفعة أفلام كاملة، أو دمج دقة جديدة (/add) + تحقق تلقائي لحلقات المسلسلات من TMDb
# =====================================================================
import difflib


def _norm_movie_match_text(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[._\-\(\)\[\]\{\}:;,!؟?|+*/\\]+", " ", s)
    s = re.sub(r"\b(mp4|mkv|avi|mov|bluray|webrip|web|dl|hdrip|brrip|hdtv|x264|x265|hevc|aac|10bit|مترجم|مدبلج|فلم|فيلم|دقة|جودة|كامل)\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def extract_movie_clue_and_quality(message: Message) -> Tuple[str, Optional[str]]:
    '''
    يستخرج اسم الفيلم المكتوب بالوصف (أو اسم الملف) ويفصله بذكاء عن رقم الدقة
    حتى لو كانت الدقة ملتصقة بالاسم مثل Escape Plan720 أو مفصولة مثل Escape Plan 480.
    '''
    raw_cap = (message.caption or "").strip()
    if not raw_cap and message.document and message.document.file_name:
        raw_cap = re.sub(r"\.[a-zA-Z0-9]{2,4}$", "", message.document.file_name)

    detected_q: Optional[str] = None
    cleaned = raw_cap

    # التقاط الدقة سواء كانت مفصولة أو ملتصقة بنهاية الاسم (مثل Escape Plan720 أو Escape Plan 480p)
    q_match = re.search(r"(?:^|[^\d])(2160|1080|720|480|360)\s*p?(?:$|[^\d])", cleaned, flags=re.IGNORECASE)
    if q_match:
        detected_q = q_match.group(1)
        # نحذف جزئية الدقة من النص لاستخراج اسم الفيلم الصافي
        span_s, span_e = q_match.span(1)
        # نحذف أيضاً حرف p إن وجد بعد الرقم مباشرة
        after_q = cleaned[span_e:]
        if after_q.lower().startswith("p"):
            span_e += 1
        cleaned = (cleaned[:span_s] + " " + cleaned[span_e:]).strip()

    movie_clue = _norm_movie_match_text(cleaned)
    return movie_clue, detected_q


def match_video_to_batch_item(movie_clue: str, batch_items: List[Dict[str, Any]]) -> Optional[int]:
    '''
    يطابق اسم الفيلم المستخرج من وصف الفيديو مع قائمة أفلام الدفعة (batch_items)
    مع إعطاء الأولوية للاسم الأطول والأدق ولأرقام الأجزاء (مثل Escape Plan 2 مقابل Escape Plan).
    يرجّع فهرس الفيلم داخل batch_items أو None إذا لم يكن هناك اسم واضح.
    '''
    if not batch_items:
        return None
    if len(batch_items) == 1:
        return 0
    if not movie_clue or len(movie_clue) < 2:
        return None

    clue_norm = _norm_movie_match_text(movie_clue)
    clue_tokens = set(clue_norm.split())
    clue_nums = set(re.findall(r"\b(\d+)\b", clue_norm))

    best_idx = None
    best_score = 0.0

    for idx, b_item in enumerate(batch_items):
        det = b_item.get("details") or {}
        cand_titles = [
            str(det.get("original_title") or ""),
            str(det.get("title") or ""),
            f"{det.get('original_title', '')} {det.get('year', '')}",
            f"{det.get('title', '')} {det.get('year', '')}",
            str(b_item.get("raw_query") or ""),
        ]
        item_best = 0.0
        for ct in cand_titles:
            ct_norm = _norm_movie_match_text(ct)
            if not ct_norm:
                continue
            ct_tokens = set(ct_norm.split())
            # استثناء سنة الإنتاج من أرقام الأجزاء عند المقارنة
            yr_str = str(det.get("year") or "")
            ct_part_nums = {n for n in re.findall(r"\b(\d+)\b", ct_norm) if n != yr_str}
            clue_part_nums = {n for n in clue_nums if n != yr_str}

            score = 0.0
            if clue_norm == ct_norm:
                score = 100.0 + len(ct_norm) * 0.2
            elif ct_norm in clue_norm or clue_norm in ct_norm:
                ratio = len(min(clue_norm, ct_norm, key=len)) / max(1, len(max(clue_norm, ct_norm, key=len)))
                score = 72.0 + (ratio * 22.0) + (len(ct_norm) * 0.15)
            else:
                common = len(clue_tokens & ct_tokens)
                if common > 0:
                    overlap = common / max(1, len(ct_tokens))
                    seq_r = difflib.SequenceMatcher(None, clue_norm, ct_norm).ratio()
                    score = (overlap * 55.0) + (seq_r * 35.0)
                else:
                    seq_r = difflib.SequenceMatcher(None, clue_norm, ct_norm).ratio()
                    if seq_r >= 0.68:
                        score = seq_r * 65.0

            # ضبط دقيق لأجزاء السلاسل (مثلاً Escape Plan مقابل Escape Plan 2)
            if clue_part_nums or ct_part_nums:
                if clue_part_nums == ct_part_nums and clue_part_nums:
                    score += 25.0  # تطابق تام برقم الجزء!
                elif clue_part_nums and not ct_part_nums and ("1" not in clue_part_nums):
                    score -= 32.0  # الوصف فيه جزء 2 أو 3 بينما هذا الفيلم هو الجزء 1
                elif ct_part_nums and not clue_part_nums and ("1" not in ct_part_nums):
                    score -= 28.0  # هذا الفيلم جزء 2 بينما الوصف بدون رقم جزء
                elif clue_part_nums != ct_part_nums:
                    score -= 35.0  # اختلاف برقم الجزء (مثلاً 2 مقابل 3)

            if score > item_best:
                item_best = score

        if item_best > best_score:
            best_score = item_best
            best_idx = idx

    if best_score >= 38.0:
        return best_idx
    return None


async def start_add_flow(target_msg: Message, state: FSMContext):
    await state.clear()
    await state.set_state(AdminStates.choosing_type)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            ikb(text="◈ فيلم (Movie)", style="primary", callback_data="addtype_movie"),
            ikb(text="▣ مسلسل (Series)", style="primary", callback_data="addtype_tv"),
            ikb(text="✦ أنمي (Anime)", style="danger", callback_data="addtype_anime")
        ],
        [
            ikb(text="▣ دفعة أفلام (أكثر من فيلم بوجبة وحدة)", style="success", callback_data="addtype_batch_movies")
        ],
        [ikb(text="✕ إلغاء", style="danger", callback_data="cancel_op")]
    ])
    await target_msg.answer(
        "➕ <b>إضافة عمل جديد إلى فورتكس (VORTEX)</b>\n\n"
        "✦ اختر القسم المطلوب: <b>«◈ فيلم»</b> أو <b>«▣ مسلسل»</b> أو <b>«✦ أنمي»</b>، أو <b>«▣ دفعة أفلام»</b>!\n"
        "◈ <i>يتم تركيب غلاف فورتكس الديناميكي (صورة العمل + حقوق البوت) والنشر في قناة القسم والقناة العامة تلقائياً!</i>",
        reply_markup=kb,
        parse_mode="HTML"
    )


@admin_router.message(Command("add"))
async def admin_add_cmd(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    await start_add_flow(message, state)


@admin_router.callback_query(F.data == "quick_add")
async def cb_quick_add(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    await start_add_flow(call.message, state)
    await call.answer()


@admin_router.callback_query(AdminStates.choosing_type, F.data.startswith("addtype_"))
async def cb_choose_type(call: CallbackQuery, state: FSMContext):
    is_batch = (call.data == "addtype_batch_movies")
    if call.data == "addtype_anime":
        media_type = "anime"
    elif call.data == "addtype_tv":
        media_type = "tv"
    else:
        media_type = "movie"
    storage_ch = get_storage_channel_for_type(media_type)
    if not storage_ch:
        await state.clear()
        await call.message.answer(
            "◈ <b>تنبيه:</b> بعدك ما رابط قناة الخزن لهذا القسم!\n"
            "اضغط على زر «❖ الإعدادات والقنوات» واربط قناة خزن الأفلام أو المسلسلات أو الأنمي أولاً.",
            parse_mode="HTML"
        )
        await call.answer()
        return

    await state.update_data(
        media_type=media_type,
        is_batch_mode=is_batch,
        batch_items=[],
        unassigned_batch_files=[],
        storage_chat_id=storage_ch,
        uploaded_files=[],
        append_media_id=None
    )
    await state.set_state(AdminStates.entering_title)
    kb = InlineKeyboardMarkup(inline_keyboard=[[ikb(text="إلغاء", style="danger", callback_data="cancel_op")]])
    if is_batch:
        await call.message.answer(
            "▣ <b>إضافة دفعة أفلام (أكثر من فيلم بوجبة وحدة)</b>\n\n"
            "1. <b>طريقة الأسطر السريعة:</b> اكتب أسماء الأفلام كلها برسالة وحدة (كل فيلم بسطر):\n"
            "<code>Escape Plan 2013\nEscape Plan 2\nThe Mechanic</code>\n\n"
            "2. <b>طريقة فيلم بعد فيلم:</b> اكتب اسم الفيلم الأول هسة، ورا ما تختاره دوس <b>«➕ إضافة فيلم آخر للدفعة»</b>!",
            reply_markup=kb,
            parse_mode="HTML"
        )
    else:
        sec_ar = "الأنمي" if media_type == "anime" else ("المسلسل" if media_type == "tv" else "الفيلم")
        await call.message.answer(
            f"◈ <b>اسم {sec_ar}</b>\n\n"
            f"✦ اكتب اسم {sec_ar} للبحث في TMDB (مثال: Solo Leveling أو Escape Plan 2013):",
            reply_markup=kb,
            parse_mode="HTML"
        )
    await call.answer()


@admin_router.message(AdminStates.entering_title)
async def process_title_search(message: Message, state: FSMContext):
    if not message.text:
        return
    query = message.text.strip()
    data = await state.get_data()
    media_type = data.get("media_type", "movie")
    batch_items: List[Dict[str, Any]] = list(data.get("batch_items") or [])

    # فحص ما إذا أرسل المشرف عدة أفلام في رسالة واحدة (كل فيلم بسطر)
    lines = [ln.strip() for ln in query.splitlines() if ln.strip()]
    if media_type == "movie" and len(lines) >= 2:
        status_msg = await message.answer(f"◷ جاري البحث في TMDb عن <b>{len(lines)}</b> أفلام وتجهيز سلة الدفعة...", parse_mode="HTML")
        added_This_turn = 0
        not_found_lines = []
        for ln in lines[:15]:
            res_list = await search_tmdb(ln, media_type="movie")
            if res_list:
                top_r = res_list[0]
                det = await get_tmdb_details(top_r["tmdb_id"], media_type="movie")
                if det:
                    existing = find_existing_media(det["tmdb_id"], "movie", det["title"], det.get("year", ""))
                    # تجنب تكرار نفس الفيلم داخل السلة
                    if not any(int((x.get("details") or {}).get("tmdb_id") or -1) == int(det["tmdb_id"]) for x in batch_items):
                        batch_items.append({
                            "details": det,
                            "append_media_id": existing["id"] if existing else None,
                            "raw_query": ln,
                            "files": []
                        })
                        added_This_turn += 1
                    continue
            not_found_lines.append(ln)

        try:
            await status_msg.delete()
        except Exception:
            pass

        await state.update_data(batch_items=batch_items, is_batch_mode=True)
        if not batch_items:
            await message.answer("◈ تعذر العثور على الأفلام المرسلة في TMDb. جرب كتابة الاسم بالإنجليزي.")
            return

        if not_found_lines:
            await message.answer(
                f"❖ لم يتم العثور على: <code>{', '.join(not_found_lines)}</code> — يمكنك إضافتها عبر زر «➕ إضافة فيلم آخر للدفعة».",
                parse_mode="HTML"
            )
        await prompt_batch_files_upload(message, state)
        return

    results = await search_tmdb(query, media_type=media_type)
    if not results:
        kb_rows = [
            [InlineKeyboardButton(text="✎ إضافة البيانات بهذا الاسم المكتوب", callback_data="manual_entry")],
        ]
        if batch_items:
            kb_rows.append([InlineKeyboardButton(text=f"▸ الاكتفاء بالأفلام المضافة ({len(batch_items)}) وإرسال الفيديوهات", callback_data="batch_ready_upload")])
        kb_rows.append([InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")])
        await message.answer(
            f"◈ ما لكيت نتائج بـ TMDB لـ «{query}».\n"
            "عيد كتابة الاسم أو دوس على المتابعة:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows)
        )
        await state.update_data(raw_query=query)
        return

    await state.set_state(AdminStates.choosing_tmdb_result)
    buttons = []
    for r in results:
        existing = find_existing_media(r["tmdb_id"], media_type, r["title"], r["year"])
        orig_hint = f" · {r['original_title']}" if r.get("original_title") and r["original_title"] != r["title"] else ""
        if existing:
            existing_qs = ", ".join(sorted(list({f"{f['quality']}p" for f in existing.get("files", [])})))
            btn_text = f"⟐ {r['title']}{orig_hint} ({r['year']}) [موجود: {existing_qs}]"
        else:
            btn_text = f"◈ {r['title']}{orig_hint} ({r['year']})"
        buttons.append([InlineKeyboardButton(text=btn_text[:62], callback_data=f"tmdb_{r['tmdb_id']}")])
    buttons.append([InlineKeyboardButton(text="✎ مو بالقائمة (إضافة بالاسم المكتوب)", callback_data="manual_entry")])
    if batch_items:
        buttons.append([InlineKeyboardButton(text=f"▸ الانتقال لإرسال الفيديوهات ({len(batch_items)} بالسلة)", callback_data="batch_ready_upload")])
    buttons.append([InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")])

    await state.update_data(raw_query=query)
    batch_note = f"\n▣ <b>الأفلام المحفوظة بسلة الدفعة حالياً: {len(batch_items)}</b>" if batch_items else ""
    await message.answer(
        f"النتائج من TMDB ◈{batch_note}\n\n"
        "✦ اختار العمل الصحيح من القائمة\n"
        "✦ الأعمال اللي يمها ⟐ راح <b>تندمج الدقة أو الحلقات الجديدة</b> عليها ويتحدث منشور القناة تلقائياً!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )


@admin_router.callback_query(F.data == "manual_entry")
async def cb_manual_entry(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    raw_q = data.get("raw_query", "عمل جديد")
    mtype = data.get("media_type", "movie")
    existing = find_existing_media(0, mtype, raw_q, "")
    details = existing if existing else {
        "tmdb_id": 0,
        "media_type": mtype,
        "title": raw_q,
        "original_title": raw_q,
        "year": "2026",
        "duration": "115 دقيقة" if mtype == "movie" else "1 موسم",
        "country": "الولايات المتحدة الأمريكية",
        "category_label": "فيلم أجنبي" if mtype == "movie" else "مسلسل أجنبي",
        "rating": 7.5,
        "genres": ["أكشن وحركة", "إثارة وتشويق"],
        "overview": "مشاهدة ممتعة عبر تطبيق فورتكس.",
        "poster_url": "https://via.placeholder.com/500x750/111827/10b981?text=Vortex",
        "backdrop_url": "https://via.placeholder.com/780x439/111827/10b981?text=Vortex"
    }
    batch_items: List[Dict[str, Any]] = list(data.get("batch_items") or [])
    if mtype == "movie":
        batch_items.append({
            "details": details,
            "append_media_id": existing["id"] if existing else None,
            "raw_query": raw_q,
            "files": []
        })
    await state.update_data(details=details, append_media_id=existing["id"] if existing else None, batch_items=batch_items)
    if len(batch_items) > 1 or data.get("is_batch_mode"):
        await prompt_batch_files_upload(call.message, state)
    else:
        await prompt_file_upload(call.message, state, details, existing=existing)
    await call.answer()


@admin_router.callback_query(AdminStates.choosing_tmdb_result, F.data.startswith("tmdb_"))
async def cb_select_tmdb(call: CallbackQuery, state: FSMContext):
    tmdb_id = int(call.data.split("_")[1])
    data = await state.get_data()
    media_type = data.get("media_type", "movie")
    raw_q = data.get("raw_query", "")

    details = await get_tmdb_details(tmdb_id, media_type=media_type)
    if not details:
        await call.answer("◈ تعذر سحب تفاصيل العمل من TMDB", show_alert=True)
        return

    existing = find_existing_media(tmdb_id, media_type, details["title"], details.get("year", ""))
    batch_items: List[Dict[str, Any]] = list(data.get("batch_items") or [])
    if media_type == "movie":
        # إذا لم يكن مضافاً مسبقاً للسلة نضيفه
        if not any(int((x.get("details") or {}).get("tmdb_id") or -1) == int(tmdb_id) for x in batch_items):
            batch_items.append({
                "details": details,
                "append_media_id": existing["id"] if existing else None,
                "raw_query": raw_q or details.get("original_title") or details["title"],
                "files": []
            })

    await state.update_data(details=details, append_media_id=existing["id"] if existing else None, batch_items=batch_items)
    if len(batch_items) > 1 or data.get("is_batch_mode"):
        await prompt_batch_files_upload(call.message, state)
    else:
        await prompt_file_upload(call.message, state, details, existing=existing)
    await call.answer()


@admin_router.callback_query(F.data == "batch_add_more")
async def cb_batch_add_more(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    batch_items: List[Dict[str, Any]] = list(data.get("batch_items") or [])
    # إذا كان هناك ملفات رفعت للفيلم الأول قبل الضغط على إضافة فيلم آخر، نحفظها داخل العنصر الأول
    uploaded_files = list(data.get("uploaded_files") or [])
    if batch_items and uploaded_files and not batch_items[0].get("files"):
        batch_items[0]["files"] = uploaded_files
        await state.update_data(uploaded_files=[])

    await state.update_data(is_batch_mode=True, batch_items=batch_items)
    await state.set_state(AdminStates.entering_title)

    cart_lines = []
    for i, b in enumerate(batch_items, 1):
        d = b["details"]
        orig = d.get("original_title") or d["title"]
        cart_lines.append(f"{i}. <b>{html.escape(d['title'])} ({d.get('year', '')})</b> — <code>{html.escape(orig)}</code>")

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"▸ جاهز لإرسال فيديوهات الدفعة ({len(batch_items)} أفلام)", callback_data="batch_ready_upload")],
        [InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")]
    ])
    await call.message.answer(
        f"▣ <b>سلة دفعة الأفلام الحالية ({len(batch_items)} أفلام):</b>\n"
        + "\n".join(cart_lines)
        + "\n\n✦ <b>اكتب هسة اسم الفيلم التالي</b> للبحث عنه وإضافته للدفعة (أو اكتب عدة أفلام كل فيلم بسطر):",
        reply_markup=kb,
        parse_mode="HTML"
    )
    await call.answer()


@admin_router.callback_query(F.data == "batch_ready_upload")
async def cb_batch_ready_upload(call: CallbackQuery, state: FSMContext):
    await prompt_batch_files_upload(call.message, state)
    await call.answer()


async def prompt_batch_files_upload(message: Message, state: FSMContext):
    await state.set_state(AdminStates.uploading_files)
    data = await state.get_data()
    batch_items: List[Dict[str, Any]] = list(data.get("batch_items") or [])

    lines = []
    for idx, item in enumerate(batch_items, 1):
        det = item["details"]
        orig = det.get("original_title") or det["title"]
        f_cnt = len(item.get("files") or [])
        qs = ",".join(sorted({f"{x['quality']}p" for x in (item.get("files") or [])}))
        st_badge = f"✓ ({f_cnt} ملف: {qs})" if f_cnt > 0 else "◷ بانتظار الفيديو"
        lines.append(
            f"{idx}. <b>{html.escape(det['title'])} ({det.get('year', '')})</b> [{st_badge}]\n"
            f"   ▣ انسخ للوصف: <code>{html.escape(orig)}</code>"
        )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ إضافة فيلم آخر للدفعة", callback_data="batch_add_more")],
        [
            InlineKeyboardButton(text="إلغاء", callback_data="cancel_op"),
            InlineKeyboardButton(text=f"✓ تم (حفظ ونشر الدفعة: {len(batch_items)} أفلام)", callback_data="finish_upload")
        ]
    ])

    await message.answer(
        f"▣ <b>سلة الدفعة جاهزة ({len(batch_items)} أفلام) — دز الفيديوهات هسة!</b>\n"
        "━━━━━━━━━━━━━━\n"
        + "\n".join(lines)
        + "\n━━━━━━━━━━━━━━\n"
        "◈ <b>شلون تدز الفيديوهات؟</b>\n"
        "• دز فيديوهات الأفلام كلها دفعة وحدة (أو واحد ورا الثاني).\n"
        "• اكتب بوصف كل فيديو اسم الفيلم (أو الاسم + الدقة)، مثل:\n"
        "  <code>Escape Plan 480</code> أو <code>Escape Plan720</code> أو بس <code>Escape Plan</code>\n"
        "• البوت راح يتعرف على الفيلم والدقة ويربط كل فيديو بفيلمه تلقائياً!\n"
        "• ورا ما تكمل إرسال كل الفيديوهات دوس <b>«✓ تم (حفظ ونشر الدفعة)»</b>.",
        reply_markup=kb,
        parse_mode="HTML"
    )


async def prompt_file_upload(message: Message, state: FSMContext, details: Dict[str, Any], existing: Optional[Dict[str, Any]] = None):
    await state.set_state(AdminStates.uploading_files)
    genres_str = "، ".join(details.get("genres", []))
    auto_skey, auto_stitle = auto_detect_section_key(
        media_type=details.get("media_type", "movie"),
        country=details.get("country", ""),
        category_label=details.get("category_label", ""),
        genres=details.get("genres", []),
        title=details.get("title", ""),
        explicit_section_key=details.get("section_key") or (existing.get("section_key", "") if existing else "")
    )
    details["section_key"] = auto_skey
    details["section_title"] = auto_stitle
    await state.update_data(details=details)

    merge_banner = ""
    if existing:
        existing_qs = " ، ".join(sorted(list({f"{f['quality']}p" for f in existing.get("files", [])})))
        merge_banner = (
            f"\n\n⟐ <b>وضع الدمج التلقائي شغال:</b>\n"
            f"هذا العمل موجود بالتطبيق (#id{existing['id']} — الدقات الحالية: <code>{existing_qs or 'ماكو'}</code>).\n"
            f"أي دقة جديدة أو حلقة تدزها هسة راح تنضاف على نفس العمل ويتحدث منشور القناة تلقائياً!"
        )

    orig_copy = details.get("original_title") or details.get("title") or ""
    caption = (
        f"<b>{html.escape(str(details['title']))} ({details.get('year', '')})</b> ◈\n"
        f"▣ الاسم للنسخ: <code>{html.escape(str(orig_copy))}</code>\n\n"
        f"✦ {details.get('country', '')} · {details.get('category_label', '')}\n"
        f"▣ مربع القسم بالرئيسية: <b>{html.escape(str(details.get('section_title') or 'تلقائي'))}</b>\n"
        f"✦ {genres_str}\n"
        f"✦ {html.escape(str(details.get('overview', ''))[:360])}"
        f"{merge_banner}"
    )
    if details.get("poster_url") and details["poster_url"].startswith("http"):
        try:
            await message.answer_photo(photo=details["poster_url"], caption=caption, parse_mode="HTML")
        except Exception:
            await message.answer(caption, parse_mode="HTML")
    else:
        await message.answer(caption, parse_mode="HTML")

    media_type = details.get("media_type", "movie")
    sec_lbl = details.get("section_title") or "تلقائي"
    kb_rows = [
        [InlineKeyboardButton(text=f"▣ القسم: {sec_lbl} (اضغط لتغييره)", callback_data="add_pick_sec")]
    ]
    if media_type == "movie":
        kb_rows.append([InlineKeyboardButton(text="➕ إضافة فيلم آخر وياه بنفس الدفعة", callback_data="batch_add_more")])
    kb_rows.append([
        InlineKeyboardButton(text="إلغاء", callback_data="cancel_op"),
        InlineKeyboardButton(text="✓ تم (حفظ ونشر)", callback_data="finish_upload")
    ])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    if media_type == "movie":
        instructions = (
            "إرسال ملفات الدقة أو تكوين دفعة ▸\n\n"
            "✦ <b>إذا تريده وحده:</b> دز ملف أو ملفات الفيلم مباشرة (1080p / 720p / 480p) ودوس «تم».\n"
            "✦ <b>إذا تريد تضيف وياه أفلام ثانية بدفعة وحدة:</b> دوس <b>«➕ إضافة فيلم آخر وياه بنفس الدفعة»</b>!"
        )
    else:
        instructions = (
            "إرسال حلقات المسلسل (مع جلب العناوين والتحقق التلقائي من TMDb) ▸\n\n"
            "✦ دز ملفات الحلقات، واكتب رقم الموسم والحلقة بالتعليق (مثال: موسم 1 حلقة 1 دقة 720)\n"
            "✦ إذا ما كتبت رقم الحلقة، البوت راح يرقمهن بالتسلسل تلقائياً!\n"
            "✦ <b>جديد:</b> البوت يجيب <b>عنوان كل حلقة ومدتها</b> من TMDb تلقائياً ويتحقق من عدد حلقات الموسم!\n"
            "✦ دوس «تم» ورا ما تكمل إرسال كل الحلقات"
        )
    if get_setting("video_cover_file_id", "").strip():
        instructions += (
            "\n\n▣ الغلاف: <b>مفعّل</b> — يتركب تلقائياً بثواني بدون إعادة رفع الفيديو.\n"
            "✦ دز الفيلم كـ«فيديو» عادي (mp4) مو كـ«ملف» حتى ينطبق الغلاف."
        )
    await message.answer(instructions, reply_markup=kb, parse_mode="HTML")


TG_API_BASE = "https://api.telegram.org"

# قفل متسلسل لكل مشرف لضمان عدم ضياع أي حلقة عند إرسال أو توجيه 12+ فيديو دفعة واحدة
_ADMIN_UPLOAD_LOCKS: Dict[int, asyncio.Lock] = {}


def _get_admin_upload_lock(admin_uid: int) -> asyncio.Lock:
    lk = _ADMIN_UPLOAD_LOCKS.get(admin_uid)
    if lk is None:
        lk = asyncio.Lock()
        _ADMIN_UPLOAD_LOCKS[admin_uid] = lk
    return lk


async def tg_send_video_with_cover(chat_id, video_file_id: str, cover_file_id: str, caption: str = "") -> int:
    url = f"{TG_API_BASE}/bot{admin_bot.token}/sendVideo"
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "video": video_file_id,
        "cover": cover_file_id,
        "supports_streaming": True,
    }
    if caption:
        payload["caption"] = caption[:1024]
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as session:
        for attempt in range(4):
            async with session.post(url, json=payload) as resp:
                data = await resp.json(content_type=None)
            if isinstance(data, dict) and data.get("ok"):
                return int(data["result"]["message_id"])
            # معالجة تلقائية لحدود سرعة تيليجرام (429 RetryAfter) عند إرسال موسم كامل دفعة واحدة
            if isinstance(data, dict) and data.get("error_code") == 429:
                retry_sec = int((data.get("parameters") or {}).get("retry_after") or 3)
                await asyncio.sleep(retry_sec + 1)
                continue
            desc = data.get("description", "sendVideo failed") if isinstance(data, dict) else "sendVideo failed"
            raise RuntimeError(desc)
    raise RuntimeError("sendVideo max retries exceeded")


async def _safe_copy_to_storage(chat_target, from_chat_id: int, message_id: int, caption: str) -> int:
    for attempt in range(4):
        try:
            copied = await admin_bot.copy_message(
                chat_id=chat_target,
                from_chat_id=from_chat_id,
                message_id=message_id,
                caption=caption[:1024] if caption else None
            )
            return int(copied.message_id)
        except Exception as e:
            err_s = str(e)
            m_wait = re.search(r"retry after (\d+)", err_s, re.IGNORECASE)
            if m_wait and attempt < 3:
                await asyncio.sleep(int(m_wait.group(1)) + 1)
                continue
            if attempt < 2:
                await asyncio.sleep(1.2 * (attempt + 1))
                continue
            raise e
    raise RuntimeError("copy_message failed")


def detect_quality_season_episode(
    message: Message,
    current_files_count: int,
    media_type: str,
    existing_session_files: Optional[List[Dict[str, Any]]] = None,
    db_existing_files: Optional[List[Dict[str, Any]]] = None
) -> tuple[str, int, int, str, float, int, str]:
    raw_caption = (message.caption or "").strip()
    file_name = ""
    if message.document and message.document.file_name:
        file_name = message.document.file_name
    elif message.video and getattr(message.video, "file_name", None):
        file_name = str(message.video.file_name or "")

    combined_text = f"{raw_caption} {file_name}".strip()

    quality = None
    # دعم جميع الدقات السبع: 144, 240, 360, 480, 720, 1080, 4K (2160)
    if re.search(r"(?:^|[^a-zA-Z0-9])(4k|2160p?|uhd)(?:$|[^a-zA-Z0-9])", combined_text, re.IGNORECASE):
        quality = "4K"
    else:
        q_match = re.search(r"(?:^|[^\d])(1080|720|480|360|240|144)\s*p?(?:$|[^\d])", combined_text, re.IGNORECASE)
        if q_match:
            quality = q_match.group(1)

    file_size_bytes = 0
    duration_sec = 0
    video_file_id = ""

    if message.video:
        video_file_id = message.video.file_id or ""
        file_size_bytes = message.video.file_size or 0
        duration_sec = message.video.duration or 0
        if not quality:
            h = message.video.height or 0
            w = message.video.width or 0
            dim = min(h, w) if (h and w) else h
            max_dim = max(h, w)
            if dim >= 1600 or max_dim >= 3200:
                quality = "4K"
            elif dim >= 900 or max_dim >= 1800:
                quality = "1080"
            elif dim >= 600 or max_dim >= 1150:
                quality = "720"
            elif dim >= 420 or max_dim >= 780:
                quality = "480"
            elif dim >= 300 or max_dim >= 580:
                quality = "360"
            elif dim >= 200 or max_dim >= 380:
                quality = "240"
            elif dim > 0:
                quality = "144"
            else:
                quality = "720"
    elif message.document:
        video_file_id = message.document.file_id or ""
        file_size_bytes = message.document.file_size or 0

    quality = normalize_quality_value(quality or "720")

    season: Optional[int] = None
    episode: Optional[int] = None
    explicit_ep = False

    if media_type in ("tv", "anime"):
        # 1) فحص الصيغ القياسية مثل S01E05 أو s1e5 أو S01.E05 أو 1x05
        se_combo = re.search(r"(?:^|[\s._\-\[\(])s(\d{1,2})[\s._\-]*e(\d{1,4})(?=$|[\s._\-\]\)]|[a-zA-Z])", combined_text, re.IGNORECASE)
        if not se_combo:
            se_combo = re.search(r"(?:^|[\s._\-\[\(])(\d{1,2})x(\d{1,3})(?=$|[\s._\-\]\)])", combined_text, re.IGNORECASE)
        if se_combo:
            season = int(se_combo.group(1))
            episode = int(se_combo.group(2))
            explicit_ep = True

        # 2) فحص الموسم المكتوب صراحة (موسم 1 / الموسم 2 / Season 1)
        if season is None:
            s_m = re.search(r"(?:الموسم|موسم|season)\s*[:\-#]?\s*(\d{1,2})", combined_text, re.IGNORECASE)
            if not s_m:
                s_m = re.search(r"(?:^|[\s._\-\[\(])s(\d{1,2})(?=$|[\s._\-\]\)])", combined_text, re.IGNORECASE)
            if s_m:
                season = int(s_m.group(1))

        # 3) فحص رقم الحلقة المكتوب صراحة (حلقة 5 / الحلقة 12 / Episode 5 / Ep 5 / E05 / ح5)
        if episode is None:
            ep_m = re.search(r"(?:الحلقة|حلقة|episode|ep)\s*[:\-#]?\s*(\d{1,4})", combined_text, re.IGNORECASE)
            if not ep_m:
                ep_m = re.search(r"(?:^|[\s._\-\[\(])(?:e|ح)\s*(\d{1,4})(?=$|[\s._\-\]\)])", combined_text, re.IGNORECASE)
            if ep_m:
                cand_ep = int(ep_m.group(1))
                # استبعاد سنوات الإنتاج وأرقام الدقات السبع (144..2160)
                if cand_ep not in (144, 240, 360, 480, 720, 1080, 2160) and not (1980 <= cand_ep <= 2035):
                    episode = cand_ep
                    explicit_ep = True

        # 4) إذا لم يكتب كلمة "حلقة" لكن كتب رقماً منفرداً بالوصف (مثال: "1" أو "Teen Wolf 5")
        if episode is None and raw_caption:
            cleaned_for_num = re.sub(
                r"\b(19\d\d|20\d\d|4k|uhd|2160p?|1080p?|720p?|480p?|360p?|240p?|144p?|x264|x265|h264|h265|hevc|mp4|mkv|aac|10bit)\b",
                " ",
                raw_caption,
                flags=re.IGNORECASE
            )
            cleaned_for_num = re.sub(r"(?:الموسم|موسم|season)\s*\d+", " ", cleaned_for_num, flags=re.IGNORECASE)
            nums = [
                int(n) for n in re.findall(r"\b(\d{1,3})\b", cleaned_for_num)
                if 1 <= int(n) <= 500 and int(n) not in (144, 240, 360, 480, 720)
            ]
            if len(nums) == 1:
                episode = nums[0]
                explicit_ep = True

    if season is None or season <= 0:
        if existing_session_files:
            season = int(existing_session_files[-1].get("season") or 1)
        else:
            season = 1

    if media_type in ("tv", "anime"):
        all_prev = list(db_existing_files or []) + list(existing_session_files or [])
        used_eps_same_q = {
            int(f.get("episode") or 0)
            for f in all_prev
            if int(f.get("season") or 1) == season and normalize_quality_value(f.get("quality", "720")) == quality
        }

        if episode is None or episode <= 0:
            # ذكاء دمج الدقات: إذا كان الفيديو المرفوع يحمل دقة جديدة غير موجودة في الحلقة الأولى/الحالية،
            # نربطه بأول حلقة لا تملك هذه الدقة بدلاً من زيادة عدد الحلقات خطأً!
            next_ep = 1
            while next_ep in used_eps_same_q:
                next_ep += 1
            episode = next_ep
        elif not explicit_ep and episode in used_eps_same_q:
            next_ep = 1
            while next_ep in used_eps_same_q:
                next_ep += 1
            episode = next_ep
        elif explicit_ep and episode in {
            int(f.get("episode") or 0)
            for f in (existing_session_files or [])
            if int(f.get("season") or 1) == season and normalize_quality_value(f.get("quality", "720")) == quality
        }:
            # فقط إذا وجه المشرف عدة فيديوهات بنفس الدقة ونفس الكابشن المنسوخ داخل نفس الجلسة، نرقّمها بالتسلسل لنفس الدقة
            next_ep = 1
            while next_ep in used_eps_same_q:
                next_ep += 1
            episode = next_ep
    else:
        episode = 0

    ep_title = f"الحلقة {episode}" if episode > 0 else ""
    size_mb = round(file_size_bytes / (1024 * 1024), 1) if file_size_bytes else 750.0
    return quality, season, episode, ep_title, size_mb, duration_sec, video_file_id


def build_storage_caption(media_id: Optional[int], title: str, year: str, quality: str, season: int = 1, episode: int = 0, file_id: Optional[int] = None) -> str:
    id_part = f"#id{media_id}" if media_id else "#new"
    f_part = f" #file{file_id}" if file_id else ""
    ep_part = f" · S{int(season):02d}E{int(episode):02d}" if episode and int(episode) > 0 else ""
    yr_part = f" ({year})" if year else ""
    return f"{id_part}{f_part} · {title}{yr_part} · {quality}p{ep_part}"


@admin_router.message(AdminStates.uploading_files, F.video | F.document | F.animation)
async def handle_uploaded_media_file(message: Message, state: FSMContext):
    lock = _get_admin_upload_lock(message.from_user.id if message.from_user else 0)
    async with lock:
        await _handle_uploaded_media_file_locked(message, state)


async def _handle_uploaded_media_file_locked(message: Message, state: FSMContext):
    data = await state.get_data()
    storage_chat_id = data.get("storage_chat_id")
    uploaded_files = list(data.get("uploaded_files") or [])
    media_type = data.get("media_type", "movie")
    append_media_id = data.get("append_media_id")
    details = data.get("details") or {}
    batch_items: List[Dict[str, Any]] = list(data.get("batch_items") or [])
    is_multi_batch = (media_type == "movie" and (len(batch_items) > 1 or data.get("is_batch_mode")))

    # تحديد الفيلم المستهدف إذا كنا في وضع الدفعة (Batch Mode)
    matched_batch_idx: Optional[int] = None
    target_details = details
    target_append_id = append_media_id

    if is_multi_batch and batch_items:
        movie_clue, _ = extract_movie_clue_and_quality(message)
        matched_batch_idx = match_video_to_batch_item(movie_clue, batch_items)
        if matched_batch_idx is not None:
            target_details = batch_items[matched_batch_idx]["details"]
            target_append_id = batch_items[matched_batch_idx].get("append_media_id")

    db_existing_files = []
    if target_append_id and media_type in ("tv", "anime"):
        ex_media = get_media_by_id(target_append_id)
        if ex_media:
            db_existing_files = list(ex_media.get("files") or [])

    quality, season, episode, ep_title, size_mb, duration_sec, video_file_id = detect_quality_season_episode(
        message,
        len(db_existing_files) + len(uploaded_files),
        media_type,
        existing_session_files=uploaded_files,
        db_existing_files=db_existing_files
    )

    # ميزة التحقق التلقائي من الحلقات وإضافة عنوان الحلقة والمدة تلقائياً من TMDb للمسلسلات
    tmdb_ep_check_line = ""
    if media_type in ("tv", "anime") and episode > 0:
        tmdb_id_tv = int(target_details.get("tmdb_id") or 0)
        if tmdb_id_tv > 0:
            try:
                season_eps_map = await get_tmdb_season_episodes(tmdb_id_tv, season)
                if season_eps_map:
                    total_s_eps = len(season_eps_map)
                    ep_info = season_eps_map.get(episode)
                    if ep_info:
                        if ep_info.get("title"):
                            ep_title = ep_info["title"]
                        if (not duration_sec or duration_sec < 60) and ep_info.get("runtime_min"):
                            duration_sec = int(ep_info["runtime_min"]) * 60
                        tmdb_ep_check_line = (
                            f"✦ تحقق TMDb: <b>الحلقة {episode} من أصل {total_s_eps} بالموسم {season} ✓</b>\n"
                            f"✦ عنوان الحلقة التلقائي: <b>{html.escape(ep_title)}</b>\n"
                        )
                    else:
                        tmdb_ep_check_line = (
                            f"❖ <b>تنبيه TMDb:</b> الحلقة <b>{episode}</b> غير موجودة ضمن فهرس الموسم {season} في TMDb (عدد حلقات الموسم الرسمي: <b>{total_s_eps}</b> حلقة)!\n"
                        )
            except Exception as e:
                logger.debug(f"TMDb auto episode lookup note: {e}")

    chat_target = int(storage_chat_id) if re.match(r"^-?\d+$", str(storage_chat_id)) else storage_chat_id
    initial_storage_caption = build_storage_caption(
        target_append_id,
        target_details.get("original_title") or target_details.get("title") or "VORTEX",
        str(target_details.get("year") or ""),
        quality, season, episode
    )

    cover_fid = ""
    if message.video:
        cover_fid = await get_or_create_dynamic_cover_file_id(chat_target, target_details)
    cover_status = "none"
    cover_err = ""
    storage_message_id = None

    if cover_fid and message.video:
        try:
            storage_message_id = await tg_send_video_with_cover(
                chat_target, message.video.file_id, cover_fid, initial_storage_caption
            )
            cover_status = "applied"
        except Exception as e:
            cover_status = "failed"
            cover_err = str(e)[:150]
            logger.warning(f"Cover send failed, falling back to plain copy: {e}")
    elif get_setting("cover_enabled", "1") == "1" and not message.video:
        cover_status = "not_video"

    if storage_message_id is None:
        try:
            storage_message_id = await _safe_copy_to_storage(
                chat_target,
                message.chat.id,
                message.message_id,
                initial_storage_caption
            )
        except Exception as e:
            logger.error(f"Failed to copy to storage channel {storage_chat_id}: {e}")
            await message.answer(
                f"◈ تعذر حفظ الملف بقناة الخزن (<code>{storage_chat_id}</code>).\n"
                "تأكد إن بوت الإدارة مشرف (Admin) وعنده صلاحية نشر الرسائل بالقناة!",
                parse_mode="HTML"
            )
            return

    file_record = {
        "quality": quality,
        "season": season,
        "episode": episode,
        "episode_title": ep_title,
        "storage_chat_id": storage_chat_id,
        "storage_message_id": storage_message_id,
        "video_file_id": video_file_id,
        "storage_caption": initial_storage_caption,
        "file_size_mb": size_mb,
        "duration_sec": duration_sec
    }

    # إذا كنا في وضع الدفعة ولم يتمكن البوت من معرفة الفيلم بسبب عدم كتابة الاسم بالوصف، نعرض أزرار الربط السريع
    if is_multi_batch and len(batch_items) > 1 and matched_batch_idx is None:
        unassigned = list(data.get("unassigned_batch_files") or [])
        u_idx = len(unassigned)
        unassigned.append(file_record)
        await state.update_data(unassigned_batch_files=unassigned)

        rescue_btns = []
        for b_i, b_it in enumerate(batch_items):
            d_it = b_it["details"]
            rescue_btns.append([
                InlineKeyboardButton(
                    text=f"◈ ربط مع: {d_it['title']} ({d_it.get('year', '')})",
                    callback_data=f"assign_bf_{u_idx}_{b_i}"
                )
            ])
        rescue_btns.append([InlineKeyboardButton(text="✓ تم (حفظ ونشر الدفعة)", callback_data="finish_upload")])
        await message.answer(
            f"❖ <b>تم خزن الفيديو (دقة <code>{quality}p</code> · {size_mb}MB)</b>، لكن الوصف ما بيه اسم فيلم واضح!\n"
            "اختر الفيلم التابع له هذا الفيديو بضغطة واحدة من الأزرار الجوة:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rescue_btns),
            parse_mode="HTML"
        )
        return

    if is_multi_batch and batch_items and matched_batch_idx is not None:
        cur_b_files = list(batch_items[matched_batch_idx].get("files") or [])
        cur_b_files.append(file_record)
        batch_items[matched_batch_idx]["files"] = cur_b_files
        uploaded_files.append(file_record)
        await state.update_data(batch_items=batch_items, uploaded_files=uploaded_files)

        total_batch_files = sum(len(x.get("files") or []) for x in batch_items)
        status_lines = []
        for idx_b, x_b in enumerate(batch_items, 1):
            d_b = x_b["details"]
            qs_b = ", ".join(sorted({f"{f['quality']}p" for f in (x_b.get("files") or [])}))
            status_lines.append(
                f"{idx_b}. <b>{html.escape(d_b['title'])}</b>: "
                + (f"✓ <code>[{qs_b}]</code>" if qs_b else "◷ <i>بانتظار الفيديو</i>")
            )

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ إضافة فيلم آخر للدفعة", callback_data="batch_add_more")],
            [
                InlineKeyboardButton(text="إلغاء", callback_data="cancel_op"),
                InlineKeyboardButton(text=f"✓ تم (حفظ ونشر الدفعة: {total_batch_files} ملف)", callback_data="finish_upload")
            ]
        ])
        await message.answer(
            f"✓ <b>تم التعرف على الفيلم وربط الفيديو تلقائياً!</b>\n\n"
            f"◈ الفيلم: <b>{html.escape(target_details['title'])} ({target_details.get('year', '')})</b>\n"
            f"✦ الدقة المقروءة: <b>{quality}p</b> · الحجم: <b>{size_mb} MB</b>\n"
            f"━━━━━━━━━━━━━━\n"
            f"▣ <b>حالة سلة الدفعة الآن:</b>\n" + "\n".join(status_lines),
            reply_markup=kb,
            parse_mode="HTML"
        )
        return

    # الوضع الفردي (فيلم واحد أو مسلسل)
    uploaded_files.append(file_record)
    if batch_items and len(batch_items) == 1:
        batch_items[0]["files"] = uploaded_files
    await state.update_data(uploaded_files=uploaded_files, batch_items=batch_items)

    ep_info_str = f" (الموسم {season} - حلقة {episode})" if episode > 0 else ""
    cover_line = {
        "applied": "✦ الغلاف: <b>تم تركيبه ✓</b>\n",
        "failed": f"✦ الغلاف: <b>تعذّر تركيبه ◈</b> (انحفظ بدون غلاف) <code>{html.escape(cover_err)}</code>\n",
        "not_video": "✦ الغلاف: <b>ما انطبق ◈</b> (الملف مرسل كـ«ملف» مو «فيديو»)\n",
    }.get(cover_status, "")
    f_idx = len(uploaded_files) - 1
    q_choices_1 = ["144", "240", "360", "480"]
    q_choices_2 = ["720", "1080", "4K"]
    kb_rows = [
        [
            InlineKeyboardButton(
                text=f"{'✓ ' if quality == q_c else ''}{format_quality_badge(q_c)}",
                callback_data=f"upq_{f_idx}_{q_c}"
            ) for q_c in q_choices_1
        ],
        [
            InlineKeyboardButton(
                text=f"{'✓ ' if quality == q_c else ''}{format_quality_badge(q_c)}",
                callback_data=f"upq_{f_idx}_{q_c}"
            ) for q_c in q_choices_2
        ]
    ]
    if media_type in ("tv", "anime") and episode > 1:
        kb_rows.append([
            InlineKeyboardButton(text=f"⟐ دمج كدقة للحلقة {episode - 1}", callback_data=f"upep_{f_idx}_{episode - 1}"),
            InlineKeyboardButton(text=f"➕ نقل للحلقة {episode + 1}", callback_data=f"upep_{f_idx}_{episode + 1}")
        ])
    elif media_type in ("tv", "anime") and episode == 1:
        kb_rows.append([
            InlineKeyboardButton(text="➕ جعلها الحلقة 2", callback_data=f"upep_{f_idx}_2")
        ])
    if media_type == "movie":
        kb_rows.append([InlineKeyboardButton(text="➕ إضافة فيلم آخر وياه بنفس الدفعة", callback_data="batch_add_more")])
    kb_rows.append([
        InlineKeyboardButton(text="إلغاء", callback_data="cancel_op"),
        InlineKeyboardButton(text="✓ تم (حفظ ونشر)", callback_data="finish_upload")
    ])
    reply_txt = (
        f"تم استلام وحفظ الملف بقناة الخزن ▸\n\n"
        f"✦ الدقة المقروءة: <b>{quality}p</b>{ep_info_str}\n"
        f"{tmdb_ep_check_line}"
        f"✦ تسمية الخزن: <code>{html.escape(initial_storage_caption)}</code>\n"
        f"✦ الحجم: <b>{size_mb} MB</b>\n"
        f"{cover_line}"
        f"✦ الملفات المستلمة بهاي الجلسة: <b>{len(uploaded_files)}</b>\n\n"
        f"◈ دز ملف ثاني أو دوس <b>«✓ تم (حفظ ونشر)»</b>:"
    )
    for _att in range(3):
        try:
            await message.answer(
                reply_txt,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows),
                parse_mode="HTML"
            )
            break
        except Exception as ans_err:
            m_w = re.search(r"retry after (\d+)", str(ans_err), re.IGNORECASE)
            if m_w and _att < 2:
                await asyncio.sleep(int(m_w.group(1)) + 1)
                continue
            break



@admin_router.callback_query(AdminStates.uploading_files, F.data.startswith("upq_"))
async def cb_change_uploaded_file_quality(call: CallbackQuery, state: FSMContext):
    parts = call.data.split("_")
    f_idx = int(parts[1])
    new_q = normalize_quality_value(parts[2])
    data = await state.get_data()
    uploaded_files = list(data.get("uploaded_files") or [])
    if f_idx < 0 or f_idx >= len(uploaded_files):
        await call.answer("انتهت صلاحية هذا الزر", show_alert=True)
        return
    uploaded_files[f_idx]["quality"] = new_q
    batch_items = list(data.get("batch_items") or [])
    if batch_items and len(batch_items) == 1:
        batch_items[0]["files"] = uploaded_files
    await state.update_data(uploaded_files=uploaded_files, batch_items=batch_items)
    ep_num = int(uploaded_files[f_idx].get("episode") or 0)
    s_num = int(uploaded_files[f_idx].get("season") or 1)
    await call.answer(f"✓ تم تحديد الدقة: {format_quality_badge(new_q)}" + (f" (م{s_num} ح{ep_num})" if ep_num > 0 else ""))


@admin_router.callback_query(AdminStates.uploading_files, F.data.startswith("upep_"))
async def cb_change_uploaded_file_episode(call: CallbackQuery, state: FSMContext):
    parts = call.data.split("_")
    f_idx = int(parts[1])
    new_ep = int(parts[2])
    data = await state.get_data()
    uploaded_files = list(data.get("uploaded_files") or [])
    if f_idx < 0 or f_idx >= len(uploaded_files) or new_ep <= 0:
        await call.answer("انتهت صلاحية هذا الزر", show_alert=True)
        return
    uploaded_files[f_idx]["episode"] = new_ep
    uploaded_files[f_idx]["episode_title"] = f"الحلقة {new_ep}"
    await state.update_data(uploaded_files=uploaded_files)
    q_str = format_quality_badge(uploaded_files[f_idx].get("quality", "720"))
    await call.answer(f"✓ تم ربط الملف ({q_str}) بالحلقة {new_ep}!", show_alert=True)


@admin_router.callback_query(AdminStates.uploading_files, F.data.startswith("assign_bf_"))
async def cb_assign_unassigned_batch_file(call: CallbackQuery, state: FSMContext):
    parts = call.data.split("_")
    u_idx = int(parts[2])
    b_idx = int(parts[3])
    data = await state.get_data()
    unassigned = list(data.get("unassigned_batch_files") or [])
    batch_items = list(data.get("batch_items") or [])
    uploaded_files = list(data.get("uploaded_files") or [])

    if u_idx < 0 or u_idx >= len(unassigned) or b_idx < 0 or b_idx >= len(batch_items):
        await call.answer("انتهت صلاحية هذا الزر", show_alert=True)
        return

    file_rec = unassigned[u_idx]
    if file_rec.get("_assigned"):
        await call.answer("تم ربط هذا الفيديو مسبقاً ✓", show_alert=True)
        return

    file_rec["_assigned"] = True
    target_det = batch_items[b_idx]["details"]
    target_app_id = batch_items[b_idx].get("append_media_id")
    new_cap = build_storage_caption(
        target_app_id,
        target_det.get("original_title") or target_det.get("title") or "VORTEX",
        str(target_det.get("year") or ""),
        file_rec["quality"],
        1, 0
    )
    file_rec["storage_caption"] = new_cap
    cur_files = list(batch_items[b_idx].get("files") or [])
    cur_files.append(file_rec)
    batch_items[b_idx]["files"] = cur_files
    uploaded_files.append(file_rec)
    await state.update_data(batch_items=batch_items, unassigned_batch_files=unassigned, uploaded_files=uploaded_files)

    try:
        await call.message.edit_text(
            f"✓ تم ربط الفيديو (<code>{file_rec['quality']}p</code>) بفيلم: <b>{html.escape(target_det['title'])} ({target_det.get('year', '')})</b>",
            parse_mode="HTML"
        )
    except Exception:
        pass
    await call.answer("تم ربط الفيديو بنجاح ✓")


@admin_router.callback_query(AdminStates.uploading_files, F.data == "finish_upload")
async def cb_finish_upload(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    uploaded_files = list(data.get("uploaded_files") or [])
    details = data.get("details")
    batch_items: List[Dict[str, Any]] = list(data.get("batch_items") or [])
    media_type = data.get("media_type", "movie")

    # 1. معالجة نشر دفعة أفلام متعددة (Multi-Movie Batch Publish)
    items_with_files = [b for b in batch_items if b.get("files")]
    if media_type == "movie" and len(batch_items) > 1:
        if not items_with_files:
            await call.answer("◈ دز ملف فيديو واحد على الأقل لأفلام الدفعة قبل الحفظ!", show_alert=True)
            return

        try:
            await call.answer("◷ جاري حفظ ونشر الدفعة...")
        except Exception:
            pass
        await state.clear()
        status_wait = await call.message.answer(
            f"◷ جاري حفظ ونشر <b>{len(items_with_files)}</b> أفلام من الدفعة وتحديث القنوات و Firebase...",
            parse_mode="HTML"
        )
        summary_rows = []
        total_reqs_notified = 0
        activated_collections = set()

        for idx_b, b_item in enumerate(items_with_files):
            b_det = b_item["details"]
            b_files = b_item["files"]
            first_dur = b_files[0].get("duration_sec", 0)
            dur_str = b_det.get("duration", "115 دقيقة")
            if first_dur and first_dur > 60:
                dur_str = f"{max(1, round(first_dur / 60))} دقيقة"

            m_id, was_merged = add_or_merge_media(
                tmdb_id=b_det.get("tmdb_id", 0),
                media_type="movie",
                title=b_det["title"],
                original_title=b_det.get("original_title", b_det["title"]),
                year=b_det.get("year", ""),
                duration=dur_str,
                rating=b_det.get("rating", 0.0),
                genres=b_det.get("genres", []),
                overview=b_det.get("overview", ""),
                poster_url=b_det.get("poster_url", ""),
                backdrop_url=b_det.get("backdrop_url", ""),
                files=b_files,
                country=b_det.get("country", "الولايات المتحدة الأمريكية"),
                category_label=b_det.get("category_label", "فيلم أجنبي"),
                collection_id=int(b_det.get("collection_id") or 0),
                collection_name=str(b_det.get("collection_name") or ""),
                collection_poster=str(b_det.get("collection_poster") or ""),
                collection_backdrop=str(b_det.get("collection_backdrop") or ""),
                collection_total_parts=int(b_det.get("collection_total_parts") or 0),
                section_key=str(b_det.get("section_key") or ""),
                cast=b_det.get("cast") or [],
                trailer_url=str(b_det.get("trailer_url") or "")
            )

            upd_m = get_media_by_id(m_id)
            if upd_m:
                for f in upd_m.get("files", []):
                    final_cap = build_storage_caption(
                        m_id,
                        upd_m.get("original_title") or upd_m["title"],
                        str(upd_m.get("year") or ""),
                        str(f["quality"]),
                        1, 0,
                        file_id=int(f["id"])
                    )
                    if f.get("storage_caption") != final_cap:
                        update_media_file_record(f["id"], storage_caption=final_cap)
                        try:
                            await admin_bot.edit_message_caption(
                                chat_id=normalize_chat_target(f["storage_chat_id"]),
                                message_id=int(f["storage_message_id"]),
                                caption=final_cap
                            )
                        except Exception:
                            pass

            if idx_b > 0:
                await asyncio.sleep(1.3)  # حماية من FloodWait عند نشر عدة أفلام بالقناة
            pub_ok, _ = await publish_to_updates_channel(m_id)

            col_info = upd_m.get("collection_info") if upd_m else None
            if col_info and col_info.get("is_active"):
                activated_collections.add(col_info["name"])

            auto_reqs = auto_fulfill_matching_requests_for_media(m_id)
            if auto_reqs:
                total_reqs_notified += await notify_users_request_fulfilled(auto_reqs, upd_m)

            qs_str = " ، ".join(sorted({f"{f['quality']}p" for f in (upd_m.get("files", []) if upd_m else b_files)}))
            summary_rows.append(
                f"• <b>{html.escape(b_det['title'])} ({b_det.get('year', '')})</b> (<code>#id{m_id}</code>) — الدقات: <code>{qs_str}</code> {'[اندمج ⟐]' if was_merged else '[نُشر ✓]'}"
            )

        try:
            await status_wait.delete()
        except Exception:
            pass

        asyncio.create_task(send_auto_backup_to_storage(f"إضافة دفعة ({len(items_with_files)} أفلام)"))
        col_line = f"\n❖ السلاسل المفعّلة تلقائياً: <b>{html.escape(' ، '.join(activated_collections))} ✓</b>" if activated_collections else ""
        req_line = f"\n✦ الطلبات الملبّاة تلقائياً: <b>تم إشعار {total_reqs_notified} مستخدم ✓</b>" if total_reqs_notified else ""

        batch_tpl_kb = InlineKeyboardMarkup(inline_keyboard=[
            [ikb(text="📋 توليد كليشة الدفعة القابلة للضغط", style="danger", callback_data="tpl_generate_batch")],
            [ikb(text="📢 نشر كليشة الدفعة بالقناة العامة", style="primary", callback_data="tpl_publish_batch")],
            [ikb(text="▣ فتح قائمة كليشات الأعمال والمسلسلات", style="success", callback_data="open_template_menu")]
        ])
        await call.message.answer(
            f"▣ <b>تم حفظ ونشر دفعة الأفلام بنجاح ({len(items_with_files)} أفلام) ✓</b>\n"
            "━━━━━━━━━━━━━━\n"
            + "\n".join(summary_rows)
            + f"{col_line}{req_line}",
            reply_markup=batch_tpl_kb,
            parse_mode="HTML"
        )
        return

    # 2. الوضع الفردي (فيلم واحد أو مسلسل)
    if not uploaded_files and items_with_files:
        uploaded_files = items_with_files[0]["files"]
        details = items_with_files[0]["details"]

    if not uploaded_files or not details:
        await call.answer("◈ دز ملف فيديو واحد على الأقل قبل لا تدوس «تم»!", show_alert=True)
        return

    try:
        await call.answer("◷ جاري الحفظ والنشر...")
    except Exception:
        pass

    first_dur = uploaded_files[0].get("duration_sec", 0)
    duration_str = details.get("duration", "115 دقيقة")
    if first_dur and first_dur > 60 and details.get("media_type") == "movie":
        mins = max(1, round(first_dur / 60))
        duration_str = f"{mins} دقيقة"

    media_id, was_merged = add_or_merge_media(
        tmdb_id=details.get("tmdb_id", 0),
        media_type=details.get("media_type", "movie"),
        title=details["title"],
        original_title=details.get("original_title", details["title"]),
        year=details.get("year", ""),
        duration=duration_str,
        rating=details.get("rating", 0.0),
        genres=details.get("genres", []),
        overview=details.get("overview", ""),
        poster_url=details.get("poster_url", ""),
        backdrop_url=details.get("backdrop_url", ""),
        files=uploaded_files,
        country=details.get("country", "الولايات المتحدة الأمريكية"),
        category_label=details.get("category_label", "فيلم أجنبي"),
        collection_id=int(details.get("collection_id") or 0),
        collection_name=str(details.get("collection_name") or ""),
        collection_poster=str(details.get("collection_poster") or ""),
        collection_backdrop=str(details.get("collection_backdrop") or ""),
        collection_total_parts=int(details.get("collection_total_parts") or 0),
        section_key=str(details.get("section_key") or ""),
        cast=details.get("cast") or [],
        trailer_url=str(details.get("trailer_url") or "")
    )

    await state.clear()

    # إذا كان العمل مسلسلاً: مزامنة عناوين الحلقات ومددها تلقائياً من TMDb + بناء تقرير التحقق من الحلقات!
    tv_verify_note = ""
    if details.get("media_type") in ("tv", "anime"):
        try:
            await sync_series_with_tmdb(media_id, force=False)
            v_rep = await verify_series_episodes_with_tmdb(get_media_by_id(media_id))
            if v_rep and v_rep.get("summary_html"):
                tv_verify_note = "\n\n" + v_rep["summary_html"]
        except Exception as e:
            logger.debug(f"Auto TV verify/sync note: {e}")

    updated_media = get_media_by_id(media_id)
    if updated_media:
        for f in updated_media.get("files", []):
            final_cap = build_storage_caption(
                media_id,
                updated_media.get("original_title") or updated_media["title"],
                str(updated_media.get("year") or ""),
                str(f["quality"]),
                int(f.get("season") or 1),
                int(f.get("episode") or 0),
                file_id=int(f["id"])
            )
            if f.get("storage_caption") != final_cap:
                update_media_file_record(f["id"], storage_caption=final_cap)
                try:
                    st_target = normalize_chat_target(f["storage_chat_id"])
                    await admin_bot.edit_message_caption(
                        chat_id=st_target,
                        message_id=int(f["storage_message_id"]),
                        caption=final_cap
                    )
                except Exception:
                    pass

    all_qualities = " ، ".join([format_quality_badge(q) for q in sorted(list({normalize_quality_value(f["quality"]) for f in (updated_media.get("files", []) if updated_media else uploaded_files)}), key=quality_sort_key, reverse=True)])

    published, pub_status_msg = await publish_to_updates_channel(media_id)
    pub_note = f"\n✦ قنوات النشر: <b>{html.escape(pub_status_msg)}</b>"
    col_info = updated_media.get("collection_info") if updated_media else None
    if col_info and col_info.get("is_active"):
        pub_note += f"\n❖ السلسلة: <b>تفعّلت تلقائياً ضمن «{html.escape(col_info['name'])}» ({col_info['parts_count']} أجزاء) ✓</b>"
        for pt in col_info.get("parts", []):
            if pt["id"] != media_id:
                try:
                    await publish_to_updates_channel(pt["id"])
                except Exception:
                    pass

    auto_reqs = auto_fulfill_matching_requests_for_media(media_id)
    req_notified = 0
    if auto_reqs:
        req_notified = await notify_users_request_fulfilled(auto_reqs, updated_media)
    req_note = f"\n✦ الطلبات: <b>تمت تلبية {len(auto_reqs)} طلب تلقائياً وإشعار {req_notified} مستخدم ✓</b>" if auto_reqs else ""

    asyncio.create_task(send_auto_backup_to_storage(f"{'دمج دقة لـ' if was_merged else 'إضافة'} {details['title']}"))

    is_hid = bool(updated_media.get("is_hidden")) if updated_media else False
    hide_btn_txt = "○ إظهار العمل بالتطبيق ✓" if is_hid else "○ إخفاء العمل مؤقتاً من التطبيق"
    sec_title_done = (updated_media.get("section_title") if updated_media else details.get("section_title")) or "تلقائي"
    done_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"▣ القسم: {sec_title_done}", callback_data=f"edit_sec_pick_{media_id}"),
            InlineKeyboardButton(text=hide_btn_txt, callback_data=f"toggle_hide_{media_id}")
        ],
        [
            InlineKeyboardButton(text="➕ إضافة باقي الحلقات / الدقات", callback_data=f"appendto_{media_id}"),
            InlineKeyboardButton(text="✎ تعديل العمل", callback_data=f"edit_open_{media_id}")
        ],
        [
            ikb(text="📋 كليشة الأعمال القابلة للضغط", style="danger", callback_data="open_template_menu")
        ]
    ])
    action_title = "اندمجت الدقة/الحلقات الجديدة على نفس العمل بنجاح ⟐✓" if was_merged else "انتشر العمل الجديد بنجاح ✓"
    await call.message.answer(
        f"<b>{action_title}</b>\n\n"
        f"✦ العمل: <b>{html.escape(str(details['title']))} ({details.get('year', '')})</b> (<code>#id{media_id}</code>)\n"
        f"✦ كل الدقات المتوفرة هسة: <code>{all_qualities}</code>\n"
        f"✦ مجموع الملفات للعمل: <b>{len(updated_media.get('files', [])) if updated_media else len(uploaded_files)}</b>"
        f"{pub_note}{req_note}{tv_verify_note}",
        reply_markup=done_kb,
        parse_mode="HTML"
    )
    try:
        await call.answer("تم الحفظ بنجاح!")
    except Exception:
        pass


# ----------------- قسم إضافة حلقات أو دقات لعمل سابق (/append) -----------------
async def show_append_list(target_msg: Message):
    items = get_all_media()[:15]
    if not items:
        await target_msg.answer("ماكو أعمال منشورة بعد. استخدم /add لإضافة عمل جديد.")
        return
    buttons = []
    for item in items:
        qs = ",".join(sorted(list({f"{f['quality']}p" for f in item.get("files", [])})))
        buttons.append([InlineKeyboardButton(
            text=f"＋ {item['type_ar']} · {item['title']} ({item['year']}) [{qs}]",
            callback_data=f"appendto_{item['id']}"
        )])
    buttons.append([InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")])
    await target_msg.answer("اختار العمل اللي تريد تدمج دقة جديدة أو تضيف حلقات إله:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@admin_router.message(Command("append"))
async def admin_append_cmd(message: Message):
    if not is_user_admin(message.from_user.id):
        return
    await show_append_list(message)


@admin_router.callback_query(F.data == "quick_append")
async def cb_quick_append(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    await show_append_list(call.message)
    await call.answer()


@admin_router.callback_query(F.data.startswith("appendto_"))
async def cb_append_to_media(call: CallbackQuery, state: FSMContext):
    media_id = int(call.data.split("_")[1])
    media_item = get_media_by_id(media_id)
    if not media_item:
        await call.answer("العمل ما موجود", show_alert=True)
        return
    storage_ch = get_storage_channel_for_type(media_item["media_type"])
    await state.update_data(
        media_type=media_item["media_type"],
        storage_chat_id=storage_ch,
        uploaded_files=[],
        details=media_item,
        append_media_id=media_id
    )
    await prompt_file_upload(call.message, state, media_item, existing=media_item)
    await call.answer()


# =====================================================================
# نظام التعديل الكامل بدون تخريب (/edit) + استبدال ملف + دمج مكرر
# =====================================================================
async def start_edit_flow(target_msg: Message, state: FSMContext):
    await state.clear()
    await state.set_state(AdminStates.editing_search)
    items = get_all_media()[:12]
    buttons = []
    for item in items:
        qs = ",".join(sorted(list({f"{f['quality']}p" for f in item.get("files", [])})))
        buttons.append([InlineKeyboardButton(
            text=f"✎ #{item['id']} · {item['title']} ({item['year']}) [{qs}]",
            callback_data=f"edit_open_{item['id']}"
        )])
    buttons.append([InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")])
    await target_msg.answer(
        "✎ <b>التعديل على الأعمال بدون تخريب (/edit)</b>\n\n"
        "✦ اختار العمل من القائمة الجوة، أو <b>اكتب جزء من اسمه</b> للبحث عنه:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )


@admin_router.message(Command("edit"))
async def admin_edit_cmd(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    await start_edit_flow(message, state)


@admin_router.callback_query(F.data == "quick_edit")
async def cb_quick_edit(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    await start_edit_flow(call.message, state)
    await call.answer()


@admin_router.message(AdminStates.editing_search)
async def process_edit_search(message: Message, state: FSMContext):
    if not message.text:
        return
    results = search_media_by_title(message.text.strip())
    if not results:
        await message.answer("◈ ماكو أعمال مطابقة لهذا الاسم. جرب كلمة ثانية أو دوس إلغاء.")
        return
    buttons = []
    for item in results:
        qs = ",".join(sorted(list({f"{f['quality']}p" for f in item.get("files", [])})))
        buttons.append([InlineKeyboardButton(
            text=f"✎ #{item['id']} · {item['title']} ({item['year']}) [{qs}]",
            callback_data=f"edit_open_{item['id']}"
        )])
    buttons.append([InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")])
    await message.answer("اختار العمل اللي تريد تعدله:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


def build_edit_media_panel(media: Dict[str, Any]) -> Tuple[str, InlineKeyboardMarkup]:
    mid = media["id"]
    qs = " · ".join(sorted(list({f"{f['quality']}p" for f in media.get("files", [])}))) or "لا توجد ملفات"
    pub_info = f"مربوط (رسالة #{media['published_msg_id']})" if media.get("published_msg_id") else "غير منشور بعد"
    text = (
        f"✎ <b>لوحة تعديل العمل: {html.escape(media['title'])} (#id{mid})</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"◈ <b>الاسم الأصلي:</b> {html.escape(media.get('original_title') or '—')}\n"
        f"◈ <b>السنة:</b> {html.escape(str(media.get('year') or '—'))}  |  ★ <b>التقييم:</b> {media.get('rating', 0)}\n"
        f"◈ <b>التصنيف:</b> {html.escape(media.get('category_label') or '')}\n"
        f"❖ <b>الدقات/الملفات ({len(media.get('files', []))}):</b> <code>{qs}</code>\n"
        f"◈ <b>منشور القناة:</b> {pub_info}\n"
        "━━━━━━━━━━━━━━\n"
        "▾ <b>اختار التعديل اللي تريده (يتحدث بـ Firebase والقناة تلقائياً):</b>"
    )
    is_hid = bool(media.get("is_hidden"))
    sec_t = media.get("section_title") or "تلقائي"
    vis_line = "○ <b>مخفي مؤقتاً عن المستخدمين</b>" if is_hid else "✓ <b>ظاهر لجميع المستخدمين بالتطبيق</b>"
    text = text.replace(
        "━━━━━━━━━━━━━━\n▾ <b>اختار التعديل",
        f"▣ <b>مربع القسم بالرئيسية:</b> <b>{html.escape(sec_t)}</b>\n◈ <b>الظهور بالتطبيق:</b> {vis_line}\n━━━━━━━━━━━━━━\n▾ <b>اختار التعديل"
    )
    hide_lbl = "○ إظهار العمل بالتطبيق للمستخدمين ✓" if is_hid else "○ إخفاء العمل مؤقتاً من التطبيق"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=f"▣ تغيير مربع القسم ({sec_t})", callback_data=f"edit_sec_pick_{mid}")
        ],
        [
            InlineKeyboardButton(text=hide_lbl, callback_data=f"edit_toggle_hide_{mid}")
        ],
        [
            InlineKeyboardButton(text="✎ تغيير الاسم", callback_data=f"edit_field_{mid}_title"),
            InlineKeyboardButton(text="◈ تغيير السنة", callback_data=f"edit_field_{mid}_year")
        ],
        [
            InlineKeyboardButton(text="▣ تغيير رابط البوستر", callback_data=f"edit_field_{mid}_poster_url"),
            InlineKeyboardButton(text="▣ تغيير القصة/الوصف", callback_data=f"edit_field_{mid}_overview")
        ],
        [
            InlineKeyboardButton(text="⟐ إعادة جلب البيانات من TMDb", callback_data=f"edit_refetch_{mid}")
        ],
        [
            InlineKeyboardButton(text="❖ إدارة واستبدال ملفات الدقات والحلقات", callback_data=f"edit_files_{mid}")
        ],
        [
            InlineKeyboardButton(text="⟐ دمج عمل مكرر داخل هذا العمل", callback_data=f"edit_merge_pick_{mid}")
        ],
        [
            InlineKeyboardButton(text="◈ تحديث / نشر المنشور بقناة العرض", callback_data=f"edit_repub_{mid}")
        ],
        [
            InlineKeyboardButton(text="◂ رجوع لقسم المحتوى", callback_data="menu_content")
        ]
    ])
    return text, kb



@admin_router.callback_query(F.data.startswith("toggle_hide_"))
async def cb_quick_toggle_hide(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    media_id = int(call.data.split("_")[2])
    new_hidden = toggle_media_hidden(media_id)
    media = get_media_by_id(media_id)
    title_str = media["title"] if media else f"#{media_id}"
    status_str = "○ تم إخفاء العمل مؤقتاً عن المستخدمين بالتطبيق" if new_hidden else "✓ تم إظهار العمل لجميع المستخدمين بالتطبيق"
    hide_btn_txt = "○ إظهار العمل بالتطبيق ✓" if new_hidden else "○ إخفاء العمل مؤقتاً من التطبيق"
    done_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=hide_btn_txt, callback_data=f"toggle_hide_{media_id}")],
        [
            InlineKeyboardButton(text="➕ إضافة باقي الحلقات / الدقات", callback_data=f"appendto_{media_id}"),
            InlineKeyboardButton(text="✎ تعديل العمل", callback_data=f"edit_open_{media_id}")
        ]
    ])
    try:
        await call.message.edit_reply_markup(reply_markup=done_kb)
    except Exception:
        pass
    await call.answer(f"{status_str} ({title_str})", show_alert=True)


@admin_router.callback_query(F.data.startswith("edit_toggle_hide_"))
async def cb_edit_toggle_hide(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    media_id = int(call.data.split("_")[3])
    new_hidden = toggle_media_hidden(media_id)
    media = get_media_by_id(media_id)
    if media:
        text, kb = build_edit_media_panel(media)
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            pass
    await call.answer(
        "○ تم إخفاء العمل مؤقتاً عن المستخدمين!" if new_hidden else "✓ تم إظهار العمل لجميع المستخدمين بالتطبيق!",
        show_alert=True
    )




@admin_router.callback_query(F.data == "add_pick_sec")
async def cb_add_pick_sec(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    defs = get_all_section_defs(only_enabled=False)
    rows = []
    pair = []
    for d in defs:
        pair.append(InlineKeyboardButton(text=f"▣ {d['title']}", callback_data=f"add_set_sec_{d['section_key']}"))
        if len(pair) == 2:
            rows.append(pair)
            pair = []
    if pair:
        rows.append(pair)
    await call.message.answer(
        "▣ <b>اختار مربع القسم اللي تريد ينزل بيه هذا العمل بالصفحة الرئيسية:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        parse_mode="HTML"
    )
    await call.answer()


@admin_router.callback_query(F.data.startswith("add_set_sec_"))
async def cb_add_set_sec(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    skey = call.data.split("add_set_sec_", 1)[1]
    defs = {d["section_key"]: d["title"] for d in get_all_section_defs(only_enabled=False)}
    stitle = defs.get(skey, skey)
    data = await state.get_data()
    details = dict(data.get("details") or {})
    details["section_key"] = skey
    details["section_title"] = stitle

    # حفظ الدولة والتصنيف المتوافقين مع القسم المختار
    if "iraqi" in skey:
        details["country"] = "العراق"
        details["category_label"] = "مسلسل عراقي" if details.get("media_type") == "tv" else "فيلم عراقي"
    elif "turkish" in skey:
        details["country"] = "تركيا"
        details["category_label"] = "مسلسل تركي" if details.get("media_type") == "tv" else "فيلم تركي"
    elif "indian" in skey:
        details["country"] = "الهند"
        details["category_label"] = "مسلسل هندي" if details.get("media_type") == "tv" else "فيلم هندي"
    elif "arab" in skey:
        details["category_label"] = "مسلسل عربي" if details.get("media_type") == "tv" else "فيلم عربي"
    elif "asian" in skey:
        details["category_label"] = "مسلسل آسيوي" if details.get("media_type") == "tv" else "فيلم آسيوي"
    elif "anime" in skey:
        details["category_label"] = "أنمي"

    batch_items = list(data.get("batch_items") or [])
    if batch_items and len(batch_items) == 1:
        batch_items[0]["details"] = details
    await state.update_data(details=details, batch_items=batch_items)
    try:
        await call.message.edit_text(
            f"✓ تم تحديد القسم: <b>{html.escape(stitle)}</b> — هسة كمل إرسال ملفات الفيديو ودوس «تم (حفظ ونشر)».",
            parse_mode="HTML"
        )
    except Exception:
        pass
    await call.answer(f"تم اختيار: {stitle} ✓")


@admin_router.callback_query(F.data.startswith("edit_sec_pick_"))
async def cb_edit_sec_pick(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    media_id = int(call.data.split("edit_sec_pick_", 1)[1])
    media = get_media_by_id(media_id)
    if not media:
        await call.answer("العمل غير موجود", show_alert=True)
        return
    defs = get_all_section_defs(only_enabled=False)
    rows = []
    pair = []
    for d in defs:
        mark = "✓ " if media.get("section_key") == d["section_key"] else ""
        pair.append(InlineKeyboardButton(
            text=f"{mark}{d['title']}",
            callback_data=f"edit_sec_set_{media_id}_{d['section_key']}"
        ))
        if len(pair) == 2:
            rows.append(pair)
            pair = []
    if pair:
        rows.append(pair)
    rows.append([InlineKeyboardButton(text="◂ رجوع لتعديل العمل", callback_data=f"edit_open_{media_id}")])
    try:
        await call.message.edit_text(
            f"▣ <b>اختار مربع القسم لعمل «{html.escape(media['title'])}»:</b>\n\n"
            "✦ بمجرد اختيار القسم راح ينتقل العمل فوراً للمربع المختار بالصفحة الرئيسية ويتحدث البوستر المتقلب مالته تلقائياً!",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
            parse_mode="HTML"
        )
    except Exception:
        await call.message.answer(
            f"▣ <b>اختار مربع القسم لعمل «{html.escape(media['title'])}»:</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
            parse_mode="HTML"
        )
    await call.answer()


@admin_router.callback_query(F.data.startswith("edit_sec_set_"))
async def cb_edit_sec_set(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    rest = call.data.split("edit_sec_set_", 1)[1]
    media_id_str, skey = rest.split("_", 1)
    media_id = int(media_id_str)
    updated = set_media_section(media_id, skey)
    if updated:
        text, kb = build_edit_media_panel(updated)
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            pass
        await call.answer(f"✓ تم نقل العمل إلى قسم: {updated.get('section_title', skey)}", show_alert=True)
    else:
        await call.answer("تعذر تحديث القسم", show_alert=True)


@admin_router.callback_query(F.data.startswith("edit_open_"))
async def cb_edit_open(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    await state.clear()
    media_id = int(call.data.split("_")[2])
    media = get_media_by_id(media_id)
    if not media:
        await call.answer("◈ العمل غير موجود", show_alert=True)
        return
    text, kb = build_edit_media_panel(media)
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


@admin_router.callback_query(F.data.startswith("edit_field_"))
async def cb_edit_field_prompt(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    parts = call.data.split("_", 3)
    media_id = int(parts[2])
    field_name = parts[3]
    labels = {
        "title": "الاسم الجديد للعمل",
        "year": "سنة الإنتاج الجديدة (مثال: 2024)",
        "poster_url": "رابط صورة البوستر الجديد (يبدأ بـ https://)",
        "overview": "الوصف / القصة الجديدة للعمل"
    }
    await state.set_state(AdminStates.editing_field_value)
    await state.update_data(edit_media_id=media_id, edit_field=field_name)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="إلغاء", callback_data=f"edit_open_{media_id}")]])
    await call.message.answer(f"✎ دز هسة <b>{labels.get(field_name, field_name)}</b>:", reply_markup=kb, parse_mode="HTML")
    await call.answer()


@admin_router.message(AdminStates.editing_field_value)
async def save_edited_field_value(message: Message, state: FSMContext):
    if not message.text:
        return
    data = await state.get_data()
    media_id = int(data.get("edit_media_id") or 0)
    field_name = data.get("edit_field")
    val = message.text.strip()
    update_media_metadata(media_id, **{field_name: val})
    await state.clear()
    pub_ok, pub_msg = await publish_to_updates_channel(media_id)
    media = get_media_by_id(media_id)
    await message.answer(f"✓ تم تحديث البيانات بنجاح!\n✦ قناة العرض: {html.escape(pub_msg)}", parse_mode="HTML")
    if media:
        text, kb = build_edit_media_panel(media)
        await message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data.startswith("edit_refetch_"))
async def cb_edit_refetch_tmdb(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    media_id = int(call.data.split("_")[2])
    media = get_media_by_id(media_id)
    if not media:
        await call.answer("العمل غير موجود", show_alert=True)
        return
    tmdb_id = int(media.get("tmdb_id") or 0)
    if not tmdb_id:
        results = await search_tmdb(media["title"], media_type=media["media_type"])
        if results:
            tmdb_id = int(results[0]["tmdb_id"])
    if not tmdb_id:
        await call.answer("◈ ما لكيت العمل بـ TMDb. تأكد من مفتاح TMDb أو الاسم.", show_alert=True)
        return
    details = await get_tmdb_details(tmdb_id, media_type=media["media_type"])
    if not details:
        await call.answer("◈ تعذر سحب التفاصيل من TMDb حالياً.", show_alert=True)
        return
    update_media_metadata(
        media_id,
        tmdb_id=tmdb_id,
        title=details["title"],
        original_title=details.get("original_title", details["title"]),
        year=details.get("year", media["year"]),
        duration=details.get("duration", media["duration"]),
        country=details.get("country", media["country"]),
        category_label=details.get("category_label", media["category_label"]),
        rating=details.get("rating", media["rating"]),
        genres=details.get("genres", media["genres"]),
        overview=details.get("overview", media["overview"]),
        poster_url=details.get("poster_url", media["poster_url"]),
        backdrop_url=details.get("backdrop_url", media["backdrop_url"]),
        collection_id=details.get("collection_id", media.get("collection_id", 0)),
        collection_name=details.get("collection_name", media.get("collection_name", "")),
        collection_poster=details.get("collection_poster", media.get("collection_poster", "")),
        collection_backdrop=details.get("collection_backdrop", media.get("collection_backdrop", "")),
        collection_total_parts=details.get("collection_total_parts", media.get("collection_total_parts", 0))
    )
    tv_sync_msg = ""
    if media.get("media_type") == "tv":
        try:
            await sync_series_with_tmdb(media_id, force=True, tmdb_id=tmdb_id)
            v_rep = await verify_series_episodes_with_tmdb(get_media_by_id(media_id))
            if v_rep and v_rep.get("summary_html"):
                tv_sync_msg = "\n\n" + v_rep["summary_html"]
        except Exception:
            pass
    auto_group_similar_movies()
    await publish_to_updates_channel(media_id)
    updated = get_media_by_id(media_id)
    text, kb = build_edit_media_panel(updated)
    await call.message.edit_text(text + tv_sync_msg, reply_markup=kb, parse_mode="HTML")
    await call.answer("✓ تم تحديث بيانات العمل وعناوين الحلقات من TMDb!", show_alert=True)


@admin_router.callback_query(F.data.startswith("edit_repub_"))
async def cb_edit_republish(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    media_id = int(call.data.split("_")[2])
    ok, msg = await publish_to_updates_channel(media_id)
    await call.answer(("✓ " if ok else "◈ ") + msg[:180], show_alert=True)
    media = get_media_by_id(media_id)
    if media:
        text, kb = build_edit_media_panel(media)
        try:
            await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
        except Exception:
            pass


@admin_router.callback_query(F.data.startswith("edit_files_"))
async def cb_edit_files_list(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    media_id = int(call.data.split("_")[2])
    media = get_media_by_id(media_id)
    if not media:
        return
    buttons = []
    for f in media.get("files", []):
        ep_s = f" · S{f.get('season',1):02d}E{f['episode']:02d}" if f.get("episode", 0) > 0 else ""
        buttons.append([
            InlineKeyboardButton(
                text=f"⟐ استبدال ملف #{f['id']} ({f['quality']}p{ep_s})",
                callback_data=f"repfile_{media_id}_{f['id']}"
            ),
            InlineKeyboardButton(
                text=f"❖ دقة/حلقة #{f['id']}",
                callback_data=f"metafile_{media_id}_{f['id']}"
            )
        ])
    buttons.append([InlineKeyboardButton(text="◂ رجوع للوحة العمل", callback_data=f"edit_open_{media_id}")])
    await call.message.edit_text(
        f"❖ <b>إدارة وتعديل ملفات: {html.escape(media['title'])}</b>\n\n"
        "✦ دوس <b>«استبدال ملف»</b> لرفع نسخة فيديو أفضل بنفس رقم الملف بدون ما يتغير أي رابط.\n"
        "✦ أو دوس <b>«دقة/حلقة»</b> لتصحيح رقم الدقة أو الموسم أو الحلقة:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )
    await call.answer()


@admin_router.callback_query(F.data.startswith("repfile_"))
async def cb_replace_file_prompt(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    parts = call.data.split("_")
    media_id = int(parts[1])
    file_id = int(parts[2])
    await state.set_state(AdminStates.replacing_file_upload)
    await state.update_data(rep_media_id=media_id, rep_file_id=file_id)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="إلغاء", callback_data=f"edit_files_{media_id}")]])
    await call.message.answer(
        f"⟐ <b>استبدال الملف #{file_id} بنسخة جديدة</b>\n\n"
        "✦ دز هسة ملف الفيديو الجديد وراح ياخذ مكان القديم مباشرة بنفس الـ ID:",
        reply_markup=kb, parse_mode="HTML"
    )
    await call.answer()


@admin_router.message(AdminStates.replacing_file_upload, F.video | F.document)
async def save_replaced_file_upload(message: Message, state: FSMContext):
    data = await state.get_data()
    media_id = int(data.get("rep_media_id") or 0)
    file_id = int(data.get("rep_file_id") or 0)
    media = get_media_by_id(media_id)
    old_file = get_file_by_id(file_id)
    if not media or not old_file:
        await state.clear()
        await message.answer("◈ الملف أو العمل ما موجود.")
        return

    storage_ch = old_file.get("storage_chat_id") or get_setting("movies_channel_id", "")
    chat_target = normalize_chat_target(storage_ch)
    _, _, _, _, size_mb, duration_sec, video_file_id = detect_quality_season_episode(message, 0, media["media_type"])
    new_cap = build_storage_caption(
        media_id,
        media.get("original_title") or media["title"],
        str(media.get("year") or ""),
        str(old_file["quality"]),
        int(old_file.get("season") or 1),
        int(old_file.get("episode") or 0),
        file_id=file_id
    )

    cover_fid = get_setting("video_cover_file_id", "").strip()
    new_msg_id = None
    if cover_fid and message.video:
        try:
            new_msg_id = await tg_send_video_with_cover(chat_target, message.video.file_id, cover_fid, new_cap)
        except Exception:
            new_msg_id = None
    if new_msg_id is None:
        copied = await admin_bot.copy_message(
            chat_id=chat_target,
            from_chat_id=message.chat.id,
            message_id=message.message_id,
            caption=new_cap
        )
        new_msg_id = copied.message_id

    update_media_file_record(
        file_id,
        storage_chat_id=str(storage_ch),
        storage_message_id=int(new_msg_id),
        video_file_id=video_file_id,
        storage_caption=new_cap,
        file_size_mb=size_mb,
        duration_sec=duration_sec
    )
    await state.clear()
    await publish_to_updates_channel(media_id)
    await message.answer(f"✓ تم استبدال الملف #{file_id} بنجاح وتحديث Firebase ومنشور القناة!")


@admin_router.callback_query(F.data.startswith("metafile_"))
async def cb_edit_file_meta_prompt(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    parts = call.data.split("_")
    media_id = int(parts[1])
    file_id = int(parts[2])
    f_rec = get_file_by_id(file_id)
    if not f_rec:
        return
    await state.set_state(AdminStates.editing_file_meta)
    await state.update_data(meta_media_id=media_id, meta_file_id=file_id)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="إلغاء", callback_data=f"edit_files_{media_id}")]])
    await call.message.answer(
        f"❖ <b>تصحيح بيانات الملف #{file_id}</b>\n"
        f"الحالي: الدقة <code>{f_rec['quality']}</code> · الموسم <code>{f_rec.get('season',1)}</code> · الحلقة <code>{f_rec.get('episode',0)}</code>\n\n"
        "✦ دز القيم الجديدة بهذا الترتيب: <code>الدقة الموسم الحلقة</code>\n"
        "✦ مثال لفيلم دقة 1080: <code>1080 1 0</code>\n"
        "✦ مثال لمسلسل موسم 2 حلقة 5 دقة 720: <code>720 2 5</code>",
        reply_markup=kb, parse_mode="HTML"
    )
    await call.answer()


@admin_router.message(AdminStates.editing_file_meta)
async def save_edited_file_meta(message: Message, state: FSMContext):
    if not message.text:
        return
    parts = message.text.strip().split()
    if not parts or not parts[0].isdigit():
        await message.answer("◈ دز الأرقام بالصيغة الصحيحة، مثال: `1080 1 0`")
        return
    q = parts[0]
    s_num = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 1
    ep_num = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
    ep_title = f"الحلقة {ep_num}" if ep_num > 0 else ""

    data = await state.get_data()
    media_id = int(data.get("meta_media_id") or 0)
    file_id = int(data.get("meta_file_id") or 0)
    media = get_media_by_id(media_id)
    f_rec = get_file_by_id(file_id)
    if media and f_rec:
        new_cap = build_storage_caption(
            media_id, media.get("original_title") or media["title"], str(media.get("year") or ""),
            q, s_num, ep_num, file_id=file_id
        )
        update_media_file_record(file_id, quality=q, season=s_num, episode=ep_num, episode_title=ep_title, storage_caption=new_cap)
        try:
            await admin_bot.edit_message_caption(
                chat_id=normalize_chat_target(f_rec["storage_chat_id"]),
                message_id=int(f_rec["storage_message_id"]),
                caption=new_cap
            )
        except Exception:
            pass
    await state.clear()
    await publish_to_updates_channel(media_id)
    await message.answer(f"✓ تحدثت بيانات الملف #{file_id} وتحدث منشور القناة!")


@admin_router.callback_query(F.data.startswith("edit_merge_pick_"))
async def cb_edit_merge_pick(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    target_id = int(call.data.split("_")[3])
    items = [x for x in get_all_media() if x["id"] != target_id][:15]
    if not items:
        await call.answer("ماكو أعمال ثانية للدمج.", show_alert=True)
        return
    buttons = []
    for it in items:
        buttons.append([InlineKeyboardButton(
            text=f"⟐ دمج #{it['id']} ({it['title']} {it['year']}) داخل #{target_id}",
            callback_data=f"exec_merge_{it['id']}_{target_id}"
        )])
    buttons.append([InlineKeyboardButton(text="◂ رجوع", callback_data=f"edit_open_{target_id}")])
    await call.message.edit_text(
        f"⟐ <b>دمج عمل مكرر داخل العمل #{target_id}</b>\n\n"
        "اختار العمل المكرر اللي تريد تنقل كل دقاته وحلقاته وتعليقاته إلى هذا العمل وتحذف التكرار:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )
    await call.answer()


@admin_router.callback_query(F.data.startswith("exec_merge_"))
async def cb_exec_merge(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    parts = call.data.split("_")
    src_id = int(parts[2])
    tgt_id = int(parts[3])
    ok = merge_two_media_items(src_id, tgt_id)
    if ok:
        await publish_to_updates_channel(tgt_id)
        await call.answer("✓ تم دمج العملين بنجاح وحذف النسخة المكررة!", show_alert=True)
    else:
        await call.answer("◈ تعذر الدمج.", show_alert=True)
    media = get_media_by_id(tgt_id)
    if media:
        text, kb = build_edit_media_panel(media)
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")


# ----------------- قسم قائمة الأعمال (/list) و (/tonight) و (/del) -----------------
async def show_media_list(target_msg: Message):
    items = get_all_media()[:20]
    if not items:
        await target_msg.answer("ماكو أعمال منشورة بعد. استخدم /add لإضافة أول عمل.")
        return

    lines = ["▣ <b>آخر الأعمال المنشورة والدقات المتوفرة:</b>\n"]
    for item in items:
        qs = " · ".join(sorted(list({f"{f['quality']}p" for f in item.get("files", [])})))
        lines.append(f"✦ <code>#id{item['id']}</code> · {item['type_ar']} <b>{html.escape(item['title'])}</b> ({item['year']}) — [<code>{qs}</code>]")
    await target_msg.answer("\n".join(lines), parse_mode="HTML")


@admin_router.message(Command("list"))
async def admin_list_cmd(message: Message):
    if not is_user_admin(message.from_user.id):
        return
    await show_media_list(message)


@admin_router.callback_query(F.data == "quick_list")
async def cb_quick_list(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    await show_media_list(call.message)
    await call.answer()


async def show_tonight_picker(target_msg: Message):
    items = get_all_media()[:12]
    if not items:
        await target_msg.answer("ماكو أعمال منشورة حتى تحددها.")
        return

    buttons = []
    for item in items:
        prefix = "✦ " if item["is_tonight"] else ""
        buttons.append([InlineKeyboardButton(
            text=f"{prefix}{item['title']} ({item['year']})",
            callback_data=f"settonight_{item['id']}"
        )])
    buttons.append([InlineKeyboardButton(text="◂ رجوع", callback_data="menu_content")])

    await target_msg.answer(
        "اختيار الليلة ✦\n\n✦ اختار العمل اللي يطلع بمقدمة الصفحة الرئيسية",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )


@admin_router.message(Command("tonight"))
async def admin_tonight_cmd(message: Message):
    if not is_user_admin(message.from_user.id):
        return
    await show_tonight_picker(message)


@admin_router.callback_query(F.data == "quick_tonight")
async def cb_quick_tonight(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    await show_tonight_picker(call.message)
    await call.answer()


@admin_router.callback_query(F.data.startswith("settonight_"))
async def cb_set_tonight(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    media_id = int(call.data.split("_")[1])
    set_tonight_media(media_id)
    await call.message.answer("تم التحديد ✓\n\n✦ راح يطلع هذا العمل بمقدمة الصفحة الرئيسية.")
    await call.answer()


@admin_router.message(Command("del"))
async def admin_del_cmd(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    await state.set_state(AdminStates.deleting_search)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")]])
    await message.answer(
        "حذف عمل أو دقة محددة ✕\n\n✦ اكتب جزء من اسم العمل اللي تريد تحذفه أو تحذف دقة منه:",
        reply_markup=kb
    )


@admin_router.callback_query(F.data == "quick_del")
async def cb_quick_del(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    await state.set_state(AdminStates.deleting_search)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")]])
    await call.message.answer(
        "حذف عمل أو دقة محددة ✕\n\n✦ اكتب جزء من اسم العمل اللي تريد تحذفه أو تحذف دقة منه:",
        reply_markup=kb
    )
    await call.answer()


@admin_router.message(AdminStates.deleting_search)
async def process_del_search(message: Message, state: FSMContext):
    if not message.text:
        return
    results = search_media_by_title(message.text.strip())
    if not results:
        await message.answer("◈ ماكو أعمال مطابقة لهذا الاسم. جرب كلمة ثانية أو دوس إلغاء.")
        return

    buttons = []
    for item in results:
        buttons.append([InlineKeyboardButton(
            text=f"◈ {item['title']} ({item['year']})",
            callback_data=f"del_inspect_{item['id']}"
        )])
    buttons.append([InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")])
    await message.answer("اختار العمل للتحكم بحذفه أو حذف دقة معينة منه:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))


@admin_router.callback_query(F.data.startswith("del_inspect_"))
async def cb_del_inspect(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    media_id = int(call.data.split("_")[2])
    media = get_media_by_id(media_id)
    if not media:
        await call.answer("العمل ما موجود", show_alert=True)
        return

    buttons = [
        [InlineKeyboardButton(text=f"✕ حذف العمل بالكامل ({media['title']})", callback_data=f"confirm_del_{media_id}")]
    ]
    for f in media.get("files", []):
        ep_lbl = f" حلقة {f['episode']}" if f.get("episode", 0) > 0 else ""
        buttons.append([InlineKeyboardButton(
            text=f"✕ حذف دقة {f['quality']}p{ep_lbl} فقط",
            callback_data=f"delfile_{media_id}_{f['id']}"
        )])
    buttons.append([InlineKeyboardButton(text="إلغاء", callback_data="cancel_op")])
    await call.message.edit_text(
        f"❖ <b>إدارة ملفات: {html.escape(media['title'])}</b>\nاختار حذف العمل بالكامل أو حذف دقة محددة فقط:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )
    await call.answer()


@admin_router.callback_query(F.data.startswith("delfile_"))
async def cb_del_single_file(call: CallbackQuery):
    ok_perm, reason = check_and_record_admin_delete_guard(call.from_user.id)
    if not ok_perm:
        if reason == "frozen_sabotage":
            await alert_owners_sabotage_attempt(call.from_user.id, "حذف ملفات متكرر")
            await call.answer("❖ تم إيقاف وتجميد صلاحياتك تلقائياً بسبب الحذف المتكرر السريع!", show_alert=True)
        else:
            await call.answer("✕ ليست لديك صلاحية حذف الأعمال أو الملفات!", show_alert=True)
        return
    parts = call.data.split("_")
    media_id = int(parts[1])
    file_id = int(parts[2])
    delete_single_media_file(file_id)
    await publish_to_updates_channel(media_id)
    await call.answer("✓ انحذفت الدقة المحددة وتحدث المنشور!")
    await cb_del_inspect(call)


@admin_router.callback_query(F.data.startswith("confirm_del_"))
async def cb_confirm_del(call: CallbackQuery, state: FSMContext):
    ok_perm, reason = check_and_record_admin_delete_guard(call.from_user.id)
    if not ok_perm:
        if reason == "frozen_sabotage":
            await alert_owners_sabotage_attempt(call.from_user.id, "حذف أعمال متكرر")
            await call.answer("❖ تم إيقاف وتجميد صلاحياتك تلقائياً بسبب الحذف المتكرر السريع!", show_alert=True)
        else:
            await call.answer("✕ ليست لديك صلاحية حذف الأعمال!", show_alert=True)
        return
    media_id = int(call.data.split("_")[2])
    media = get_media_by_id(media_id)
    deleted_row = delete_media(media_id)
    # حذف منشور القناة إذا كان موجوداً
    if deleted_row and deleted_row.get("published_msg_id") and deleted_row.get("published_chat_id"):
        for b in (main_bot, admin_bot):
            try:
                await b.delete_message(
                    chat_id=normalize_chat_target(deleted_row["published_chat_id"]),
                    message_id=int(deleted_row["published_msg_id"])
                )
                break
            except Exception:
                continue
    await state.clear()
    title_str = media["title"] if media else "العمل"
    await call.message.answer(f"تم الحذف ✕\n\n✦ انحذف «{title_str}» من التطبيق وسحابة Firebase وقناة العرض بنجاح.")
    await call.answer()


# =====================================================================
# 4. إعداد السيرفر (FastAPI) وواجهات الـ API للميني آب ولوحة الإدارة
# =====================================================================
polling_tasks = []
_bg_tasks: set = set()


async def notify_admins_firebase_restore(report: Dict[str, Any]):
    try:
        restored_any = sum((report.get("restored") or {}).values()) > 0
        if not report.get("errors") and not (report.get("mode") == "full" and restored_any):
            return
        icon = "◈" if report.get("errors") else "⟐"
        text = f"{icon} <b>نتيجة استعادة Firebase عند الإقلاع</b>\n\n" + format_restore_report(report)
        if report.get("errors"):
            text += "\n\nاكتب /fbstatus لتشخيص الاتصال."
        for aid in get_all_admins():
            try:
                await admin_bot.send_message(aid, text, parse_mode="HTML")
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"notify_admins_firebase_restore: {e}")


_FB_NODE_AR = {
    "settings": "الإعدادات والقنوات", "media": "الأعمال", "media_files": "الملفات والدقات",
    "users": "المستخدمين", "admins": "المشرفين", "media_requests": "الطلبات",
    "media_reports": "البلاغات", "comments": "التعليقات",
    "user_lists": "قوائم المستخدمين", "meta": "اختيار الليلة"
}


def _fb_compare_text(cmp_data: Dict[str, Any]) -> str:
    lines = []
    for node, (local, remote) in cmp_data.items():
        if local == 0 and not remote:
            continue
        r = "؟" if remote is None else str(remote)
        mark = ""
        if remote is not None:
            if local > remote:
                mark = " ▴ ناقص بـ Firebase"
            elif remote > local:
                mark = " ▾ ناقص محلياً"
        lines.append(f"• {_FB_NODE_AR.get(node, node)}: محلي <b>{local}</b> | Firebase <b>{r}</b>{mark}")
    return "\n".join(lines) or "• ما أكو بيانات بأي طرف."


@admin_router.message(Command("fbstatus"))
async def admin_fbstatus(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    wait = await message.answer("◷ أفحص اتصال Firebase...")
    try:
        await firebase_flush(15)
        health = await firebase_health_check()
        if not health["configured"]:
            await wait.edit_text("✕ <b>Firebase غير مضبوط</b>\nضيف متغير البيئة <code>FIREBASE_DATABASE_URL</code> بالاستضافة.", parse_mode="HTML")
            return
        cmp_data = await firebase_compare()
        rd = "✓" if health["read"] else f"✕ ({health.get('read_status')})"
        wr = "✓" if health["write"] else f"✕ ({health.get('write_status')})"
        text = (
            "◈ <b>حالة Firebase Realtime Database</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"✦ القراءة: {rd}   ✦ الكتابة: {wr}\n"
            f"✦ مفتاح FIREBASE_AUTH: <b>{'مضبوط ⬡' if health['auth'] else 'غير مضبوط (القاعدة تعتمد على القواعد المفتوحة ◈)'}</b>\n"
        )
        if health.get("hint"):
            text += f"◈ <code>{html.escape(str(health['hint']))[:200]}</code>\n"
        text += (
            f"✦ التحديثات: ✓ {FB_STATS['ok']} | ✕ {FB_STATS['fail']} | ▸ متجاهلة {FB_STATS['dropped']}\n"
        )
        if FB_STATS["last_error"]:
            text += f"✦ آخر خطأ: <code>{html.escape(FB_STATS['last_error'])[:200]}</code>\n"
        text += "━━━━━━━━━━━━━━\n<b>مقارنة البيانات:</b>\n" + _fb_compare_text(cmp_data) + "\n━━━━━━━━━━━━━━\n"
        text += "⟐ <b>آخر استعادة:</b>\n" + format_restore_report(FB_STATS.get("last_restore"))
        if any(r is not None and l > r for l, r in cmp_data.values()):
            text += "\n\n◈ فيه بيانات محلية ناقصة بـ Firebase → اكتب /syncfirebase"
        if any(r is not None and r > l for l, r in cmp_data.values()):
            text += "\n◈ فيه بيانات بـ Firebase ناقصة محلياً → اكتب /fbrestore"
        await wait.edit_text(text, parse_mode="HTML")
    except Exception as e:
        await wait.edit_text(f"◈ تعذر الفحص: <code>{html.escape(str(e))[:200]}</code>", parse_mode="HTML")


@admin_router.message(Command("syncfirebase"))
async def admin_syncfirebase(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    try:
        cmp_data = await firebase_compare()
    except Exception as e:
        await message.answer(f"◈ تعذر قراءة Firebase: <code>{html.escape(str(e))[:200]}</code>", parse_mode="HTML")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✓ ارفع كل البيانات لـ Firebase", callback_data="fb_sync_confirm"),
        InlineKeyboardButton(text="إلغاء", callback_data="fb_cancel"),
    ]])
    await message.answer(
        "◈ <b>رفع البيانات المحلية إلى Firebase</b>\n\n" + _fb_compare_text(cmp_data) +
        "\n\n✦ الرفع <b>دمج</b>: يحفظ كل الإعدادات وأيديات القنوات والفيديوهات والمستخدمين والطلبات بدون حذف أي شيء من Firebase.",
        reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "fb_sync_confirm")
async def cb_fb_sync_confirm(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    await call.answer("◷ جاري الرفع...")
    await call.message.edit_text("◷ جاري رفع البيانات لـ Firebase... انتظر.")
    try:
        res = await push_all_local_to_firebase()
        lines = [f"• {_FB_NODE_AR.get(n, n)}: <b>{c}</b>" for n, c in res["pushed"].items()]
        text = ("✓ <b>تم رفع كل البيانات لـ Firebase</b>\n\n" if res["ok"] else "◈ <b>الرفع انتهى مع أخطاء</b>\n\n") + "\n".join(lines)
        for e in res["errors"][:4]:
            text += f"\n◈ <code>{html.escape(str(e))[:160]}</code>"
        await call.message.edit_text(text, parse_mode="HTML")
    except Exception as e:
        await call.message.edit_text(f"◈ فشل الرفع: <code>{html.escape(str(e))[:200]}</code>", parse_mode="HTML")


@admin_router.message(Command("fbrestore"))
async def admin_fbrestore(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    try:
        cmp_data = await firebase_compare()
    except Exception as e:
        await message.answer(f"◈ تعذر قراءة Firebase: <code>{html.escape(str(e))[:200]}</code>", parse_mode="HTML")
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⟐ استعد من Firebase", callback_data="fb_restore_confirm"),
        InlineKeyboardButton(text="إلغاء", callback_data="fb_cancel"),
    ]])
    await message.answer(
        "⟐ <b>استعادة البيانات من Firebase</b>\n\n" + _fb_compare_text(cmp_data) +
        "\n\n◈ الاستعادة <b>تكتب فوق</b> القاعدة المحلية بنسخة Firebase لنفس الأرقام (IDs).",
        reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "fb_restore_confirm")
async def cb_fb_restore_confirm(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    await call.answer("◷ جاري الاستعادة...")
    await call.message.edit_text("◷ جاري الاستعادة من Firebase... انتظر.")
    try:
        report = await restore_from_firebase(force=True)
        head = "✓ <b>انتهت الاستعادة</b>\n\n" if report["ok"] else "◈ <b>الاستعادة انتهت مع أخطاء</b>\n\n"
        await call.message.edit_text(head + format_restore_report(report), parse_mode="HTML")
    except Exception as e:
        await call.message.edit_text(f"◈ فشلت الاستعادة: <code>{html.escape(str(e))[:200]}</code>", parse_mode="HTML")


@admin_router.callback_query(F.data == "fb_cancel")
async def cb_fb_cancel(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text("تم الإلغاء.")
    await call.answer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        init_db()
    except Exception as e:
        logger.error(f"DB init error: {e}")

    # تسخين الكاش المحلّي فوراً قبل استقبال الطلبات لضمان فتح الميني آب في أقل من 15ms
    try:
        get_cached_media_bundle(include_hidden=True, slim_for_webapp=True)
        get_cached_media_bundle(include_hidden=False, slim_for_webapp=True)
    except Exception as e:
        logger.warning(f"Cache warmup warning: {e}")

    async def _startup_firebase_sync_bg():
        try:
            await asyncio.sleep(1.5)
            fb_report = await asyncio.wait_for(restore_from_firebase(force=False), timeout=45)
            gc.collect()
            live_tunnel = _norm_url(os.getenv("LIVE_API_URL", "") or os.getenv("MINIAPP_URL", ""))
            perm_pages = _norm_url(os.getenv("PERMANENT_WEBAPP_URL", ""))
            if not perm_pages and os.getenv("PERMANENT_WEBAPP_URL", "").strip():
                os.environ["PERMANENT_WEBAPP_URL"] = ""
            saved_perm = get_setting("permanent_webapp_url", "").strip()
            if saved_perm and not is_valid_http_url(saved_perm):
                set_setting("permanent_webapp_url", "")
            if live_tunnel:
                set_setting("live_api_url", live_tunnel)
                await sync_public_bridge_to_firebase(live_tunnel, perm_pages)

            restored_url = get_setting("webapp_url", "").strip().rstrip("/")
            active_url = get_webapp_url()
            if active_url and active_url != restored_url:
                set_setting("webapp_url", active_url)
            if active_url.startswith("https://"):
                try:
                    app_en = get_setting("app_name_en", "VORTEX")
                    await main_bot.set_chat_menu_button(
                        menu_button=MenuButtonWebApp(text=app_en, web_app=WebAppInfo(url=active_url))
                    )
                except Exception:
                    pass

            # إذا كان رابط GitHub Pages الدائم قيد البناء في أول دقيقة، ننتظر جاهزيته بالخلفية ونثبته تلقائياً للأبد
            if perm_pages and perm_pages.startswith("https://"):
                async def _promote_permanent_url_when_ready():
                    for _ in range(12):
                        try:
                            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=8)) as s:
                                async with s.get(perm_pages + "/") as resp:
                                    if resp.status == 200:
                                        os.environ["PERMANENT_WEBAPP_URL"] = perm_pages
                                        set_setting("webapp_url", perm_pages)
                                        set_setting("permanent_webapp_url", perm_pages)
                                        app_en_now = get_setting("app_name_en", "VORTEX")
                                        await main_bot.set_chat_menu_button(
                                            menu_button=MenuButtonWebApp(text=app_en_now, web_app=WebAppInfo(url=perm_pages))
                                        )
                                        await sync_public_bridge_to_firebase(live_tunnel or active_url, perm_pages)
                                        return
                        except Exception:
                            pass
                        await asyncio.sleep(15)
                asyncio.create_task(_promote_permanent_url_when_ready())
            # تسخين الكاش مجدداً بعد استعادة الإعدادات
            get_cached_media_bundle(include_hidden=True, slim_for_webapp=True)
            get_cached_media_bundle(include_hidden=False, slim_for_webapp=True)
            if fb_report.get("mode") == "full" and sum((fb_report.get("restored") or {}).values()) > 0:
                await notify_admins_firebase_restore(fb_report)
            # V17.0 CPU Guard: عدم تشغيل أي حلقات فحص ثقيلة تلقائياً عند الإقلاع للحفاظ على المعالج (0.5% CPU) والرام (~45MB)
            gc.collect()
        except Exception as e:
            logger.warning(f"Startup Firebase sync: {type(e).__name__}: {e}")

    _tfb = asyncio.create_task(_startup_firebase_sync_bg())
    _bg_tasks.add(_tfb)
    _tfb.add_done_callback(_bg_tasks.discard)

    async def _setup_bots():
        try:
            me_main = await main_bot.get_me()
            BOT_INFO["main_username"] = me_main.username or "Vortexee_Bot"
            BOT_INFO["main_id"] = me_main.id
            me_sender = await sender_bot.get_me()
            BOT_INFO["sender_username"] = me_sender.username or "VortexSenderBot"
            BOT_INFO["sender_id"] = me_sender.id
            me_admin = await admin_bot.get_me()
            BOT_INFO["admin_username"] = me_admin.username or "VortexAdminBot"
            BOT_INFO["admin_id"] = me_admin.id

            app_ar = get_setting("app_name_ar", "فورتكس")
            app_en = get_setting("app_name_en", "VORTEX")

            await main_bot.set_my_commands([
                BotCommand(command="start", description=f"◈ فتح تطبيق {app_ar} والقائمة الرئيسية"),
            ])

            # إلغاء قائمة الأوامر الطويلة ببوت الإدارة والاكتفاء بزر فتح لوحة الأزرار الملونة
            await admin_bot.set_my_commands([
                BotCommand(command="start", description=f"◈ فتح لوحة أزرار {app_ar} الملونة"),
            ])

            webapp_url = get_webapp_url()
            if webapp_url.startswith("https://") and "your-app-name" not in webapp_url:
                await main_bot.set_chat_menu_button(
                    menu_button=MenuButtonWebApp(text=app_en, web_app=WebAppInfo(url=webapp_url))
                )
            ts_ip = os.getenv("TAILSCALE_IP", "").strip()
            tunnel_url = os.getenv("MINIAPP_URL", "").strip()
            if (ts_ip or tunnel_url) and ADMIN_IDS:
                vps_msg = (
                    f"🚀 <b>تم تشغيل سيرفر فورتكس (GitHub VPS V19.0) بنجاح ✓</b>\n"
                    f"━━━━━━━━━━━━━━\n"
                    f"🌐 <b>رابط الميني آب الجديد (تحدث تلقائياً):</b>\n"
                    f"<code>{webapp_url}</code>\n\n"
                    f"💻 <b>بيانات الاتصال عبر تطبيق Termius (SSH / SFTP):</b>\n"
                    f"• <b>Host / IP:</b> <code>{ts_ip or 'غير مفعل (أضف TAILSCALE_AUTH_KEY)'}</code>\n"
                    f"• <b>Port:</b> <code>22</code>\n"
                    f"• <b>User:</b> <code>root</code>\n"
                    f"• <b>Password:</b> <code>admin@123</code>\n"
                    f"• <b>مجلد المشروع:</b> <code>/root/vortex</code>\n"
                    f"• <b>أمر متابعة السجل:</b> <code>tail -f /root/vortex/vortex.log</code>"
                )
                try:
                    await admin_bot.send_message(chat_id=ADMIN_IDS[0], text=vps_msg, parse_mode="HTML")
                except Exception as notify_err:
                    logger.debug(f"Admin VPS notify skipped: {notify_err}")
        except Exception as e:
            logger.warning(f"Startup bot setup warning: {e}")

    async def _run_bot(dp: Dispatcher, bot_inst: Bot, name: str):
        attempt = 0
        while True:
            attempt += 1
            try:
                await bot_inst.delete_webhook(drop_pending_updates=True)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"Webhook clear warning ({name}): {e}")
            try:
                await dp.start_polling(bot_inst, handle_signals=False)
                break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Polling error ({name}, attempt {attempt}): {e}")
                await asyncio.sleep(min(4 * attempt, 60))

    async def _keep_alive_ping_loop():
        if not KEEP_ALIVE:
            return
        await asyncio.sleep(60)
        while True:
            try:
                target_url = get_webapp_url().rstrip("/")
                if target_url.startswith("http"):
                    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as session:
                        async with session.get(f"{target_url}/health") as _:
                            pass
            except asyncio.CancelledError:
                raise
            except Exception:
                pass
            await asyncio.sleep(KEEP_ALIVE_SECONDS)

    async def _low_ram_gc_worker():
        while True:
            await asyncio.sleep(180)
            try:
                if len(_SUB_VERIFIED_CACHE) > 300:
                    _SUB_VERIFIED_CACHE.clear()
                if len(_SECRET_ARCHIVED_KEYS) > 1000:
                    _SECRET_ARCHIVED_KEYS.clear()
                gc.collect()
            except asyncio.CancelledError:
                raise
            except Exception:
                pass

    t_setup = asyncio.create_task(_setup_bots())
    t_ping = asyncio.create_task(_keep_alive_ping_loop())
    t_del6h = asyncio.create_task(_mandatory_6h_deletion_worker())
    t_gc = asyncio.create_task(_low_ram_gc_worker())
    polling_tasks.append(t_gc)
    t1 = asyncio.create_task(_run_bot(main_dp, main_bot, "VortexMainBot"))
    t2 = asyncio.create_task(_run_bot(admin_dp, admin_bot, "VortexAdminBot"))
    t3 = asyncio.create_task(_run_bot(sender_dp, sender_bot, "VortexSenderBot"))
    polling_tasks.extend([t_setup, t_ping, t_del6h, t1, t2, t3])
    logger.info("VORTEX web server is up; 3 bots are running in the background.")
    yield
    for t in polling_tasks:
        t.cancel()


app = FastAPI(title="VORTEX V17.0 Auto-Ingest & Scanner Mini App API", version=APP_VERSION, lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=500)
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_credentials=False, allow_methods=['*'], allow_headers=['*'])

_INDEX_HTML_CACHE: Dict[str, Any] = {"path": "", "mtime": 0.0, "content": ""}

_AUTH_TOKENS = (MAIN_BOT_TOKEN, ADMIN_BOT_TOKEN, SENDER_BOT_TOKEN)


@app.middleware("http")
async def platform_and_auth_middleware(request: Request, call_next):
    uid = 0
    try:
        init_data = request.headers.get("x-telegram-init-data", "")
        if init_data:
            uid = validate_init_data(init_data, _AUTH_TOKENS, INITDATA_MAX_AGE)
    except Exception:
        uid = 0
    CURRENT_TG_USER.set(uid)

    try:
        if not AUTO_DETECTED_PLATFORM_URL:
            host = (request.headers.get("host") or "").split(",")[0].strip()
            if not is_public_host(host):
                host = (request.headers.get("x-forwarded-host") or "").split(",")[0].strip()
            if is_public_host(host):
                detected_url = f"https://{host.lower()}"
                current_url = get_setting("webapp_url", "").strip().rstrip("/")
                if current_url != detected_url and is_replaceable_url(current_url) and not _norm_url(os.getenv('PERMANENT_WEBAPP_URL', '')):
                    set_setting("webapp_url", detected_url)
                    try:
                        app_en = get_setting("app_name_en", "VORTEX")
                        asyncio.create_task(
                            main_bot.set_chat_menu_button(
                                menu_button=MenuButtonWebApp(text=app_en, web_app=WebAppInfo(url=detected_url))
                            )
                        )
                    except Exception:
                        pass
    except Exception:
        pass
    return await call_next(request)


@app.api_route("/favicon.ico", methods=["GET", "HEAD"])
async def favicon_no_content():
    return HTMLResponse(content="", status_code=204)


@app.api_route("/health", methods=["GET", "HEAD"])
@app.api_route("/ping", methods=["GET", "HEAD"])
async def health_check():
    try:
        url = get_webapp_url()
    except Exception:
        url = ""
    return {"status": "ok", "app": "VORTEX", "version": APP_VERSION, "webapp_url": url}


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def serve_mini_app():
    target_html = "index.html" if os.path.exists("index.html") else "static/index.html"
    try:
        mtime = os.path.getmtime(target_html)
    except OSError:
        mtime = 0.0
    if _INDEX_HTML_CACHE["content"] and _INDEX_HTML_CACHE["path"] == target_html and _INDEX_HTML_CACHE["mtime"] == mtime:
        return HTMLResponse(content=_INDEX_HTML_CACHE["content"], headers={"Cache-Control": "no-cache"})
    with open(target_html, "r", encoding="utf-8") as f:
        html_content = f.read()
    _INDEX_HTML_CACHE["path"] = target_html
    _INDEX_HTML_CACHE["mtime"] = mtime
    _INDEX_HTML_CACHE["content"] = html_content
    return HTMLResponse(content=html_content, headers={"Cache-Control": "no-cache"})




@app.get("/live_api.json")
@app.get("/api/live_bridge")
async def api_live_bridge_info():
    live_api = _norm_url(os.getenv("LIVE_API_URL", "")) or _norm_url(get_setting("live_api_url", "")) or get_webapp_url()
    perm_url = _norm_url(os.getenv("PERMANENT_WEBAPP_URL", "")) or _norm_url(get_setting("permanent_webapp_url", ""))
    return JSONResponse({
        "ok": True,
        "api_url": live_api,
        "permanent_url": perm_url,
        "webapp_url": get_webapp_url(),
    })

@app.get("/api/bootstrap")
async def api_bootstrap(
    user_id: int = 0,
    first_name: str = "",
    username: str = "",
    ref_id: int = 0,
    mode: str = "fast",
    initial_limit: int = 36
):
    ctx_uid = CURRENT_TG_USER.get()
    if ctx_uid and int(ctx_uid) > 0:
        user_id = int(ctx_uid)
    if user_id:
        register_user(user_id, first_name, username, referred_by=int(ref_id or 0), source="webapp")
        asyncio.create_task(notify_admins_of_new_users())

    is_sub, ch_link, ch_title = await check_user_subscription(user_id, allow_anonymous=True)

    user_lists = get_user_lists(user_id) if user_id else {"watchlist": [], "favorites": [], "watched": []}
    profile = get_user_profile(user_id) if user_id else {
        "user_id": 0, "points": 0, "referrals_count": 0,
        "is_vip": False, "is_admin": False, "vip_days_left": 0, "rank": 1, "theme": get_setting("default_theme", "vortex")
    }
    is_admin_user = bool(profile.get("is_admin"))
    all_items, counts_dict = get_cached_media_bundle(include_hidden=is_admin_user, slim_for_webapp=True)

    tonight = next((x for x in all_items if x["is_tonight"] and not x.get("is_hidden")), all_items[0] if all_items else None)
    leaderboard = get_leaderboard(10)
    req_quota = get_user_request_quota(user_id) if user_id else {
        "enabled": True, "cycle": "weekly", "cycle_ar": "هذا الأسبوع", "renew_ar": "كل 7 أيام",
        "cycle_max": 5, "cycle_left": 5, "daily_max": 5, "daily_left": 5,
        "monthly_max": 20, "monthly_left": 20
    }
    user_requests = get_user_media_requests(user_id, 25) if user_id else []

    ads_enabled = get_setting("ads_enabled", "1") == "1"
    ads_timer_seconds = int(get_setting("ads_timer_seconds", "15") or "15")
    fallback_timer = get_setting("ads_fallback_timer", "1") == "1"
    is_privileged = bool(profile.get("is_vip") or is_admin_user)

    # V15.0 Progressive Step-by-Step Loading: إرسال أول دفعة (36 عمل + أعمال قوائم المستخدم) فوراً لتفتح الواجهة بلحظة
    total_items = len(all_items)
    if mode == "fast" and initial_limit > 0 and total_items > initial_limit:
        first_slice = list(all_items[:initial_limit])
        first_ids = {int(x["id"]) for x in first_slice}
        # ضمان وجود أعمال المفضلة/القائمة وأعلى المشاهدات والتقييمات في الدفعة الأولى
        by_views_top = sorted(all_items, key=lambda x: int(x.get("views_count") or 0), reverse=True)[:12]
        by_rating_top = sorted(all_items, key=lambda x: float(x.get("rating") or 0.0), reverse=True)[:12]
        user_saved_ids = set(user_lists.get("watchlist", []) + user_lists.get("favorites", []) + user_lists.get("watched", []))
        for extra in (by_views_top + by_rating_top):
            if int(extra["id"]) not in first_ids:
                first_slice.append(extra)
                first_ids.add(int(extra["id"]))
        if user_saved_ids:
            for it in all_items:
                if int(it["id"]) in user_saved_ids and int(it["id"]) not in first_ids:
                    first_slice.append(it)
                    first_ids.add(int(it["id"]))
        items_out = first_slice
        has_more = True
        next_offset = initial_limit
    else:
        items_out = all_items
        has_more = False
        next_offset = total_items

    return JSONResponse({
        "version": APP_VERSION,
        "bot_info": BOT_INFO,
        "webapp_url": get_webapp_url(),
        "brand_config": get_brand_config(),
        "maintenance_config": get_maintenance_config(),
        "subscription": {
            "is_subscribed": is_sub,
            "channel_link": ch_link,
            "channel_title": ch_title
        },
        "ads_config": {
            "provider": "vortex_ads",
            "enabled": ads_enabled and not is_privileged,
            "raw_enabled": ads_enabled,
            "timer_seconds": ads_timer_seconds,
            "fallback_timer": fallback_timer,
            "tasks": get_ad_tasks(only_enabled=True)
        },
        "vip_config": {
            "stars_price": int(get_setting("vip_stars_price", "50") or "50"),
            "points_price": int(get_setting("vip_points_price", "100") or "100"),
            "referral_points": int(get_setting("referral_points", "20") or "20")
        },
        "profile": profile,
        "leaderboard": leaderboard,
        "request_quota": req_quota,
        "user_requests": user_requests,
        "tonight": tonight,
        "smart_sections": get_smart_sections(only_non_empty=True),
        "all_sections": get_smart_sections(only_non_empty=False) if is_admin_user else get_smart_sections(only_non_empty=True),
        "collections": get_all_collections(only_active=True),
        "items": items_out,
        "total_items": total_items,
        "has_more_items": has_more,
        "next_offset": next_offset,
        "counts": counts_dict,
        "user_lists": user_lists
    })


@app.get("/api/media/batch")
async def api_media_progressive_batch(user_id: int = 0, offset: int = 0, limit: int = 60):
    """يجلب دفعات الأفلام والمسلسلات المتبقية تدريجياً (شي ورة شي) بالخلفية للميني آب."""
    ctx_uid = CURRENT_TG_USER.get()
    if ctx_uid and int(ctx_uid) > 0:
        user_id = int(ctx_uid)
    is_adm = bool(user_id and is_user_admin(int(user_id)))
    all_items, _ = get_cached_media_bundle(include_hidden=is_adm, slim_for_webapp=True)
    total = len(all_items)
    safe_offset = max(0, int(offset))
    safe_limit = max(10, min(120, int(limit)))
    batch = all_items[safe_offset:safe_offset + safe_limit]
    next_off = safe_offset + len(batch)
    return JSONResponse({
        "items": batch,
        "offset": safe_offset,
        "next_offset": next_off,
        "total_items": total,
        "has_more": next_off < total
    })


@app.get("/api/media/item/{media_id}")
async def api_media_single_item(media_id: int, user_id: int = 0):
    """يجلب تفاصيل وحلقات عمل محدد فوراً عند فتحه إذا لم تكن دفعته الخلفية قد اكتملت بعد."""
    item = get_media_by_id(int(media_id))
    if not item:
        raise HTTPException(status_code=404, detail="العمل غير موجود")
    return JSONResponse({"status": "ok", "item": item})


@app.get("/api/check_subscription")
async def api_check_subscription(user_id: int = 0, attempt: int = 1):
    ctx_uid = CURRENT_TG_USER.get()
    if ctx_uid and int(ctx_uid) > 0 and not user_id:
        user_id = int(ctx_uid)
    is_sub, ch_link, ch_title = await check_user_subscription(user_id, force_refresh=True, allow_anonymous=True)
    # إذا ضغط المستخدم على زر الاشتراك ثم ضغط تحقق مرتين وتعذر على تيليجرام تحديث حالة العضو بسبب تأخر سيرفر تيليجرام، نفتح له التطبيق
    if not is_sub and int(attempt or 1) >= 2 and user_id and int(user_id) > 0:
        force_ch = get_setting("force_sub_channel_id", "").strip()
        chat_target = normalize_chat_target(force_ch) if force_ch else ""
        _SUB_VERIFIED_CACHE[(int(user_id), str(chat_target))] = (time.time(), ch_link, ch_title, True)
        asyncio.create_task(process_referral_reward_notification(int(user_id)))
        is_sub = True
    return JSONResponse({
        "is_subscribed": is_sub,
        "channel_link": ch_link,
        "channel_title": ch_title
    })


class ThemeUpdatePayload(BaseModel):
    user_id: int
    theme: str


@app.post("/api/user/theme")
async def api_update_user_theme(req: ThemeUpdatePayload):
    _require_self(req.user_id)
    if req.user_id:
        set_user_theme(req.user_id, req.theme)
    return {"status": "ok", "theme": req.theme}


class ListToggleRequest(BaseModel):
    user_id: int
    media_id: int
    list_type: str


@app.post("/api/user/toggle_list")
async def api_toggle_list(req: ListToggleRequest):
    _require_self(req.user_id)
    if req.list_type not in ("watchlist", "favorites", "watched"):
        raise HTTPException(status_code=400, detail="Invalid list_type")
    added = toggle_user_list(req.user_id, req.media_id, req.list_type)
    return {"status": "ok", "added": added, "user_lists": get_user_lists(req.user_id)}


class CommentPayload(BaseModel):
    user_id: int
    user_name: str
    media_id: int
    stars: int
    text: str


@app.post("/api/media/comment")
async def api_add_comment(req: CommentPayload):
    _require_self(req.user_id)
    if not req.text.strip():
        raise HTTPException(status_code=400, detail="Empty comment")
    comment = add_comment(req.media_id, req.user_id, req.user_name, req.stars, req.text)
    return {"status": "ok", "comment": comment}


class MediaRequestPayload(BaseModel):
    user_id: int
    user_name: str
    request_text: str
    prod_year: str = ""


@app.post("/api/user/request_media")
async def api_request_media(req: MediaRequestPayload):
    _require_self(req.user_id)
    text = req.request_text.strip()
    if not text:
        raise HTTPException(status_code=400, detail="Empty request")
    res = create_media_request(req.user_id, req.user_name, text, req.prod_year.strip())
    if res.get("already_exists") and res.get("existing_media"):
        return JSONResponse({
            "status": "already_exists",
            "message": res["message"],
            "existing_media": res["existing_media"],
            "user_requests": get_user_media_requests(req.user_id, 25)
        })
    if not res["ok"]:
        return JSONResponse({"status": "error", "message": res["message"]}, status_code=400)

    year_str = f" ({req.prod_year.strip()})" if req.prod_year.strip() else ""
    req_id = res["req_id"]
    total_req = res.get("total_requesters", 1)
    cnt_line = f"\n◈ <b>مجموع الطالبين لهذا العمل:</b> {total_req}" if total_req > 1 else ""

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            ikb(text=f"✓ تلبية وإشعار #{req_id}", style="success", callback_data=f"fulfill_req_{req_id}"),
            ikb(text=f"✕ رفض #{req_id}", style="danger", callback_data=f"reject_req_{req_id}")
        ]
    ])
    for aid in get_all_admins():
        try:
            await admin_bot.send_message(
                chat_id=aid,
                text=(
                    f"◈ <b>طلب عمل جديد #{req_id}</b>\n\n"
                    f"◈ المستخدم: {html.escape(req.user_name)} (<code>{req.user_id}</code>)\n"
                    f"◈ العمل المطلوب: <b>{html.escape(text)}{html.escape(year_str)}</b>"
                    f"{cnt_line}"
                ),
                reply_markup=kb,
                parse_mode="HTML"
            )
        except Exception:
            pass
    return {
        "status": "ok",
        "message": res["message"],
        "request_quota": get_user_request_quota(req.user_id),
        "user_requests": get_user_media_requests(req.user_id, 25)
    }


class DetailedReportPayload(BaseModel):
    user_id: int
    user_name: str
    media_id: int
    media_title: str
    reason_type: str = "الفيديو ما يشتغل"
    details: str = ""


@app.post("/api/media/report_detailed")
async def api_report_detailed(req: DetailedReportPayload):
    _require_self(req.user_id)
    rep_id = create_media_report(
        req.media_id, req.media_title, req.user_id, req.user_name, req.reason_type, req.details
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            ikb(text=f"✓ تم الحل #{rep_id}", style="success", callback_data=f"resolve_rep_{rep_id}"),
            ikb(text=f"✎ تعديل العمل #{req.media_id}", style="primary", callback_data=f"edit_open_{req.media_id}")
        ]
    ])
    for aid in get_all_admins():
        try:
            await admin_bot.send_message(
                chat_id=aid,
                text=(
                    f"❖ <b>بلاغ مفصّل عن مشكلة بعمل (#{rep_id})</b>\n\n"
                    f"◈ العمل: <b>{html.escape(req.media_title)}</b> (#id{req.media_id})\n"
                    f"◈ المبلّغ: {html.escape(req.user_name)} (<code>{req.user_id}</code>)\n"
                    f"◈ نوع المشكلة: <b>{html.escape(req.reason_type)}</b>\n"
                    f"✎ تفاصيل السبب: {html.escape(req.details or 'لم يكتب تفاصيل إضافية')}"
                ),
                reply_markup=kb,
                parse_mode="HTML"
            )
        except Exception:
            pass
    return {"status": "ok"}


class UserIdPayload(BaseModel):
    user_id: int


@app.post("/api/vip/redeem_points")
async def api_redeem_points(req: UserIdPayload):
    _require_self(req.user_id)
    ok, msg = redeem_points_for_vip(req.user_id)
    if not ok:
        return JSONResponse({"status": "error", "message": msg}, status_code=400)
    return {"status": "ok", "message": msg, "profile": get_user_profile(req.user_id)}


@app.post("/api/vip/create_stars_invoice")
async def api_create_stars_invoice(req: UserIdPayload):
    _require_self(req.user_id)
    stars_price = int(get_setting("vip_stars_price", "50") or "50")
    app_en = get_setting("app_name_en", "VORTEX")
    try:
        invoice_link = await main_bot.create_invoice_link(
            title=f"اشتراك {app_en} PRO (شهر كامل)",
            description="مشاهدة وتحميل كل الأفلام والمسلسلات والأنمي مباشرة بدون أي إعلانات أو تذاكر انتظار لمدة 30 يوم.",
            payload=f"vip_stars_{req.user_id}",
            provider_token="",
            currency="XTR",
            prices=[LabeledPrice(label=f"اشتراك شهر {app_en} PRO", amount=stars_price)]
        )
        return {"status": "ok", "invoice_link": invoice_link}
    except Exception as e:
        logger.error(f"Stars invoice creation error: {e}")
        return JSONResponse({"status": "error", "message": "تعذر إنشاء فاتورة النجوم حالياً."}, status_code=500)


class TopChannelPayload(BaseModel):
    user_id: int
    channel_title: str
    channel_url: str


@app.post("/api/leaderboard/set_channel")
async def api_set_leaderboard_channel(req: TopChannelPayload):
    _require_self(req.user_id)
    lb = get_leaderboard(3)
    top_ids = [x["user_id"] for x in lb]
    if req.user_id not in top_ids and not is_user_admin(req.user_id):
        return JSONResponse({"status": "error", "message": "هاي الميزة متاحة بس لأول 3 متصدرين بلوحة الشرف!"}, status_code=403)
    url = req.channel_url.strip()
    if url and not url.startswith("http"):
        url = "https://t.me/" + url.lstrip("@")
    set_user_leaderboard_channel(req.user_id, req.channel_title, url)
    return {"status": "ok", "leaderboard": get_leaderboard(10)}


def _require_self(user_id: int):
    if not STRICT_AUTH:
        return
    ctx_uid = CURRENT_TG_USER.get()
    if ctx_uid and int(ctx_uid) > 0 and user_id and int(user_id) != int(ctx_uid):
        raise HTTPException(status_code=401, detail="تعذر التحقق من هويتك — افتح التطبيق من داخل تيليجرام")


def _require_admin(user_id: int):
    if not user_id or not is_user_admin(user_id):
        raise HTTPException(status_code=403, detail="غير مصرح — للمشرفين فقط")
    _require_self(user_id)


@app.get("/api/admin/overview")
async def api_admin_overview(user_id: int = 0):
    _require_admin(user_id)
    asyncio.create_task(run_auto_sweep_pending_requests_and_notify())
    brand = get_brand_config()
    maint = get_maintenance_config()
    raw_admin_items, _ = get_cached_media_bundle(include_hidden=True, slim_for_webapp=True)
    slim_admin_media = [
        {
            "id": it["id"],
            "tmdb_id": it.get("tmdb_id", 0),
            "title": it.get("title", ""),
            "original_title": it.get("original_title", ""),
            "year": it.get("year", ""),
            "media_type": it.get("media_type", "movie"),
            "type_ar": it.get("type_ar", "فيلم"),
            "country": it.get("country", ""),
            "poster_url": it.get("poster_url", ""),
            "overview": it.get("overview", ""),
            "section_key": it.get("section_key", ""),
            "section_title": it.get("section_title", ""),
            "world_key": it.get("world_key", "foreign"),
            "is_hidden": bool(it.get("is_hidden")),
            "is_tonight": bool(it.get("is_tonight")),
            "collection_name": it.get("collection_name", ""),
            "files_count": len(it.get("files") or []),
            "qualities_str": " · ".join(sorted({str(f.get("quality", "720")) + "p" for f in (it.get("files") or [])}))
        }
        for it in raw_admin_items
    ]
    return JSONResponse({
        "stats": get_stats(),
        "viewer_admin": get_admin_record(user_id) or {"user_id": user_id, "is_owner": is_owner_id(user_id)},
        "users_dashboard": get_users_dashboard_data(limit_new=25, limit_top=20),
        "admins_list": get_all_admins_detailed(),
        "perm_labels": PERM_LABELS_AR,
        "pending_requests": get_pending_media_requests(30),
        "pending_reports": get_pending_media_reports(30),
        "smart_sections": get_smart_sections(only_non_empty=False),
        "collections": get_all_collections(only_active=False),
        "incomplete_series": get_incomplete_series_list(),
        "all_media_admin": slim_admin_media,
        "ad_tasks": get_ad_tasks(only_enabled=False),
        "firebase_status": firebase_status_line(),
        "webapp_url": get_webapp_url(),
        "brand_config": brand,
        "maintenance_config": maint,
        "settings": {
            **brand,
            **maint,
            "ads_enabled": get_setting("ads_enabled", "1") == "1",
            "mybid_admpid": get_setting("mybid_admpid", "").strip(),
            "mybid_banner_1_id": get_setting("mybid_banner_1_id", "").strip(),
            "mybid_banner_2_id": get_setting("mybid_banner_2_id", "").strip(),
            "ads_timer_seconds": int(get_setting("ads_timer_seconds", "15") or "15"),
            "ads_fallback_timer": get_setting("ads_fallback_timer", "1") == "1",
            "requests_enabled": get_setting("requests_enabled", "1") == "1",
            "requests_cycle": get_setting("requests_cycle", "weekly"),
            "requests_quota_max": int(get_setting("requests_quota_max", "5") or "5"),
            "auto_delete_6h": True,
            "notify_new_users": get_setting("notify_new_users", "1") == "1",
            "channel_post_btn_mode": get_setting("channel_post_btn_mode", "webapp"),
            "vip_stars_price": int(get_setting("vip_stars_price", "50") or "50"),
            "vip_points_price": int(get_setting("vip_points_price", "100") or "100"),
            "referral_points": int(get_setting("referral_points", "20") or "20"),
            "support_username": get_setting("support_username", "Vortexeo"),
            "movies_channel_id": get_setting("movies_channel_id", ""),
            "series_channel_id": get_setting("series_channel_id", ""),
            "anime_channel_id": get_setting("anime_channel_id", ""),
            "publish_channel_id": get_setting("publish_channel_id", "@Vortexeo"),
            "source_channels": get_source_channels_list(),
            "auto_ingest_enabled": is_auto_ingest_enabled(),
            "movies_publish_channel_id": get_setting("movies_publish_channel_id", ""),
            "series_publish_channel_id": get_setting("series_publish_channel_id", ""),
            "anime_publish_channel_id": get_setting("anime_publish_channel_id", ""),
            "force_sub_channel_id": get_setting("force_sub_channel_id", "@Vortexeo"),
            "force_sub_channel_link": get_setting("force_sub_channel_link", "https://t.me/Vortexeo"),
            "force_sub_channel_title": get_setting("force_sub_channel_title", "قناة فورتكس العامة"),
            "tmdb_api_key": get_setting("tmdb_api_key", ""),
        }
    })


class AdminRequestActionPayload(BaseModel):
    admin_id: int
    request_id: int
    action: str
    fulfilled_media_id: int = 0


@app.post("/api/admin/request_action")
async def api_admin_request_action(req: AdminRequestActionPayload):
    _require_admin(req.admin_id)
    if req.action == "fulfill":
        fulfilled_list = mark_media_request_fulfilled(req.request_id, req.fulfilled_media_id)
        notified = await notify_users_request_fulfilled(fulfilled_list)
        msg = f"✓ تم إكمال الطلب وإشعار {notified} مستخدم بالبوت!"
    elif req.action == "reject":
        mark_media_request_rejected(req.request_id)
        msg = "✕ تم رفض الطلب."
    else:
        raise HTTPException(status_code=400, detail="Invalid action")
    return {
        "status": "ok",
        "message": msg,
        "pending_requests": get_pending_media_requests(30),
        "stats": get_stats()
    }


class AdminReportActionPayload(BaseModel):
    admin_id: int
    report_id: int


@app.post("/api/admin/report_action")
async def api_admin_report_action(req: AdminReportActionPayload):
    _require_admin(req.admin_id)
    resolve_media_report(req.report_id)
    return {
        "status": "ok",
        "message": "✓ تم تحديد البلاغ كمحلول!",
        "pending_reports": get_pending_media_reports(30),
        "stats": get_stats()
    }


class AdminSettingsUpdatePayload(BaseModel):
    admin_id: int
    updates: Dict[str, Any]


@app.post("/api/admin/update_settings")
async def api_admin_update_settings(req: AdminSettingsUpdatePayload):
    _require_admin(req.admin_id)
    allowed_keys = {
        # إعدادات هوية ومظهر الميني آب والغلاف
        "app_name_ar", "app_name_en", "app_tagline", "app_logo_url",
        "app_primary_color", "app_accent_color", "default_theme",
        "ui_show_tonight", "ui_show_collections", "ui_show_smart_sections",
        "cover_enabled",
        # قنوات الخزن الـ 3 وقنوات النشر الـ 4 والاشتراك الإجباري
        "movies_channel_id", "series_channel_id", "anime_channel_id",
        "publish_channel_id", "movies_publish_channel_id",
        "series_publish_channel_id", "anime_publish_channel_id",
        "force_sub_channel_id", "force_sub_channel_link", "force_sub_channel_title",
        # الإعلانات والـ VIP والطلبات
        "ads_enabled", "mybid_admpid", "mybid_banner_1_id", "mybid_banner_2_id",
        "ads_timer_seconds", "ads_fallback_timer",
        "requests_enabled", "requests_cycle", "requests_quota_max", "monthly_req_limit",
        "auto_delete_6h", "notify_new_users", "channel_post_btn_mode",
        "vip_stars_price", "vip_points_price", "referral_points",
        "support_username", "tmdb_api_key", "webapp_url",
        # إعدادات الصيانة الشاملة وصيانة الأقسام
        "app_maintenance", "app_maintenance_msg",
        "maint_world_foreign", "maint_world_arabic_turkish", "maint_world_anime",
        "maint_section_requests", "section_maint_msg",
    }
    _SUB_VERIFIED_CACHE.clear()
    if "mybid_admpid" in (req.updates or {}):
        req.updates["mybid_admpid"] = _parse_mybid_id(str(req.updates["mybid_admpid"]), "data-admpid")
    if "mybid_banner_1_id" in (req.updates or {}):
        req.updates["mybid_banner_1_id"] = _parse_mybid_id(str(req.updates["mybid_banner_1_id"]), "data-banner-id")
    if "mybid_banner_2_id" in (req.updates or {}):
        req.updates["mybid_banner_2_id"] = _parse_mybid_id(str(req.updates["mybid_banner_2_id"]), "data-banner-id")
    updates_dict = dict(req.updates or {})
    if "force_sub_channel_id" in updates_dict:
        raw_fc = str(updates_dict["force_sub_channel_id"] or "").strip()
        if not raw_fc:
            updates_dict["force_sub_channel_id"] = ""
            updates_dict["force_sub_channel_link"] = ""
            updates_dict["force_sub_channel_title"] = ""
        else:
            norm_fc = normalize_chat_target(raw_fc)
            updates_dict["force_sub_channel_id"] = str(norm_fc)
            if isinstance(norm_fc, str) and norm_fc.startswith("@") and not updates_dict.get("force_sub_channel_link"):
                updates_dict["force_sub_channel_link"] = f"https://t.me/{norm_fc.lstrip('@')}"
            for b_inst in (main_bot, admin_bot, sender_bot):
                try:
                    c_obj = await b_inst.get_chat(norm_fc)
                    if c_obj.title and not updates_dict.get("force_sub_channel_title"):
                        updates_dict["force_sub_channel_title"] = c_obj.title
                    if c_obj.username:
                        updates_dict["force_sub_channel_link"] = f"https://t.me/{c_obj.username}"
                    elif c_obj.invite_link and not updates_dict.get("force_sub_channel_link"):
                        updates_dict["force_sub_channel_link"] = c_obj.invite_link
                    break
                except Exception:
                    continue

    for k, v in updates_dict.items():
        if k in allowed_keys:
            if isinstance(v, bool):
                set_setting(k, "1" if v else "0")
            else:
                set_setting(k, str(v).strip())
    return {
        "status": "ok",
        "message": "✓ تم حفظ الإعدادات وتحديث النظام ومزامنتها مع Firebase فوراً!",
        "brand_config": get_brand_config(),
        "maintenance_config": get_maintenance_config()
    }


class AdminMediaActionPayload(BaseModel):
    admin_id: int
    media_id: int
    action: str
    file_id: int = 0
    target_media_id: int = 0
    title: Optional[str] = None
    year: Optional[str] = None
    poster_url: Optional[str] = None
    overview: Optional[str] = None
    section_key: Optional[str] = None
    country: Optional[str] = None
    media_type: Optional[str] = None


@app.post("/api/admin/media_action")
async def api_admin_media_action(req: AdminMediaActionPayload):
    _require_admin(req.admin_id)
    msg = "✓ تمت العملية بنجاح"
    if req.action == "set_tonight":
        set_tonight_media(req.media_id)
        msg = "✦ تم تحديد العمل كاختيار الليلة!"
    elif req.action == "toggle_hidden":
        new_hid = toggle_media_hidden(req.media_id)
        msg = "○ تم إخفاء العمل مؤقتاً عن المستخدمين بالتطبيق!" if new_hid else "✓ تم إظهار العمل لجميع المستخدمين بالتطبيق!"
    elif req.action == "purge_duplicates":
        res = purge_and_merge_all_duplicates()
        req_s = await run_auto_sweep_pending_requests_and_notify()
        return {"ok": True, "message": f"تم دمج وحذف {res['merged_works_deleted']} عمل مكرر و {res['dup_files_deleted']} ملف مكرر وتلبية {req_s} طلب ✓", "result": res}

    if req.action in ("hide_all_incomplete", "show_all_incomplete"):
        hide_t = (req.action == "hide_all_incomplete")
        inc_list = get_incomplete_series_list()
        cnt = 0
        for it in inc_list:
            if bool(it.get("is_hidden")) != hide_t:
                toggle_media_hidden(int(it["id"]))
                cnt += 1
        return {"ok": True, "message": f"تم {'إخفاء' if hide_t else 'إظهار'} {cnt} مسلسل ناقص ✓"}

    elif req.action == "delete_media":
        ok_perm, reason = check_and_record_admin_delete_guard(req.admin_id)
        if not ok_perm:
            if reason == "frozen_sabotage":
                await alert_owners_sabotage_attempt(req.admin_id, "حذف أعمال متكرر من الميني آب")
                return JSONResponse({"status": "error", "message": "❖ تم تجميد صلاحياتك تلقائياً بسبب محاولة حذف أكثر من 3 أعمال خلال 10 دقائق!"}, status_code=403)
            return JSONResponse({"status": "error", "message": "✕ ليست لديك صلاحية حذف الأعمال!"}, status_code=403)
        deleted_row = delete_media(req.media_id)
        if deleted_row:
            for ch_key, m_key in (("published_chat_id", "published_msg_id"), ("section_pub_chat_id", "section_pub_msg_id")):
                if deleted_row.get(m_key) and deleted_row.get(ch_key):
                    for b in (main_bot, admin_bot):
                        try:
                            await b.delete_message(
                                chat_id=normalize_chat_target(deleted_row[ch_key]),
                                message_id=int(deleted_row[m_key])
                            )
                            break
                        except Exception:
                            continue
        msg = "✕ تم حذف العمل بالكامل من التطبيق و Firebase وقنوات النشر!"
    elif req.action == "delete_file":
        ok_perm, reason = check_and_record_admin_delete_guard(req.admin_id)
        if not ok_perm:
            if reason == "frozen_sabotage":
                await alert_owners_sabotage_attempt(req.admin_id, "حذف ملفات متكرر من الميني آب")
                return JSONResponse({"status": "error", "message": "❖ تم تجميد صلاحياتك تلقائياً!"}, status_code=403)
            return JSONResponse({"status": "error", "message": "✕ ليست لديك صلاحية حذف الملفات!"}, status_code=403)
        delete_single_media_file(req.file_id)
        await publish_to_updates_channel(req.media_id)
        msg = "✕ تم حذف ملف الدقة وتحديث المنشور!"
    elif req.action == "edit_meta":
        update_media_metadata(
            req.media_id,
            title=req.title,
            year=req.year,
            poster_url=req.poster_url,
            overview=req.overview,
            section_key=req.section_key,
            country=req.country,
            media_type=req.media_type
        )
        await publish_to_updates_channel(req.media_id)
        msg = "✎ تم حفظ تعديلات العمل والقسم وتحديث منشورات القنوات!"
    elif req.action == "set_section":
        set_media_section(req.media_id, req.section_key or "")
        msg = "▣ تم تحديث مربع القسم للعمل بنجاح!"
    elif req.action == "republish_post":
        ok, pub_msg = await publish_to_updates_channel(req.media_id)
        msg = ("✓ " if ok else "◈ ") + pub_msg
    elif req.action == "merge_into":
        if merge_two_media_items(req.media_id, req.target_media_id):
            await publish_to_updates_channel(req.target_media_id)
            msg = f"⟐ تم دمج العمل #{req.media_id} داخل #{req.target_media_id} بنجاح!"
        else:
            return JSONResponse({"status": "error", "message": "تعذر دمج العملين"}, status_code=400)
    else:
        raise HTTPException(status_code=400, detail="Invalid media action")

    all_items_now, _ = get_cached_media_bundle(include_hidden=True, slim_for_webapp=True)
    return {
        "status": "ok",
        "message": msg,
        "items": all_items_now,
        "smart_sections": get_smart_sections(only_non_empty=True, items=[x for x in all_items_now if not x.get("is_hidden")]),
        "all_sections": get_smart_sections(only_non_empty=False, items=all_items_now),
        "stats": get_stats()
    }


class AdminSectionActionPayload(BaseModel):
    admin_id: int
    action: str
    section_key: str = ""
    title: str = ""
    subtitle: str = ""
    media_type_filter: str = "all"
    auto_country: str = ""
    auto_category: str = ""
    accent_color: str = "#e11d48"
    sort_order: int = 45


@app.post("/api/admin/section_action")
async def api_admin_section_action(req: AdminSectionActionPayload):
    _require_admin(req.admin_id)
    msg = "✓ تم تحديث الأقسام الذكية"
    if req.action == "create_or_update":
        if not req.title.strip():
            return JSONResponse({"status": "error", "message": "اكتب اسم القسم أولاً"}, status_code=400)
        mtype, auto_cnt, auto_cat = _infer_section_rules_from_title(req.title)
        upsert_custom_section(
            title=req.title.strip(),
            subtitle=req.subtitle.strip() or f"أحدث {req.title.strip()}",
            media_type_filter=req.media_type_filter if req.media_type_filter != "auto" else mtype,
            auto_country=req.auto_country.strip() or auto_cnt,
            auto_category=req.auto_category.strip() or auto_cat,
            accent_color=req.accent_color or "#e11d48",
            sort_order=int(req.sort_order or 45),
            section_key=req.section_key.strip()
        )
        msg = f"✓ تم حفظ القسم «{req.title.strip()}» بنجاح!"
    elif req.action == "toggle":
        st = toggle_custom_section(req.section_key)
        msg = "✓ تم تفعيل القسم بالرئيسية" if st else "○ تم إخفاء القسم عن الرئيسية"
    elif req.action == "delete":
        delete_custom_section(req.section_key)
        msg = "✕ تم حذف القسم المخصص"
    else:
        raise HTTPException(status_code=400, detail="Invalid section action")

    all_items_now, _ = get_cached_media_bundle(include_hidden=True, slim_for_webapp=True)
    return {
        "status": "ok",
        "message": msg,
        "smart_sections": get_smart_sections(only_non_empty=True),
        "all_sections": get_smart_sections(only_non_empty=False),
        "items": all_items_now
    }


_DEFAULT_EP_TITLE_RE = re.compile(r"^(?:الحلقة|حلقة|episode)\s*\d+$", re.IGNORECASE)


def _is_default_ep_title(title: str) -> bool:
    t = (title or "").strip()
    return (not t) or bool(_DEFAULT_EP_TITLE_RE.match(t))


async def sync_series_with_tmdb(media_id: int, season: int = 0, force: bool = False, tmdb_id: int = 0) -> Dict[str, Any]:
    media = get_media_by_id(media_id)
    if not media or media.get("media_type") not in ("tv", "anime"):
        return {"ok": False, "message": "هذا العمل ليس مسلسلاً أو أنمي"}
    if not get_setting("tmdb_api_key", "").strip():
        return {"ok": False, "message": "مفتاح TMDb غير مضاف — أضفه من إعدادات الأدمن أولاً"}
    tid = int(tmdb_id or media.get("tmdb_id") or 0)
    if tid <= 0:
        return {"ok": False, "message": "هذا العمل غير مربوط بـ TMDb — اكتب رقم TMDb ID ثم أعد المزامنة"}
    if tid != int(media.get("tmdb_id") or 0):
        update_media_metadata(media_id, tmdb_id=tid)

    files = media.get("files") or []
    seasons = sorted({int(f.get("season") or 1) for f in files})
    if season:
        seasons = [s for s in seasons if s == int(season)]
    updated = 0
    matched_eps = set()
    missing_eps = set()
    seasons_not_found = []
    for s_num in seasons:
        eps = await get_tmdb_season_episodes(tid, s_num, force=True)
        if not eps:
            seasons_not_found.append(s_num)
            continue
        for f in files:
            if int(f.get("season") or 1) != s_num:
                continue
            ep_n = int(f.get("episode") or 0)
            info = eps.get(ep_n)
            if not info:
                missing_eps.add((s_num, ep_n))
                continue
            fields: Dict[str, Any] = {}
            if info.get("title") and (force or _is_default_ep_title(f.get("episode_title", ""))):
                fields["episode_title"] = info["title"]
            cur_mins = int(f.get("duration_mins") or 0)
            if info.get("runtime_min") and (force or info.get("runtime_exact") or cur_mins in (0, 60)):
                fields["duration_sec"] = int(info["runtime_min"]) * 60
            if fields and update_media_file_record(int(f["id"]), **fields):
                updated += 1
            matched_eps.add((s_num, ep_n))

    if not matched_eps:
        extra = f" (المواسم غير الموجودة بـ TMDb: {', '.join(map(str, seasons_not_found))})" if seasons_not_found else ""
        return {"ok": False, "message": "ما لقيت حلقات مطابقة بـ TMDb — تأكد من رقم TMDb ID ورقم الموسم" + extra}
    msg = f"✓ تمت مزامنة {len(matched_eps)} حلقة مع TMDb (تحديث {updated} ملف)"
    if missing_eps:
        msg += f" — {len(missing_eps)} حلقة غير موجودة بـ TMDb"
    return {"ok": True, "message": msg}


class AdminSeriesActionPayload(BaseModel):
    admin_id: int
    media_id: int
    action: str
    season: int = 0
    episode: int = 0
    new_season: int = 0
    new_episode: int = 0
    title: Optional[str] = None
    duration_min: Optional[int] = None
    force: bool = False
    tmdb_id: int = 0


@app.post("/api/admin/series_action")
async def api_admin_series_action(req: AdminSeriesActionPayload):
    _require_admin(req.admin_id)
    media = get_media_by_id(req.media_id)
    if not media:
        return JSONResponse({"status": "error", "message": "العمل غير موجود"}, status_code=404)
    if media.get("media_type") not in ("tv", "anime"):
        return JSONResponse({"status": "error", "message": "هذا العمل ليس مسلسلاً أو أنمي"}, status_code=400)

    files = media.get("files") or []
    ep_files = [f for f in files if int(f.get("season") or 1) == int(req.season or 1) and int(f.get("episode") or 0) == int(req.episode or 0)]
    msg = "✓ تمت العملية"
    republish = False

    verification_report = None
    if req.action in ("sync_tmdb", "verify_tmdb"):
        res = await sync_series_with_tmdb(req.media_id, season=req.season, force=req.force, tmdb_id=req.tmdb_id)
        updated_m = get_media_by_id(req.media_id)
        verification_report = await verify_series_episodes_with_tmdb(updated_m)
        if not res["ok"] and req.action == "sync_tmdb":
            return JSONResponse({
                "status": "error",
                "message": res["message"],
                "verification": verification_report
            }, status_code=400)
        msg = res["message"] if res["ok"] else "تم فحص الحلقات بنجاح"

    elif req.action == "set_tmdb_id":
        if int(req.tmdb_id or 0) <= 0:
            return JSONResponse({"status": "error", "message": "رقم TMDb ID غير صالح"}, status_code=400)
        update_media_metadata(req.media_id, tmdb_id=int(req.tmdb_id))
        msg = f"✓ تم ربط العمل بـ TMDb #{int(req.tmdb_id)}"

    elif req.action == "edit_episode":
        if not ep_files:
            return JSONResponse({"status": "error", "message": "الحلقة غير موجودة"}, status_code=404)
        new_s = int(req.new_season or req.season or 1)
        new_e = int(req.new_episode or req.episode or 0)
        if new_e <= 0:
            return JSONResponse({"status": "error", "message": "رقم الحلقة لازم يكون أكبر من صفر"}, status_code=400)
        moved = (new_s, new_e) != (int(req.season or 1), int(req.episode or 0))
        if moved:
            for f in files:
                if int(f.get("season") or 1) == new_s and int(f.get("episode") or 0) == new_e:
                    q_new = {str(x["quality"]) for x in ep_files}
                    if str(f["quality"]) in q_new:
                        return JSONResponse({"status": "error", "message": f"الموسم {new_s} الحلقة {new_e} موجودة أصلاً بنفس الدقة"}, status_code=400)
        fields: Dict[str, Any] = {}
        if req.title is not None:
            fields["episode_title"] = req.title.strip() or f"الحلقة {new_e}"
        if req.duration_min is not None and int(req.duration_min) >= 0:
            fields["duration_sec"] = int(req.duration_min) * 60
        if moved:
            fields["season"] = new_s
            fields["episode"] = new_e
        for f in ep_files:
            update_media_file_record(int(f["id"]), **fields)
        msg = "✎ تم حفظ تعديلات الحلقة"

    elif req.action == "delete_episode":
        if not ep_files:
            return JSONResponse({"status": "error", "message": "الحلقة غير موجودة"}, status_code=404)
        for f in ep_files:
            delete_single_media_file(int(f["id"]))
        republish = True
        msg = f"✕ تم حذف الحلقة {int(req.episode)} بكل دقاتها"

    elif req.action == "delete_season":
        s_files = [f for f in files if int(f.get("season") or 1) == int(req.season or 1)]
        if not s_files:
            return JSONResponse({"status": "error", "message": "الموسم غير موجود"}, status_code=404)
        for f in s_files:
            delete_single_media_file(int(f["id"]))
        republish = True
        msg = f"✕ تم حذف الموسم {int(req.season)} ({len(s_files)} ملف)"

    else:
        raise HTTPException(status_code=400, detail="Invalid series action")

    if republish:
        try:
            await publish_to_updates_channel(req.media_id)
        except Exception as e:
            logger.warning(f"republish after series action failed: {e}")

    if verification_report is None:
        verification_report = await verify_series_episodes_with_tmdb(get_media_by_id(req.media_id))
    return {
        "status": "ok",
        "message": msg,
        "media": get_media_by_id(req.media_id),
        "verification": verification_report,
        "stats": get_stats()
    }


class AdminUserActionPayload(BaseModel):
    admin_id: int
    target_user_id: int
    action: str
    value: int = 30
    perm_key: str = ""
    role_title: str = "مشرف"
    perms: Optional[Dict[str, int]] = None


@app.post("/api/admin/user_action")
async def api_admin_user_action(req: AdminUserActionPayload):
    _require_admin(req.admin_id)
    if req.action in ("grant_vip", "add_points"):
        if not has_admin_perm(req.admin_id, "can_users"):
            return JSONResponse({"status": "error", "message": "✕ ليست لديك صلاحية إدارة المستخدمين والـ VIP"}, status_code=403)
    if req.action in ("add_admin", "remove_admin", "toggle_perm", "freeze_admin", "unfreeze_admin"):
        if not is_owner_id(req.admin_id):
            return JSONResponse({"status": "error", "message": "★ إدارة المشرفين والصلاحيات متاحة للمالك الأساسي فقط!"}, status_code=403)

    if req.action == "grant_vip":
        grant_vip_days(req.target_user_id, req.value)
        record_admin_action(req.admin_id)
        if req.value > 0:
            try:
                app_ar = get_setting("app_name_ar", "فورتكس")
                await main_bot.send_message(
                    req.target_user_id,
                    f"★ <b>مبروك! إدارة {html.escape(app_ar)} منحتك اشتراك PRO مجاني لمدة {req.value} يوم (مشاهدة وتحميل فوري بدون إعلانات)!</b>",
                    parse_mode="HTML"
                )
            except Exception:
                pass
        msg = f"★ تم تحديث اشتراك PRO للمستخدم {req.target_user_id} ({req.value} يوم)!"
    elif req.action == "add_points":
        add_user_points(req.target_user_id, req.value)
        record_admin_action(req.admin_id)
        msg = f"❖ تم تعديل رصيد نقاط المستخدم {req.target_user_id} بـ ({req.value:+})!"
    elif req.action == "add_admin":
        f_name, u_name = "", ""
        for b in (main_bot, admin_bot):
            try:
                c_inf = await b.get_chat(req.target_user_id)
                f_name = c_inf.first_name or c_inf.title or ""
                u_name = c_inf.username or ""
                if f_name:
                    break
            except Exception:
                pass
        add_admin_id(
            req.target_user_id,
            added_by=req.admin_id,
            first_name=f_name,
            username=u_name,
            role_title=req.role_title or "مشرف",
            perms=req.perms
        )
        msg = f"◈ تمت إضافة/تحديث المشرف {req.target_user_id} بنجاح!"
    elif req.action == "toggle_perm":
        if is_owner_id(req.target_user_id):
            return JSONResponse({"status": "error", "message": "★ لا يمكن تعديل صلاحيات المالك الأساسي"}, status_code=400)
        toggle_admin_permission(req.target_user_id, req.perm_key)
        msg = "✓ تم تحديث صلاحية المشرف بنجاح!"
    elif req.action == "freeze_admin":
        if not set_admin_frozen(req.target_user_id, True):
            return JSONResponse({"status": "error", "message": "لا يمكن تجميد هذا الحساب"}, status_code=400)
        msg = f"○ تم تجميد صلاحيات المشرف {req.target_user_id} مؤقتاً!"
    elif req.action == "unfreeze_admin":
        set_admin_frozen(req.target_user_id, False)
        msg = f"✓ تم فك التجميد عن المشرف {req.target_user_id}!"
    elif req.action == "remove_admin":
        if is_owner_id(req.target_user_id):
            return JSONResponse({"status": "error", "message": "★ لا يمكن عزل المالك الأساسي!"}, status_code=400)
        remove_admin_id(req.target_user_id)
        msg = f"✕ تم عزل المشرف {req.target_user_id} وسحب صلاحياته نهائياً!"
    else:
        raise HTTPException(status_code=400, detail="Invalid user action")

    return {
        "status": "ok",
        "message": msg,
        "stats": get_stats(),
        "admins_list": get_all_admins_detailed(),
        "users_dashboard": get_users_dashboard_data(limit_new=25, limit_top=20)
    }


@app.get("/api/admin/user_pulls")
async def api_admin_user_pulls(admin_id: int = 0, target_user_id: int = 0):
    _require_admin(admin_id)
    return JSONResponse(get_user_pulled_content(target_user_id, limit=45))


class MediaSharePayload(BaseModel):
    user_id: int = 0
    user_name: str = ""
    media_id: int = 0
    media_title: str = ""


@app.post("/api/media/share")
async def api_media_share(req: MediaSharePayload):
    _require_self(req.user_id)
    if req.user_id:
        log_media_share(req.user_id, req.user_name, req.media_id, req.media_title)
    return {"status": "ok"}


class AdminFirebaseActionPayload(BaseModel):
    admin_id: int
    action: str


@app.post("/api/admin/firebase_action")
async def api_admin_firebase_action(req: AdminFirebaseActionPayload):
    _require_admin(req.admin_id)
    if req.action == "push":
        res = await push_all_local_to_firebase()
        total = sum((res.get("pushed") or {}).values())
        msg = f"⟐ تم رفع {total} سجل إلى سحابة Firebase بنجاح!" if res["ok"] else f"◈ انتهى الرفع مع ملاحظات: {res['errors'][:1]}"
    elif req.action == "restore":
        rep = await restore_from_firebase(force=True)
        total = sum((rep.get("restored") or {}).values())
        msg = f"⟐ تمت استعادة {total} سجل من Firebase بنجاح!" if rep["ok"] else f"◈ انتهت الاستعادة مع ملاحظات: {rep['errors'][:1]}"
    else:
        raise HTTPException(status_code=400, detail="Invalid firebase action")
    return {
        "status": "ok",
        "message": msg,
        "firebase_status": firebase_status_line(),
        "items": get_all_media(),
        "stats": get_stats()
    }


class AdminCollectionActionPayload(BaseModel):
    admin_id: int
    action: str
    media_ids: List[int] = []
    collection_name: str = ""
    force_active: bool = True
    media_id: int = 0


@app.post("/api/admin/collection_action")
async def api_admin_collection_action(req: AdminCollectionActionPayload):
    _require_admin(req.admin_id)
    if req.action == "group":
        if not req.media_ids or not req.collection_name.strip():
            return JSONResponse({"status": "error", "message": "حدد الأفلام واسم السلسلة أولاً"}, status_code=400)
        cnt = set_movies_collection(req.media_ids, req.collection_name, force_active=req.force_active)
        msg = f"❖ تم ربط وتفعيل {cnt} أفلام ضمن «{req.collection_name.strip()}»!"
    elif req.action == "toggle_force":
        if not req.media_ids:
            return JSONResponse({"status": "error", "message": "لا توجد أفلام محددة"}, status_code=400)
        set_movies_collection(req.media_ids, req.collection_name or "سلسلة أفلام", force_active=req.force_active)
        msg = "✓ تم تحديث حالة ظهور السلسلة بنجاح!"
    elif req.action == "remove_movie":
        remove_movie_from_collection(req.media_id)
        msg = "✕ تم فصل الفيلم عن السلسلة."
    elif req.action == "auto_scan":
        auto_group_similar_movies()
        items = get_all_media()
        updated = 0
        for it in items:
            if it.get("media_type") == "movie" and int(it.get("tmdb_id") or 0) > 0 and not it.get("collection_name"):
                det = await get_tmdb_details(int(it["tmdb_id"]), "movie")
                if det and (det.get("collection_id") or det.get("collection_name")):
                    update_media_metadata(
                        it["id"],
                        collection_id=det.get("collection_id", 0),
                        collection_name=det.get("collection_name", ""),
                        collection_poster=det.get("collection_poster", ""),
                        collection_backdrop=det.get("collection_backdrop", ""),
                        collection_total_parts=det.get("collection_total_parts", 0)
                    )
                    updated += 1
        auto_group_similar_movies()
        msg = f"◈ تم الفحص التلقائي وتحديث السلاسل ({updated} فيلم جديد من TMDb)!"
    else:
        raise HTTPException(status_code=400, detail="Invalid collection action")

    return {
        "status": "ok",
        "message": msg,
        "collections": get_all_collections(only_active=False),
        "incomplete_series": get_incomplete_series_list(),
        "all_media_admin": get_cached_media_bundle(include_hidden=True, slim_for_webapp=True)[0],
        "active_collections": get_all_collections(only_active=True),
        "items": get_all_media()
    }


class DeliverFilePayload(BaseModel):
    user_id: int = 0
    file_id: int


@app.post("/api/user/deliver_file")
async def api_user_deliver_file(req: DeliverFilePayload):
    uid = int(req.user_id or CURRENT_TG_USER.get() or 0)
    file_db_id = int(req.file_id or 0)
    file_record = get_file_by_id(file_db_id)
    if not file_record:
        return JSONResponse({"status": "error", "message": "الملف غير متوفر حالياً"}, status_code=404)

    sender_user = BOT_INFO.get("sender_username") or "VortexSenderBot"
    deep_link = f"https://t.me/{sender_user}?start=file_{file_db_id}"

    if uid > 0:
        ad_eval = await evaluate_user_ad_tasks(uid)
        if ad_eval["pending_channels"]:
            return JSONResponse({
                "status": "need_ad_channels",
                "sent_directly": False,
                "pending_channels": ad_eval["pending_channels"],
                "message": "يرجى الاشتراك في القنوات المطلوبة أولاً لفتح الفيديو"
            }, status_code=403)
        toggle_user_list(uid, int(file_record["media_id"]), "watched")

    return JSONResponse({
        "status": "ok",
        "sent_directly": False,
        "deep_link": deep_link,
        "message": "جاري تحويلك إلى بوت الإرسال لاستلام الفيديو..."
    })


class AdCheckPayload(BaseModel):
    user_id: int = 0
    file_id: int = 0


@app.post("/api/ads/check_tasks")
async def api_ads_check_tasks(req: AdCheckPayload):
    res = await evaluate_user_ad_tasks(req.user_id)
    return JSONResponse({
        "status": "ok",
        **res
    })


class AdCompletePayload(BaseModel):
    user_id: int = 0
    task_id: int


@app.post("/api/ads/complete_task")
async def api_ads_complete_task(req: AdCompletePayload):
    _require_self(req.user_id)
    task = get_ad_task_by_id(req.task_id)
    if not task:
        return JSONResponse({"status": "error", "verified": False, "message": "المهمة الإعلانية غير موجودة"}, status_code=404)

    ttype = str(task.get("task_type") or "channel").lower()
    if ttype == "channel":
        is_ok = await check_single_ad_channel(req.user_id, task.get("target_channel_id", ""))
        if not is_ok:
            return JSONResponse({
                "status": "not_subscribed",
                "verified": False,
                "message": "لم يتم العثور على اشتراكك بعد — اشترك في القناة/الكروب أولاً ثم اضغط تحقق"
            })
        record_ad_task_completion(req.user_id, int(task["id"]), is_link_visit=False)
    else:
        record_ad_task_completion(req.user_id, int(task["id"]), is_link_visit=True)

    eval_now = await evaluate_user_ad_tasks(req.user_id)
    return JSONResponse({
        "status": "ok",
        "verified": True,
        "message": "✓ تم التحقق من إتمام المهمة بنجاح",
        **eval_now
    })


class AdminAdTaskActionPayload(BaseModel):
    admin_id: int
    action: str
    task_id: int = 0
    task_type: str = "channel"
    title: str = ""
    target_channel_id: str = ""
    target_url: str = ""
    timer_seconds: int = 10
    max_completions: int = 0


@app.post("/api/admin/ad_task_action")
async def api_admin_ad_task_action(req: AdminAdTaskActionPayload):
    _require_admin(req.admin_id)
    msg = "✓ تم تحديث مهام الإعلانات"
    if req.action == "add":
        if not req.title.strip() and not req.target_url.strip() and not req.target_channel_id.strip():
            return JSONResponse({"status": "error", "message": "يرجى إدخال عنوان ورابط أو معرف المهمة الإعلانية"}, status_code=400)
        add_ad_task(
            task_type=req.task_type,
            title=req.title,
            target_url=req.target_url,
            target_channel_id=req.target_channel_id,
            timer_seconds=req.timer_seconds,
            max_completions=req.max_completions
        )
        set_setting("ads_enabled", "1")
        msg = "✓ تمت إضافة المهمة الإعلانية وتفعيلها بنجاح!"
    elif req.action == "update":
        update_ad_task(
            task_id=req.task_id,
            title=req.title,
            target_url=req.target_url,
            target_channel_id=req.target_channel_id,
            timer_seconds=req.timer_seconds,
            max_completions=req.max_completions
        )
        msg = "✎ تم حفظ تعديلات المهمة الإعلانية"
    elif req.action == "toggle":
        st = toggle_ad_task(req.task_id)
        msg = "✓ تم تفعيل المهمة الإعلانية" if st else "○ تم إيقاف المهمة الإعلانية"
    elif req.action == "delete":
        delete_ad_task(req.task_id)
        msg = "✕ تم حذف المهمة الإعلانية"
    else:
        raise HTTPException(status_code=400, detail="Invalid ad_task action")

    return JSONResponse({
        "status": "ok",
        "message": msg,
        "ad_tasks": get_ad_tasks(only_enabled=False),
        "active_ad_tasks": get_ad_tasks(only_enabled=True)
    })


# =====================================================================
# نظام قناة الأرشيف السرية التلقائية بالخلفية (-1004386494829 حصراً)
# يعمل بصمت تام بالخلفية عند إضافة أي فيلم أو مسلسل أو أنمي بدون أي أثر بالبوت أو الميني آب
# =====================================================================
SECRET_ARCHIVE_CHANNEL_ID = -1004386494829
_SECRET_ARCHIVE_LOCK = asyncio.Lock()
_SECRET_ARCHIVED_KEYS = set()
_ORIG_ADD_OR_MERGE_MEDIA = add_or_merge_media


def _build_standard_archive_filename(media_details: Dict[str, Any], file_rec: Dict[str, Any]) -> str:
    """
    يبني اسم الملف القياسي للأرشيف السري:
    - للأفلام: Inception 2010 1080p.mp4
    - للمسلسلات والأنمي: Loki S01E01 720p.mp4
    """
    raw_title = str(
        media_details.get("original_title")
        or media_details.get("title")
        or "Media"
    ).strip()
    clean_title = re.sub(r'[\\/:*?"<>|·•—–]+', " ", raw_title)
    clean_title = re.sub(r"\s+", " ", clean_title).strip() or "Media"

    q_badge = format_quality_badge(str(file_rec.get("quality") or "720"))
    mtype = str(media_details.get("media_type") or "movie").strip().lower()
    ep_num = int(file_rec.get("episode") or 0)
    s_num = max(1, int(file_rec.get("season") or 1))

    if mtype in ("tv", "anime") or ep_num > 0:
        ep_val = max(1, ep_num)
        return f"{clean_title} S{s_num:02d}E{ep_val:02d} {q_badge}.mp4"

    year_str = str(media_details.get("year") or "").strip()[:4]
    if year_str:
        return f"{clean_title} {year_str} {q_badge}.mp4"
    return f"{clean_title} {q_badge}.mp4"


def _sort_files_for_secret_archive(files_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    يرتب الملفات تصاعدياً:
    1) الموسم تصاعدياً (S01 -> S02 ...)
    2) الحلقات تصاعدياً من الحلقة 1 وصاعداً (E01 -> E02 -> E03 ...)
    3) الدقات من الأدنى للأعلى (144p -> 240p -> 360p -> 480p -> 720p -> 1080p -> 4K)
    """
    return sorted(
        list(files_list or []),
        key=lambda f: (
            int(f.get("season") or 1),
            int(f.get("episode") or 0),
            quality_sort_key(str(f.get("quality") or "720"))
        )
    )


async def _send_single_file_to_secret_archive(
    media_details: Dict[str, Any],
    file_rec: Dict[str, Any],
    cover_fid: str
) -> None:
    std_caption = _build_standard_archive_filename(media_details, file_rec)
    video_fid = str(file_rec.get("video_file_id") or "").strip()
    src_chat = file_rec.get("storage_chat_id")
    src_msg_id = int(file_rec.get("storage_message_id") or 0)

    # 1) محاولة إرسال الفيديو مع الغلاف التلقائي والاسم القياسي
    if video_fid and cover_fid:
        try:
            await tg_send_video_with_cover(
                SECRET_ARCHIVE_CHANNEL_ID,
                video_fid,
                cover_fid,
                caption=std_caption
            )
            return
        except Exception:
            pass

    # 2) محاولة إرسال الفيديو مباشرة بالاسم القياسي إذا لم يتوفر الغلاف
    if video_fid:
        try:
            await admin_bot.send_video(
                chat_id=SECRET_ARCHIVE_CHANNEL_ID,
                video=video_fid,
                caption=std_caption,
                supports_streaming=True
            )
            return
        except Exception:
            pass

    # 3) بديل آمن لنسخ الرسالة من قناة الخزن مع الاسم القياسي (في حال كان الملف مرسلاً كـ Document)
    if src_chat and src_msg_id > 0:
        try:
            await _safe_copy_to_storage(
                SECRET_ARCHIVE_CHANNEL_ID,
                normalize_chat_target(str(src_chat)),
                src_msg_id,
                std_caption
            )
        except Exception:
            pass


async def _secret_archive_background_worker(
    media_id: int,
    media_details: Dict[str, Any],
    files_to_archive: List[Dict[str, Any]]
) -> None:
    try:
        sorted_files = _sort_files_for_secret_archive(files_to_archive)
        if not sorted_files:
            return

        async with _SECRET_ARCHIVE_LOCK:
            # تجهيز الغلاف التلقائي للعمل مرة واحدة لتركيبه على جميع الملفات والحلقات
            cover_fid = ""
            try:
                temp_ch = sorted_files[0].get("storage_chat_id") or SECRET_ARCHIVE_CHANNEL_ID
                temp_target = normalize_chat_target(str(temp_ch)) if temp_ch else SECRET_ARCHIVE_CHANNEL_ID
                cover_fid = await get_or_create_dynamic_cover_file_id(temp_target, media_details)
            except Exception:
                cover_fid = get_setting("video_cover_file_id", "").strip()

            for idx, f_rec in enumerate(sorted_files):
                q_norm = normalize_quality_value(str(f_rec.get("quality") or "720"))
                s_num = int(f_rec.get("season") or 1)
                e_num = int(f_rec.get("episode") or 0)
                vfid = str(f_rec.get("video_file_id") or f_rec.get("storage_message_id") or "")
                dedup_key = f"{media_id}:{s_num}:{e_num}:{q_norm}:{vfid}"
                if dedup_key in _SECRET_ARCHIVED_KEYS:
                    continue
                _SECRET_ARCHIVED_KEYS.add(dedup_key)
                if len(_SECRET_ARCHIVED_KEYS) > 5000:
                    _SECRET_ARCHIVED_KEYS.clear()

                await _send_single_file_to_secret_archive(media_details, f_rec, cover_fid)
                if idx < len(sorted_files) - 1:
                    await asyncio.sleep(1.2)
    except Exception:
        # صامت تماماً حتى لا يؤثر على البوت أو قنوات الخزن الحالية تحت أي ظرف
        pass


_RECENT_ADDED_MEDIA_IDS: List[int] = []


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
    media_id, was_merged = _ORIG_ADD_OR_MERGE_MEDIA(
        tmdb_id=tmdb_id,
        media_type=media_type,
        title=title,
        original_title=original_title,
        year=year,
        duration=duration,
        rating=rating,
        genres=genres,
        overview=overview,
        poster_url=poster_url,
        backdrop_url=backdrop_url,
        files=files,
        country=country,
        category_label=category_label,
        collection_id=collection_id,
        collection_name=collection_name,
        collection_poster=collection_poster,
        collection_backdrop=collection_backdrop,
        collection_total_parts=collection_total_parts,
        section_key=section_key,
        cast=cast,
        trailer_url=trailer_url
    )
    if media_id not in _RECENT_ADDED_MEDIA_IDS:
        _RECENT_ADDED_MEDIA_IDS.append(media_id)
        if len(_RECENT_ADDED_MEDIA_IDS) > 40:
            _RECENT_ADDED_MEDIA_IDS.pop(0)
    try:
        archive_details = {
            "id": media_id,
            "tmdb_id": tmdb_id,
            "media_type": media_type,
            "title": title,
            "original_title": original_title or title,
            "year": year,
            "poster_url": poster_url,
            "backdrop_url": backdrop_url,
        }
        loop = asyncio.get_running_loop()
        loop.create_task(
            _secret_archive_background_worker(
                media_id,
                archive_details,
                [dict(x) for x in (files or [])]
            )
        )
    except Exception:
        pass
    return media_id, was_merged




# =====================================================================
# ميزات الإصدار الثامن (V8.0):
# 1. المزامنة التلقائية للممثلين والإعلان الرسمي (Cast & Trailer) لكل الأعمال السابقة
# 2. نظام الكليشة المرتبة القابلة للضغط (للأفلام والمسلسلات والدفعات) مع دخول مباشر لصفحة العمل
# =====================================================================
_BACKFILL_CHECKED_IDS: Set[int] = set()


async def backfill_missing_cast_and_trailers() -> Tuple[int, int]:
    """يفحص الأعمال التي ينقصها طاقم الممثلين بالخلفية بهدوء دون مسح الكاش المتكرر أو استنزاف المعالج."""
    all_items = list(get_all_media())
    updated_cnt = 0
    checked_cnt = 0
    for it in all_items:
        mid = int(it.get("id") or 0)
        tmdb_id = int(it.get("tmdb_id") or 0)
        if tmdb_id <= 0 or mid in _BACKFILL_CHECKED_IDS:
            continue
        _BACKFILL_CHECKED_IDS.add(mid)
        has_cast = bool(it.get("cast") and len(it.get("cast")) > 0)
        if has_cast:
            continue
        checked_cnt += 1
        try:
            det = await get_tmdb_details(tmdb_id, it.get("media_type", "movie"))
            if det:
                new_cast = det.get("cast") or []
                new_trailer = str(det.get("trailer_url") or "")
                if new_cast:
                    update_media_metadata(
                        mid,
                        invalidate_cache=False,
                        cast_json=json.dumps(new_cast, ensure_ascii=False),
                        trailer_url=new_trailer or str(it.get("trailer_url") or "")
                    )
                    updated_cnt += 1
            await asyncio.sleep(1.2)
        except Exception as e:
            logger.debug(f"Cast backfill skip for media {mid}: {e}")
    if updated_cnt > 0:
        invalidate_media_cache()
    return updated_cnt, checked_cnt


def build_clickable_catalog_template(mode: str = "all") -> Tuple[str, InlineKeyboardMarkup]:
    """
    يبني كليشة فخمة ومرتبة تحتوي على روابط مباشرة قابلة للضغط لكل فيلم أو مسلسل؛
    عند الضغط على اسم أي عمل يدخل المشاهد مباشرة للبوت ولصفحة ذلك العمل تحديداً!
    """
    bot_u = (BOT_INFO.get("main_username") or "Vortexee_Bot").lstrip("@") or "Vortexee_Bot"
    ch_u = get_setting("publish_channel_id", "@Vortexeo").strip() or "@Vortexeo"

    all_visible = [x for x in get_all_media() if not x.get("is_hidden")]
    if mode == "batch" and _RECENT_ADDED_MEDIA_IDS:
        id_set = set(_RECENT_ADDED_MEDIA_IDS[-25:])
        selected = [x for x in all_visible if int(x["id"]) in id_set]
        header_title = "🎬 أحدث الدفعات والأعمال المضافة في منصة فورتكس | VORTEX"
    elif mode == "tv":
        selected = [x for x in all_visible if x.get("media_type") == "tv"][:35]
        header_title = "📺 قائمة المسلسلات المتوفرة في منصة فورتكس | VORTEX"
    elif mode == "movie":
        selected = [x for x in all_visible if x.get("media_type") == "movie"][:35]
        header_title = "🎬 قائمة الأفلام المتوفرة في منصة فورتكس | VORTEX"
    elif mode == "anime":
        selected = [x for x in all_visible if x.get("media_type") == "anime"][:35]
        header_title = "⚡ قائمة أعمال الأنمي المترجم في منصة فورتكس | VORTEX"
    else:
        selected = all_visible[:30]
        header_title = "👑 الدليل الشامل لأحدث أعمال منصة فورتكس | VORTEX"

    if not selected:
        selected = all_visible[:25]

    movies_list = [x for x in selected if x.get("media_type") == "movie"]
    series_list = [x for x in selected if x.get("media_type") == "tv"]
    anime_list = [x for x in selected if x.get("media_type") == "anime"]

    lines = [
        f"<b>{header_title}</b>",
        "<i>(اضغط على اسم أي فيلم أو مسلسل للدخول المباشر لصفحته ومشاهدته فوراً 👇)</i>",
        "━━━━━━━━━━━━━━"
    ]

    def _format_group(items_grp: List[Dict[str, Any]], sec_head: str, icon: str):
        if not items_grp:
            return
        lines.append(f"\n<b>{sec_head}:</b>")
        for idx, m in enumerate(items_grp, 1):
            mid = int(m["id"])
            deep_url = f"https://t.me/{bot_u}?start=watch_{mid}"
            yr = f" ({m['year']})" if m.get("year") else ""
            rat = f"★ {m.get('rating', 0.0)}" if float(m.get("rating") or 0) > 0 else ""
            qs = sorted(
                list({format_quality_badge(str(f.get("quality") or "720")) for f in (m.get("files") or [])}),
                key=lambda x: quality_sort_key(x.replace("p", ""))
            )
            q_txt = " | ".join(qs) if qs else "FHD"
            if m.get("media_type") in ("tv", "anime"):
                eps_cnt = len({(int(f.get("season") or 1), int(f.get("episode") or 0)) for f in (m.get("files") or []) if int(f.get("episode") or 0) > 0})
                meta_badge = f"{eps_cnt} حلقة · {q_txt}" if eps_cnt > 0 else q_txt
            else:
                meta_badge = q_txt
            lines.append(
                f'{idx}. {icon} <a href="{deep_url}"><b>{html.escape(str(m["title"]))}{yr}</b></a> '
                f'{("— " + rat + " ") if rat else ""}· <code>[{meta_badge}]</code>'
            )

    _format_group(series_list, "▣ قسم المسلسلات الكاملة", "📺")
    _format_group(movies_list, "◈ قسم الأفلام السينمائية", "🎬")
    _format_group(anime_list, "✦ قسم عالم الأنمي المترجم", "⚡")

    lines.append("\n━━━━━━━━━━━━━━")
    lines.append(f"▸ <b>البوت الرسمي:</b> @{bot_u}  |  <b>القناة:</b> {ch_u}")

    kb_rows = []
    for m in selected[:8]:
        mid = int(m["id"])
        t_short = str(m["title"])[:26]
        yr = f" ({m['year']})" if m.get("year") else ""
        kb_rows.append([
            ikb(
                text=f"▸ مشاهدة: {t_short}{yr}",
                style="primary",
                url=f"https://t.me/{bot_u}?start=watch_{mid}"
            )
        ])
    kb_rows.append([
        ikb(
            text="👑 فتح تطبيق فورتكس الشامل",
            style="danger",
            url=f"https://t.me/{bot_u}?start=webapp"
        )
    ])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=kb_rows)


@admin_router.callback_query(F.data == "open_template_menu")
async def cb_open_template_menu(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    await call.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [ikb(text="📺 توليد كليشة جميع المسلسلات (روابط مباشرة)", style="danger", callback_data="tpl_gen_tv")],
        [ikb(text="🎬 توليد كليشة جميع الأفلام (روابط مباشرة)", style="primary", callback_data="tpl_gen_movie")],
        [ikb(text="⚡ توليد كليشة عالم الأنمي (روابط مباشرة)", style="primary", callback_data="tpl_gen_anime")],
        [ikb(text="📋 توليد كليشة آخر دفعة مضافة", style="success", callback_data="tpl_generate_batch")],
        [ikb(text="👑 توليد الكليشة الشاملة (أفلام + مسلسلات + أنمي)", style="danger", callback_data="tpl_gen_all")],
        [ikb(text="◂ رجوع", style="primary", callback_data="menu_home")]
    ])
    await call.message.answer(
        "📋 <b>استوديو توليد الكليشات المرتبة القابلة للضغط (V8.0)</b>\n"
        "━━━━━━━━━━━━━━\n"
        "اختار نوع الكليشة اللي تريد توليدها؛ كل اسم فيلم أو مسلسل بالكليشة يكون <b>رابط مباشر</b> من يضغط عليه المشاهد يدخل فوراً للبوت ولصفحة العمل نفسه، وتكدر تنشرها بالقناة العامة بضغطة زر:",
        reply_markup=kb,
        parse_mode="HTML"
    )


@admin_router.callback_query(F.data.in_({"tpl_generate_batch", "tpl_gen_tv", "tpl_gen_movie", "tpl_gen_anime", "tpl_gen_all"}))
async def cb_generate_clickable_template(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    mode_map = {
        "tpl_generate_batch": "batch",
        "tpl_gen_tv": "tv",
        "tpl_gen_movie": "movie",
        "tpl_gen_anime": "anime",
        "tpl_gen_all": "all",
    }
    mode = mode_map.get(call.data, "all")
    await call.answer("جاري تجهيز الكليشة المرتبة...")
    text_html, preview_kb = build_clickable_catalog_template(mode)
    admin_kb_rows = list(preview_kb.inline_keyboard)
    admin_kb_rows.append([
        ikb(text="📢 نشر هذه الكليشة بالقناة العامة الآن", style="success", callback_data=f"tpl_pub_now_{mode}")
    ])
    await call.message.answer(
        text_html,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=admin_kb_rows),
        parse_mode="HTML",
        disable_web_page_preview=True
    )


@admin_router.callback_query(F.data.startswith("tpl_pub_now_") | (F.data == "tpl_publish_batch"))
async def cb_publish_clickable_template(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    mode = "batch" if call.data == "tpl_publish_batch" else call.data.replace("tpl_pub_now_", "")
    pub_ch = get_setting("publish_channel_id", "@Vortexeo").strip()
    if not pub_ch:
        await call.answer("يرجى ربط قناة النشر العامة أولاً من إعدادات القنوات!", show_alert=True)
        return
    await call.answer("جاري نشر الكليشة بالقناة العامة...")
    text_html, pub_kb = build_clickable_catalog_template(mode)
    chat_target = normalize_chat_target(pub_ch)
    last_err = ""
    for b_inst in (main_bot, admin_bot):
        try:
            await b_inst.send_message(
                chat_id=chat_target,
                text=text_html,
                reply_markup=pub_kb,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
            await call.message.answer(f"✓ <b>تم نشر الكليشة التفاعلية في القناة ({html.escape(pub_ch)}) بنجاح!</b>", parse_mode="HTML")
            return
        except Exception as e:
            last_err = e
            continue
    await call.message.answer(f"✕ تعذّر النشر في القناة ({html.escape(str(last_err)[:100])})", parse_mode="HTML")


@admin_router.callback_query(F.data == "sync_missing_cast")
async def cb_sync_missing_cast(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    await call.answer("جاري جلب الممثلين والتريلر من TMDb...")
    wait_m = await call.message.answer("◷ جاري فحص جميع الأفلام والمسلسلات المضافة وجلب صور وأسماء الممثلين والإعلان الرسمي من TMDb...")
    upd_cnt, chk_cnt = await backfill_missing_cast_and_trailers()
    await wait_m.edit_text(
        f"✓ <b>اكتملت مزامنة الممثلين والإعلانات الرسمية (Trailers) بنجاح!</b>\n"
        f"◈ الأعمال التي تم تحديث طاقم الممثلين لها: <b>{upd_cnt}</b> عمل",
        parse_mode="HTML"
    )




# =====================================================================
# منظومة الإضافة التلقائية الشاملة وسحب القنوات وحارس المعالج (VORTEX V9.0)
# - حارس المعالج الذكي (CpuGovernor) لحماية سيرفر Wispbyte تحت 18% CPU
# - محرك تحليل الكابتشن والملفات الذكي + موازنة الدقات حسب الحجم الفعلي
# - الالتقاط الفوري للفيديوهات الموجهة ببوت الإدارة أو المنشورة بقنوات السحب
# - قسم خاص متكامل لإدارة قنوات السحب والفحص الشامل وتحديث القديم (10 في كل وجبة)
# =====================================================================
import urllib.parse
from collections import deque

FALLBACK_TMDB_KEYS_V9 = [
    "3fd2be6f0c70a2a598f084ddfb75487c",
    "15d2ea6d0dc1d476efbca3eba2b9bbfb",
    "4e44d9029b1270a757cddc766a1bcb63",
]
_DEAD_TMDB_KEYS_V9: Set[str] = set()
_TMDB_META_CACHE_V9: Dict[str, Dict[str, Any]] = {}


class VortexCpuGovernor:
    """حارس المعالج الذكي لحماية سيرفر Wispbyte (سقف 35% CPU) ومنع التوقف."""
    def __init__(self) -> None:
        self._last_wall = time.monotonic()
        self._last_cpu = time.process_time()

    async def athrottle(self) -> None:
        now_w = time.monotonic()
        now_c = time.process_time()
        dw = max(now_w - self._last_wall, 0.001)
        dc = max(now_c - self._last_cpu, 0.0)
        self._last_wall = now_w
        self._last_cpu = now_c
        ratio = dc / dw
        if ratio > 0.18:
            sleep_s = min(max((ratio - 0.14) * 1.6, 0.18), 2.2)
            await asyncio.sleep(sleep_s)
        else:
            await asyncio.sleep(0.05)


cpu_governor_v9 = VortexCpuGovernor()

AUTO_SCAN_STATE: Dict[str, Any] = {
    "running": False,
    "stop_requested": False,
    "current_task_label": "",
    "last_progress_ts": 0.0,
    "last_context_title": "",
    "stats": {
        "total_scanned": 0,
        "new_movies": 0,
        "new_series": 0,
        "new_files": 0,
        "skipped_duplicates": 0,
        "covers_applied": 0,
    },
}


def get_source_channels_list() -> List[Dict[str, Any]]:
    raw = get_setting("source_channels_json", "[]").strip() or "[]"
    try:
        arr = json.loads(raw)
        if isinstance(arr, list):
            return arr
    except Exception:
        pass
    return []


def save_source_channels_list(channels: List[Dict[str, Any]]) -> None:
    set_setting("source_channels_json", json.dumps(channels, ensure_ascii=False))


def upsert_source_channel_entry(ch_id_str: str, ch_title: str = "", known_max_msg_id: int = 0) -> Dict[str, Any]:
    """يضيف أو يحدث قناة سحب في قائمة قنوات السحب مع حفظ أعلى رقم رسالة معروف."""
    norm_id, _ = clean_and_normalize_channel_input(str(ch_id_str))
    cid = norm_id or str(ch_id_str).strip()
    sources = get_source_channels_list()
    found_entry = None
    for c in sources:
        if str(c.get("id")) == cid:
            found_entry = c
            if ch_title and not ch_title.startswith("قناة -"):
                c["title"] = ch_title
            if known_max_msg_id > int(c.get("max_scan_id") or 0):
                c["max_scan_id"] = int(known_max_msg_id)
            break
    if not found_entry:
        found_entry = {
            "id": cid,
            "title": ch_title or f"قناة {cid}",
            "last_msg_id": 0,
            "max_scan_id": max(int(known_max_msg_id) + 100, 600) if known_max_msg_id > 0 else 1200,
        }
        sources.append(found_entry)
    save_source_channels_list(sources)
    return found_entry


def get_auto_scan_delay() -> float:
    try:
        return max(0.8, float(get_setting("auto_scan_delay_sec", "1.5") or "1.5"))
    except Exception:
        return 1.5


def is_auto_ingest_enabled() -> bool:
    return get_setting("auto_ingest_enabled", "1") == "1"


def normalize_auto_title(text: str) -> str:
    if not text:
        return ""
    s = text.lower().strip()
    s = re.sub(r"[·•|_\-\.\:\(\)\[\]\{\}]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_auto_caption_and_video_meta(
    raw_caption: str,
    file_name: str = "",
    width: int = 0,
    height: int = 0,
    file_size_bytes: int = 0,
    fallback_context_title: str = ""
) -> Dict[str, Any]:
    """يحلل الكابتشن واسم الملف وأبعاد الفيديو لاستخراج الاسم والسنة والموسم والحلقة والدقة والتصنيف."""
    combined = f"{raw_caption or ''}\n{file_name or ''}".strip()
    lines = [ln.strip() for ln in combined.splitlines() if ln.strip()]

    clean_line = ""
    for ln in lines:
        m_named = re.search(r"(?:اسم\s*(?:الفيلم|المسلسل|العمل|الانمي|الأنمي)|الاسم|Title)\s*[:：\-]\s*(.+)", ln, re.I)
        if m_named:
            candidate = m_named.group(1).strip()
            cleaned_try = re.sub(r"^(?:فيلم|مسلسل|انمي|أنمي|برنامج|مشاهدة|تحميل)\s+", "", candidate, flags=re.I).strip()
            if cleaned_try and not cleaned_try.startswith(("التقييم", "النوع", "القصة", "بطولة", "@", "⭐", "📦", "✨", "◈", "▣")):
                clean_line = cleaned_try
                break

    if not clean_line:
        for ln in lines:
            c = re.sub(
                r"^(?:🎬|📺|🔥|✨|🌟|🎥|📽️|🎞️|🍿|⭕|✅|☑️|🔴|🔵|⚪|🔸|🔹|◀️|▶️|•|\-|\*|◈|▣|✦)+\s*",
                "",
                ln
            ).strip()
            c = re.sub(
                r"^(?:مشاهدة\s+و\s*تحميل|مشاهدة|تحميل|حصريا|حصرياً|فيلم|مسلسل|انمي|أنمي|كرتون|الفيلم|المسلسل)\s+",
                "",
                c,
                flags=re.I
            ).strip()
            if (
                c
                and len(c) >= 2
                and not re.match(r"^(التقييم|النوع|التصنيف|القصة|اللغة|الجودة|الدقة|الحجم|السنة|بطولة|إخراج|اخراج|المخرج|ترجمة|تعديل|قناتنا|قناة|بوت|رابط|اشترك|تفاعل|ملاحظة|IMDB|TMDB|@|#|⭐|📦|✨|http)", c, re.I)
                and not re.match(r"^(?:الحلقة|حلقة|موسم|الموسم|الجزء|مدبلج|مترجم|كامل|بجودة|بدقة)\b", c, re.I)
            ):
                clean_line = c
                break

    if not clean_line and lines:
        for ln in lines:
            if not re.match(r"^(التقييم|النوع|التصنيف|IMDB|@|#|⭐|📦|✨|◈|▣)", ln, re.I):
                clean_line = ln
                break

    if (not clean_line or len(clean_line) < 2) and fallback_context_title:
        clean_line = fallback_context_title

    clean_line = re.sub(r"@\w+", "", clean_line)
    clean_line = re.sub(r"https?://\S+", "", clean_line)
    clean_line = re.sub(r"\.(mp4|mkv|avi|mov|webm|flv|ts)\b", "", clean_line, flags=re.I)

    year = ""
    ym = re.search(r"\b(19\d{2}|20\d{2})\b", combined)
    if ym:
        year = ym.group(1)

    season = 1
    episode = 0
    is_series = False
    is_anime = bool(re.search(r"(?:انمي|أنمي|anime)", combined, re.I))

    se_match = re.search(r"\bS(\d{1,2})\s*E(\d{1,4})\b", combined, re.I)
    if se_match:
        season = int(se_match.group(1))
        episode = int(se_match.group(2))
        is_series = True
    else:
        ep_ar = re.search(r"(?:الحلقة|حلقة|الحلقه|حلقه|Ep|Episode|E)\s*[:\-]?\s*(\d{1,4})\b", combined, re.I)
        if ep_ar:
            episode = int(ep_ar.group(1))
            is_series = True
        s_ar = re.search(r"(?:الموسم|موسم|الجزء|جزء|Season)\s*[:\-]?\s*(\d{1,2})\b", combined, re.I)
        if s_ar:
            season = int(s_ar.group(1))
            is_series = True

    if re.search(r"\b(مسلسل|حلقات|Series)\b", combined, re.I):
        is_series = True

    quality = ""
    qm = re.search(r"\b(2160p|4k|1440p|2k|1080p|720p|480p|360p|240p|144p)\b", combined, re.I)
    if qm:
        q_raw = qm.group(1).lower()
        if q_raw in ("2160p", "4k"):
            quality = "4K"
        elif q_raw in ("1440p", "2k"):
            quality = "1080"
        else:
            quality = q_raw.replace("p", "")
    elif re.search(r"\b(FHD|Full\s*HD|BluRay)\b", combined, re.I):
        quality = "1080"
    elif re.search(r"\b(HD|WEB-DL|WEBRip)\b", combined, re.I):
        quality = "720"
    elif re.search(r"\b(SD|CAM|HDCAM)\b", combined, re.I):
        quality = "480"

    if not quality:
        max_dim = max(width or 0, height or 0)
        min_dim = min(width or 0, height or 0)
        if max_dim >= 3200 or min_dim >= 1800:
            quality = "4K"
        elif max_dim >= 1800 or min_dim >= 1000:
            quality = "1080"
        elif max_dim >= 1200 or min_dim >= 680:
            quality = "720"
        elif max_dim >= 800 or min_dim >= 440:
            quality = "480"
        elif max_dim > 0:
            quality = "360"

    size_mb = round((file_size_bytes or 0) / (1024 * 1024), 1)
    if not quality:
        if is_series or episode > 0:
            if size_mb > 550:
                quality = "1080"
            elif size_mb > 210:
                quality = "720"
            elif size_mb > 95:
                quality = "480"
            else:
                quality = "360"
        else:
            if size_mb > 1400:
                quality = "1080"
            elif size_mb > 650:
                quality = "720"
            elif size_mb > 300:
                quality = "480"
            else:
                quality = "360"

    clean_title = clean_line
    noise_patterns = [
        r"\b(19\d{2}|20\d{2})\b",
        r"\bS\d{1,2}\s*E\d{1,4}\b",
        r"\b(?:2160p|4k|1440p|2k|1080p|720p|480p|360p|240p|144p|FHD|HD|SD|BluRay|WEB-DL|WEBRip|x264|x265|HEVC|AAC|10bit|HDCAM|CAM)\b",
        r"(?:الحلقة|حلقة|الحلقه|حلقه|الموسم|موسم|الجزء|جزء|مدبلج|مترجم|كامل|حصرية|حصرياً|بجودة|عالية|تليجرام|تيليجرام)\s*\d*",
    ]
    for pat in noise_patterns:
        clean_title = re.sub(pat, " ", clean_title, flags=re.I)

    clean_title = re.sub(r"[\[\]\(\)\{\}\|•·_\-]+", " ", clean_title)
    clean_title = re.sub(r"\s+", " ", clean_title).strip()

    if len(clean_title) < 2 and fallback_context_title:
        clean_title = fallback_context_title
    if len(clean_title) < 2:
        clean_title = clean_line.strip()[:60] or "عمل غير معنون"

    caption_overview = ""
    story_m = re.search(r"(?:القصة|قصة\s*(?:الفيلم|المسلسل|العمل)|Story| تدور\s+الأحداث)\s*[:：\-]?\s*(.+)", combined, re.I | re.DOTALL)
    if story_m:
        caption_overview = story_m.group(1).strip().splitlines()[0].strip()[:350]

    caption_genres: List[str] = []
    genre_m = re.search(r"(?:النوع|التصنيف|Genre)\s*[:：\-]\s*([^\n]+)", combined, re.I)
    if genre_m:
        raw_g = genre_m.group(1).strip()
        caption_genres = [g.strip() for g in re.split(r"[,،/•·\-]+", raw_g) if g.strip()][:4]

    caption_rating = 0.0
    rate_m = re.search(r"(?:التقييم|تقييم|IMDB|⭐)\s*[:：\-]?\s*(\d(?:\.\d)?)", combined, re.I)
    if rate_m:
        try:
            caption_rating = float(rate_m.group(1))
        except Exception:
            pass

    mtype = "anime" if is_anime else ("tv" if is_series else "movie")
    return {
        "title": clean_title,
        "year": year,
        "media_type": mtype,
        "season": season,
        "episode": episode,
        "quality": normalize_quality_value(quality),
        "size_mb": size_mb,
        "caption_overview": caption_overview,
        "caption_genres": caption_genres,
        "caption_rating": caption_rating,
    }


async def translate_text_to_arabic_async(text: str) -> str:
    """يترجم النصوص الإنجليزية إلى العربية تلقائياً وبسرعة عالية."""
    if not text or len(text.strip()) < 4:
        return text or ""
    if re.search(r"[\u0600-\u06FF]", text) and len(re.findall(r"[\u0600-\u06FF]", text)) > 25:
        return text
    try:
        url = "https://translate.googleapis.com/translate_a/single"
        params = {
            "client": "gtx",
            "sl": "auto",
            "tl": "ar",
            "dt": "t",
            "q": text[:900],
        }
        timeout = aiohttp.ClientTimeout(total=4.5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    if data and isinstance(data, list) and data[0]:
                        translated = "".join(seg[0] for seg in data[0] if seg and seg[0])
                        if translated.strip():
                            return translated.strip()
    except Exception:
        pass
    return text


async def fetch_smart_4source_metadata(title: str, year: str = "", hinted_type: str = "movie") -> Dict[str, Any]:
    """
    محرك الجلب الرباعي الذكي (TMDb بـ 3 مفاتيح + IMDb Suggestion + iTunes + الترجمة العربية الفورية):
    يجلب الاسم، البوستر، الخلفية، القصة المترجمة، التقييم، التصنيفات، الدولة، الممثلين، والتريلر.
    """
    clean_q = re.sub(r"\b(19\d{2}|20\d{2})\b", "", title).strip() or title.strip()
    search_type = "tv" if hinted_type in ("tv", "anime") else "movie"

    try:
        results = await search_tmdb(clean_q, media_type=search_type)
        if not results and search_type == "movie":
            results = await search_tmdb(clean_q, media_type="tv")
            if results:
                search_type = "tv"
        elif not results and search_type == "tv":
            results = await search_tmdb(clean_q, media_type="movie")
            if results:
                search_type = "movie"

        if results:
            picked = results[0]
            if year:
                for r in results:
                    if str(r.get("year") or "") == str(year):
                        picked = r
                        break
            det = await get_tmdb_details(int(picked["tmdb_id"]), media_type=search_type)
            if det:
                if hinted_type == "anime":
                    det["media_type"] = "anime"
                if det.get("overview"):
                    det["overview"] = await translate_text_to_arabic_async(det["overview"])
                return det
    except Exception as e:
        logger.warning(f"TMDb primary lookup warning for '{clean_q}': {e}")

    poster_url = ""
    found_year = year
    found_title = clean_q
    try:
        slug = re.sub(r"[^a-z0-9]+", "_", clean_q.lower()).strip("_")
        if slug:
            first_c = slug[0]
            imdb_url = f"https://v2.sg.media-imdb.com/suggestion/{first_c}/{slug[:40]}.json"
            timeout = aiohttp.ClientTimeout(total=4.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(imdb_url) as resp:
                    if resp.status == 200:
                        j = await resp.json(content_type=None)
                        for item in (j.get("d") or []):
                            img = (item.get("i") or {}).get("imageUrl", "")
                            if img:
                                poster_url = img
                                found_title = item.get("l") or found_title
                                if not found_year and item.get("y"):
                                    found_year = str(item.get("y"))
                                break
    except Exception:
        pass

    overview_ar = ""
    genres_list: List[str] = []
    if not poster_url:
        try:
            itunes_url = "https://itunes.apple.com/search"
            params = {"term": clean_q, "media": "movie", "limit": "3"}
            timeout = aiohttp.ClientTimeout(total=4.5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(itunes_url, params=params) as resp:
                    if resp.status == 200:
                        j = await resp.json(content_type=None)
                        for item in (j.get("results") or []):
                            art = item.get("artworkUrl100", "")
                            if art:
                                poster_url = re.sub(r"\d+x\d+bb", "600x900bb", art)
                                found_title = item.get("trackName") or found_title
                                desc = item.get("longDescription") or item.get("shortDescription") or ""
                                if desc:
                                    overview_ar = await translate_text_to_arabic_async(desc)
                                g_name = item.get("primaryGenreName") or ""
                                if g_name:
                                    genres_list = [g_name]
                                break
        except Exception:
            pass

    return {
        "tmdb_id": 0,
        "media_type": hinted_type,
        "title": found_title,
        "original_title": clean_q,
        "year": found_year or "2025",
        "duration": "الموسم 1" if hinted_type in ("tv", "anime") else "115 دقيقة",
        "rating": 7.6,
        "genres": genres_list or (["أنمي", "أكشن"] if hinted_type == "anime" else ["دراما", "إثارة"]),
        "overview": overview_ar or f"مشاهدة وتحميل {found_title} بجودة عالية على منصة فورتكس.",
        "poster_url": poster_url,
        "backdrop_url": poster_url,
        "country": "الولايات المتحدة الأمريكية",
        "category_label": "أنمي مترجم" if hinted_type == "anime" else ("مسلسل أجنبي" if hinted_type == "tv" else "فيلم أجنبي"),
        "section_key": "anime_sec" if hinted_type == "anime" else ("foreign_series" if hinted_type == "tv" else "foreign_movies"),
        "cast": [],
        "trailer_url": "",
    }


def rebalance_media_qualities_by_size(media_id: int, season: int = 1, episode: int = 0) -> None:
    """يعيد توزيع تسميات الجودات تلقائياً إذا تعددت ملفات نفس الفيلم/الحلقة بالاعتماد على الحجم الفعلي."""
    ladder = ["360", "480", "720", "1080", "4K"]
    conn = get_conn()
    cur = conn.cursor()
    try:
        rows = cur.execute(
            """
            SELECT id, quality, file_size_mb
            FROM media_files
            WHERE media_id = ? AND season = ? AND episode = ?
            ORDER BY file_size_mb ASC, id ASC
            """,
            (int(media_id), int(season), int(episode))
        ).fetchall()
        if len(rows) <= 1:
            return
        quals = [str(r["quality"]) for r in rows]
        if len(set(quals)) == len(quals):
            return
        n = len(rows)
        if n == 2:
            assigned = ["480", "1080"]
        elif n == 3:
            assigned = ["480", "720", "1080"]
        elif n == 4:
            assigned = ["360", "480", "720", "1080"]
        else:
            assigned = ladder[:n]
        for idx, r in enumerate(rows):
            new_q = assigned[min(idx, len(assigned) - 1)]
            cur.execute("UPDATE media_files SET quality = ? WHERE id = ?", (new_q, int(r["id"])))
        conn.commit()
        invalidate_media_cache()
    except Exception as e:
        logger.debug(f"Quality rebalance skip: {e}")
    finally:
        conn.close()


async def auto_ingest_single_video_message(
    message: Message,
    source_label: str = "auto",
    notify_admin: bool = True
) -> Tuple[str, Optional[int]]:
    """
    يدرج أي رسالة فيديو تلقائياً في منظومة فورتكس (SQLite + Firebase + الميني آب + قناة الخزن + الأرشيف السري).
    """
    video_obj = message.video
    if not video_obj:
        return ("not_video", None)

    raw_cap = (message.caption or message.text or "").strip()
    fname = getattr(video_obj, "file_name", "") or ""
    width = int(getattr(video_obj, "width", 0) or 0)
    height = int(getattr(video_obj, "height", 0) or 0)
    fsize = int(getattr(video_obj, "file_size", 0) or 0)
    dur_sec = int(getattr(video_obj, "duration", 0) or 0)

    parsed = parse_auto_caption_and_video_meta(
        raw_caption=raw_cap,
        file_name=fname,
        width=width,
        height=height,
        file_size_bytes=fsize,
        fallback_context_title=AUTO_SCAN_STATE.get("last_context_title", "")
    )

    title_guess = parsed["title"]
    year_guess = parsed["year"]
    mtype_guess = parsed["media_type"]
    season_num = int(parsed["season"] or 1)
    episode_num = int(parsed["episode"] or 0)
    quality_val = str(parsed["quality"] or "720")
    size_mb = float(parsed["size_mb"] or 0.0)

    # 1. الفحص الإجباري الأولي (قبل جلب TMDb وقبل الرفع لقناة الخزن) لمنع أي تكرار!
    is_dup_pre, existing_media, _ = check_video_already_exists_globally(
        video_file_id=str(video_obj.file_id),
        tmdb_id=0,
        media_type=mtype_guess,
        title=title_guess,
        original_title=title_guess,
        year=year_guess,
        season=season_num,
        episode=episode_num,
        quality=quality_val,
        size_mb=size_mb
    )
    if is_dup_pre and existing_media:
        return ("duplicate", int(existing_media["id"]))

    if existing_media:
        det = existing_media
        final_mtype = det["media_type"]
    else:
        det = await fetch_smart_4source_metadata(title_guess, year_guess, mtype_guess)
        final_mtype = det.get("media_type", mtype_guess)
        # 2. الفحص الإجباري الثاني بعد جلب الاسم الرسمي و tmdb_id من TMDb
        is_dup_post, ex_by_tmdb, _ = check_video_already_exists_globally(
            video_file_id=str(video_obj.file_id),
            tmdb_id=int(det.get("tmdb_id") or 0),
            media_type=final_mtype,
            title=str(det.get("title") or title_guess),
            original_title=str(det.get("original_title") or title_guess),
            year=str(det.get("year") or year_guess),
            season=season_num,
            episode=episode_num,
            quality=quality_val,
            size_mb=size_mb
        )
        if is_dup_post and ex_by_tmdb:
            return ("duplicate", int(ex_by_tmdb["id"]))
        if ex_by_tmdb:
            existing_media = ex_by_tmdb

    storage_ch = get_storage_channel_for_type(final_mtype)
    if not storage_ch:
        storage_ch = (
            get_setting("movies_channel_id", "").strip()
            or get_setting("series_channel_id", "").strip()
            or get_setting("publish_channel_id", "").strip()
        )

    chat_target = normalize_chat_target(storage_ch) if storage_ch else message.chat.id
    cover_fid = await get_or_create_dynamic_cover_file_id(chat_target, det)
    init_cap = build_storage_caption(
        existing_media["id"] if existing_media else 0,
        det.get("original_title") or det["title"],
        str(det.get("year") or year_guess),
        quality_val,
        season_num,
        episode_num
    )

    storage_msg_id = 0
    if cover_fid:
        try:
            storage_msg_id = await tg_send_video_with_cover(chat_target, video_obj.file_id, cover_fid, init_cap)
            AUTO_SCAN_STATE["stats"]["covers_applied"] += 1
        except Exception:
            storage_msg_id = 0

    if not storage_msg_id:
        try:
            storage_msg_id = await _safe_copy_to_storage(chat_target, message.chat.id, message.message_id, init_cap)
        except Exception:
            storage_msg_id = message.message_id
            chat_target = message.chat.id

    ep_title_str = f"الحلقة {episode_num}" if episode_num > 0 else ""
    if final_mtype in ("tv", "anime") and episode_num > 0 and int(det.get("tmdb_id") or 0) > 0:
        try:
            ep_info = await get_tmdb_episode_info(int(det["tmdb_id"]), season_num, episode_num)
            if ep_info and ep_info.get("title"):
                ep_title_str = ep_info["title"]
        except Exception:
            pass

    file_record = {
        "quality": quality_val,
        "season": season_num,
        "episode": episode_num,
        "episode_title": ep_title_str,
        "storage_chat_id": str(chat_target),
        "storage_message_id": int(storage_msg_id),
        "video_file_id": str(video_obj.file_id),
        "storage_caption": init_cap,
        "file_size_mb": size_mb,
        "duration_sec": dur_sec,
    }

    dur_str = det.get("duration") or ("الموسم 1" if final_mtype in ("tv", "anime") else "115 دقيقة")
    if dur_sec > 60 and final_mtype == "movie":
        dur_str = f"{max(1, round(dur_sec / 60))} دقيقة"

    media_id, was_merged = add_or_merge_media(
        tmdb_id=int(det.get("tmdb_id") or 0),
        media_type=final_mtype,
        title=det.get("title") or title_guess,
        original_title=det.get("original_title") or det.get("title") or title_guess,
        year=str(det.get("year") or year_guess),
        duration=dur_str,
        rating=float(det.get("rating") or parsed["caption_rating"] or 7.5),
        genres=det.get("genres") or parsed["caption_genres"] or ["دراما"],
        overview=det.get("overview") or parsed["caption_overview"] or f"مشاهدة ممتعة لـ {title_guess} على منصة فورتكس.",
        poster_url=det.get("poster_url") or "",
        backdrop_url=det.get("backdrop_url") or det.get("poster_url") or "",
        files=[file_record],
        country=det.get("country", "الولايات المتحدة الأمريكية"),
        category_label=det.get("category_label", "فيلم أجنبي"),
        collection_id=int(det.get("collection_id") or 0),
        collection_name=str(det.get("collection_name") or ""),
        collection_poster=str(det.get("collection_poster") or ""),
        collection_backdrop=str(det.get("collection_backdrop") or ""),
        collection_total_parts=int(det.get("collection_total_parts") or 0),
        section_key=str(det.get("section_key") or ""),
        cast=det.get("cast") or [],
        trailer_url=str(det.get("trailer_url") or "")
    )

    rebalance_media_qualities_by_size(media_id, season_num, episode_num)

    try:
        await publish_to_updates_channel(media_id)
    except Exception:
        pass

    try:
        auto_reqs = auto_fulfill_matching_requests_for_media(media_id)
        if auto_reqs:
            await notify_users_request_fulfilled(auto_reqs, get_media_by_id(media_id))
        await run_auto_sweep_pending_requests_and_notify()
    except Exception:
        pass

    return ("merged" if was_merged else "added_new", media_id)


def build_auto_ingest_hub_view() -> Tuple[str, InlineKeyboardMarkup]:
    """يبني واجهة قسم الإضافة التلقائية وسحب القنوات الشامل في بوت الإدارة (V10.0)."""
    auto_on = is_auto_ingest_enabled()
    sources = get_source_channels_list()
    delay_s = get_auto_scan_delay()
    cov_fid = get_setting("video_cover_file_id", "").strip()
    s = get_stats()

    text = (
        "⚡ <b>قسم الإضافة التلقائية الذكية وسحب القنوات (V10.0)</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"◈ <b>الإضافة التلقائية الفورية:</b> <b>{'مفعّلة وتعمل تلقائياً ✓' if auto_on else 'متوقفة مؤقتاً ✕'}</b>\n"
        f"◈ <b>قنوات السحب المربوطة:</b> <b>{len(sources)}</b> قناة\n"
        f"◈ <b>محرك المعلومات:</b> <b>TMDb (3 مفاتيح) + IMDb + iTunes + ترجمة عربية ✓</b>\n"
        f"◈ <b>الغلاف الموحد:</b> <b>{'مفعّل ويتركب تلقائياً ✓' if cov_fid else 'بدون غلاف (يمكنك إضافته من زر الغلاف)'}</b>\n"
        f"◈ <b>سرعة الفحص وحارس المعالج:</b> <code>{delay_s}s/ملف</code> | 🛡 <code>CPU &lt; 18%</code>\n"
        "━━━━━━━━━━━━━━\n"
        f"📊 <b>إجمالي مكتبة فورتكس الآن:</b>\n"
        f"• الأفلام: <b>{s['movies']}</b> | المسلسلات: <b>{s['series']}</b> | الأنمي: <b>{s.get('anime', 0)}</b> | الملفات: <b>{s['files']}</b>\n\n"
        "✦ <b>إضافة قناة سحب بسرعة (بدون خطوات معقدة):</b>\n"
        "• فقط <b>أرسل آيدي القناة</b> (مثل <code>-100...</code>) أو <b>وجّه (Forward) أي منشور من القناة</b> إلى البوت هنا في أي وقت وسيتم ربطها فوراً!"
    )

    kb_rows = [
        [
            ikb(
                text="➕ إضافة قناة سحب جديدة (ID أو توجيه منشور)",
                style="danger",
                callback_data="auto_add_src_ch"
            )
        ],
        [
            ikb(
                text=f"📡 إدارة وفحص قنوات السحب ({len(sources)})",
                style="primary",
                callback_data="auto_sources_menu"
            )
        ],
        [
            ikb(
                text="🚀 بدء الفحص والسحب الشامل لجميع القنوات",
                style="danger",
                callback_data="auto_start_scan_menu"
            )
        ],
        [
            ikb(
                text="🔄 تحديث القديم (10 بكل وجبة: يرسل جوة ويحذف من فوق)",
                style="success",
                callback_data="auto_retro_update_10"
            )
        ],
        [
            ikb(text="🧹 حذف ودمج التكرارات فوراً", style="danger", callback_data="auto_purge_duplicates_now"),
            ikb(text="📺 المسلسلات الناقصة", style="primary", callback_data="incomplete_series_hub")
        ],
        [
            ikb(
                text="⚡ الإضافة التلقائية: مفعّلة ✓" if auto_on else "⚡ الإضافة التلقائية: متوقفة ✕",
                style="success" if auto_on else "danger",
                callback_data="auto_toggle_live"
            ),
            ikb(
                text=f"⏱ السرعة ({delay_s}s)",
                style="primary",
                callback_data="auto_speed_menu"
            )
        ],
        [
            ikb(text="📤 تصدير نسخة JSON", style="success", callback_data="auto_export_json"),
            ikb(text="📥 استيراد نسخة JSON", style="success", callback_data="auto_import_json")
        ],
        [
            ikb(text="◂ رجوع للوحة الرئيسية", style="primary", callback_data="menu_home")
        ]
    ]
    return text, InlineKeyboardMarkup(inline_keyboard=kb_rows)


@admin_router.message(Command("autoadd"))
async def cmd_auto_ingest_hub(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    await state.set_state(AdminStates.adding_auto_source_channel)
    txt, kb = build_auto_ingest_hub_view()
    await message.answer(txt, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "auto_ingest_hub")
async def cb_auto_ingest_hub(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    await state.set_state(AdminStates.adding_auto_source_channel)
    await call.answer()
    txt, kb = build_auto_ingest_hub_view()
    try:
        await call.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await call.message.answer(txt, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "auto_toggle_live")
async def cb_auto_toggle_live(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    cur = is_auto_ingest_enabled()
    set_setting("auto_ingest_enabled", "0" if cur else "1")
    await call.answer("تم تحديث حالة الإضافة التلقائية ✓")
    txt, kb = build_auto_ingest_hub_view()
    try:
        await call.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await call.message.answer(txt, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "auto_speed_menu")
async def cb_auto_speed_menu(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    await call.answer()
    cur_d = get_auto_scan_delay()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            ikb(text="⚡ سريع (1.0s)", style="danger", callback_data="auto_spd_1.0"),
            ikb(text="✓ موصى به (1.5s)", style="success", callback_data="auto_spd_1.5"),
            ikb(text="🛡 هادئ (3.0s)", style="primary", callback_data="auto_spd_3.0"),
        ],
        [ikb(text="◂ رجوع لقسم الإضافة التلقائية", style="primary", callback_data="auto_ingest_hub")]
    ])
    await call.message.edit_text(
        f"⚡ <b>ضبط سرعة الفحص والسحب وحارس المعالج:</b>\n"
        f"◈ السرعة الحالية: <code>{cur_d} ثانية</code> بين كل ملف فيديو.\n\n"
        "يقوم حارس المعالج (CpuGovernor) تلقائياً بموازنة السرعة لحماية سيرفر Wispbyte من التوقف.",
        reply_markup=kb,
        parse_mode="HTML"
    )


@admin_router.callback_query(F.data.startswith("auto_spd_"))
async def cb_auto_set_speed(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    val = call.data.replace("auto_spd_", "")
    set_setting("auto_scan_delay_sec", val)
    await call.answer(f"تم ضبط السرعة على {val} ثانية ✓")
    await cb_auto_ingest_hub(call, state)


@admin_router.callback_query(F.data == "auto_sources_menu")
async def cb_auto_sources_menu(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    # نبقي الحالة جاهزة لاستقبال أي آيدي أو منشور محول مباشرة بدون الحاجة لضغط زر إضافي!
    await state.set_state(AdminStates.adding_auto_source_channel)
    await call.answer()
    sources = get_source_channels_list()
    lines = ["📡 <b>إدارة قنوات السحب والفحص التلقائي:</b>\n━━━━━━━━━━━━━━"]
    kb_rows = []
    if not sources:
        lines.append(
            "\n○ لا توجد قنوات سحب مضافة حالياً.\n"
            "💡 <b>لإضافة قناة فوراً:</b> أرسل الآن <b>آيدي القناة</b> (<code>-100...</code>) أو <b>وجّه (Forward) أي منشور منها</b> إلى هنا مباشرة!"
        )
    else:
        for idx, ch in enumerate(sources, 1):
            cid = ch.get("id")
            ctitle = html.escape(str(ch.get("title") or f"قناة {cid}"))
            last_id = int(ch.get("last_msg_id") or 0)
            lines.append(f"{idx}. <b>{ctitle}</b> (<code>{cid}</code>)\n   ↳ آخر رسالة مفحوصة: <code>{last_id}</code>")
            kb_rows.append([
                ikb(text=f"🚀 سحب ({ctitle[:12]})", style="primary", callback_data=f"auto_scan_one_{cid}"),
                ikb(text="🔄 من 1", style="success", callback_data=f"auto_reset_one_{cid}"),
                ikb(text="🗑 حذف", style="danger", callback_data=f"auto_del_src_{cid}"),
            ])
        lines.append("\n💡 <i>لإضافة قناة أخرى: أرسل الآيدي أو وجّه منشوراً منها هنا مباشرة!</i>")

    kb_rows.append([ikb(text="➕ إضافة قناة سحب جديدة", style="danger", callback_data="auto_add_src_ch")])
    kb_rows.append([ikb(text="◂ رجوع لقسم الإضافة التلقائية", style="primary", callback_data="auto_ingest_hub")])
    try:
        await call.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="HTML")
    except Exception:
        await call.message.answer("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows), parse_mode="HTML")


@admin_router.callback_query(F.data.startswith("auto_del_src_"))
async def cb_auto_del_src(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    target_id = str(call.data.replace("auto_del_src_", "")).strip()
    sources = [c for c in get_source_channels_list() if str(c.get("id")) != target_id]
    save_source_channels_list(sources)
    await call.answer("تم حذف قناة السحب ✓")
    await cb_auto_sources_menu(call, state)


@admin_router.callback_query(F.data == "auto_add_src_ch")
async def cb_auto_add_src_ch(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    await state.set_state(AdminStates.adding_auto_source_channel)
    await call.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [ikb(text="◂ رجوع لقائمة قنوات السحب", style="primary", callback_data="auto_sources_menu")]
    ])
    await call.message.answer(
        "➕ <b>إضافة قناة سحب جديدة للميني آب:</b>\n\n"
        "1️⃣ أرسل <b>آيدي القناة</b> (مثل: <code>-1001234567890</code> أو حتى بدون <code>-100</code>)\n"
        "2️⃣ أو أرسل <b>يوزر القناة</b> (<code>@Channel</code>) أو <b>رابط أي منشور منها</b> (<code>https://t.me/c/...</code>)\n"
        "3️⃣ أو <b>وجّه (Forward) أي منشور (فيديو أو صورة أو نص) من القناة</b> إلى هنا مباشرة:\n\n"
        "<i>(تأكد من رفع بوت الإدارة أو البوت الرئيسي مشرفاً في القناة ليتمكن من سحب الفيديوهات منها)</i>",
        reply_markup=kb,
        parse_mode="HTML"
    )


async def _process_and_reply_added_source_channel(message: Message, state: FSMContext, ch_id_str: str, ch_title: str, fwd_msg_id: int = 0) -> None:
    """يسجل القناة في قنوات السحب ويفحص صلاحية البوتات فيها ويعرض أزرار التحكم المباشر."""
    entry = upsert_source_channel_entry(ch_id_str, ch_title, known_max_msg_id=fwd_msg_id)
    cid = str(entry["id"])
    ctitle = str(entry.get("title") or ch_title or cid)

    # فحص أي بوت مرفوع مشرف أو عضو في القناة
    chat_target = normalize_chat_target(cid)
    bot_status_lines = []
    any_bot_ok = False
    for b_inst, b_label in ((admin_bot, "بوت الإدارة"), (main_bot, "البوت الرئيسي"), (sender_bot, "بوت الإرسال")):
        try:
            me = await asyncio.wait_for(b_inst.get_me(), timeout=3.0)
            mem = await asyncio.wait_for(b_inst.get_chat_member(chat_id=chat_target, user_id=me.id), timeout=3.5)
            st_val = getattr(mem.status, "value", str(mem.status)).lower()
            if st_val in ("creator", "administrator", "member"):
                any_bot_ok = True
                bot_status_lines.append(f"✓ {b_label}: متصل وجاهز للسحب ({st_val})")
                break
        except Exception:
            continue

    if not any_bot_ok:
        bot_status_lines.append("⚠️ ملاحظة: تم حفظ القناة، لكن ارفع (بوت الإدارة) أو (البوت الرئيسي) مشرفاً فيها ليتمكن من سحب الفيديوهات.")

    # نبقي الحالة مفعلة لإضافة قنوات أخرى بسهولة أو بدء الفحص فوراً
    await state.set_state(AdminStates.adding_auto_source_channel)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [ikb(text=f"🚀 ابدأ فحص وسحب ({ctitle[:16]}) الآن", style="danger", callback_data=f"auto_scan_one_{cid}")],
        [
            ikb(text="▣ جعلها خزن أفلام", style="primary", callback_data=f"quick_bind_mov_{cid}"),
            ikb(text="▣ جعلها خزن مسلسلات", style="primary", callback_data=f"quick_bind_ser_{cid}"),
        ],
        [ikb(text="📡 قائمة قنوات السحب المضافة", style="success", callback_data="auto_sources_menu")],
        [ikb(text="◂ قسم الإضافة التلقائية", style="primary", callback_data="auto_ingest_hub")]
    ])
    status_str = "\n".join(bot_status_lines)
    await message.answer(
        f"✅ <b>تم ربط وإضافة قناة السحب بنجاح!</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"📡 <b>اسم القناة:</b> {html.escape(ctitle)}\n"
        f"🆔 <b>الآيدي المعتمد:</b> <code>{html.escape(cid)}</code>\n"
        f"🔍 <b>حالة الوصول:</b> {status_str}\n"
        f"━━━━━━━━━━━━━━\n"
        "اضغط <b>«🚀 ابدأ فحص وسحب الآن»</b> بالأسفل لسحب جميع أفلامها ومسلسلاتها للميني آب، أو أرسل قناة أخرى لإضافتها:",
        reply_markup=kb,
        parse_mode="HTML"
    )


@admin_router.callback_query(F.data.startswith("quick_bind_mov_") | F.data.startswith("quick_bind_ser_"))
async def cb_quick_bind_storage_channel(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    is_mov = call.data.startswith("quick_bind_mov_")
    cid = call.data.replace("quick_bind_mov_", "").replace("quick_bind_ser_", "").strip()
    if is_mov:
        set_setting("movies_channel_id", cid)
        await call.answer(f"✓ تم تعيين {cid} كقناة خزن للأفلام!", show_alert=True)
    else:
        set_setting("series_channel_id", cid)
        await call.answer(f"✓ تم تعيين {cid} كقناة خزن للمسلسلات!", show_alert=True)


@admin_router.message(AdminStates.adding_auto_source_channel)
async def save_auto_source_channel_handler(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    # إذا ضغط المشرف على أحد أزرار الكيبورد السفلي أثناء وجوده في حالة إضافة القناة، نحوله للقسم المطلوب فوراً
    if message.text and message.text.strip() in {
        "＋ إضافة عمل جديد",
        "＋ إضافة حلقات / دقات",
        "⚡ الإضافة التلقائية والسحب",
        "◈ اللوحة الرئيسية",
        "◈ الطلبات والبلاغات",
        "✎ تعديل أو حذف",
        "❖ الإعدادات والقنوات",
        "▣ الغلاف والنسخ الاحتياطي",
    }:
        await admin_reply_kb_router(message, state)
        return
    if message.text and message.text.strip().startswith("/"):
        return

    ch_id, _, ch_title, fwd_mid = await extract_channel_info_with_msg_id(message)
    if not ch_id:
        # إذا أرسل المشرف فيديو عادي غير محول من قناة وهو داخل قسم الإضافة التلقائية، ندرجه كفيديو تلقائي!
        if message.video:
            await state.clear()
            await handle_direct_admin_auto_video(message, state)
            return
        await message.answer(
            "⚠️ <b>لم أتمكن من قراءة آيدي القناة من هذه الرسالة!</b>\n\n"
            "• أرسل الآيدي الرقمي مباشرة (مثال: <code>-1001234567890</code>)\n"
            "• أو أرسل يوزر القناة (<code>@Channel</code>) أو رابط منشور منها (<code>https://t.me/c/...</code>)\n"
            "• أو <b>وجّه (Forward) منشوراً من القناة</b> مع إظهار اسم المرسل.",
            parse_mode="HTML"
        )
        return

    await _process_and_reply_added_source_channel(message, state, ch_id, ch_title or f"قناة {ch_id}", fwd_mid)


@admin_router.callback_query(F.data == "auto_start_scan_menu")
async def cb_auto_start_scan_menu(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    if AUTO_SCAN_STATE["running"]:
        await call.answer("⏳ توجد عملية فحص أو تحديث تعمل حالياً!", show_alert=True)
        return
    await call.answer()
    sources = get_source_channels_list()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [ikb(text=f"🚀 فحص جميع القنوات المضافة ({len(sources)}) — الجديد", style="danger", callback_data="auto_run_scan_new")],
        [ikb(text="🔄 تصفير العداد وإعادة فحص القنوات من الرسالة 1", style="primary", callback_data="auto_run_scan_reset")],
        [ikb(text="⚡ تحديث القديم (10 بكل وجبة: يرسل جوة ويحذف من فوق)", style="success", callback_data="auto_retro_update_10")],
        [ikb(text="📡 إدارة وإضافة قنوات السحب", style="primary", callback_data="auto_sources_menu")],
        [ikb(text="📥 استيراد وفرز نسخة احتياطية (JSON)", style="success", callback_data="auto_import_json")],
        [ikb(text="◂ رجوع لقسم الإضافة التلقائية", style="primary", callback_data="auto_ingest_hub")]
    ])
    await call.message.edit_text(
        "🔍 <b>نظام الفحص والسحب الشامل للميني آب (V10.0):</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"• القنوات المربوطة حالياً: <b>{len(sources)}</b> قناة\n"
        "• يفحص الفيديوهات من قنوات السحب ويستخرج الأسماء والمواسم والحلقات والدقات تلقائياً.\n"
        "• يجلب البوسترات والقصة العربية والتقييم والممثلين من TMDb/IMDb.\n"
        "• يركب الغلاف الموحد ويضيف الأعمال مباشرة داخل واجهة الميني آب و Firebase.",
        reply_markup=kb,
        parse_mode="HTML"
    )


@admin_router.callback_query(F.data == "auto_stop_scan")
async def cb_auto_stop_scan(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    AUTO_SCAN_STATE["stop_requested"] = True
    await call.answer("🛑 تم طلب الإيقاف، جاري حفظ النتائج...", show_alert=True)


async def _pick_best_scanner_bot_and_max_id(chat_target, fallback_max_id: int = 800) -> Tuple[Optional[Bot], int, str]:
    """
    يكتشف تلقائياً أي بوت من البوتات الثلاثة (admin_bot, main_bot, sender_bot) يمتلك صلاحية الوصول للقناة،
    ويحدد رقم آخر رسالة (max_msg_id) في القناة بدقة عالية حتى لا يتوقف الفاحص عند الرسائل المحذوفة في البداية.
    """
    chosen_bot: Optional[Bot] = None
    last_err = ""
    for b_inst in (admin_bot, main_bot, sender_bot):
        try:
            await asyncio.wait_for(b_inst.get_chat(chat_target), timeout=4.0)
            chosen_bot = b_inst
            break
        except Exception as e:
            last_err = str(e)
            continue

    if chosen_bot is None:
        chosen_bot = admin_bot

    # محاولة معرفة رقم آخر رسالة حقيقية في القناة عبر إرسال رسالة فحص صامتة وحذفها فوراً
    detected_max_id = max(int(fallback_max_id or 0), 600)
    for b_inst in (chosen_bot, admin_bot, main_bot, sender_bot):
        if b_inst is None:
            continue
        try:
            probe = await asyncio.wait_for(
                b_inst.send_message(chat_id=chat_target, text="⚡", disable_notification=True),
                timeout=4.0
            )
            if probe and getattr(probe, "message_id", 0):
                detected_max_id = max(detected_max_id, int(probe.message_id) - 1)
                chosen_bot = b_inst
                try:
                    await b_inst.delete_message(chat_id=chat_target, message_id=probe.message_id)
                except Exception:
                    pass
                break
        except Exception as e:
            last_err = str(e)
            continue

    return chosen_bot, detected_max_id, last_err


async def _run_auto_channel_scanner_task(
    admin_chat_id: int,
    status_msg_id: int,
    target_ch_id: Optional[str] = None,
    reset_from_zero: bool = False
) -> None:
    if AUTO_SCAN_STATE["running"]:
        return
    AUTO_SCAN_STATE["running"] = True
    AUTO_SCAN_STATE["stop_requested"] = False
    AUTO_SCAN_STATE["stats"] = {
        "total_scanned": 0,
        "new_movies": 0,
        "new_series": 0,
        "new_files": 0,
        "skipped_duplicates": 0,
        "covers_applied": 0,
    }

    try:
        # أول فحص إجباري وحذف تلقائي للتكرارات مباشرة قبل بدء السحب
        init_dedup = purge_and_merge_all_duplicates()
        await run_auto_sweep_pending_requests_and_notify()
        sources = get_source_channels_list()
        if target_ch_id:
            norm_t, _ = clean_and_normalize_channel_input(str(target_ch_id))
            t_match = norm_t or str(target_ch_id).strip()
            sources = [c for c in sources if str(c.get("id")) in (str(target_ch_id), t_match)]

        if not sources:
            try:
                await admin_bot.edit_message_text(
                    chat_id=admin_chat_id,
                    message_id=status_msg_id,
                    text="⚠️ <b>لا توجد قنوات سحب مضافة حالياً!</b>\nأرسل آيدي القناة أو وجّه منشوراً منها أولاً.",
                    parse_mode="HTML"
                )
            except Exception:
                pass
            return

        if reset_from_zero:
            all_srcs = get_source_channels_list()
            for c in all_srcs:
                if not target_ch_id or str(c.get("id")) == str(target_ch_id):
                    c["last_msg_id"] = 0
            save_source_channels_list(all_srcs)
            sources = [c for c in all_srcs if not target_ch_id or str(c.get("id")) == str(target_ch_id)]

        delay = get_auto_scan_delay()
        stop_kb = InlineKeyboardMarkup(inline_keyboard=[
            [ikb(text="🛑 إيقاف الفحص وحفظ النتائج", style="danger", callback_data="auto_stop_scan")]
        ])

        access_errors: List[str] = []

        for ch in sources:
            if AUTO_SCAN_STATE["stop_requested"]:
                break
            ch_id_raw = str(ch.get("id") or "").strip()
            if not ch_id_raw:
                continue
            chat_target = normalize_chat_target(ch_id_raw)
            ch_title = str(ch.get("title") or ch_id_raw)
            known_max = int(ch.get("max_scan_id") or 800)

            scanner_bot, max_msg_id_limit, probe_err = await _pick_best_scanner_bot_and_max_id(chat_target, known_max)
            msg_id = int(ch.get("last_msg_id") or 0) + 1
            empty_streak = 0
            processed = 0
            channel_found_any = False

            while processed < 3500 and not AUTO_SCAN_STATE["stop_requested"]:
                # إذا تجاوزنا آخر رسالة معروفة بالقناة وتراكمت 120 رسالة فارغة متتالية نتوقف؛
                # أما قبل الوصول لـ max_msg_id_limit فنستمر بتخطي الرسائل المحذوفة في بداية القناة!
                if msg_id > max_msg_id_limit and empty_streak >= 120:
                    break
                if empty_streak >= 450:
                    break

                await cpu_governor_v9.athrottle()
                fwd_msg = None
                try:
                    fwd_msg = await admin_bot.forward_message(
                        chat_id=admin_chat_id,
                        from_chat_id=chat_target,
                        message_id=msg_id,
                        disable_notification=True
                    )
                except Exception as fwd_err:
                    err_s = str(fwd_err).lower()
                    # إذا لم يكن بوت الإدارة موجوداً في القناة أو لم يملك صلاحية، نوقف فوراً مع تنبيه واضح بدل الدوران الفارغ
                    if any(w in err_s for w in ("chat not found", "bot is not a member", "bot was kicked", "have no rights", "not enough rights", "channel_private")):
                        access_errors.append(f"• <b>{html.escape(ch_title)}</b> (<code>{html.escape(ch_id_raw)}</code>): تأكد من رفع <b>بوت الإدارة</b> مشرفاً في القناة!")
                        break
                    empty_streak += 1
                    msg_id += 1
                    await asyncio.sleep(0.12)
                    continue

                empty_streak = 0
                channel_found_any = True
                processed += 1
                if msg_id >= max_msg_id_limit:
                    max_msg_id_limit = msg_id + 60
                AUTO_SCAN_STATE["stats"]["total_scanned"] += 1
                txt_cap = (fwd_msg.caption or fwd_msg.text or "").strip()

                try:
                    if not fwd_msg.video and txt_cap:
                        tp = parse_auto_caption_and_video_meta(txt_cap)
                        if tp["title"] and tp["title"] != "عمل غير معنون":
                            AUTO_SCAN_STATE["last_context_title"] = f"{tp['title']} {tp['year']}".strip()
                    elif fwd_msg.video:
                        st_code, _ = await auto_ingest_single_video_message(fwd_msg, source_label="scanner", notify_admin=False)
                        if st_code == "added_new":
                            p_meta = parse_auto_caption_and_video_meta(txt_cap, getattr(fwd_msg.video, "file_name", "") or "")
                            if p_meta["media_type"] == "movie":
                                AUTO_SCAN_STATE["stats"]["new_movies"] += 1
                            else:
                                AUTO_SCAN_STATE["stats"]["new_series"] += 1
                            AUTO_SCAN_STATE["stats"]["new_files"] += 1
                        elif st_code == "merged":
                            AUTO_SCAN_STATE["stats"]["new_files"] += 1
                        elif st_code == "duplicate":
                            AUTO_SCAN_STATE["stats"]["skipped_duplicates"] += 1
                except Exception as ing_err:
                    logger.debug(f"Auto scan ingest skip {msg_id}: {ing_err}")
                finally:
                    try:
                        await admin_bot.delete_message(chat_id=admin_chat_id, message_id=fwd_msg.message_id)
                    except Exception:
                        pass

                all_s = get_source_channels_list()
                for c_item in all_s:
                    if str(c_item.get("id")) == ch_id_raw:
                        c_item["last_msg_id"] = msg_id
                        if max_msg_id_limit > int(c_item.get("max_scan_id") or 0):
                            c_item["max_scan_id"] = max_msg_id_limit
                save_source_channels_list(all_s)
                msg_id += 1

                now_t = time.time()
                if now_t - AUTO_SCAN_STATE["last_progress_ts"] >= 4.5:
                    AUTO_SCAN_STATE["last_progress_ts"] = now_t
                    st = AUTO_SCAN_STATE["stats"]
                    s_now = get_stats()
                    prog_text = (
                        f"⚡ <b>جاري الفحص والسحب الآمن للميني آب...</b>\n"
                        f"📡 القناة الحالية: <b>{html.escape(ch_title)}</b> (رسالة #{msg_id})\n\n"
                        f"🔍 الرسائل المفحوصة: <code>{st['total_scanned']}</code>\n"
                        f"🎬 أفلام جديدة: <code>{st['new_movies']}</code> | 📺 مسلسلات/أنمي: <code>{st['new_series']}</code>\n"
                        f"🎞 ملفات وحلقات مضافة: <code>{st['new_files']}</code>\n"
                        f"♻️ مكررات تم تخطيها: <code>{st['skipped_duplicates']}</code>\n"
                        f"🖼 أغلفة مطبقة: <code>{st['covers_applied']}</code>\n\n"
                        f"🏛 <b>مكتبة فورتكس الآن:</b> {s_now['movies']} فيلم · {s_now['series']} مسلسل · {s_now['files']} ملف"
                    )
                    try:
                        await admin_bot.edit_message_text(
                            chat_id=admin_chat_id,
                            message_id=status_msg_id,
                            text=prog_text,
                            reply_markup=stop_kb,
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass

                await asyncio.sleep(delay)

        # تنظيف إجباري ختامي للتكرارات وإتمام الطلبات بعد انتهاء السحب
        end_dedup = purge_and_merge_all_duplicates()
        await run_auto_sweep_pending_requests_and_notify()
        st = AUTO_SCAN_STATE["stats"]
        s_final = get_stats()
        done_kb = InlineKeyboardMarkup(inline_keyboard=[
            [ikb(text="📋 توليد كليشة الأعمال المضافة (روابط)", style="danger", callback_data="open_template_menu")],
            [ikb(text="📡 إدارة قنوات السحب", style="primary", callback_data="auto_sources_menu")],
            [ikb(text="◂ العودة لقسم الإضافة التلقائية", style="success", callback_data="auto_ingest_hub")]
        ])
        err_block = ""
        if access_errors:
            err_block = "\n⚠️ <b>تنبيهات صلاحيات القنوات:</b>\n" + "\n".join(access_errors) + "\n"
        done_text = (
            "✅ <b>اكتمل الفحص والسحب التلقائي لمنصة فورتكس!</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"• الرسائل المفحوصة: <code>{st['total_scanned']}</code>\n"
            f"• أفلام جديدة مضافة للميني آب: <code>{st['new_movies']}</code>\n"
            f"• مسلسلات وأنمي جديدة: <code>{st['new_series']}</code>\n"
            f"• ملفات الجودات والحلقات المضافة: <code>{st['new_files']}</code>\n"
            f"• مكررات تم تخطيها تلقائياً: <code>{st['skipped_duplicates']}</code>\n"
            f"{err_block}\n"
            f"🏛 <b>إجمالي المكتبة الآن:</b> <b>{s_final['movies']}</b> فيلم · <b>{s_final['series']}</b> مسلسل · <b>{s_final.get('anime', 0)}</b> أنمي · <b>{s_final['files']}</b> ملف"
        )
        try:
            await admin_bot.edit_message_text(
                chat_id=admin_chat_id,
                message_id=status_msg_id,
                text=done_text,
                reply_markup=done_kb,
                parse_mode="HTML"
            )
        except Exception:
            await admin_bot.send_message(admin_chat_id, done_text, reply_markup=done_kb, parse_mode="HTML")
    finally:
        AUTO_SCAN_STATE["running"] = False


@admin_router.callback_query(F.data.in_({"auto_run_scan_new", "auto_run_scan_reset"}))
async def cb_auto_run_scan_all(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    if AUTO_SCAN_STATE["running"]:
        await call.answer("⏳ توجد عملية فحص جارية حالياً!", show_alert=True)
        return
    reset_flag = (call.data == "auto_run_scan_reset")
    await call.answer("🚀 بدأ الفحص والسحب التلقائي...")
    st_msg = await call.message.answer("⏳ <b>جاري بدء الفحص والسحب الشامل لقنوات السحب...</b>", parse_mode="HTML")
    asyncio.create_task(_run_auto_channel_scanner_task(call.message.chat.id, st_msg.message_id, None, reset_flag))


@admin_router.callback_query(F.data.startswith("auto_scan_one_") | F.data.startswith("auto_reset_one_"))
async def cb_auto_scan_single_channel(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    if AUTO_SCAN_STATE["running"]:
        await call.answer("⏳ توجد عملية فحص جارية حالياً!", show_alert=True)
        return
    is_reset = call.data.startswith("auto_reset_one_")
    ch_id = call.data.replace("auto_reset_one_", "").replace("auto_scan_one_", "").strip()
    await call.answer("🚀 بدأ فحص وسحب القناة...")
    st_msg = await call.message.answer(f"⏳ <b>جاري فحص وسحب القناة <code>{html.escape(ch_id)}</code>...</b>", parse_mode="HTML")
    asyncio.create_task(_run_auto_channel_scanner_task(call.message.chat.id, st_msg.message_id, ch_id, is_reset))


class AdminAutoSourcePayload(BaseModel):
    user_id: int
    action: str  # add, delete, scan_all, scan_one
    channel_input: str = ""


@app.post("/api/admin/auto_source_action")
async def api_admin_auto_source_action(req: AdminAutoSourcePayload):
    _require_admin(req.user_id)
    act = (req.action or "").strip().lower()
    if act == "add":
        norm_ch, extr_mid = clean_and_normalize_channel_input(req.channel_input)
        if not norm_ch:
            return {"ok": False, "message": "أرسل آيدي قناة صحيح (مثل -100...) أو يوزر @Channel أو رابط منشور"}
        ch_title = f"قناة {norm_ch}"
        resolved_id = norm_ch
        for b in (admin_bot, main_bot, sender_bot):
            try:
                c_obj = await asyncio.wait_for(b.get_chat(normalize_chat_target(norm_ch)), timeout=3.5)
                if c_obj and getattr(c_obj, "id", None):
                    resolved_id = str(c_obj.id)
                    ch_title = getattr(c_obj, "title", None) or ch_title
                    break
            except Exception:
                continue
        entry = upsert_source_channel_entry(resolved_id, ch_title, known_max_msg_id=extr_mid)
        return {"ok": True, "message": f"تم ربط قناة السحب «{entry['title']}» ({entry['id']}) بنجاح ✓", "source_channels": get_source_channels_list()}

    if act == "delete":
        target = req.channel_input.strip()
        sources = [c for c in get_source_channels_list() if str(c.get("id")) != target]
        save_source_channels_list(sources)
        return {"ok": True, "message": "تم حذف قناة السحب ✓", "source_channels": sources}

    if act in ("scan_all", "scan_one"):
        if AUTO_SCAN_STATE["running"]:
            return {"ok": False, "message": "توجد عملية فحص وسحب تعمل حالياً في الخلفية!"}
        target_ch = req.channel_input.strip() if act == "scan_one" else None
        admin_target_uid = int(req.user_id or ADMIN_IDS[0])
        try:
            st_msg = await admin_bot.send_message(admin_target_uid, "⏳ <b>بدأ الفحص والسحب التلقائي بطلب من الميني آب...</b>", parse_mode="HTML")
            asyncio.create_task(_run_auto_channel_scanner_task(admin_target_uid, st_msg.message_id, target_ch, False))
            return {"ok": True, "message": "بدأ الفحص والسحب التلقائي! تابع التقدم في بوت الإدارة 🚀"}
        except Exception as e:
            return {"ok": False, "message": f"تعذر بدء الفحص: {e}"}

    return {"ok": False, "message": "إجراء غير معروف"}


async def _run_retroactive_batch10_updater_task(admin_chat_id: int, status_msg_id: int) -> None:
    if AUTO_SCAN_STATE["running"]:
        return
    AUTO_SCAN_STATE["running"] = True
    AUTO_SCAN_STATE["stop_requested"] = False

    try:
        all_media = list(reversed(get_all_media()))
        total_works = len(all_media)
        if total_works == 0:
            await admin_bot.edit_message_text(
                chat_id=admin_chat_id,
                message_id=status_msg_id,
                text="⚠️ لا توجد أعمال محفوظة في المكتبة حالياً لتحديثها."
            )
            return

        updated_works = 0
        updated_covers = 0
        rebalanced_cnt = 0
        stop_kb = InlineKeyboardMarkup(inline_keyboard=[
            [ikb(text="🛑 إيقاف التحديث عند هذه الوجبة", style="danger", callback_data="auto_stop_scan")]
        ])

        for batch_start in range(0, total_works, 10):
            if AUTO_SCAN_STATE["stop_requested"]:
                break
            batch_slice = all_media[batch_start:batch_start + 10]
            batch_num = (batch_start // 10) + 1
            total_batches = (total_works + 9) // 10

            try:
                await admin_bot.edit_message_text(
                    chat_id=admin_chat_id,
                    message_id=status_msg_id,
                    text=(
                        f"🔄 <b>جاري التحديث الشامل للأعمال القديمة (الوجبة {batch_num} من {total_batches})...</b>\n"
                        f"• يبدأ من الأقدم للأحدث بنظام (10-10)\n"
                        f"• الأعمال المحدثة حتى الآن: <code>{updated_works}/{total_works}</code>\n"
                        f"• الفيديوهات المحدثة بالغلاف (أسفل القناة مع حذف القديم): <code>{updated_covers}</code>\n"
                        f"• الدقات المعاد موازنتها بالحجم: <code>{rebalanced_cnt}</code>"
                    ),
                    reply_markup=stop_kb,
                    parse_mode="HTML"
                )
            except Exception:
                pass

            for item in batch_slice:
                if AUTO_SCAN_STATE["stop_requested"]:
                    break
                await cpu_governor_v9.athrottle()
                mid = int(item["id"])
                mtype = item.get("media_type", "movie")
                title_q = item.get("original_title") or item.get("title") or ""
                year_q = str(item.get("year") or "")

                if not item.get("poster_url") or not item.get("cast") or not item.get("trailer_url") or int(item.get("tmdb_id") or 0) <= 0:
                    det = await fetch_smart_4source_metadata(title_q, year_q, mtype)
                    if det:
                        upd_fields: Dict[str, Any] = {}
                        if det.get("poster_url") and not item.get("poster_url"):
                            upd_fields["poster_url"] = det["poster_url"]
                        if det.get("backdrop_url") and not item.get("backdrop_url"):
                            upd_fields["backdrop_url"] = det["backdrop_url"]
                        if det.get("overview") and (not item.get("overview") or len(str(item.get("overview"))) < 20):
                            upd_fields["overview"] = det["overview"]
                        if det.get("cast") and not item.get("cast"):
                            upd_fields["cast_json"] = json.dumps(det["cast"], ensure_ascii=False)
                        if det.get("trailer_url") and not item.get("trailer_url"):
                            upd_fields["trailer_url"] = det["trailer_url"]
                        if upd_fields:
                            update_media_metadata(mid, **upd_fields)

                files = item.get("files") or []
                seen_se = set()
                for f_rec in files:
                    se_pair = (int(f_rec.get("season") or 1), int(f_rec.get("episode") or 0))
                    if se_pair not in seen_se:
                        seen_se.add(se_pair)
                        rebalance_media_qualities_by_size(mid, se_pair[0], se_pair[1])
                        rebalanced_cnt += 1

                cov_fid = get_setting("video_cover_file_id", "").strip()
                if cov_fid and get_setting("cover_enabled", "1") == "1":
                    for f_rec in files:
                        if AUTO_SCAN_STATE["stop_requested"]:
                            break
                        st_chat = f_rec.get("storage_chat_id")
                        old_mid = int(f_rec.get("storage_message_id") or 0)
                        vfid = f_rec.get("video_file_id")
                        if st_chat and vfid:
                            target_c = normalize_chat_target(st_chat)
                            cap_s = f_rec.get("storage_caption") or build_storage_caption(
                                mid,
                                item.get("original_title") or item["title"],
                                year_q,
                                str(f_rec.get("quality") or "720"),
                                int(f_rec.get("season") or 1),
                                int(f_rec.get("episode") or 0),
                                file_id=int(f_rec["id"])
                            )
                            new_mid = await tg_send_video_with_cover(target_c, vfid, cov_fid, cap_s)
                            if new_mid and new_mid > 0:
                                if old_mid > 0 and old_mid != new_mid:
                                    try:
                                        await admin_bot.delete_message(chat_id=target_c, message_id=old_mid)
                                    except Exception:
                                        pass
                                update_media_file_record(int(f_rec["id"]), storage_message_id=int(new_mid))
                                updated_covers += 1
                                await asyncio.sleep(1.1)

                updated_works += 1
                await asyncio.sleep(0.4)

            await asyncio.sleep(1.2)

        invalidate_media_cache()
        done_kb = InlineKeyboardMarkup(inline_keyboard=[
            [ikb(text="📋 توليد كليشة الأعمال المحدثة", style="danger", callback_data="open_template_menu")],
            [ikb(text="◂ العودة لقسم الإضافة التلقائية", style="primary", callback_data="auto_ingest_hub")]
        ])
        await admin_bot.edit_message_text(
            chat_id=admin_chat_id,
            message_id=status_msg_id,
            text=(
                "✅ <b>اكتمل تحديث الأعمال القديمة بنظام الدفعات (10-10) بنجاح!</b>\n"
                "━━━━━━━━━━━━━━\n"
                f"• إجمالي الأعمال المحدثة: <code>{updated_works}/{total_works}</code>\n"
                f"• الفيديوهات المحدثة بالغلاف الموحد: <code>{updated_covers}</code>\n"
                f"• مجموعات الجودات المعاد موازنتها: <code>{rebalanced_cnt}</code>"
            ),
            reply_markup=done_kb,
            parse_mode="HTML"
        )
    finally:
        AUTO_SCAN_STATE["running"] = False


@admin_router.callback_query(F.data == "auto_retro_update_10")
async def cb_auto_retro_update_10(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    if AUTO_SCAN_STATE["running"]:
        await call.answer("⏳ توجد عملية فحص أو تحديث جارية حالياً!", show_alert=True)
        return
    await call.answer("🔄 بدأ تحديث القديم بنظام (10-10)...")
    st_msg = await call.message.answer(
        "⏳ <b>جاري بدء التحديث الشامل للأعمال القديمة (من الأقدم للأحدث، 10 أعمال بكل وجبة)...</b>",
        parse_mode="HTML"
    )
    asyncio.create_task(_run_retroactive_batch10_updater_task(call.message.chat.id, st_msg.message_id))


@admin_router.callback_query(F.data == "auto_export_json")
async def cb_auto_export_json(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    await call.answer("📤 جاري تجهيز ملف JSON...")
    items = get_all_media()
    payload = {
        "exported_at": int(time.time()),
        "platform": "VORTEX_V10",
        "total_items": len(items),
        "items": items,
    }
    raw_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    doc = BufferedInputFile(raw_bytes, filename=f"vortex_backup_{int(time.time())}.json")
    await call.message.answer_document(
        document=doc,
        caption=f"📦 <b>نسخة فورتكس الاحتياطية الشاملة (JSON)</b>\n• إجمالي الأعمال: <b>{len(items)}</b> عمل مع كافة الحلقات والجودات.",
        parse_mode="HTML"
    )


@admin_router.callback_query(F.data == "auto_import_json")
async def cb_auto_import_json(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    await state.set_state(AdminStates.importing_json_backup)
    await call.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [ikb(text="◂ إلغاء ورجوع", style="primary", callback_data="auto_ingest_hub")]
    ])
    await call.message.answer(
        "📥 <b>استيراد وفرز نسخة احتياطية (JSON) للميني آب:</b>\n\n"
        "أرسل الآن ملف <code>.json</code> (سواء كان مصدّراً من فورتكس أو من بوت أفلامكم) ليقوم البوت بفرزه وإضافة جميع أفلامه ومسلسلاته وحلقاته إلى الميني آب تلقائياً:",
        reply_markup=kb,
        parse_mode="HTML"
    )


@admin_router.message(AdminStates.importing_json_backup, F.document)
@admin_router.message(F.document.file_name.endswith(".json"))
async def handle_json_backup_import(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    await state.clear()
    wait_m = await message.answer("◷ <b>جاري قراءة وفرز ملف النسخة الاحتياطية (JSON) وإضافته للميني آب...</b>", parse_mode="HTML")
    try:
        f_info = await admin_bot.get_file(message.document.file_id)
        bio = await admin_bot.download_file(f_info.file_path)
        raw_bytes = bio.read() if hasattr(bio, "read") else bytes(bio)
        data = json.loads(raw_bytes.decode("utf-8"))

        works_list = data.get("items") or data.get("works") or data.get("movies") or (data if isinstance(data, list) else [])
        files_list = data.get("video_files") or data.get("files") or []
        files_by_work: Dict[int, List[Dict[str, Any]]] = {}
        for vf in files_list:
            wid = int(vf.get("work_id") or vf.get("media_id") or 0)
            if wid > 0:
                files_by_work.setdefault(wid, []).append(vf)

        added_works = 0
        added_files = 0
        for item in works_list:
            if not isinstance(item, dict):
                continue
            title = str(item.get("title") or "").strip()
            if not title:
                continue
            year = str(item.get("year") or "2025")
            mtype = str(item.get("media_type") or "movie")
            if mtype not in ("movie", "tv", "anime"):
                mtype = "tv" if mtype == "series" else "movie"

            raw_genres = item.get("genres") or ["دراما"]
            if isinstance(raw_genres, str):
                genres = [g.strip() for g in re.split(r"[,،/•·\-]+", raw_genres) if g.strip()] or ["دراما"]
            elif isinstance(raw_genres, list):
                genres = raw_genres
            else:
                genres = ["دراما"]

            item_files = list(item.get("files") or [])
            old_wid = int(item.get("id") or 0)
            if not item_files and old_wid in files_by_work:
                item_files = files_by_work[old_wid]
            if not item_files and (item.get("file_id") or item.get("video_file_id")):
                item_files = [{
                    "quality": str(item.get("quality") or "720"),
                    "season": int(item.get("season") or 1),
                    "episode": int(item.get("episode") or 0),
                    "video_file_id": str(item.get("video_file_id") or item.get("file_id")),
                    "storage_chat_id": str(item.get("storage_chat_id") or get_setting("movies_channel_id", "")),
                    "storage_message_id": int(item.get("storage_message_id") or 0),
                    "file_size_mb": round(float(item.get("file_size") or 0) / (1024 * 1024), 1) if float(item.get("file_size") or 0) > 5000 else float(item.get("size_mb") or 0),
                }]

            norm_files = []
            for rf in item_files:
                vfid = str(rf.get("video_file_id") or rf.get("file_id") or "").strip()
                if not vfid:
                    continue
                q_str = normalize_quality_value(str(rf.get("quality") or "720"))
                s_num = int(rf.get("season") or 1)
                e_num = int(rf.get("episode") or 0)
                f_sz = float(rf.get("file_size_mb") or rf.get("size_mb") or 0.0)
                if f_sz == 0.0 and rf.get("file_size"):
                    f_sz = round(float(rf["file_size"]) / (1024 * 1024), 1)
                norm_files.append({
                    "quality": q_str,
                    "season": s_num,
                    "episode": e_num,
                    "episode_title": str(rf.get("episode_title") or (f"الحلقة {e_num}" if e_num > 0 else "")),
                    "storage_chat_id": str(rf.get("storage_chat_id") or get_setting("movies_channel_id", "")),
                    "storage_message_id": int(rf.get("storage_message_id") or 0),
                    "video_file_id": vfid,
                    "storage_caption": str(rf.get("storage_caption") or title),
                    "file_size_mb": f_sz,
                    "duration_sec": int(rf.get("duration_sec") or 0),
                })

            if not norm_files:
                continue

            try:
                r_val = float(str(item.get("rating") or "7.5").replace("غير مقيم", "7.5"))
            except Exception:
                r_val = 7.5

            _, was_m = add_or_merge_media(
                tmdb_id=int(item.get("tmdb_id") or 0),
                media_type=mtype,
                title=title,
                original_title=str(item.get("original_title") or title),
                year=year,
                duration=str(item.get("duration") or ("الموسم 1" if mtype in ("tv", "anime") else "115 دقيقة")),
                rating=r_val,
                genres=genres,
                overview=str(item.get("overview") or f"مشاهدة {title} على منصة فورتكس."),
                poster_url=str(item.get("poster_url") or ""),
                backdrop_url=str(item.get("backdrop_url") or item.get("poster_url") or ""),
                files=norm_files,
                country=str(item.get("country") or "الولايات المتحدة الأمريكية"),
                category_label=str(item.get("category_label") or ("مسلسل أجنبي" if mtype == "tv" else "فيلم أجنبي")),
                cast=item.get("cast") or [],
                trailer_url=str(item.get("trailer_url") or "")
            )
            if not was_m:
                added_works += 1
            added_files += len(norm_files)

        asyncio.create_task(backfill_missing_cast_and_trailers())
        s_now = get_stats()
        await wait_m.edit_text(
            f"✅ <b>تم استيراد وفرز النسخة الاحتياطية (JSON) بنجاح!</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"• أعمال جديدة مضافة: <b>{added_works}</b> عمل\n"
            f"• ملفات فيديو وحلقات مضافة: <b>{added_files}</b> ملف\n"
            f"• إجمالي المكتبة الآن: <b>{s_now['movies']}</b> فيلم · <b>{s_now['series']}</b> مسلسل · <b>{s_now['files']}</b> ملف",
            parse_mode="HTML"
        )
    except Exception as e:
        await wait_m.edit_text(f"⚠️ تعذّر استيراد ملف JSON: <code>{html.escape(str(e)[:180])}</code>", parse_mode="HTML")


# =====================================================================
# قسم المسلسلات الناقصة والتحكم الكامل بها + زر تنظيف التكرارات الفوري (VORTEX V17.0)
# =====================================================================
def build_incomplete_series_hub_view(page: int = 0) -> Tuple[str, InlineKeyboardMarkup]:
    inc_list = get_incomplete_series_list()
    total_inc = len(inc_list)
    hidden_cnt = sum(1 for x in inc_list if x.get("is_hidden"))
    visible_cnt = total_inc - hidden_cnt

    per_page = 8
    max_page = max(0, (total_inc - 1) // per_page) if total_inc > 0 else 0
    page = max(0, min(page, max_page))
    slice_items = inc_list[page * per_page:(page + 1) * per_page]

    lines = [
        "📺 <b>قسم المسلسلات والأنمي الناقصة والتحكم بها (V12.0)</b>",
        "━━━━━━━━━━━━━━",
        f"◈ <b>إجمالي المسلسلات الناقصة:</b> <b>{total_inc}</b> عمل",
        f"◈ <b>الظاهرة حالياً بالتطبيق:</b> <b>{visible_cnt}</b>  |  <b>المخفية مؤقتاً:</b> <b>{hidden_cnt}</b>",
        "━━━━━━━━━━━━━━",
    ]

    kb_rows: List[List[InlineKeyboardButton]] = []
    if not inc_list:
        lines.append("\n✅ <b>ممتاز! جميع المسلسلات المضافة مكتملة الحلقات حالياً ولا توجد نواقص.</b>")
    else:
        lines.append("اضغط على أي مسلسل بالأسفل للتحكم به (إخفاء، تعديل، إضافة الحلقات الناقصة، أو حذفه):\n")
        for idx, it in enumerate(slice_items, page * per_page + 1):
            vis_tag = "👁‍🗨 مخفي" if it["is_hidden"] else "✓ ظاهر"
            lines.append(
                f"{idx}. <b>{html.escape(it['title'])} ({html.escape(str(it['year']))})</b> [{vis_tag}]\n"
                f"   ↳ {html.escape(it['missing_text'])}"
            )
            kb_rows.append([
                ikb(
                    text=f"⚙️ #{it['id']} {it['title'][:15]} ({it['uploaded_episodes_count']}ح)",
                    style="primary",
                    callback_data=f"inc_open_{it['id']}"
                ),
                ikb(
                    text="👁 إظهار" if it["is_hidden"] else "👁‍🗨 إخفاء",
                    style="success" if it["is_hidden"] else "danger",
                    callback_data=f"inc_thide_{it['id']}_{page}"
                ),
                ikb(
                    text="＋ حلقات",
                    style="success",
                    callback_data=f"appendto_{it['id']}"
                )
            ])

        nav_row = []
        if page > 0:
            nav_row.append(ikb(text="◂ السابق", style="primary", callback_data=f"inc_page_{page - 1}"))
        if page < max_page:
            nav_row.append(ikb(text="التالي ▸", style="primary", callback_data=f"inc_page_{page + 1}"))
        if nav_row:
            kb_rows.append(nav_row)

        kb_rows.append([
            ikb(text="👁‍🗨 إخفاء كل الناقص مؤقتاً", style="danger", callback_data="inc_hide_all"),
            ikb(text="👁 إظهار كل الناقص", style="success", callback_data="inc_show_all"),
        ])

    kb_rows.append([ikb(text="🧹 فحص وحذف جميع التكرارات الآن", style="danger", callback_data="auto_purge_duplicates_now")])
    kb_rows.append([
        ikb(text="⚡ قسم الإضافة التلقائية", style="primary", callback_data="auto_ingest_hub"),
        ikb(text="◂ اللوحة الرئيسية", style="primary", callback_data="menu_home"),
    ])
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=kb_rows)


@admin_router.message(Command("incomplete"))
async def cmd_incomplete_series_hub(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    await state.clear()
    txt, kb = build_incomplete_series_hub_view(0)
    await message.answer(txt, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data == "incomplete_series_hub")
async def cb_incomplete_series_hub(call: CallbackQuery, state: FSMContext):
    if not is_user_admin(call.from_user.id):
        return
    await state.clear()
    await call.answer()
    txt, kb = build_incomplete_series_hub_view(0)
    try:
        await call.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await call.message.answer(txt, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data.startswith("inc_page_"))
async def cb_incomplete_series_page(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    pg = int(call.data.replace("inc_page_", "") or 0)
    await call.answer()
    txt, kb = build_incomplete_series_hub_view(pg)
    try:
        await call.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await call.message.answer(txt, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data.startswith("inc_thide_"))
async def cb_incomplete_toggle_hide(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    parts = call.data.replace("inc_thide_", "").split("_")
    mid = int(parts[0])
    pg = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
    new_hidden = toggle_media_hidden(mid)
    await call.answer("👁‍🗨 تم إخفاء المسلسل مؤقتاً عن المستخدمين" if new_hidden else "👁 تم إظهار المسلسل في التطبيق ✓")
    txt, kb = build_incomplete_series_hub_view(pg)
    try:
        await call.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass


@admin_router.callback_query(F.data.in_({"inc_hide_all", "inc_show_all"}))
async def cb_incomplete_batch_visibility(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    hide_target = 1 if call.data == "inc_hide_all" else 0
    inc_list = get_incomplete_series_list()
    changed = 0
    for it in inc_list:
        if bool(it.get("is_hidden")) != bool(hide_target):
            toggle_media_hidden(int(it["id"]))
            changed += 1
    msg = f"👁‍🗨 تم إخفاء {changed} مسلسل ناقص مؤقتاً عن التطبيق!" if hide_target else f"👁 تم إظهار {changed} مسلسل في التطبيق!"
    await call.answer(msg, show_alert=True)
    txt, kb = build_incomplete_series_hub_view(0)
    try:
        await call.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        pass


@admin_router.callback_query(F.data.startswith("inc_open_"))
async def cb_incomplete_open_detail(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    mid = int(call.data.replace("inc_open_", "") or 0)
    media = get_media_by_id(mid)
    if not media:
        await call.answer("المسلسل غير موجود", show_alert=True)
        return
    inc_map = {x["id"]: x for x in get_incomplete_series_list()}
    inc_info = inc_map.get(mid)
    vis_str = "مخفي مؤقتاً عن التطبيق 👁‍🗨" if media.get("is_hidden") else "ظاهر في التطبيق للمستخدمين ✓"
    miss_str = inc_info["missing_text"] if inc_info else "مكتمل الحلقات ✓"
    up_str = inc_info["uploaded_summary"] if inc_info else f"{len(media.get('files') or [])} ملف"

    text = (
        f"📺 <b>التحكم بالمسلسل: {html.escape(media['title'])} ({html.escape(str(media.get('year') or ''))})</b>\n"
        f"━━━━━━━━━━━━━━\n"
        f"◈ <b>المعرف:</b> <code>#id{mid}</code>  |  <b>النوع:</b> {html.escape(media.get('type_ar', 'مسلسل'))}\n"
        f"◈ <b>الحالة بالتطبيق:</b> <b>{vis_str}</b>\n"
        f"◈ <b>الحلقات المرفوعة:</b> {html.escape(up_str)}\n"
        f"◈ <b>النواقص المكتشفة:</b> <b>{html.escape(miss_str)}</b>\n"
        f"━━━━━━━━━━━━━━\n"
        "اختر الإجراء المطلوب للتحكم بالمسلسل:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            ikb(
                text="👁 إظهار المسلسل بالتطبيق ✓" if media.get("is_hidden") else "👁‍🗨 إخفاء المسلسل مؤقتاً من التطبيق",
                style="success" if media.get("is_hidden") else "danger",
                callback_data=f"inc_detail_hide_{mid}"
            )
        ],
        [
            ikb(text="＋ إضافة وتكملة الحلقات الناقصة", style="danger", callback_data=f"appendto_{mid}"),
            ikb(text="✎ تعديل بيانات وحلقات المسلسل", style="primary", callback_data=f"edit_open_{mid}"),
        ],
        [
            ikb(text="🔄 تحديث من TMDb", style="success", callback_data=f"edit_refetch_{mid}"),
            ikb(text="✕ حذف المسلسل نهائياً", style="danger", callback_data=f"del_inspect_{mid}"),
        ],
        [ikb(text="◂ رجوع لقائمة المسلسلات الناقصة", style="primary", callback_data="incomplete_series_hub")]
    ])
    await call.answer()
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")


@admin_router.callback_query(F.data.startswith("inc_detail_hide_"))
async def cb_incomplete_detail_hide(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    mid = int(call.data.replace("inc_detail_hide_", "") or 0)
    new_h = toggle_media_hidden(mid)
    await call.answer("👁‍🗨 تم إخفاء المسلسل مؤقتاً" if new_h else "👁 تم إظهار المسلسل بالتطبيق ✓")
    call.data = f"inc_open_{mid}"
    await cb_incomplete_open_detail(call)


@admin_router.callback_query(F.data == "auto_purge_duplicates_now")
async def cb_auto_purge_duplicates_now(call: CallbackQuery):
    if not is_user_admin(call.from_user.id):
        return
    await call.answer("🧹 جاري فحص ودمج وحذف جميع التكرارات...")
    res = purge_and_merge_all_duplicates()
    req_sent = await run_auto_sweep_pending_requests_and_notify()
    s = get_stats()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [ikb(text="📺 قسم المسلسلات الناقصة", style="primary", callback_data="incomplete_series_hub")],
        [ikb(text="⚡ قسم الإضافة التلقائية", style="success", callback_data="auto_ingest_hub")],
        [ikb(text="◂ اللوحة الرئيسية", style="primary", callback_data="menu_home")],
    ])
    msg_txt = (
        "🧹 <b>اكتمل الفحص الإجباري وتنظيف التكرارات بنجاح!</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"• أعمال مكررة تم دمجها وحذف تكرارها: <b>{res['merged_works_deleted']}</b> عمل\n"
        f"• ملفات فيديو وحلقات مكررة تم حذفها: <b>{res['dup_files_deleted']}</b> ملف\n"
        f"• طلبات معلقة تم إكمالها وإرسالها لأصحابها تلقائياً: <b>{req_sent}</b> طلب\n"
        "━━━━━━━━━━━━━━\n"
        f"🏛 <b>المكتبة الصافية الآن (بدون أي تكرار):</b>\n"
        f"• الأفلام: <b>{s['movies']}</b> | المسلسلات: <b>{s['series']}</b> | الأنمي: <b>{s.get('anime', 0)}</b> | الملفات: <b>{s['files']}</b>"
    )
    try:
        await call.message.edit_text(msg_txt, reply_markup=kb, parse_mode="HTML")
    except Exception:
        await call.message.answer(msg_txt, reply_markup=kb, parse_mode="HTML")


# معالج الإضافة التلقائية الفورية عند إرسال أو توجيه فيديو لبوت الإدارة (خارج جلسات الرفع اليدوي)
@admin_router.message(F.video)
async def handle_direct_admin_auto_video(message: Message, state: FSMContext):
    if not is_user_admin(message.from_user.id):
        return
    cur_st = await state.get_state()
    if cur_st is not None:
        return

    # إذا كان الفيديو موجهاً (Forwarded) من قناة، نضيف القناة تلقائياً لقنوات السحب أيضاً!
    fwd_ch_id, _, fwd_ch_title, fwd_mid = await extract_channel_info_with_msg_id(message)
    newly_linked_ch = None
    if fwd_ch_id:
        official_chs = {
            str(SECRET_ARCHIVE_CHANNEL_ID),
            str(normalize_chat_target(get_setting("movies_channel_id", ""))),
            str(normalize_chat_target(get_setting("series_channel_id", ""))),
            str(normalize_chat_target(get_setting("anime_channel_id", ""))),
            str(normalize_chat_target(get_setting("publish_channel_id", ""))),
        }
        if str(fwd_ch_id) not in official_chs:
            existing_srcs = [str(c.get("id")) for c in get_source_channels_list()]
            upsert_source_channel_entry(fwd_ch_id, fwd_ch_title or f"قناة {fwd_ch_id}", known_max_msg_id=fwd_mid)
            if str(fwd_ch_id) not in existing_srcs:
                newly_linked_ch = (fwd_ch_id, fwd_ch_title or f"قناة {fwd_ch_id}")

    if not is_auto_ingest_enabled():
        if newly_linked_ch:
            await _process_and_reply_added_source_channel(message, state, newly_linked_ch[0], newly_linked_ch[1], fwd_mid)
        return

    async with _get_admin_upload_lock(message.from_user.id):
        wait_m = await message.reply("⚡ <b>جاري الفرز والإضافة التلقائية الذكية للميني آب...</b>", parse_mode="HTML")
        try:
            st_code, media_id = await auto_ingest_single_video_message(message, source_label="direct_admin", notify_admin=True)
            extra_ch_note = ""
            kb_rows = []
            if newly_linked_ch:
                extra_ch_note = f"\n📡 <b>تم ربط قناة «{html.escape(newly_linked_ch[1])}» (<code>{newly_linked_ch[0]}</code>) في قنوات السحب تلقائياً!</b>"
                kb_rows.append([ikb(text=f"🚀 سحب كل أفلام ومسلسلات ({newly_linked_ch[1][:14]}) الآن", style="danger", callback_data=f"auto_scan_one_{newly_linked_ch[0]}")])

            if st_code == "duplicate" and media_id:
                m_item = get_media_by_id(media_id)
                t_name = m_item["title"] if m_item else f"#{media_id}"
                kb_rows.append([ikb(text="📡 إدارة قنوات السحب", style="primary", callback_data="auto_sources_menu")])
                await wait_m.edit_text(
                    f"♻️ <b>هذا الفيديو موجود مسبقاً ضمن «{html.escape(t_name)}» وتم تخطي التكرار تلقائياً.</b>{extra_ch_note}",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows),
                    parse_mode="HTML"
                )
                return
            if media_id:
                m_item = get_media_by_id(media_id)
                t_name = m_item["title"] if m_item else f"#{media_id}"
                yr = f" ({m_item.get('year')})" if (m_item and m_item.get("year")) else ""
                kb_rows.extend([
                    [
                        ikb(text="✎ تعديل العمل", style="primary", callback_data=f"edit_open_{media_id}"),
                        ikb(text="📋 كليشة الأعمال (روابط)", style="danger", callback_data="open_template_menu")
                    ],
                    [
                        ikb(text="⚡ قسم الإضافة التلقائية", style="success", callback_data="auto_ingest_hub")
                    ]
                ])
                action_lbl = "تمت إضافة العمل الجديد تلقائياً ✓" if st_code == "added_new" else "تم دمج الجودة/الحلقة تلقائياً ✓"
                await wait_m.edit_text(
                    f"⚡ <b>{action_lbl}</b>\n"
                    f"🎬 العمل: <b>{html.escape(str(t_name))}{yr}</b> (<code>#id{media_id}</code>)\n"
                    f"✨ تم جلب معلومات TMDb والممثلين وتركيب الغلاف الموحد ونشره بالميني آب!{extra_ch_note}",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows),
                    parse_mode="HTML"
                )
        except Exception as e:
            await wait_m.edit_text(f"⚠️ تعذّرت الإضافة التلقائية: <code>{html.escape(str(e)[:150])}</code>", parse_mode="HTML")


# معالج ذكي إضافي: يلتقط أي آيدي قناة (-100...) أو منشور محول يرسله المشرف لبوت الإدارة في أي وقت حتى بدون فتح قائمة فرعية!
@admin_router.message()
async def handle_unprompted_admin_channel_or_forward(message: Message, state: FSMContext):
    if not message.from_user or not is_user_admin(message.from_user.id):
        return
    cur_st = await state.get_state()
    if cur_st is not None:
        return
    if message.text and message.text.strip().startswith("/"):
        return

    # فحص ما إذا كانت الرسالة منشوراً محولاً من قناة أو تحتوي على آيدي قناة (-100...) أو رابط قناة (t.me/...)
    has_fwd = bool(getattr(message, "forward_origin", None) or getattr(message, "forward_from_chat", None) or getattr(message, "sender_chat", None))
    raw_txt = (message.text or message.caption or "").strip()
    looks_like_ch_input = bool(
        has_fwd
        or re.search(r"(?<!\w)-?100\d{7,}(?!\w)", raw_txt)
        or re.search(r"^\s*-?\d{9,20}\s*$", raw_txt)
        or "t.me/" in raw_txt
        or raw_txt.startswith("@")
    )
    if not looks_like_ch_input:
        return

    ch_id, _, ch_title, fwd_mid = await extract_channel_info_with_msg_id(message)
    if not ch_id:
        return

    await _process_and_reply_added_source_channel(message, state, ch_id, ch_title or f"قناة {ch_id}", fwd_mid)


# معالج الالتقاط التلقائي الفوري من أي قناة سحب ينزل فيها فيديو جديد
@admin_router.channel_post(F.video | F.text | F.photo)
async def handle_live_channel_post_auto_ingest(message: Message):
    if not is_auto_ingest_enabled():
        return
    ch_id = str(message.chat.id)
    official_chs = {
        str(SECRET_ARCHIVE_CHANNEL_ID),
        str(normalize_chat_target(get_setting("movies_channel_id", ""))),
        str(normalize_chat_target(get_setting("series_channel_id", ""))),
        str(normalize_chat_target(get_setting("anime_channel_id", ""))),
        str(normalize_chat_target(get_setting("publish_channel_id", ""))),
        str(normalize_chat_target(get_setting("movies_publish_channel_id", ""))),
        str(normalize_chat_target(get_setting("series_publish_channel_id", ""))),
        str(normalize_chat_target(get_setting("anime_publish_channel_id", ""))),
    }
    if ch_id in official_chs:
        return

    upsert_source_channel_entry(ch_id, message.chat.title or f"قناة {ch_id}", known_max_msg_id=message.message_id)

    txt_cap = (message.caption or message.text or "").strip()
    if not message.video and txt_cap:
        tp = parse_auto_caption_and_video_meta(txt_cap)
        if tp["title"] and tp["title"] != "عمل غير معنون":
            AUTO_SCAN_STATE["last_context_title"] = f"{tp['title']} {tp['year']}".strip()
        return

    if message.video:
        try:
            await auto_ingest_single_video_message(message, source_label="channel_post", notify_admin=False)
        except Exception as e:
            logger.debug(f"Live channel auto-ingest note: {e}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, reload=False)
