"""التحقق من توقيع Telegram Mini App (initData) — بدون أي مكتبات خارجية."""
import hashlib
import hmac
import json
import time
import contextvars
from typing import Iterable
from urllib.parse import parse_qsl

# أيدي مستخدم تيليجرام الموثّق للطلب الحالي (0 = غير موثّق)
CURRENT_TG_USER: contextvars.ContextVar = contextvars.ContextVar("current_tg_user", default=0)


def validate_init_data(init_data: str, bot_tokens: Iterable[str], max_age: int = 0) -> int:
    """يرجّع أيدي المستخدم إذا التوقيع صحيح من أحد توكنات البوتات، وإلا 0."""
    if not init_data or len(init_data) > 8192:
        return 0
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True, strict_parsing=True))
    except ValueError:
        return 0
    recv_hash = pairs.pop("hash", "")
    if not recv_hash:
        return 0
    check_string = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
    ok = False
    for tok in bot_tokens:
        if not tok:
            continue
        secret = hmac.new(b"WebAppData", tok.encode(), hashlib.sha256).digest()
        calc = hmac.new(secret, check_string.encode(), hashlib.sha256).hexdigest()
        if hmac.compare_digest(calc, recv_hash):
            ok = True
            break
    if not ok:
        return 0
    if max_age > 0:
        try:
            if time.time() - int(pairs.get("auth_date", "0")) > max_age:
                return 0
        except ValueError:
            return 0
    try:
        return int(json.loads(pairs.get("user", "{}")).get("id", 0) or 0)
    except (ValueError, TypeError, AttributeError):
        return 0
