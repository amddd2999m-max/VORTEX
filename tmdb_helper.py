import re
import time
import asyncio
import aiohttp
from typing import List, Dict, Any, Optional
from database import get_setting

TMDB_GENRES_AR = {
    28: "أكشن وحركة",
    12: "مغامرة",
    16: "أنميشن",
    35: "كوميديا",
    80: "جريمة",
    99: "وثائقي",
    18: "دراما",
    10751: "عائلي",
    14: "فانتازيا",
    36: "تاريخي",
    27: "رعب",
    10402: "موسيقى",
    9648: "غموض",
    10749: "رومانسية",
    878: "خيال علمي",
    10770: "فيلم تلفزيوني",
    53: "إثارة وتشويق",
    10752: "حرب",
    37: "غربي",
    10759: "أكشن وحركة",
    10762: "أطفال",
    10765: "فانتازيا",
    10768: "حرب وسياسة"
}

COUNTRY_NAMES_AR = {
    "US": "الولايات المتحدة الأمريكية",
    "GB": "المملكة المتحدة",
    "KR": "كوريا الجنوبية",
    "JP": "اليابان",
    "CN": "الصين",
    "IN": "الهند",
    "TR": "تركيا",
    "EG": "مصر",
    "SA": "السعودية",
    "IQ": "العراق",
    "SY": "سوريا",
    "LB": "لبنان",
    "KW": "الكويت",
    "AE": "الإمارات",
    "FR": "فرنسا",
    "ES": "إسبانيا",
    "DE": "ألمانيا",
    "IT": "إيطاليا",
    "CA": "كندا",
    "AU": "أستراليا",
    "RU": "روسيا",
    "NL": "هولندا",
    "JO": "الأردن",
    "MA": "المغرب",
    "TN": "تونس",
    "DZ": "الجزائر",
    "QA": "قطر",
    "BH": "البحرين",
    "OM": "عُمان",
    "TH": "تايلاند",
    "ID": "إندونيسيا",
    "IR": "إيران",
    "PK": "باكستان"
}


def parse_query_and_year(raw_query: str):
    match = re.search(r"\b(19\d{2}|20\d{2})\b", raw_query)
    year = None
    clean_query = raw_query
    if match:
        year = match.group(1)
        clean_query = raw_query.replace(year, "").strip()
    return clean_query or raw_query, year


def _clean_collection_name(raw_name: str, fallback_title: str = "") -> str:
    if not raw_name:
        return f"سلسلة أفلام {fallback_title}" if fallback_title else ""
    cleaned = re.sub(r"\s*-\s*السلسلة\s*$", "", raw_name).strip()
    cleaned = re.sub(r"\s*Collection\s*$", "", cleaned, flags=re.IGNORECASE).strip()
    if not cleaned.startswith("سلسلة"):
        return f"سلسلة {cleaned}"
    return cleaned


async def fetch_tmdb_collection_info(session: aiohttp.ClientSession, collection_id: int, api_key: str, headers: dict, fallback_title: str = "") -> Dict[str, Any]:
    url = f"https://api.themoviedb.org/3/collection/{collection_id}"
    params = {"api_key": api_key, "language": "ar-SA"}
    if "Authorization" in headers:
        params.pop("api_key", None)
    try:
        async with session.get(url, params=params, headers=headers) as resp:
            if resp.status == 200:
                c_data = await resp.json()
                parts = c_data.get("parts") or []
                now_parts = [p for p in parts if (p.get("release_date") or "")[:4].isdigit()]
                total_parts = len(now_parts) if now_parts else len(parts)
                c_name = _clean_collection_name(c_data.get("name") or "", fallback_title)
                p_path = c_data.get("poster_path")
                b_path = c_data.get("backdrop_path") or p_path
                return {
                    "collection_id": int(collection_id),
                    "collection_name": c_name,
                    "collection_poster": f"https://image.tmdb.org/t/p/w500{p_path}" if p_path else "",
                    "collection_backdrop": f"https://image.tmdb.org/t/p/w780{b_path}" if b_path else "",
                    "collection_total_parts": max(1, total_parts)
                }
    except Exception:
        pass
    return {
        "collection_id": int(collection_id),
        "collection_name": _clean_collection_name("", fallback_title),
        "collection_poster": "",
        "collection_backdrop": "",
        "collection_total_parts": 3
    }


async def search_tmdb(query: str, media_type: str = "movie") -> List[Dict[str, Any]]:
    api_key = get_setting("tmdb_api_key", "").strip() or "3fd2be6f0c70a2a598f084ddfb75487c"

    clean_q, year = parse_query_and_year(query)
    headers = {}
    if len(api_key) > 60:
        headers["Authorization"] = f"Bearer {api_key}"

    async with aiohttp.ClientSession() as session:
        if media_type == "anime":
            results_combined = []
            for ep_type in ("tv", "movie"):
                url = f"https://api.themoviedb.org/3/search/{ep_type}"
                params = {"query": clean_q, "language": "ar-SA", "include_adult": "false"}
                if not headers:
                    params["api_key"] = api_key
                if year:
                    params["first_air_date_year" if ep_type == "tv" else "year"] = year
                async with session.get(url, params=params, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for it in (data.get("results") or [])[:6]:
                            it["_tmdb_ep"] = ep_type
                            results_combined.append(it)
            results_combined.sort(key=lambda x: (16 in (x.get("genre_ids") or []), x.get("popularity") or 0), reverse=True)
            formatted = []
            for item in results_combined[:6]:
                title = item.get("title") or item.get("name") or item.get("original_title") or "بدون عنوان"
                orig_title = item.get("original_title") or item.get("original_name") or title
                date_str = item.get("release_date") or item.get("first_air_date") or ""
                item_year = date_str[:4] if date_str else "غير معروف"
                formatted.append({
                    "tmdb_id": item.get("id"),
                    "media_type": "anime",
                    "tmdb_endpoint": item.get("_tmdb_ep", "tv"),
                    "title": title,
                    "original_title": orig_title,
                    "year": item_year
                })
            return formatted

        endpoint = "movie" if media_type == "movie" else "tv"
        url = f"https://api.themoviedb.org/3/search/{endpoint}"
        params = {
            "query": clean_q,
            "language": "ar-SA",
            "include_adult": "false"
        }
        if not headers:
            params["api_key"] = api_key
        if year:
            if endpoint == "movie":
                params["year"] = year
            else:
                params["first_air_date_year"] = year

        async with session.get(url, params=params, headers=headers) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            results = data.get("results", [])[:6]
            formatted = []
            for item in results:
                title = item.get("title") or item.get("name") or item.get("original_title") or "بدون عنوان"
                orig_title = item.get("original_title") or item.get("original_name") or title
                date_str = item.get("release_date") or item.get("first_air_date") or ""
                item_year = date_str[:4] if date_str else "غير معروف"
                formatted.append({
                    "tmdb_id": item.get("id"),
                    "media_type": media_type,
                    "tmdb_endpoint": endpoint,
                    "title": title,
                    "original_title": orig_title,
                    "year": item_year
                })
            return formatted


async def get_tmdb_details(tmdb_id: int, media_type: str = "movie", tmdb_endpoint: str = "") -> Optional[Dict[str, Any]]:
    api_key = get_setting("tmdb_api_key", "").strip() or "3fd2be6f0c70a2a598f084ddfb75487c"

    is_anime_forced = (media_type == "anime")
    endpoint = tmdb_endpoint if tmdb_endpoint in ("movie", "tv") else ("movie" if media_type == "movie" else "tv")

    params = {"language": "ar-SA", "append_to_response": "credits,videos", "include_video_language": "ar,en,null"}
    headers = {}
    if len(api_key) > 60:
        headers["Authorization"] = f"Bearer {api_key}"
    else:
        params["api_key"] = api_key

    async with aiohttp.ClientSession() as session:
        url = f"https://api.themoviedb.org/3/{endpoint}/{tmdb_id}"
        resp = await session.get(url, params=params, headers=headers)
        if resp.status != 200 and is_anime_forced and not tmdb_endpoint:
            await resp.release()
            endpoint = "movie" if endpoint == "tv" else "tv"
            url = f"https://api.themoviedb.org/3/{endpoint}/{tmdb_id}"
            resp = await session.get(url, params=params, headers=headers)

        async with resp:
            if resp.status != 200:
                return None
            item = await resp.json()

            overview = (item.get("overview") or "").strip()
            if not overview:
                params_en = dict(params)
                params_en["language"] = "en-US"
                async with session.get(url, params=params_en, headers=headers) as resp_en:
                    if resp_en.status == 200:
                        item_en = await resp_en.json()
                        overview = (item_en.get("overview") or "لا يوجد وصف متاح لهذا العمل.").strip()

            title = item.get("title") or item.get("name") or "بدون عنوان"
            orig_title = item.get("original_title") or item.get("original_name") or title
            date_str = item.get("release_date") or item.get("first_air_date") or ""
            year = date_str[:4] if date_str else ""

            runtime = item.get("runtime")
            if not runtime and item.get("episode_run_time"):
                runtime = item["episode_run_time"][0]
            if endpoint == "tv":
                seasons_cnt = item.get("number_of_seasons") or 1
                duration_str = f"{seasons_cnt} موسم"
            else:
                duration_str = f"{runtime} دقيقة" if runtime else "115 دقيقة"

            country_code = "US"
            if item.get("origin_country") and len(item["origin_country"]) > 0:
                country_code = item["origin_country"][0]
            elif item.get("production_countries") and len(item["production_countries"]) > 0:
                country_code = item["production_countries"][0].get("iso_3166_1", "US")

            country_ar = COUNTRY_NAMES_AR.get(country_code, "الولايات المتحدة الأمريكية")
            orig_lang = item.get("original_language", "en")

            genres = []
            for g in item.get("genres", []):
                gid = g.get("id")
                gname = TMDB_GENRES_AR.get(gid, g.get("name", ""))
                if gname and gname not in genres:
                    genres.append(gname)

            is_anime_detected = is_anime_forced or ("أنميشن" in genres and (orig_lang == "ja" or country_code == "JP"))
            final_media_type = "anime" if is_anime_detected else media_type

            if final_media_type == "anime":
                cat_label = "أنمي"
            elif country_code == "IQ" or country_ar == "العراق":
                cat_label = "فيلم عراقي" if final_media_type == "movie" else "مسلسل عراقي"
            elif country_code == "TR" or orig_lang == "tr":
                cat_label = "فيلم تركي" if final_media_type == "movie" else "مسلسل تركي"
            elif country_code == "IN" or orig_lang in ("hi", "ta", "te", "ml", "kn", "pa", "bn"):
                cat_label = "فيلم هندي" if final_media_type == "movie" else "مسلسل هندي"
            elif orig_lang == "ar" or country_code in ("EG", "SY", "LB", "KW", "SA", "AE", "JO", "MA", "TN", "DZ", "QA", "BH", "OM"):
                cat_label = "فيلم عربي" if final_media_type == "movie" else "مسلسل عربي"
            elif orig_lang in ("ko", "zh", "ja", "th", "id") or country_code in ("KR", "JP", "CN", "TH", "ID"):
                cat_label = "فيلم آسيوي" if final_media_type == "movie" else "مسلسل آسيوي"
            else:
                cat_label = "فيلم أجنبي" if final_media_type == "movie" else "مسلسل أجنبي"

            poster_path = item.get("poster_path")
            backdrop_path = item.get("backdrop_path") or poster_path
            poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else ""
            backdrop_url = f"https://image.tmdb.org/t/p/w780{backdrop_path}" if backdrop_path else poster_url

            collection_info = {
                "collection_id": 0,
                "collection_name": "",
                "collection_poster": "",
                "collection_backdrop": "",
                "collection_total_parts": 0
            }
            btc = item.get("belongs_to_collection")
            if endpoint == "movie" and isinstance(btc, dict) and btc.get("id"):
                cid = int(btc["id"])
                collection_info = await fetch_tmdb_collection_info(session, cid, api_key, headers, fallback_title=orig_title or title)
                if not collection_info["collection_poster"] and btc.get("poster_path"):
                    collection_info["collection_poster"] = f"https://image.tmdb.org/t/p/w500{btc['poster_path']}"
                if not collection_info["collection_backdrop"] and btc.get("backdrop_path"):
                    collection_info["collection_backdrop"] = f"https://image.tmdb.org/t/p/w780{btc['backdrop_path']}"

            # استخراج طاقم الممثلين (أبرز 10 ممثلين مع صورهم وأدوارهم)
            cast_list: List[Dict[str, Any]] = []
            raw_cast = ((item.get("credits") or {}).get("cast") or [])[:10]
            for c_mem in raw_cast:
                c_name = (c_mem.get("name") or c_mem.get("original_name") or "").strip()
                if not c_name:
                    continue
                p_p = c_mem.get("profile_path")
                cast_list.append({
                    "id": int(c_mem.get("id") or 0),
                    "name": c_name,
                    "character": (c_mem.get("character") or "").strip(),
                    "profile_url": f"https://image.tmdb.org/t/p/w185{p_p}" if p_p else ""
                })

            # استخراج رابط الإعلان الرسمي على يوتيوب (Trailer)
            trailer_url = ""
            vids = ((item.get("videos") or {}).get("results") or [])
            for v in vids:
                if str(v.get("site") or "").lower() == "youtube" and v.get("key"):
                    if str(v.get("type") or "").lower() in ("trailer", "teaser"):
                        trailer_url = f"https://www.youtube.com/watch?v={v['key']}"
                        break
            if not trailer_url:
                for v in vids:
                    if str(v.get("site") or "").lower() == "youtube" and v.get("key"):
                        trailer_url = f"https://www.youtube.com/watch?v={v['key']}"
                        break

            return {
                "tmdb_id": tmdb_id,
                "media_type": final_media_type,
                "title": title,
                "original_title": orig_title,
                "year": year,
                "duration": duration_str,
                "country": country_ar,
                "category_label": cat_label,
                "rating": round(float(item.get("vote_average") or 0.0), 1),
                "genres": genres[:3] if genres else ["دراما"],
                "overview": overview or "لا يوجد وصف متاح.",
                "poster_url": poster_url,
                "backdrop_url": backdrop_url,
                "runtime_min": int(runtime or 0),
                "cast": cast_list,
                "trailer_url": trailer_url,
                **collection_info
            }


_SEASON_CACHE: Dict[tuple, tuple] = {}
_SEASON_TTL = 12 * 3600


def _tmdb_auth(api_key: str):
    params = {"api_key": api_key}
    headers: Dict[str, str] = {}
    if len(api_key) > 60:
        headers["Authorization"] = f"Bearer {api_key}"
        params = {}
    return params, headers


async def _tmdb_get_json(session: aiohttp.ClientSession, url: str, api_key: str, extra: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    params, headers = _tmdb_auth(api_key)
    params.update(extra)
    for attempt in range(3):
        try:
            async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status in (401, 404):
                    return None
                if resp.status == 429:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(0.8 * (attempt + 1))
    return None


async def get_tmdb_season_episodes(tmdb_id: int, season: int = 1, force: bool = False) -> Dict[int, Dict[str, Any]]:
    api_key = get_setting("tmdb_api_key", "").strip() or "3fd2be6f0c70a2a598f084ddfb75487c"
    tmdb_id = int(tmdb_id or 0)
    season = int(season or 1)
    if tmdb_id <= 0:
        return {}

    key = (tmdb_id, season)
    cached = _SEASON_CACHE.get(key)
    if cached and not force and (time.time() - cached[0]) < _SEASON_TTL:
        return cached[1]

    base = f"https://api.themoviedb.org/3/tv/{tmdb_id}"
    async with aiohttp.ClientSession() as session:
        data_ar = await _tmdb_get_json(session, f"{base}/season/{season}", api_key, {"language": "ar-SA"}) or {}
        data_en = await _tmdb_get_json(session, f"{base}/season/{season}", api_key, {"language": "en-US"}) or {}
        ar_eps = data_ar.get("episodes") or []
        en_eps = data_en.get("episodes") or []
        if not ar_eps and not en_eps:
            return {}
        if not ar_eps:
            data_ar["episodes"] = en_eps
        en_by_num = {int(e.get("episode_number") or 0): e for e in en_eps}

        show = await _tmdb_get_json(session, base, api_key, {"language": "en-US"}) or {}
        show_runtimes = [int(x) for x in (show.get("episode_run_time") or []) if x]
        show_avg = int(sum(show_runtimes) / len(show_runtimes)) if show_runtimes else 0

    known = sorted(int(e.get("runtime") or 0) for e in data_ar["episodes"] if e.get("runtime"))
    season_median = known[len(known) // 2] if known else 0

    out: Dict[int, Dict[str, Any]] = {}
    for e in data_ar["episodes"]:
        num = int(e.get("episode_number") or 0)
        if num <= 0:
            continue
        en = en_by_num.get(num, {})
        name = (e.get("name") or "").strip()
        if re.fullmatch(r"(?:الحلقة|حلقة|episode)\s*\d+", name, flags=re.IGNORECASE):
            name = ""
        if not name:
            en_name = (en.get("name") or "").strip()
            name = "" if re.fullmatch(r"episode\s*\d+", en_name, flags=re.IGNORECASE) else en_name
        runtime = int(e.get("runtime") or en.get("runtime") or 0) or season_median or show_avg
        overview = (e.get("overview") or en.get("overview") or "").strip()
        still = e.get("still_path") or en.get("still_path")
        out[num] = {
            "title": name,
            "runtime_min": int(runtime or 0),
            "runtime_exact": bool(e.get("runtime") or en.get("runtime")),
            "overview": overview,
            "still_url": f"https://image.tmdb.org/t/p/w300{still}" if still else "",
            "air_date": e.get("air_date") or "",
        }

    _SEASON_CACHE[key] = (time.time(), out)
    return out


async def get_tmdb_episode_info(tmdb_id: int, season: int, episode: int) -> Optional[Dict[str, Any]]:
    eps = await get_tmdb_season_episodes(tmdb_id, season)
    return eps.get(int(episode or 0))


async def verify_series_episodes_with_tmdb(media_item: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not media_item or media_item.get("media_type") not in ("tv", "anime"):
        return {"ok": False, "seasons": [], "summary_html": ""}

    tmdb_id = int(media_item.get("tmdb_id") or 0)
    files = media_item.get("files") or []
    seasons_map: Dict[int, Dict[int, List[str]]] = {}
    titled_eps_set = set()

    for f in files:
        s_num = int(f.get("season") or 1)
        e_num = int(f.get("episode") or 0)
        if e_num <= 0:
            continue
        seasons_map.setdefault(s_num, {}).setdefault(e_num, []).append(str(f.get("quality") or "720"))
        ep_t = str(f.get("episode_title") or "").strip()
        if ep_t and not re.fullmatch(r"(?:الحلقة|حلقة|episode)\s*\d+", ep_t, flags=re.IGNORECASE):
            titled_eps_set.add((s_num, e_num))

    if not seasons_map:
        return {"ok": False, "seasons": [], "summary_html": "◈ لا توجد حلقات مرفوعة بعد."}

    seasons_report = []
    lines_html = ["◈ <b>تقرير التحقق التلقائي من الحلقات وعناوين TMDb:</b>"]

    for s_num in sorted(seasons_map.keys()):
        uploaded_eps = sorted(seasons_map[s_num].keys())
        tmdb_eps = await get_tmdb_season_episodes(tmdb_id, s_num) if tmdb_id > 0 else {}
        tmdb_nums = sorted(tmdb_eps.keys())
        tmdb_total = len(tmdb_nums)

        if tmdb_total > 0:
            missing = [n for n in tmdb_nums if n not in seasons_map[s_num]]
            extra = [n for n in uploaded_eps if n not in tmdb_eps]
            is_complete = (len(missing) == 0 and len(uploaded_eps) >= tmdb_total)
        else:
            max_ep = max(uploaded_eps) if uploaded_eps else 0
            missing = [n for n in range(1, max_ep + 1) if n not in seasons_map[s_num]]
            extra = []
            is_complete = (len(missing) == 0 and len(uploaded_eps) > 0)

        season_titled = sum(1 for e_n in uploaded_eps if (s_num, e_n) in titled_eps_set)
        s_entry = {
            "season": s_num,
            "uploaded_count": len(uploaded_eps),
            "tmdb_total": tmdb_total,
            "uploaded_episodes": uploaded_eps,
            "missing_episodes": missing,
            "extra_episodes": extra,
            "titled_count": season_titled,
            "is_complete": is_complete,
        }
        seasons_report.append(s_entry)

        if tmdb_total > 0:
            status_tag = "✓ مكتمل 100%" if is_complete else f"❖ ناقص ({len(missing)} حلقة)"
            lines_html.append(
                f"• <b>الموسم {s_num}:</b> مرفوع <b>{len(uploaded_eps)}</b> من أصل <b>{tmdb_total}</b> بـ TMDb ({status_tag}) · العناوين المضافة: <b>{season_titled}</b>"
            )
            if missing:
                miss_str = ", ".join(map(str, missing[:15])) + ("..." if len(missing) > 15 else "")
                lines_html.append(f"   ↳ الحلقات الناقصة: <code>{miss_str}</code>")
            if extra:
                ext_str = ", ".join(map(str, extra[:10]))
                lines_html.append(f"   ↳ أرقام حلقات خارج فهرس الموسم بـ TMDb: <code>{ext_str}</code>")
        else:
            lines_html.append(
                f"• <b>الموسم {s_num}:</b> مرفوع <b>{len(uploaded_eps)}</b> حلقة"
                + (f" (حلقات مفقودة بالتسلسل: <code>{', '.join(map(str, missing[:12]))}</code>)" if missing else " (متسلسلة بالكامل ✓)")
            )

    return {
        "ok": True,
        "seasons": seasons_report,
        "summary_html": "\n".join(lines_html)
    }
