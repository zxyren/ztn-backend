"""
Domain-specific thumbnail handlers with in-memory TTL cache.

Resolution chain (fastest → slowest):
  1. Cache hit           → ~0 ms
  2. YouTube             → ~0 ms  (deterministic URL from video ID)
  3. Pinterest            → 200-500 ms  (redirect + og:image)
  4. TikTok               → 100-300 ms  (oEmbed API)
  5. Instagram            → 100-300 ms  (oEmbed API)
  6. Generic og:image     → 200-1000 ms (HTTP GET first 16 KB)
  7. yt-dlp fallback      → 2-8 s       (full metadata extraction)
"""

import re
import logging
import threading
from urllib.parse import urlparse, parse_qs

import httpx
from cachetools import TTLCache

log = logging.getLogger(__name__)

# ── Cache ──────────────────────────────────────────────────────────────────
# Two caches: successful results live 1 hour, failures live 5 minutes
_cache_lock = threading.Lock()
_cache = TTLCache(maxsize=2048, ttl=3600)        # 1 h for successes
_fail_cache = TTLCache(maxsize=512, ttl=300)      # 5 min for failures

# Shared httpx client — connection pooling + timeouts
_http = httpx.Client(
    timeout=httpx.Timeout(connect=3, read=5, write=3, pool=5),
    follow_redirects=True,
    limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
    headers={"User-Agent": "TelegramBot (like TwitterBot)"},
)


def _normalize_url(url: str) -> str:
    """Normalize URL for cache key consistency."""
    return url.strip().rstrip("/")


# ── YouTube ────────────────────────────────────────────────────────────────

_YT_PATTERNS = [
    # youtube.com/watch?v=ID
    re.compile(r"(?:youtube\.com/watch\?.*?v=)([\w-]{11})"),
    # youtu.be/ID
    re.compile(r"youtu\.be/([\w-]{11})"),
    # youtube.com/embed/ID
    re.compile(r"youtube\.com/embed/([\w-]{11})"),
    # youtube.com/shorts/ID
    re.compile(r"youtube\.com/shorts/([\w-]{11})"),
    # youtube.com/v/ID
    re.compile(r"youtube\.com/v/([\w-]{11})"),
    # youtube.com/live/ID
    re.compile(r"youtube\.com/live/([\w-]{11})"),
]


def _extract_youtube_id(url: str) -> str | None:
    for pat in _YT_PATTERNS:
        m = pat.search(url)
        if m:
            return m.group(1)
    # Check query param as fallback
    parsed = urlparse(url)
    v = parse_qs(parsed.query).get("v")
    if v and len(v[0]) == 11:
        return v[0]
    return None


def _youtube_handler(url: str) -> dict | None:
    """Resolve YouTube thumbnail — zero HTTP calls."""
    vid = _extract_youtube_id(url)
    if not vid:
        return None
    return {
        "thumbnail": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
        "title": "",  # Title would require an API call; skip for speed
    }


# ── Pinterest ──────────────────────────────────────────────────────────────

def _is_pinterest(host: str) -> bool:
    return any(h in host for h in ("pinterest.com", "pin.it"))


def _pinterest_handler(url: str) -> dict | None:
    """Follow redirects and scrape og:image from Pinterest."""
    parsed = urlparse(url)
    if not _is_pinterest(parsed.hostname or ""):
        return None
    try:
        resp = _http.get(url, timeout=httpx.Timeout(connect=3, read=4, write=3, pool=3))
        resp.raise_for_status()
        return _parse_meta_tags(resp.text[:32_000])
    except Exception as e:
        log.debug("Pinterest handler failed for %s: %s", url, e)
        return None


# ── TikTok ─────────────────────────────────────────────────────────────────

def _is_tiktok(host: str) -> bool:
    return "tiktok.com" in host


def _tiktok_handler(url: str) -> dict | None:
    """Use TikTok oEmbed API for thumbnail."""
    parsed = urlparse(url)
    if not _is_tiktok(parsed.hostname or ""):
        return None
    try:
        resp = _http.get(
            "https://www.tiktok.com/oembed",
            params={"url": url},
            timeout=httpx.Timeout(connect=3, read=4, write=3, pool=3),
        )
        resp.raise_for_status()
        data = resp.json()
        thumb = data.get("thumbnail_url")
        title = data.get("title", "")
        if thumb:
            return {"thumbnail": thumb, "title": title}
    except Exception as e:
        log.debug("TikTok handler failed for %s: %s", url, e)
    return None


# ── Instagram ──────────────────────────────────────────────────────────────

def _is_instagram(host: str) -> bool:
    return "instagram.com" in host


def _instagram_handler(url: str) -> dict | None:
    """Use Instagram oEmbed API for thumbnail."""
    parsed = urlparse(url)
    if not _is_instagram(parsed.hostname or ""):
        return None
    try:
        resp = _http.get(
            "https://www.instagram.com/api/v1/oembed/",
            params={"url": url},
            timeout=httpx.Timeout(connect=3, read=4, write=3, pool=3),
        )
        resp.raise_for_status()
        data = resp.json()
        thumb = data.get("thumbnail_url")
        title = data.get("title", "")
        if thumb:
            return {"thumbnail": thumb, "title": title}
    except Exception as e:
        log.debug("Instagram handler failed for %s: %s", url, e)
    return None


# ── Generic og:image / twitter:image ──────────────────────────────────────

_OG_IMAGE_RE = re.compile(
    r'<meta\s+[^>]*?(?:property|name)\s*=\s*["\']'
    r'(?:og:image|twitter:image(?::src)?)'
    r'["\'][^>]*?content\s*=\s*["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_OG_IMAGE_REV_RE = re.compile(
    r'<meta\s+[^>]*?content\s*=\s*["\']([^"\']+)["\'][^>]*?'
    r'(?:property|name)\s*=\s*["\']'
    r'(?:og:image|twitter:image(?::src)?)["\']',
    re.IGNORECASE,
)
_OG_TITLE_RE = re.compile(
    r'<meta\s+[^>]*?property\s*=\s*["\']og:title["\'][^>]*?'
    r'content\s*=\s*["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_OG_TITLE_REV_RE = re.compile(
    r'<meta\s+[^>]*?content\s*=\s*["\']([^"\']+)["\'][^>]*?'
    r'property\s*=\s*["\']og:title["\']',
    re.IGNORECASE,
)
_HTML_TITLE_RE = re.compile(r"<title[^>]*>([^<]+)</title>", re.IGNORECASE)


def _parse_meta_tags(html: str) -> dict | None:
    """Extract og:image and title from HTML fragment."""
    img = None
    for pat in (_OG_IMAGE_RE, _OG_IMAGE_REV_RE):
        m = pat.search(html)
        if m:
            img = m.group(1)
            break
    if not img:
        return None

    title = ""
    for pat in (_OG_TITLE_RE, _OG_TITLE_REV_RE, _HTML_TITLE_RE):
        m = pat.search(html)
        if m:
            title = m.group(1).strip()
            break

    return {"thumbnail": img, "title": title}


def _generic_handler(url: str) -> dict | None:
    """Lightweight GET — read only first 16 KB for meta tags."""
    try:
        with _http.stream("GET", url, timeout=httpx.Timeout(connect=3, read=4, write=3, pool=3)) as resp:
            resp.raise_for_status()
            content_type = resp.headers.get("content-type", "")
            if "text/html" not in content_type and "application/xhtml" not in content_type:
                return None  # Not an HTML page
            chunks = []
            total = 0
            for chunk in resp.iter_text():
                chunks.append(chunk)
                total += len(chunk)
                if total >= 65_536:  # 64 KB is enough for <head>
                    break
            html = "".join(chunks)
        return _parse_meta_tags(html)
    except Exception as e:
        log.debug("Generic handler failed for %s: %s", url, e)
        return None


# ── yt-dlp fallback ────────────────────────────────────────────────────────

def _ytdlp_handler(url: str, ffmpeg_dir: str | None = None) -> dict | None:
    """Last-resort fallback using yt-dlp extract_info."""
    try:
        from yt_dlp import YoutubeDL
        opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "socket_timeout": 8,
            "extract_flat": False,
        }
        if ffmpeg_dir:
            opts["ffmpeg_location"] = ffmpeg_dir
        info = YoutubeDL(opts).extract_info(url, download=False)
        if not info:
            return None
        thumbs = [t for t in info.get("thumbnails", []) if t.get("url")]
        low_q = [t for t in thumbs if 240 <= t.get("width", 0) <= 640]
        thumb = (
            min(low_q, key=lambda x: abs(x.get("width", 0) - 480))["url"]
            if low_q
            else min(thumbs, key=lambda x: x.get("width", 999999))["url"]
            if thumbs
            else info.get("thumbnail")
        )
        if thumb:
            return {"thumbnail": thumb, "title": info.get("title", "")}
    except Exception as e:
        log.debug("yt-dlp handler failed for %s: %s", url, e)
    return None


# ── Public API ─────────────────────────────────────────────────────────────

# Handler chain — order matters: fastest first
_HANDLERS = [
    _youtube_handler,
    _pinterest_handler,
    _tiktok_handler,
    _instagram_handler,
    _generic_handler,
    # _ytdlp_handler is called separately with extra args
]


def resolve_thumbnail(url: str, ffmpeg_dir: str | None = None) -> dict | None:
    """
    Resolve thumbnail for a URL using the fastest available handler.

    Returns {"thumbnail": str, "title": str} or None.
    Results are cached in memory (1 h success, 5 min failure).
    """
    key = _normalize_url(url)

    # 1. Cache lookup
    with _cache_lock:
        if key in _cache:
            return _cache[key]
        if key in _fail_cache:
            return None

    # 2. Try domain-specific and generic handlers
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()

    for handler in _HANDLERS:
        try:
            result = handler(url)
            if result and result.get("thumbnail"):
                with _cache_lock:
                    _cache[key] = result
                log.info("Resolved %s via %s", url[:80], handler.__name__)
                return result
        except Exception as e:
            log.debug("Handler %s raised for %s: %s", handler.__name__, url[:80], e)

    # 3. yt-dlp fallback (slowest)
    result = _ytdlp_handler(url, ffmpeg_dir)
    if result and result.get("thumbnail"):
        with _cache_lock:
            _cache[key] = result
        log.info("Resolved %s via yt-dlp fallback", url[:80])
        return result

    # 4. Nothing worked — cache the failure
    with _cache_lock:
        _fail_cache[key] = True
    log.warning("All handlers failed for %s", url[:80])
    return None
