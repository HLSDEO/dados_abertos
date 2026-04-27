import json
import logging
import os
from hashlib import sha256

from redis import Redis

log = logging.getLogger(__name__)

_redis_client: Redis | None = None
_CACHE_ENABLED = os.environ.get("CACHE_ENABLED", "1") == "1"
_REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
_DEFAULT_TTL = int(os.environ.get("CACHE_DEFAULT_TTL_SECONDS", "60"))


def get_cache() -> Redis | None:
    global _redis_client
    if not _CACHE_ENABLED:
        return None
    if _redis_client is None:
        try:
            _redis_client = Redis.from_url(_REDIS_URL, decode_responses=True, socket_timeout=1.5)
            _redis_client.ping()
        except Exception as exc:
            log.warning(f"Redis indisponivel, seguindo sem cache: {exc}")
            _redis_client = None
    return _redis_client


def make_cache_key(prefix: str, **kwargs) -> str:
    parts = [f"{k}={kwargs[k]}" for k in sorted(kwargs)]
    raw_key = f"{prefix}|{'|'.join(parts)}"
    hashed = sha256(raw_key.encode()).hexdigest()[:24]
    return f"daberto:{prefix}:{hashed}"


def cache_get_json(key: str):
    cache = get_cache()
    if cache is None:
        return None
    try:
        payload = cache.get(key)
        if not payload:
            return None
        return json.loads(payload)
    except Exception:
        return None


def cache_set_json(key: str, value, ttl_seconds: int | None = None) -> None:
    cache = get_cache()
    if cache is None:
        return
    ttl = ttl_seconds if ttl_seconds is not None else _DEFAULT_TTL
    try:
        cache.setex(key, ttl, json.dumps(value, ensure_ascii=False, default=str))
    except Exception:
        return
