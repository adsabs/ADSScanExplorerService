import redis
import logging
import threading
import json as json_lib
from flask import current_app

logger = logging.getLogger(__name__)

MANIFEST_CACHE_TTL = 86400
MANIFEST_CACHE_PREFIX = 'scan:manifest:'
SEARCH_CACHE_TTL = 60
SEARCH_CACHE_PREFIX = 'scan:search:'

_redis_client = None
_redis_lock = threading.Lock()


def _get_redis():
    """Return the singleton Redis client, creating it on first call with double-checked locking."""
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    with _redis_lock:
        if _redis_client is not None:
            return _redis_client
        try:
            url = current_app.config.get('REDIS_URL', 'redis://redis-backend:6379/4')
            client = redis.from_url(url, decode_responses=True, socket_timeout=2, socket_connect_timeout=2)
            client.ping()
            _redis_client = client
            return _redis_client
        except Exception:
            logger.warning("Redis unavailable, caching disabled")
            return None


def _reset_redis():
    """Clear the cached Redis client so the next call to _get_redis reconnects."""
    global _redis_client
    _redis_client = None


def _redis_get(prefix, key):
    """Fetch a cached value by prefix + key, returning None on miss or failure."""
    r = _get_redis()
    if r is None:
        return None
    try:
        return r.get(prefix + key)
    except redis.ConnectionError:
        _reset_redis()
        return None
    except Exception:
        return None


def _redis_set(prefix, key, value, ttl):
    """Store a value in Redis with the given prefix, key, and TTL."""
    r = _get_redis()
    if r is None:
        return
    try:
        r.setex(prefix + key, ttl, value)
    except redis.ConnectionError:
        _reset_redis()
    except Exception:
        logger.debug("Failed to write cache for key %s%s", prefix, key, exc_info=True)


def _redis_delete(prefix, key):
    """Delete a cached entry by prefix + key."""
    r = _get_redis()
    if r is None:
        return
    try:
        r.delete(prefix + key)
    except redis.ConnectionError:
        _reset_redis()
    except Exception:
        logger.debug("Failed to delete cache for key %s%s", prefix, key, exc_info=True)


def cache_get_manifest(key):
    """Fetch a cached manifest JSON string."""
    return _redis_get(MANIFEST_CACHE_PREFIX, key)


def cache_set_manifest(key, json_str):
    """Cache a manifest JSON string with 1-hour TTL."""
    _redis_set(MANIFEST_CACHE_PREFIX, key, json_str, MANIFEST_CACHE_TTL)


def cache_delete_manifest(key):
    """Invalidate a cached manifest. Called when a collection is updated via PUT."""
    _redis_delete(MANIFEST_CACHE_PREFIX, key)


def cache_get_search(key):
    """Fetch a cached search result."""
    return _redis_get(SEARCH_CACHE_PREFIX, key)


def cache_set_search(key, json_str):
    """Cache a search result with 1-minute TTL."""
    _redis_set(SEARCH_CACHE_PREFIX, key, json_str, SEARCH_CACHE_TTL)


def to_json_and_cache(result, cache_fn, cache_key):
    """Convert a manifest/annotation result to JSON, cache it, and return the dict.
    Deduplicates the toJSON → dumps → cache_set → return pattern used across manifest endpoints."""
    result_dict = result.toJSON(top=True)
    result_json = json_lib.dumps(result_dict) if isinstance(result_dict, dict) else result_dict
    cache_fn(cache_key, result_json)
    return result_dict
