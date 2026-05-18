"""Cached zipcode allowlist + per-entity gate used by enrichment stages.

Default-allow rules (prefer over-processing to silent drops):
- Entity has no property_roles rows yet -> allow (LLC awaiting PDF pierce).
- At least one linked property has NULL/empty zip_code -> allow (ACRIS row
  not yet HPD-enriched).
- At least one linked property's zip is enabled in the allowlist -> allow
  (any-match wins).
- Zip not in allowlist table -> skip (new zips must be opted in via admin).
- Otherwise (all linked zips known, none enabled) -> skip.
"""

from __future__ import annotations

import time

import structlog

from database.client import db

log = structlog.get_logger(__name__)

_TTL_SECONDS = 60
_cache: dict[str, bool] | None = None
_cache_loaded_at: float = 0.0


def get_allowlist(force_refresh: bool = False) -> dict[str, bool]:
    global _cache, _cache_loaded_at
    now = time.time()
    if force_refresh or _cache is None or (now - _cache_loaded_at) > _TTL_SECONDS:
        rows = (
            db().table("zipcode_allowlist").select("zip_code, enabled").execute().data
            or []
        )
        _cache = {r["zip_code"]: bool(r["enabled"]) for r in rows}
        _cache_loaded_at = now
    return _cache


def invalidate_cache() -> None:
    global _cache
    _cache = None


def is_entity_allowed_by_zip(entity_id: str) -> bool:
    """Return True if this entity should be processed, False if all its zips are disabled."""
    allow = get_allowlist()
    roles = (
        db()
        .table("property_roles")
        .select("properties(zip_code)")
        .eq("entity_id", entity_id)
        .eq("is_current", True)
        .execute()
        .data
        or []
    )
    if not roles:
        # No property_roles yet — allow (LLC awaiting PDF pierce)
        return True
    saw_known_zip = False
    for r in roles:
        z = ((r.get("properties") or {}).get("zip_code") or "").strip()
        if not z:
            # Unknown zip — default allow
            return True
        saw_known_zip = True
        if allow.get(z, False):  # missing keys default to False (new zip must be opted in)
            return True
    return not saw_known_zip
