"""Cached allowlists (zipcode + borough) and per-entity gate used by enrichment.

A property is "known" by its zip if zip_code is populated. Properties with
known zips are gated solely by the zipcode allowlist. Properties with NULL
zip (typically ACRIS-only deeds, where the source dataset doesn't carry zip)
are gated by a per-borough switch derived from the BBL's first digit.

Rules:
- Entity has no property_roles rows yet -> allow (LLC awaiting PDF pierce —
  we can't filter by location until after we pierce to find the property).
- For each linked property:
    - Known zip + enabled in zipcode_allowlist -> allow.
    - NULL zip + parseable BBL with enabled borough -> allow.
    - Otherwise this property doesn't grant access; check the next one.
- If no property grants access -> skip.
"""

from __future__ import annotations

import time

import structlog

from database.client import db

log = structlog.get_logger(__name__)

_TTL_SECONDS = 60

_zip_cache: dict[str, bool] | None = None
_zip_cache_loaded_at: float = 0.0

_boro_cache: dict[str, bool] | None = None
_boro_cache_loaded_at: float = 0.0


def get_allowlist(force_refresh: bool = False) -> dict[str, bool]:
    global _zip_cache, _zip_cache_loaded_at
    now = time.time()
    if force_refresh or _zip_cache is None or (now - _zip_cache_loaded_at) > _TTL_SECONDS:
        rows = (
            db().table("zipcode_allowlist").select("zip_code, enabled").execute().data
            or []
        )
        _zip_cache = {r["zip_code"]: bool(r["enabled"]) for r in rows}
        _zip_cache_loaded_at = now
    return _zip_cache


def get_borough_allowlist(force_refresh: bool = False) -> dict[str, bool]:
    global _boro_cache, _boro_cache_loaded_at
    now = time.time()
    if force_refresh or _boro_cache is None or (now - _boro_cache_loaded_at) > _TTL_SECONDS:
        rows = (
            db().table("borough_allowlist").select("borough_code, enabled").execute().data
            or []
        )
        _boro_cache = {r["borough_code"]: bool(r["enabled"]) for r in rows}
        _boro_cache_loaded_at = now
    return _boro_cache


def invalidate_cache() -> None:
    """Clear both zip and borough caches. Called by admin write endpoints."""
    global _zip_cache, _boro_cache
    _zip_cache = None
    _boro_cache = None


def _borough_from_bbl(bbl: str | None) -> str | None:
    """First digit of a well-formed NYC BBL (10 digits, '1'-'5' prefix).
    Returns None for placeholder BBLs like 'hpd_bldg_*'."""
    if not bbl or len(bbl) < 10 or not bbl.isdigit():
        return None
    code = bbl[0]
    return code if code in ("1", "2", "3", "4", "5") else None


def is_entity_allowed_by_zip(entity_id: str) -> bool:
    """Return True if this entity should be processed, False otherwise.

    Naming kept for back-compat with enrichment modules that import this;
    the function actually checks both zip and borough allowlists.
    """
    zip_allow = get_allowlist()
    boro_allow = get_borough_allowlist()

    roles = (
        db()
        .table("property_roles")
        .select("properties(zip_code, bbl)")
        .eq("entity_id", entity_id)
        .eq("is_current", True)
        .execute()
        .data
        or []
    )
    if not roles:
        # No property_roles yet — allow (LLC awaiting PDF pierce).
        return True

    for r in roles:
        props = r.get("properties") or {}
        z = (props.get("zip_code") or "").strip()
        if z:
            # Known zip — gate solely by zipcode_allowlist.
            if zip_allow.get(z, False):
                return True
        else:
            # NULL zip — fall back to borough switch derived from BBL.
            boro = _borough_from_bbl(props.get("bbl"))
            if boro and boro_allow.get(boro, False):
                return True

    return False
