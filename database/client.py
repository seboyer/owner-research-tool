"""
database/client.py — Supabase client wrapper with helper methods.

All DB operations go through this module. It provides:
- Upsert helpers with conflict detection
- Deduplication via normalized name matching
- Enrichment queue management
- Seen-record tracking (to skip already-processed data)
"""

import hashlib
import re
import uuid
from typing import Optional

import structlog
from supabase import create_client, Client

from config import config

log = structlog.get_logger(__name__)


def _get_client() -> Client:
    return create_client(config.SUPABASE_URL, config.SUPABASE_KEY)


# Module-level singleton
_client: Optional[Client] = None


def db() -> Client:
    global _client
    if _client is None:
        _client = _get_client()
    return _client


# ============================================================
# Utilities
# ============================================================

def normalize_name(name: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    if not name:
        return ""
    n = name.lower()
    n = re.sub(r"[^\w\s]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    # Expand common abbreviations
    replacements = {
        " llc": " llc",
        " l l c": " llc",
        " l.l.c": " llc",
        " corp": " corporation",
        " inc": " incorporated",
        " mgmt": " management",
        " mgt": " management",
        " assoc": " associates",
        " realty": " realty",
        " prop": " properties",
    }
    for abbr, full in replacements.items():
        n = n.replace(abbr, full)
    return n.strip()


def checksum(data: dict) -> str:
    """MD5 hash of key fields to detect changes in source records."""
    key = "|".join(str(v) for v in sorted(data.values()) if v)
    return hashlib.md5(key.encode()).hexdigest()


def is_building_llc(name: str) -> bool:
    """
    Heuristic: is this LLC named after a specific address or building?
    Examples: "123 MAIN ST LLC", "ONE BROADWAY ASSOCIATES LLC",
              "45 PARK AVENUE REALTY LLC", "WEST 72ND ST OWNER LLC"
    """
    if not name:
        return False
    name_upper = name.upper()
    # Contains a street number at the start or after common prefixes
    street_pattern = re.compile(
        r"\b\d+\s+(WEST|EAST|NORTH|SOUTH|W\.|E\.|N\.|S\.)?\s*\w+\s+(ST|AVE|BLVD|RD|DR|LN|CT|PL|WAY|STREET|AVENUE|BOULEVARD|ROAD|DRIVE|LANE|COURT|PLACE)\b",
        re.IGNORECASE,
    )
    # Named after an ordinal address like "ONE BROADWAY"
    ordinal_pattern = re.compile(
        r"\b(ONE|TWO|THREE|FOUR|FIVE|SIX|SEVEN|EIGHT|NINE|TEN)\s+\w+\s+(ST|AVE|BLVD|RD|STREET|AVENUE)\b",
        re.IGNORECASE,
    )
    keywords = ["OWNER LLC", "PROPERTY LLC", "HOLDINGS LLC", "REALTY LLC"]
    if street_pattern.search(name_upper):
        return True
    if ordinal_pattern.search(name_upper):
        return True
    if any(k in name_upper for k in keywords) and re.search(r"\d", name_upper):
        return True
    return False


# ============================================================
# Seen Records — dedup guard
# ============================================================

def already_seen(source: str, external_id: str, new_checksum: str = None) -> bool:
    """Return True if record has been seen and hasn't changed."""
    res = db().table("seen_records")\
        .select("id, checksum")\
        .eq("source", source)\
        .eq("external_id", str(external_id))\
        .execute()
    if not res.data:
        return False
    if new_checksum and res.data[0]["checksum"] != new_checksum:
        # Record changed — update checksum so we reprocess
        db().table("seen_records")\
            .update({"checksum": new_checksum})\
            .eq("source", source)\
            .eq("external_id", str(external_id))\
            .execute()
        return False
    return True


def mark_seen(source: str, external_id: str, chk: str = None):
    db().table("seen_records").upsert({
        "source": source,
        "external_id": str(external_id),
        "checksum": chk or "",
    }, on_conflict="source,external_id").execute()


# ============================================================
# Properties
# ============================================================

def upsert_property(bbl: str, data: dict) -> str:
    """Upsert a property by BBL. Returns the property UUID."""
    data["bbl"] = bbl
    res = db().table("properties").upsert(data, on_conflict="bbl").execute()
    return res.data[0]["id"]


# ============================================================
# Entities
# ============================================================

def find_entity_by_name(name: str) -> Optional[dict]:
    """Find an existing entity using normalized name (exact match after normalization)."""
    norm = normalize_name(name)
    res = db().table("entities")\
        .select("*")\
        .eq("normalized_name", norm)\
        .limit(1)\
        .execute()
    return res.data[0] if res.data else None


def upsert_entity(name: str, entity_type: str, extra: dict = None) -> str:
    """
    Find or create an entity. Returns the UUID.
    Uses normalized name for deduplication.
    """
    norm = normalize_name(name)
    existing = find_entity_by_name(name)
    if existing:
        entity_id = existing["id"]
        # Update fields if we have new info
        if extra:
            db().table("entities").update(extra).eq("id", entity_id).execute()
        return entity_id

    payload = {
        "name": name,
        "normalized_name": norm,
        "entity_type": entity_type,
        "is_building_llc": is_building_llc(name),
        **(extra or {}),
    }
    res = db().table("entities").insert(payload).execute()
    entity_id = res.data[0]["id"]

    # Queue for enrichment if it's a new entity
    queue_for_enrichment(entity_id, enrichment_type="both")
    log.info("entity.created", name=name, entity_type=entity_type, id=entity_id)
    return entity_id


def update_entity(entity_id: str, data: dict):
    db().table("entities").update(data).eq("id", entity_id).execute()


# ============================================================
# Entity Relationships
# ============================================================

def upsert_relationship(child_entity_id: str, parent_entity_id: str, rel_type: str,
                         source: str, confidence: float = 0.7, evidence: str = None):
    db().table("entity_relationships").upsert({
        "child_entity_id": child_entity_id,
        "parent_entity_id": parent_entity_id,
        "relationship_type": rel_type,
        "source": source,
        "confidence": confidence,
        "evidence": evidence,
    }, on_conflict="child_entity_id,parent_entity_id,relationship_type").execute()


# ============================================================
# Property Roles
# ============================================================

def upsert_property_role(property_id: str, entity_id: str, role: str, source: str, extra: dict = None):
    db().table("property_roles").upsert({
        "property_id": property_id,
        "entity_id": entity_id,
        "role": role,
        "source": source,
        "is_current": True,
        **(extra or {}),
    }, on_conflict="property_id,entity_id,role,source").execute()


# ============================================================
# Contacts
# ============================================================

def upsert_contact(entity_id: str, data: dict) -> str:
    """Upsert a contact. Deduplicates on (entity_id, email)."""
    data["entity_id"] = entity_id
    if not data.get("full_name") and data.get("first_name"):
        data["full_name"] = f"{data.get('first_name', '')} {data.get('last_name', '')}".strip()

    if data.get("email"):
        res = db().table("contacts").upsert(
            data, on_conflict="entity_id,email"
        ).execute()
    else:
        # No email — check by name + entity
        existing = db().table("contacts")\
            .select("id")\
            .eq("entity_id", entity_id)\
            .eq("full_name", data.get("full_name", ""))\
            .limit(1)\
            .execute()
        if existing.data:
            db().table("contacts").update(data).eq("id", existing.data[0]["id"]).execute()
            return existing.data[0]["id"]
        res = db().table("contacts").insert(data).execute()
    return res.data[0]["id"]


# ============================================================
# Enrichment Queue
# ============================================================

def queue_for_enrichment(entity_id: str, enrichment_type: str = "both", priority: int = 5):
    """Add an entity to the enrichment queue if not already there."""
    db().table("enrichment_queue").upsert({
        "entity_id": entity_id,
        "enrichment_type": enrichment_type,
        "priority": priority,
    }, on_conflict="entity_id").execute()


def get_enrichment_batch(limit: int = 50, enrichment_type: str = None) -> list[dict]:
    """Fetch next batch of entities to enrich, ordered by priority."""
    query = db().table("enrichment_queue")\
        .select("*, entities(*)")\
        .lt("attempts", 3)\
        .order("priority", desc=False)\
        .order("created_at", desc=False)\
        .limit(limit)
    if enrichment_type:
        query = query.eq("enrichment_type", enrichment_type)
    res = query.execute()
    return res.data


def mark_enrichment_done(entity_id: str):
    db().table("enrichment_queue").delete().eq("entity_id", entity_id).execute()
    update_entity(entity_id, {"enrichment_status": "done"})


def mark_enrichment_failed(entity_id: str, error: str):
    db().table("enrichment_queue")\
        .update({"attempts": db().table("enrichment_queue")
                 .select("attempts").eq("entity_id", entity_id).execute().data[0]["attempts"] + 1,
                 "error_message": error,
                 "last_attempt_at": "now()"})\
        .eq("entity_id", entity_id)\
        .execute()
    update_entity(entity_id, {"enrichment_status": "failed"})


# ============================================================
# Ingestion Log
# ============================================================

def start_ingestion_log(source: str) -> str:
    res = db().table("ingestion_log").insert({"source": source, "status": "running"}).execute()
    return res.data[0]["id"]


def finish_ingestion_log(log_id: str, stats: dict, status: str = "success", error: str = None):
    db().table("ingestion_log").update({
        "run_finished_at": "now()",
        "status": status,
        "error_message": error,
        **stats,
    }).eq("id", log_id).execute()
