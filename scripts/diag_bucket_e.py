#!/usr/bin/env python3
"""Classify bucket E — entities in the company_enrich queue with no buildings
linked (no direct property_role, no usable child via entity_relationships).

For each, surface:
  - entity_type / role_category (prong1 sets owner_operating / management;
    upsert_entity from elsewhere sets llc / corporation / management_company)
  - created_at (recent → likely prong1; old → likely stale ingest path)
  - any entity_relationships at all (as parent OR child)
  - how many attempts the queue row has

Read-only.
"""
import os
import sys
from collections import Counter
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database.client import db


def main():
    # Pull all company_enrich entity ids (paginated)
    eids: list[str] = []
    offset, page = 0, 1000
    while True:
        rows = (
            db().table("enrichment_queue")
            .select("entity_id, attempts, created_at")
            .eq("enrichment_type", "company_enrich")
            .range(offset, offset + page - 1)
            .execute().data or []
        )
        if not rows:
            break
        eids.extend(rows)
        if len(rows) < page:
            break
        offset += page
    queue_meta = {r["entity_id"]: r for r in eids}
    eid_list = [r["entity_id"] for r in eids]
    print(f"Total pending company_enrich entities: {len(eid_list)}")

    # Re-derive bucket-E (same logic as diag_prong1_orphans.py)
    bucket_e: list[str] = []
    CHUNK = 100
    for i in range(0, len(eid_list), CHUNK):
        chunk = eid_list[i : i + CHUNK]

        direct = (
            db().table("property_roles")
            .select("entity_id")
            .in_("entity_id", chunk)
            .eq("is_current", True)
            .execute().data or []
        )
        has_direct = {r["entity_id"] for r in direct}

        rels_parent = (
            db().table("entity_relationships")
            .select("parent_entity_id, child_entity_id")
            .in_("parent_entity_id", chunk)
            .in_("relationship_type", ["managed_by", "operates_as", "owned_by"])
            .execute().data or []
        )
        rels_by_eid: dict[str, list[str]] = {}
        for r in rels_parent:
            rels_by_eid.setdefault(r["parent_entity_id"], []).append(r["child_entity_id"])

        child_ids = {c for cs in rels_by_eid.values() for c in cs}
        children_with_props = set()
        if child_ids:
            cp = (
                db().table("property_roles")
                .select("entity_id")
                .in_("entity_id", list(child_ids))
                .eq("is_current", True)
                .execute().data or []
            )
            children_with_props = {r["entity_id"] for r in cp}

        for eid in chunk:
            if eid in has_direct:
                continue
            kids = rels_by_eid.get(eid, [])
            if any(k in children_with_props for k in kids):
                continue
            # bucket E
            bucket_e.append(eid)

    print(f"Bucket E count: {len(bucket_e)}\n")

    # Fetch full metadata for bucket E
    e_meta: list[dict] = []
    for i in range(0, len(bucket_e), 100):
        chunk = bucket_e[i : i + 100]
        rows = (
            db().table("entities")
            .select("id, name, entity_type, role_category, created_at, enrichment_status")
            .in_("id", chunk)
            .execute().data or []
        )
        e_meta.extend(rows)
    by_eid = {r["id"]: r for r in e_meta}

    # Bucket E has any entity_relationships at all (as parent OR child)?
    has_any_rels: set[str] = set()
    for i in range(0, len(bucket_e), 100):
        chunk = bucket_e[i : i + 100]
        as_parent = (
            db().table("entity_relationships")
            .select("parent_entity_id")
            .in_("parent_entity_id", chunk).execute().data or []
        )
        as_child = (
            db().table("entity_relationships")
            .select("child_entity_id")
            .in_("child_entity_id", chunk).execute().data or []
        )
        for r in as_parent:
            has_any_rels.add(r["parent_entity_id"])
        for r in as_child:
            has_any_rels.add(r["child_entity_id"])

    # Classifications
    et_counter: Counter = Counter()
    rc_counter: Counter = Counter()
    rels_counter: Counter = Counter()
    age_counter: Counter = Counter()
    now = datetime.now(timezone.utc)

    for eid in bucket_e:
        m = by_eid.get(eid, {})
        et_counter[m.get("entity_type") or "(null)"] += 1
        rc_counter[m.get("role_category") or "(null)"] += 1
        rels_counter["has_rels" if eid in has_any_rels else "no_rels"] += 1

        created = m.get("created_at")
        if created:
            try:
                dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                days = (now - dt).days
                if days < 7:
                    age_counter["<7d"] += 1
                elif days < 30:
                    age_counter["7-30d"] += 1
                elif days < 90:
                    age_counter["30-90d"] += 1
                else:
                    age_counter[">90d"] += 1
            except Exception:
                age_counter["?"] += 1
        else:
            age_counter["?"] += 1

    def show(label: str, c: Counter):
        print(f"\n{label}:")
        for k, n in c.most_common():
            print(f"  {k:30s} {n}")

    show("By entity_type", et_counter)
    show("By role_category", rc_counter)
    show("By rels (any relationships at all?)", rels_counter)
    show("By age (created_at)", age_counter)

    # Sample of recent + role_category set (likely prong1)
    print("\nSample — created <30 days ago AND role_category set (likely prong1):")
    shown = 0
    for eid in bucket_e:
        m = by_eid.get(eid)
        if not m or not m.get("role_category"):
            continue
        created = m.get("created_at")
        try:
            dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            if (now - dt).days > 30:
                continue
        except Exception:
            continue
        rels_marker = "has_rels" if eid in has_any_rels else "NO_RELS"
        print(f"  {m['name'][:45]:45s} type={m.get('entity_type'):20s} "
              f"role={m.get('role_category'):20s} created={created[:10]} {rels_marker}")
        shown += 1
        if shown >= 15:
            break


if __name__ == "__main__":
    main()
