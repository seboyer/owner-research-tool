#!/usr/bin/env python3
"""Diagnostic: bucket every pending company_enrich entity by why it is /
isn't visible in allowed_enrichment_queue.

Buckets (mutually exclusive — each entity counted once):
  A. direct property_role, zip/borough allowed       → already in view
  B. direct property_role, NOT in allowlist          → allowlist-gated (by design)
  C. NO direct property_role, has child via rels,
     child has property_role in allowlist            → STUCK (view bug — would be
                                                       fixed by allowing indirect
                                                       links in the view)
  D. NO direct property_role, has child via rels,
     child not in allowlist                          → indirect + allowlist-gated
  E. No direct property_role, no child via rels      → orphan (no buildings at all)

Read-only. Run with `./venv/bin/python scripts/diag_prong1_orphans.py`.
"""
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database.client import db


def main():
    # Pull all company_enrich entity_ids — supabase capped at 1000 per page,
    # so paginate manually.
    eids: list[str] = []
    page_size = 1000
    offset = 0
    while True:
        page = (
            db().table("enrichment_queue")
            .select("entity_id")
            .eq("enrichment_type", "company_enrich")
            .range(offset, offset + page_size - 1)
            .execute()
            .data or []
        )
        if not page:
            break
        eids.extend(r["entity_id"] for r in page)
        if len(page) < page_size:
            break
        offset += page_size
    print(f"Total pending company_enrich entities: {len(eids)}")

    # Build a set of allowed zip codes + allowed boroughs (from migration's view defn)
    allowed_zips = {
        r["zip_code"] for r in
        db().table("zipcode_allowlist").select("zip_code, enabled").execute().data or []
        if r.get("enabled")
    }
    allowed_boros = {
        r["borough_code"] for r in
        db().table("borough_allowlist").select("borough_code, enabled").execute().data or []
        if r.get("enabled")
    }
    print(f"Allowlist: {len(allowed_zips)} zips ({sorted(allowed_zips)}), "
          f"{len(allowed_boros)} boroughs ({sorted(allowed_boros)})")

    def zip_ok(p: dict) -> bool:
        if (p.get("zip_code") or "") in allowed_zips:
            return True
        if (not p.get("zip_code")) and (p.get("bbl") or "")[:1] in allowed_boros:
            return True
        return False

    buckets: Counter = Counter()
    bucket_C_samples: list[str] = []  # the ones the view fix would unlock

    # Walk in chunks of 100 (in_ filter handles up to ~1000 but chunking keeps
    # the JOIN payload sane)
    CHUNK = 100
    for i in range(0, len(eids), CHUNK):
        chunk = eids[i : i + CHUNK]

        # Direct property_roles → property info
        direct = (
            db().table("property_roles")
            .select("entity_id, properties(bbl, zip_code)")
            .in_("entity_id", chunk)
            .eq("is_current", True)
            .execute().data or []
        )
        direct_by_eid: dict[str, list[dict]] = {}
        for r in direct:
            p = r.get("properties") or {}
            if p:
                direct_by_eid.setdefault(r["entity_id"], []).append(p)

        # Indirect: this entity is parent_entity_id in entity_relationships;
        # find children, then their property_roles.
        rels = (
            db().table("entity_relationships")
            .select("parent_entity_id, child_entity_id, relationship_type")
            .in_("parent_entity_id", chunk)
            .in_("relationship_type", ["managed_by", "operates_as", "owned_by"])
            .execute().data or []
        )
        rels_by_eid: dict[str, list[str]] = {}
        for r in rels:
            rels_by_eid.setdefault(r["parent_entity_id"], []).append(r["child_entity_id"])

        # Look up properties for all child ids in this chunk
        child_ids = {c for cs in rels_by_eid.values() for c in cs}
        child_props: dict[str, list[dict]] = {}
        if child_ids:
            cp = (
                db().table("property_roles")
                .select("entity_id, properties(bbl, zip_code)")
                .in_("entity_id", list(child_ids))
                .eq("is_current", True)
                .execute().data or []
            )
            for r in cp:
                p = r.get("properties") or {}
                if p:
                    child_props.setdefault(r["entity_id"], []).append(p)

        # Resolve names for bucket C samples (limit fetches)
        name_lookup: dict[str, str] = {}
        if any(eid not in direct_by_eid and eid in rels_by_eid for eid in chunk):
            need_names = [eid for eid in chunk if eid not in direct_by_eid and eid in rels_by_eid]
            if need_names:
                nm = (
                    db().table("entities")
                    .select("id, name, entity_type, role_category")
                    .in_("id", need_names[:50])  # cap names lookup
                    .execute().data or []
                )
                for e in nm:
                    name_lookup[e["id"]] = f"{e['name']} [{e.get('entity_type')}/{e.get('role_category')}]"

        for eid in chunk:
            direct_props = direct_by_eid.get(eid, [])
            if direct_props:
                if any(zip_ok(p) for p in direct_props):
                    buckets["A_direct_allowed"] += 1
                else:
                    buckets["B_direct_blocked"] += 1
                continue

            # No direct property_roles
            child_ids_for_eid = rels_by_eid.get(eid, [])
            if not child_ids_for_eid:
                buckets["E_orphan_no_buildings"] += 1
                continue

            # Has children via rels — do any have property_roles in allowlist?
            all_child_props: list[dict] = []
            for cid in child_ids_for_eid:
                all_child_props.extend(child_props.get(cid, []))

            if not all_child_props:
                # children exist but they have no property_roles — also orphan-ish
                buckets["E_orphan_no_buildings"] += 1
                continue

            if any(zip_ok(p) for p in all_child_props):
                buckets["C_indirect_allowed_STUCK"] += 1
                if len(bucket_C_samples) < 15 and eid in name_lookup:
                    bucket_C_samples.append(name_lookup[eid])
            else:
                buckets["D_indirect_blocked"] += 1

    # Report
    print("\n" + "=" * 70)
    print("Bucket counts:")
    print("=" * 70)
    total = sum(buckets.values())
    for label in [
        "A_direct_allowed",
        "B_direct_blocked",
        "C_indirect_allowed_STUCK",
        "D_indirect_blocked",
        "E_orphan_no_buildings",
    ]:
        n = buckets.get(label, 0)
        pct = (100.0 * n / total) if total else 0.0
        print(f"  {label:32s} {n:>6d}  ({pct:5.1f}%)")
    print(f"  {'TOTAL':32s} {total:>6d}")

    if bucket_C_samples:
        print("\nSample of bucket-C (STUCK — view fix would unlock):")
        for s in bucket_C_samples:
            print(f"  {s}")


if __name__ == "__main__":
    main()
