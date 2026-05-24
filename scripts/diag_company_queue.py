#!/usr/bin/env python3
"""Diagnostic for company_enrich queue + orphan reports.

Read-only — safe to run any time. Prints:
  1. Recent ingestion_log rows for company_enrich / multi_source_enrich
     (to surface orphans)
  2. enrichment_queue depth by type, raw vs. allowed_enrichment_queue view
  3. For company_enrich specifically, why pending rows are being filtered
     by the view — direct property_role missing? entity_relationships
     present?
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from database.client import db


def section(t: str):
    print(f"\n{'='*70}\n{t}\n{'='*70}")


def main():
    # ── 1. Recent ingestion_log for the two suspect sources ────────────────
    section("Recent ingestion_log (last 10 each for company_enrichment / multi_source_enrichment)")
    for src in ("company_enrichment", "multi_source_enrichment"):
        res = (
            db().table("ingestion_log")
            .select("id, status, run_started_at, run_finished_at, records_fetched, error_message")
            .eq("source", src)
            .order("run_started_at", desc=True)
            .limit(10)
            .execute()
        )
        print(f"\n-- {src} --")
        for r in res.data or []:
            err = (r.get("error_message") or "")[:60]
            print(
                f"  {r['run_started_at']}  status={r['status']:8s}  "
                f"fetched={r.get('records_fetched') or 0:<4}  err={err!r}"
            )

    # ── 2. Queue depth: raw vs allowed view ────────────────────────────────
    section("enrichment_queue depth — raw vs. allowed_enrichment_queue view")
    for tbl in ("enrichment_queue", "allowed_enrichment_queue"):
        rows = db().table(tbl).select("enrichment_type").limit(50000).execute().data or []
        counts: dict[str, int] = {}
        for r in rows:
            t = r.get("enrichment_type") or "?"
            counts[t] = counts.get(t, 0) + 1
        print(f"\n-- {tbl} --")
        for k in sorted(counts):
            print(f"  {k:20s} {counts[k]:>6d}")

    # ── 3. Why are company_enrich rows being view-filtered? ────────────────
    section("Sample of company_enrich rows visible in raw queue but NOT in allowed view")

    # Pull raw queue (limit large) and allowed view; subtract.
    raw = (
        db().table("enrichment_queue")
        .select("entity_id, attempts, last_attempt_at")
        .eq("enrichment_type", "company_enrich")
        .limit(2000)
        .execute()
        .data or []
    )
    allowed = (
        db().table("allowed_enrichment_queue")
        .select("entity_id")
        .eq("enrichment_type", "company_enrich")
        .limit(2000)
        .execute()
        .data or []
    )
    allowed_ids = {r["entity_id"] for r in allowed}
    filtered = [r for r in raw if r["entity_id"] not in allowed_ids]
    print(f"\nraw company_enrich rows:     {len(raw)}")
    print(f"allowed view rows:           {len(allowed)}")
    print(f"filtered out by view:        {len(filtered)}")

    if filtered:
        print("\nFor up to 10 filtered entities — explain why:")
        for r in filtered[:10]:
            eid = r["entity_id"]
            ent = (
                db().table("entities")
                .select("id, name, entity_type, role_category, portfolio_size, enrichment_status")
                .eq("id", eid).limit(1).execute().data
            )
            if not ent:
                continue
            ent = ent[0]
            # Has direct property_role?
            pr = (
                db().table("property_roles")
                .select("property_id", count="exact")
                .eq("entity_id", eid)
                .limit(1).execute()
            )
            pr_count = pr.count if hasattr(pr, "count") else len(pr.data or [])
            # Has child entity via entity_relationships (parent_entity_id=eid)?
            rels = (
                db().table("entity_relationships")
                .select("child_entity_id, relationship_type")
                .eq("parent_entity_id", eid)
                .limit(5).execute().data or []
            )
            rel_summary = ", ".join(
                f"{r['relationship_type']}→{r['child_entity_id'][:8]}…" for r in rels
            ) or "(none)"
            print(
                f"  {ent['name'][:40]:40s} type={ent.get('entity_type'):20s} "
                f"role={str(ent.get('role_category'))[:15]:15s} "
                f"portfolio={ent.get('portfolio_size') or 0:<3} "
                f"property_roles={pr_count} attempts={r.get('attempts') or 0} | rels: {rel_summary}"
            )


if __name__ == "__main__":
    main()
