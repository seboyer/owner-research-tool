#!/usr/bin/env python3
"""
eval_apollo_org.py — Manual validation harness for Apollo org→people endpoints.

Run this BEFORE enabling Apollo in the company enrichment cascade to verify:
  1. The org search endpoint matches known NYC management companies.
  2. The people endpoint returns senior contacts with useful email/phone data.
  3. Results are not obviously stale or wrong.

Usage:
    python scripts/eval_apollo_org.py

Requires APOLLO_API_KEY in .env.
DO NOT run in CI — this costs ~$0.05 per company (5 people hits @ $0.01 each).
"""

import asyncio
import os
import sys

# Ensure project root is on the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from enrichment.contact.sources.apollo_org import (
    apollo_org_people,
    apollo_org_search,
    apollo_person_enrich_by_id,
)

# Known NYC management / ownership companies used as ground truth.
# After running, verify:
#   - org match looks correct (right company, not a subsidiary or homonym)
#   - people have recognisable NYC real-estate titles (CEO, President, Owner, VP, etc.)
#   - emails look real (not test/placeholder accounts)
TEST_COMPANIES = [
    "Stonehenge NYC",
    "Related Companies",
    "Brookfield Properties",
    "A&E Real Estate",
    "Vornado Realty Trust",
]


async def _eval_one(name: str, enrich_top: int = 0):
    """Run org search → people discovery; if enrich_top > 0, enrich that many
    top candidates with has_email=true to retrieve full name + verified email.
    enrich_top costs ~$1 per person — set to 0 for a free discovery-only pass.
    """
    print(f"\n{'='*60}")
    print(f"Company: {name}")
    print(f"{'='*60}")

    org_id = await apollo_org_search(name)
    if not org_id:
        print("  [NO ORG MATCH]")
        return

    print(f"  Apollo org_id: {org_id}")

    people = await apollo_org_people(org_id, per_page=10)
    if not people:
        print("  [NO PEOPLE FOUND]")
        return

    print(f"  Discovery: {len(people)} people")
    for i, p in enumerate(people[:5], 1):
        raw = p.raw or {}
        flags = []
        if raw.get("has_email"):
            flags.append("email")
        if str(raw.get("has_direct_phone") or "").lower() == "yes":
            flags.append("phone")
        avail = ",".join(flags) or "no contact data"
        print(
            f"  {i}. {p.full_name or '(no name)'} | {p.title or '(no title)'} "
            f"| avail={avail}"
        )

    if enrich_top > 0:
        # Rank by data availability (email > phone) and enrich top N
        def avail_score(h):
            raw = h.raw or {}
            e = 1 if raw.get("has_email") else 0
            p_flag = str(raw.get("has_direct_phone") or "").lower() == "yes"
            return (e * 2) + (1 if p_flag else 0)

        ranked = sorted(people, key=avail_score, reverse=True)
        candidates = [
            h for h in ranked[:enrich_top]
            if (h.raw or {}).get("has_email") and (h.raw or {}).get("id")
        ]
        print(f"\n  --- Enriching top {len(candidates)} candidate(s) (~$1 each) ---")
        for i, h in enumerate(candidates, 1):
            pid = h.raw["id"]
            enriched = await apollo_person_enrich_by_id(pid)
            if not enriched:
                print(f"  {i}. {h.full_name} | [enrichment returned no data]")
                continue
            print(
                f"  {i}. {enriched.full_name} | {enriched.title or '(no title)'} | "
                f"email={enriched.email or '—'} | linkedin={enriched.linkedin_url or '—'}"
            )


async def main():
    from config import config
    if not config.APOLLO_API_KEY:
        print("ERROR: APOLLO_API_KEY not set in .env — aborting.")
        sys.exit(1)

    # Pass --enrich N on the CLI to spend Apollo credits enriching the top N
    # candidates per company. Defaults to 0 (free discovery-only mode).
    enrich_top = 0
    for arg in sys.argv[1:]:
        if arg.startswith("--enrich="):
            enrich_top = int(arg.split("=", 1)[1])

    # When enriching, restrict to first 2 companies to keep cost predictable
    # (~$1 per enriched person × 2 enrichments × 2 companies ≈ $4 total).
    companies = TEST_COMPANIES[:2] if enrich_top > 0 else TEST_COMPANIES

    print("Apollo org→people evaluation")
    print(f"Testing {len(companies)} companies "
          f"(discovery-only; pass --enrich=N to enrich top N per company)"
          if enrich_top == 0
          else f"Testing {len(companies)} companies with --enrich={enrich_top} "
               f"(~${enrich_top * len(companies)} spend estimated)")

    for company in companies:
        await _eval_one(company, enrich_top=enrich_top)
        # Small sleep to avoid hammering Apollo in a tight loop
        await asyncio.sleep(0.5)

    print("\nDone. Review output above before enabling Apollo in the cascade.")
    if enrich_top == 0:
        print("To verify real email retrieval, re-run with:")
        print("  ./venv/bin/python scripts/eval_apollo_org.py --enrich=2")


if __name__ == "__main__":
    asyncio.run(main())
