"""
test_addresses.py — Targeted test: ingest 5 specific addresses end-to-end.

Steps:
  1. Geocode each address → BBL via NYC Planning GeoSearch API
  2. Pull HPD registration data for those BBLs
  3. Pull ACRIS mortgage data for those BBLs
  4. Store everything in Supabase (properties, entities, property_roles, contacts)
  5. Run LLC piercing on owner entities found
  6. Run contact enrichment on any mortgage signers identified
"""

import asyncio
import json
import re
import sys
import httpx
import structlog
from dotenv import load_dotenv

load_dotenv()

from config import config
from database.client import (
    db, upsert_property, upsert_entity, upsert_contact,
    upsert_property_role, normalize_name,
)
from enrichment.llc_piercer import pierce_entity
from enrichment.contact import enrich_signer, enrich_pending_signers

log = structlog.get_logger(__name__)
structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(20))  # INFO

ADDRESSES = [
    "1100 Bedford Ave, Brooklyn, NY 11216",
    "67 Woodhull St, Brooklyn, NY 11231",
    "774 Lexington Ave, Brooklyn, NY 11221",
    "66-14 Stier Ave, Flushing, NY 11385",
    "10 Argyle Road, Brooklyn, NY 11218",
]

BOROUGH_CODES = {
    "Manhattan": "1", "Bronx": "2", "Brooklyn": "3", "Queens": "4", "Staten Island": "5",
}

ACRIS_DEED_DOC_TYPES = ("DEED", "DEED, LE", "DEED, RC", "DEED, TS", "DEED, PC")

HPD_CONTACT_TYPES = {
    "CorporateOwner": ("llc", "owner"),
    "Owner":           ("individual", "owner"),
    "IndividualOwner": ("individual", "owner"),
    "JointOwner":      ("individual", "owner"),
    "HeadOfficer":     ("individual", "owner"),
    "Agent":           ("management_company", "manager"),
    "Officer":         ("individual", "owner"),
}


# ============================================================
# Step 1: Geocode address → BBL
# ============================================================

async def geocode_address(address: str, client: httpx.AsyncClient) -> dict | None:
    """Use NYC Planning Labs GeoSearch to get BBL + normalized address."""
    url = "https://geosearch.planninglabs.nyc/v2/search"
    resp = await client.get(url, params={"text": address, "size": 1}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    features = data.get("features", [])
    if not features:
        print(f"  ⚠️  No geocode result for: {address}")
        return None
    props = features[0]["properties"]
    pad_bbl = (props.get("addendum") or {}).get("pad", {}).get("bbl", "") or props.get("pad_bbl", "")
    borough = props.get("borough", "")
    label = props.get("label", address)
    if not pad_bbl or pad_bbl == "0":
        print(f"  ⚠️  No BBL in geocode result for: {address}")
        return None
    return {
        "bbl": pad_bbl,
        "address": label,
        "borough": borough,
        "zip": props.get("postalcode", ""),
    }


# ============================================================
# Step 2: HPD — pull registration + contacts for a BBL
# ============================================================

async def ingest_hpd_for_bbl(bbl: str, address: str, borough: str, zip_code: str, client: httpx.AsyncClient) -> str | None:
    """Fetch HPD registrations and contacts for a single BBL, store in DB."""
    boro = bbl[0]
    block = bbl[1:6].lstrip("0")
    lot = bbl[6:].lstrip("0")

    # Query HPD Registrations to get registrationid
    reg_url = config.HPD_REGISTRATIONS_URL
    params = {
        "$where": f"boroid='{boro}' AND block='{block}' AND lot='{lot}'",
        "$limit": 5,
        "$order": "lastregistrationdate DESC",
    }
    if config.NYC_OPENDATA_APP_TOKEN:
        params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

    resp = await client.get(reg_url, params=params, timeout=20)
    regs = resp.json() if resp.status_code == 200 else []
    if not regs:
        print(f"  ℹ️  No HPD registrations for BBL {bbl}")
        return None

    # Upsert the property
    reg = regs[0]
    house_num = reg.get('housenumber', '').strip()
    street_nm = reg.get('streetname', '').strip()
    prop_address = f"{house_num} {street_nm}".strip() or address
    property_id = upsert_property(bbl, {
        "bbl": bbl,
        "borough": borough,
        "block": bbl[1:6],
        "lot": bbl[6:],
        "address": prop_address,
        "house_number": house_num or None,
        "street_name": street_nm or None,
        "zip_code": zip_code or reg.get("zipcode"),
        "unit_count": reg.get("unitcount"),
        "hpd_reg_id": reg.get("registrationid"),
        "raw_data": reg,
    })
    print(f"  ✓ Property stored: {prop_address} (BBL {bbl})")

    # Now pull contacts for all active registrations at this BBL
    reg_ids = [r["registrationid"] for r in regs]
    contacts_url = config.HPD_CONTACTS_URL
    ids_clause = " OR ".join(f"registrationid='{rid}'" for rid in reg_ids)
    c_params = {
        "$where": ids_clause,
        "$limit": 50,
    }
    if config.NYC_OPENDATA_APP_TOKEN:
        c_params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

    cresp = await client.get(contacts_url, params=c_params, timeout=20)
    contacts = cresp.json() if cresp.status_code == 200 else []

    for c in contacts:
        ctype = c.get("type", "")
        if ctype not in HPD_CONTACT_TYPES:
            continue
        entity_type, role = HPD_CONTACT_TYPES[ctype]

        corp = c.get("corporationname", "").strip()
        first = c.get("firstname", "").strip()
        last = c.get("lastname", "").strip()
        name = corp if corp else f"{first} {last}".strip()
        if not name:
            continue

        addr_parts = [c.get("businesshousenumber", ""), c.get("businessstreetname", "")]
        ent_addr = " ".join(p for p in addr_parts if p).strip()

        ent_id = upsert_entity(name, entity_type, extra={
            "address": ent_addr or None,
            "zip_code": c.get("businesszip") or None,
        })

        if first and last:
            upsert_contact(ent_id, {
                "first_name": first,
                "last_name": last,
                "full_name": f"{first} {last}",
                "source": "hpd",
                "confidence": 0.9,
            })

        upsert_property_role(property_id, ent_id, role, "hpd")
        print(f"    → HPD {ctype}: {name}")

    return property_id


# ============================================================
# Step 3: ACRIS — pull mortgage documents for a BBL
# ============================================================

async def ingest_acris_for_bbl(bbl: str, property_id: str, client: httpx.AsyncClient):
    """Fetch ACRIS mortgage records for a BBL, store owner entities."""
    boro = bbl[0]
    block = bbl[1:6].lstrip("0") or "0"
    lot = bbl[6:].lstrip("0") or "0"

    # Query ACRIS Legals (property → doc_id)
    legals_url = config.ACRIS_LEGALS_URL
    params = {
        "$where": f"borough='{boro}' AND block='{block}' AND lot='{lot}'",
        "$limit": 20,
        "$order": "doc_date DESC",
    }
    if config.NYC_OPENDATA_APP_TOKEN:
        params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

    resp = await client.get(legals_url, params=params, timeout=20)
    legals = resp.json() if resp.status_code == 200 else []
    if not legals:
        print(f"  ℹ️  No ACRIS records for BBL {bbl}")
        return

    # Get mortgage/deed doc_ids
    doc_ids = [l["document_id"] for l in legals[:10] if l.get("document_id")]
    if not doc_ids:
        return

    # Query ACRIS Master to find mortgage/deed docs
    master_url = config.ACRIS_MASTER_URL
    ids_clause = " OR ".join(f"document_id='{did}'" for did in doc_ids)
    m_params = {
        "$where": f"({ids_clause}) AND doc_type IN ('MTGE','DEED','AL&R','AGMT')",
        "$limit": 10,
        "$order": "doc_date DESC",
    }
    if config.NYC_OPENDATA_APP_TOKEN:
        m_params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

    mresp = await client.get(master_url, params=m_params, timeout=20)
    masters = mresp.json() if mresp.status_code == 200 else []
    if not masters:
        print(f"  ℹ️  No ACRIS mortgage/deed docs for BBL {bbl}")
        return

    # For each doc, get the parties (grantor = seller/borrower = typically the LLC)
    parties_url = config.ACRIS_PARTIES_URL
    for doc in masters[:5]:
        doc_id = doc.get("document_id", "")
        doc_type = doc.get("doc_type", "")
        doc_date = doc.get("doc_date", "")[:10] if doc.get("doc_date") else ""

        p_params = {
            "$where": f"document_id='{doc_id}'",
            "$limit": 20,
        }
        if config.NYC_OPENDATA_APP_TOKEN:
            p_params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

        presp = await client.get(parties_url, params=p_params, timeout=20)
        parties = presp.json() if presp.status_code == 200 else []

        for party in parties:
            ptype = party.get("party_type", "")  # "1" = grantor, "2" = grantee
            name = party.get("name", "").strip()
            if not name:
                continue

            # For deeds: party_type=1 is grantor (seller), party_type=2 is grantee (buyer/current owner)
            # For mortgages: party_type=1 is borrower (the LLC), party_type=2 is lender (bank)
            if doc_type == "DEED" and ptype == "2":
                role = "owner"
                ent_type = "llc" if "LLC" in name.upper() else "individual"
            elif doc_type == "MTGE" and ptype == "1":
                role = "owner"
                ent_type = "llc" if "LLC" in name.upper() else "individual"
            else:
                continue

            addr_parts = [
                party.get("addr1", ""), party.get("addr2", ""),
                party.get("city", ""), party.get("state", ""),
            ]
            ent_addr = ", ".join(p for p in addr_parts if p).strip(", ")

            ent_id = upsert_entity(name, ent_type, extra={
                "address": ent_addr or None,
            })
            upsert_property_role(property_id, ent_id, role, "acris", extra={
                "doc_type": doc_type,
                "doc_date": doc_date,
            })
            print(f"    → ACRIS {doc_type} ({doc_date}) [{ptype}]: {name}")


# ============================================================
# Step 3b: ACRIS Deed Fallback — for properties with no HPD registration
# ============================================================

async def ingest_acris_deed_fallback(bbl: str, address: str, borough: str, zip_code: str,
                                      client: httpx.AsyncClient) -> str | None:
    """
    For small/commercial buildings outside HPD's 3-unit threshold,
    look up the current owner via ACRIS deed records (the most recent
    DEED doc's grantee = current owner).
    """
    boro = bbl[0]
    block = str(int(bbl[1:6]))
    lot = str(int(bbl[6:]))

    # 1. Get document IDs for this BBL from ACRIS Legals
    params = {
        "$where": f"borough='{boro}' AND block='{block}' AND lot='{lot}'",
        "$limit": 50,
        "$order": "doc_date DESC",
    }
    if config.NYC_OPENDATA_APP_TOKEN:
        params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

    resp = await client.get(config.ACRIS_LEGALS_URL, params=params, timeout=20)
    legals = resp.json() if resp.status_code == 200 else []
    if not legals:
        print(f"  ⚠️  No ACRIS records at all for BBL {bbl}")
        return None

    doc_ids = list({l["document_id"] for l in legals if l.get("document_id")})[:30]

    # 2. Filter to DEED doc types only (most recent first)
    types_clause = ",".join(f"'{t}'" for t in ACRIS_DEED_DOC_TYPES)
    ids_clause = ",".join(f"'{d}'" for d in doc_ids)
    m_params = {
        "$where": f"document_id in ({ids_clause}) AND doc_type in ({types_clause})",
        "$limit": 5,
        "$order": "recorded_datetime DESC",
    }
    if config.NYC_OPENDATA_APP_TOKEN:
        m_params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

    mresp = await client.get(config.ACRIS_MASTER_URL, params=m_params, timeout=20)
    deeds = mresp.json() if mresp.status_code == 200 else []
    if not deeds:
        print(f"  ⚠️  No deed records for BBL {bbl}")
        return None

    # 3. Store property row
    most_recent = deeds[0]
    property_id = upsert_property(bbl, {
        "bbl": bbl,
        "borough": borough,
        "block": bbl[1:6],
        "lot": bbl[6:],
        "address": address,
        "zip_code": zip_code,
        "raw_data": {"source": "acris_deed_fallback"},
    })
    print(f"  ✓ Property stored via ACRIS deed: {address} (BBL {bbl})")

    # 4. For each deed, get the grantee (party_type=2 = buyer = current owner)
    found_any = False
    for deed in deeds[:3]:
        doc_id = deed.get("document_id", "")
        doc_date = (deed.get("recorded_datetime") or deed.get("doc_date") or "")[:10]

        p_params = {
            "$where": f"document_id='{doc_id}' AND party_type='2'",  # grantee
            "$limit": 10,
        }
        if config.NYC_OPENDATA_APP_TOKEN:
            p_params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

        presp = await client.get(config.ACRIS_PARTIES_URL, params=p_params, timeout=20)
        parties = presp.json() if presp.status_code == 200 else []

        for party in parties:
            name = party.get("name", "").strip()
            if not name:
                continue

            ent_type = "llc" if any(t in name.upper() for t in ("LLC", " CORP", " INC", " LP")) else "individual"
            addr_parts = [party.get("addr1", ""), party.get("addr2", ""),
                          party.get("city", ""), party.get("state", "")]
            ent_addr = ", ".join(p for p in addr_parts if p).strip(", ")

            ent_id = upsert_entity(name, ent_type, extra={"address": ent_addr or None})
            upsert_property_role(property_id, ent_id, "owner", "acris_deed",
                                 extra={"doc_type": "DEED", "doc_date": doc_date})
            print(f"    → ACRIS DEED grantee ({doc_date}): {name}")
            found_any = True

    return property_id if found_any else None


# ============================================================
# Step 4: LLC Piercing
# ============================================================

async def pierce_property_entities(property_id: str, address: str):
    """Pierce all LLC entities associated with a property."""
    res = db().table("property_roles")\
        .select("entity_id, entities(*)")\
        .eq("property_id", property_id)\
        .eq("role", "owner")\
        .execute()

    llcs = [
        r["entities"] for r in (res.data or [])
        if r.get("entities", {}).get("entity_type") in ("llc", "corporation")
           and not r.get("entities", {}).get("is_pierced")
    ]

    if not llcs:
        print(f"  ℹ️  No unpierced LLCs for {address}")
        return

    for entity in llcs:
        print(f"  🔍 Piercing LLC: {entity['name']}")
        success = await pierce_entity(entity)
        status = "✓ pierced" if success else "✗ could not pierce"
        print(f"    {status}")


# ============================================================
# Step 5: Contact Enrichment
# ============================================================

def run_contact_enrichment():
    """Enrich contacts for any signers found."""
    print("\n📞 Running contact enrichment on pending signers...")
    stats = enrich_pending_signers(limit=20)
    print(f"  ✓ {stats['processed']} signers processed")
    print(f"  ✓ {stats['contacts_added']} contacts added")
    print(f"  ✓ {stats['companies_added']} companies added")


# ============================================================
# Step 6: Print Summary
# ============================================================

def print_summary():
    """Pull final results from DB and print a clean summary."""
    print("\n" + "="*60)
    print("RESULTS SUMMARY")
    print("="*60)

    props = db().table("properties").select("*").execute().data or []
    for prop in props:
        bbl = prop.get("bbl", "")
        addr = prop.get("address", bbl)
        print(f"\n📍 {addr} (BBL: {bbl})")

        # Get owner entities
        roles = db().table("property_roles")\
            .select("role, source, entities(*)")\
            .eq("property_id", prop["id"])\
            .execute().data or []

        owners = [r for r in roles if r.get("role") == "owner" and r.get("entities")]
        managers = [r for r in roles if r.get("role") == "manager" and r.get("entities")]

        for r in owners:
            e = r["entities"]
            pierced = "✓ pierced" if e.get("is_pierced") else "○ not pierced"
            print(f"  Owner [{r['source']}]: {e['name']} ({e['entity_type']}) {pierced}")

        for r in managers:
            e = r["entities"]
            print(f"  Manager [{r['source']}]: {e['name']}")

        # Get ownership chain (entity_relationships)
        for r in owners:
            e = r["entities"]
            rels = db().table("entity_relationships")\
                .select("*, entities!entity_relationships_parent_entity_id_fkey(name, entity_type)")\
                .eq("child_entity_id", e["id"])\
                .execute().data or []
            for rel in rels:
                parent = rel.get("entities", {})
                conf = rel.get("confidence", 0)
                src = rel.get("source", "")
                print(f"    → Real owner [{src}, {conf:.0%}]: {parent.get('name')} ({parent.get('entity_type')})")

        # Get contacts
        for r in owners:
            e = r["entities"]
            contacts = db().table("contacts")\
                .select("full_name, title, email, phone, source, network_role")\
                .eq("entity_id", e["id"])\
                .execute().data or []
            for c in contacts:
                nr = c.get("network_role") or ""
                role_tag = f"[{nr}]" if nr else ""
                print(f"    Contact {role_tag}: {c.get('full_name')} | {c.get('title','')} | {c.get('email','')} | {c.get('phone','')} [{c.get('source')}]")


# ============================================================
# Main
# ============================================================

async def ingest_and_pierce():
    """Phase 1 (async): geocode, ingest HPD/ACRIS, pierce LLCs."""
    print("NYC Landlord Finder — Targeted Address Test")
    print("=" * 60)

    property_ids = []
    async with httpx.AsyncClient() as client:
        for address in ADDRESSES:
            print(f"\n📍 Processing: {address}")

            # 1. Geocode
            geo = await geocode_address(address, client)
            if not geo:
                continue
            bbl = geo["bbl"]
            print(f"  BBL: {bbl} | Normalized: {geo['address']}")

            # 2. HPD
            property_id = await ingest_hpd_for_bbl(
                bbl, geo["address"], geo["borough"], geo["zip"], client
            )

            # 3. ACRIS deed fallback — for buildings with no HPD registration
            if not property_id:
                print(f"  ℹ️  No HPD registration — trying ACRIS deed fallback")
                property_id = await ingest_acris_deed_fallback(
                    bbl, geo["address"], geo["borough"], geo["zip"], client
                )

            # 4. ACRIS mortgage data (supplements HPD owner data)
            if property_id:
                await ingest_acris_for_bbl(bbl, property_id, client)
                property_ids.append((property_id, address, bbl))

    # 5. LLC Piercing
    print("\n" + "="*60)
    print("🔬 LLC PIERCING")
    print("="*60)
    for property_id, address, bbl in property_ids:
        print(f"\n{address}")
        await pierce_property_entities(property_id, address)

    # Mark all ACRIS PDF contacts as signers (must run before enrichment)
    result = db().table("contacts").update({"network_role": "signer"}).eq("source", "acris_pdf").execute()
    print(f"\n✓ Marked {len(result.data)} ACRIS PDF contacts as signers")


def main():
    # Phase 1: async ingestion + LLC piercing
    asyncio.run(ingest_and_pierce())

    # Force STANDARD tier for test run: set portfolio_size=10 on all LLC entities
    # so Hunter, Apollo, and BatchData fire even for single-building landlords.
    # In production the tier gates spending; here we want to exercise all paid sources.
    from database.client import db
    db().table("entities").update({"portfolio_size": 10}).in_(
        "entity_type", ["llc", "corporation", "individual"]
    ).execute()
    print("\n✓ Portfolio size set to 10 → STANDARD tier (exercises Hunter + Apollo)")

    # Phase 2: sync enrichment (orchestrator uses asyncio.run() internally)
    run_contact_enrichment()

    # Phase 3: summary
    print_summary()


if __name__ == "__main__":
    main()
