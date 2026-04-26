"""
enrichment/acris_pdf.py — ACRIS Mortgage Document Signer Extraction

ACRIS publicly hosts all recorded documents as individual TIFF pages served
by the GetImage endpoint, which requires no authentication or session:

  https://a836-acris.nyc.gov/DS/DocumentSearch/GetImage?doc_id=<ID>&page=<N>

Out-of-range pages return a blank placeholder TIFF (~13 KB). End-of-document
detection: stop when two consecutive pages have the same MD5 hash, then remove
the last appended page (the first blank).

Mortgage documents contain a signature block where a real person must sign
on behalf of an LLC:

  "JOHN SMITH, as Managing Member of 123 MAIN STREET LLC"

This is one of the most reliable ways to pierce a building LLC — the
mortgage signer IS the real person behind it.
"""

import asyncio
import base64
import hashlib
import io
import json
import re

import httpx
import structlog
from anthropic import Anthropic

from config import config
from database.client import (
    db, upsert_entity, upsert_contact, upsert_relationship,
    already_seen, mark_seen,
)

log = structlog.get_logger(__name__)

ACRIS_GET_IMAGE_URL = "https://a836-acris.nyc.gov/DS/DocumentSearch/GetImage"
ACRIS_MASTER_URL = config.ACRIS_MASTER_URL

# Max pages to probe — real docs rarely exceed 100 pages
MAX_PAGES = 200

anthropic_client = Anthropic(api_key=config.ANTHROPIC_API_KEY)


# ============================================================
# Step 1: Find mortgage doc IDs for a BBL (via Socrata — no blocking)
# ============================================================

async def get_mortgage_docs_for_bbl(bbl: str) -> list[dict]:
    """
    Get mortgage document IDs for a given BBL from ACRIS via Socrata API.
    This hits data.cityofnewyork.us (NOT a836-acris.nyc.gov) — no anti-bot.
    Returns list of {document_id, doc_type, recorded_datetime}.
    """
    boro = bbl[0]
    block = bbl[1:6].lstrip("0")
    lot = bbl[6:10].lstrip("0")

    async with httpx.AsyncClient() as client:
        params = {
            "$where": f"borough='{boro}' AND block='{block}' AND lot='{lot}'",
            "$limit": 20,
            "$order": ":id",
        }
        if config.NYC_OPENDATA_APP_TOKEN:
            params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

        resp = await client.get(config.ACRIS_LEGALS_URL, params=params, timeout=20)
        resp.raise_for_status()
        legals = resp.json()

    if not legals:
        return []

    doc_ids = list({row["document_id"] for row in legals})

    async with httpx.AsyncClient() as client:
        ids_clause = ",".join(f"'{d}'" for d in doc_ids[:20])
        params = {
            "$where": (
                f"document_id in ({ids_clause}) AND "
                "doc_type in ('MTGE','CONSOLIDATION MORTGAGE','BUILDING LOAN AGREEMENT')"
            ),
            "$limit": 10,
            "$order": "recorded_datetime DESC",
        }
        if config.NYC_OPENDATA_APP_TOKEN:
            params["$$app_token"] = config.NYC_OPENDATA_APP_TOKEN

        resp = await client.get(ACRIS_MASTER_URL, params=params, timeout=20)
        resp.raise_for_status()
        docs = resp.json()

    return docs


# ============================================================
# Step 2: Download document as PDF (direct httpx — no auth required)
#
# GetImage is a publicly accessible endpoint — no browser, proxy, or session
# needed. Documents are served as individual TIFF pages, not PDFs.
# ============================================================

def _is_valid_tiff(data: bytes) -> bool:
    return len(data) > 4 and data[:4] in (b"II*\x00", b"MM\x00*")


async def _fetch_tiff_pages(document_id: str) -> list[bytes]:
    """
    Fetch all real TIFF pages for a document using direct httpx requests.

    Termination: when page N and page N+1 have identical MD5 hashes, both are
    blank placeholders. Remove page N (first blank appended) and stop.
    """
    pages: list[bytes] = []
    prev_hash: str | None = None

    async with httpx.AsyncClient(
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0.0.0"},
        timeout=30,
    ) as client:
        for page_num in range(1, MAX_PAGES + 1):
            url = f"{ACRIS_GET_IMAGE_URL}?doc_id={document_id}&page={page_num}"
            try:
                resp = await client.get(url)
                if resp.status_code != 200 or not _is_valid_tiff(resp.content):
                    break
                curr_hash = hashlib.md5(resp.content).hexdigest()
                if curr_hash == prev_hash:
                    if pages:
                        pages.pop()
                    break
                pages.append(resp.content)
                prev_hash = curr_hash
            except Exception:
                break

    return pages


def _tiffs_to_pdf(tiff_pages: list[bytes], scale: float = 1.0) -> bytes | None:
    """Convert a list of TIFF page bytes to a single PDF in memory using Pillow."""
    try:
        from PIL import Image

        def _open(data: bytes) -> "Image.Image":
            img = Image.open(io.BytesIO(data)).convert("RGB")
            if scale != 1.0:
                w, h = img.size
                img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
            return img

        images = [_open(data) for data in tiff_pages]
        buf = io.BytesIO()
        images[0].save(buf, format="PDF", save_all=True, append_images=images[1:])
        return buf.getvalue()
    except Exception as e:
        log.warning("acris_pdf.tiff_to_pdf_error", error=str(e))
        return None


def _extraction_windows(pages: list[bytes]) -> list[tuple[list[bytes], float]]:
    """
    Build a sequence of (page_subset, scale) windows to try for signer extraction.

    Mortgage signature blocks appear before any attached exhibits. Since we don't
    know how many exhibit pages are appended, we work backward through the document
    in overlapping chunks until signers are found.

    Windows (all include the first 2 pages for borrower/LLC context):
      0 — head + last 5          (scale 0.5) — sig block with no/few attachments
      1 — head + pages [-15:-5]  (scale 0.5) — sig block with ~10 attached pages
      2 — head + pages [-30:-15] (scale 0.5) — sig block with ~25 attached pages
      3 — all pages               (scale 0.25) — full-doc fallback, more compressed
    """
    n = len(pages)
    if n <= 7:
        return [(pages, 0.5)]

    head = pages[:2]
    windows: list[tuple[list[bytes], float]] = [
        (head + pages[-5:],              0.5),
        (head + pages[max(0, n-15):n-5], 0.5),
        (head + pages[max(0, n-30):n-15], 0.5),
        (pages,                           0.25),
    ]
    # Drop duplicate/empty windows that can appear in short documents
    seen: set[tuple[int, ...]] = set()
    deduped = []
    for pg_list, sc in windows:
        key = tuple(id(p) for p in pg_list)
        if key not in seen and len(pg_list) > 0:
            seen.add(key)
            deduped.append((pg_list, sc))
    return deduped


# ============================================================
# Step 3: Extract signer from PDF using Claude Vision
# ============================================================

SIGNER_EXTRACTION_PROMPT = """
You are analyzing an ACRIS (NYC property records) mortgage document PDF.

Your task: Find the name(s) of real individuals who signed this document on behalf of an LLC.

Look specifically for:
1. Signature blocks at the end of the document like:
   - "John Smith, as Managing Member of [LLC NAME]"
   - "By: Jane Doe, its Member/Manager"
   - "John Smith, individually and as Member of [LLC]"
   - Notary sections: "appeared before me, JOHN SMITH"
   - Personal guarantee sections listing a guarantor's name

2. Borrower information at the top: if the borrower is an LLC, look for
   "a New York limited liability company, by JOHN SMITH, its managing member"

For each signer found, return:
{
  "signers": [
    {
      "full_name": "John Smith",
      "role": "Managing Member",
      "llc_name": "123 MAIN STREET LLC",
      "section": "signature_block",
      "confidence": 0.95
    }
  ],
  "llc_names": ["123 MAIN STREET LLC"]
}

If you cannot find any real person's name, return {"signers": [], "llc_names": []}.
Only return names of real individuals, NOT corporate names or LLC names.
Return valid JSON only.
"""


async def extract_signers_from_pdf(pdf_bytes: bytes, document_id: str) -> dict:
    """
    Use Claude Vision to extract signer information from a mortgage PDF.
    Returns dict with 'signers' list and 'llc_names' list.
    """
    pdf_b64 = base64.standard_b64encode(pdf_bytes).decode("utf-8")

    try:
        response = anthropic_client.messages.create(
            model=config.CLAUDE_MODEL,
            max_tokens=1024,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "document",
                            "source": {
                                "type": "base64",
                                "media_type": "application/pdf",
                                "data": pdf_b64,
                            },
                        },
                        {
                            "type": "text",
                            "text": SIGNER_EXTRACTION_PROMPT,
                        },
                    ],
                }
            ],
        )

        text = response.content[0].text.strip()
        json_match = re.search(r"\{.*\}", text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group())
        return {"signers": [], "llc_names": []}

    except Exception as e:
        log.error("acris_pdf.claude_error", doc_id=document_id, error=str(e))
        return {"signers": [], "llc_names": []}


# ============================================================
# Step 4: Store results
# ============================================================

async def process_pdf_signers(
    extracted: dict,
    llc_entity_id: str,
    llc_name: str,
    document_id: str,
):
    """
    Store extracted signer information as entities + relationships.
    A signer is an individual linked to the LLC as its real owner.
    """
    for signer in extracted.get("signers", []):
        full_name = signer.get("full_name", "").strip()
        if not full_name or len(full_name) < 3:
            continue
        confidence = signer.get("confidence", 0.8)
        role = signer.get("role", "managing_member")

        signer_entity_id = upsert_entity(full_name, "individual", extra={
            "notes": f"Identified as signer on ACRIS doc {document_id} for {llc_name}",
        })

        name_parts = full_name.rsplit(" ", 1)
        first = name_parts[0] if len(name_parts) > 1 else full_name
        last = name_parts[1] if len(name_parts) > 1 else ""

        upsert_contact(signer_entity_id, {
            "first_name": first,
            "last_name": last,
            "full_name": full_name,
            "title": role,
            "source": "acris_pdf",
            "confidence": confidence,
            "network_role": "signer",
        })

        upsert_relationship(
            child_entity_id=llc_entity_id,
            parent_entity_id=signer_entity_id,
            rel_type="owned_by",
            source="acris_pdf",
            confidence=confidence,
            evidence=f"Signed ACRIS mortgage doc {document_id} as '{role}' of {llc_name}",
        )

        log.info("acris_pdf.signer_stored",
                 signer=full_name,
                 role=role,
                 llc=llc_name,
                 confidence=confidence)


# ============================================================
# Main: Pierce an LLC via its mortgage documents
# ============================================================

async def pierce_llc_via_mortgage(
    entity_id: str,
    entity_name: str,
    bbl: str,
) -> bool:
    """
    Main entry point: given an LLC entity and a BBL it owns,
    attempt to pierce the LLC by reading its mortgage documents.

    Returns True if we found at least one signer.
    """
    cache_key = f"{entity_id}_{bbl}"
    if already_seen("acris_pdf_pierce", cache_key):
        return False

    log.info("acris_pdf.starting", entity_name=entity_name, bbl=bbl)

    docs = await get_mortgage_docs_for_bbl(bbl)
    if not docs:
        log.info("acris_pdf.no_mortgages", entity_name=entity_name, bbl=bbl)
        mark_seen("acris_pdf_pierce", cache_key)
        return False

    found_signers = False

    for doc in docs[:3]:  # Process up to 3 most recent mortgages
        doc_id = doc.get("document_id")
        if not doc_id:
            continue

        if already_seen("acris_pdf_doc", doc_id):
            continue

        log.info("acris_pdf.downloading", doc_id=doc_id, doc_type=doc.get("doc_type"))

        tiff_pages = await _fetch_tiff_pages(doc_id)
        if not tiff_pages:
            log.warning("acris_pdf.download_failed", doc_id=doc_id)
            mark_seen("acris_pdf_doc", doc_id)
            continue

        log.info("acris_pdf.downloaded", doc_id=doc_id, pages=len(tiff_pages))

        extracted = {"signers": [], "llc_names": []}
        for window_pages, scale in _extraction_windows(tiff_pages):
            pdf_bytes = _tiffs_to_pdf(window_pages, scale=scale)
            if not pdf_bytes:
                continue
            log.info("acris_pdf.trying_window",
                     doc_id=doc_id, window_size=len(window_pages), scale=scale)
            extracted = await extract_signers_from_pdf(pdf_bytes, doc_id)
            if extracted.get("signers"):
                break

        signers = extracted.get("signers", [])
        if signers:
            await process_pdf_signers(extracted, entity_id, entity_name, doc_id)
            found_signers = True
            log.info("acris_pdf.signers_found",
                     entity_name=entity_name,
                     count=len(signers))

        mark_seen("acris_pdf_doc", doc_id)
        await asyncio.sleep(1)

    mark_seen("acris_pdf_pierce", cache_key)
    return found_signers


async def run_batch_pdf_pierce(batch_size: int = 20):
    """
    Process a batch of building LLCs that haven't been pierced yet.
    For each, find their properties and try PDF piercing.
    """
    res = db().table("entities")\
        .select("id, name")\
        .eq("is_building_llc", True)\
        .eq("is_pierced", False)\
        .limit(batch_size)\
        .execute()

    entities = res.data
    log.info("acris_pdf.batch_start", count=len(entities))

    for entity in entities:
        entity_id = entity["id"]
        entity_name = entity["name"]

        roles_res = db().table("property_roles")\
            .select("property_id, properties(bbl)")\
            .eq("entity_id", entity_id)\
            .limit(1)\
            .execute()

        if not roles_res.data:
            continue

        bbl = roles_res.data[0].get("properties", {}).get("bbl")
        if not bbl or bbl.startswith("hpd_bldg_"):
            continue

        found = await pierce_llc_via_mortgage(entity_id, entity_name, bbl)

        if found:
            db().table("entities")\
                .update({"is_pierced": True})\
                .eq("id", entity_id)\
                .execute()

        await asyncio.sleep(2)
