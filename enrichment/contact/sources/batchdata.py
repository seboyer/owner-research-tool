"""
enrichment/contact/sources/batchdata.py — BatchData API adapter.

Endpoints used:
    POST /api/v3/property/skip-trace
        Returns up to 3 persons with ranked phones (mobile/landline, DNC/TCPA
        flagged) and verified emails for a property address or APN.
        Used in Prong 1 (signer phone/email) and Prong 3 (building contacts).

Auth:
    Bearer token in Authorization header.
    Set BATCHDATA_API_KEY in .env.

Billing:
    Each matched property counts as 1 billable request.
    ~$0.20–$0.50 per match → BUDGET tier.

Key response fields per person:
    name.first / name.last / name.full
    phones[].number, .type, .rank, .dnc, .tcpa, .reachable
    emails[].email, .rank, .tested
    litigator (bool) — TCPA litigator flag
    deceased (bool)

Notes:
    - We request includeTCPABlacklistedPhones=true so we receive all phones
      and can make our own filtering/flagging decisions per compliance needs.
    - DNC phones are skipped entirely (brokers cannot call them).
    - TCPA status is encoded into phone_type so downstream code / brokers see it.
    - We take the best ranked non-DNC phone and best ranked email per person.
    - Up to 3 persons are returned per property; all are emitted as ContactHits.
"""

import httpx
import structlog

from config import config
from ..models import ContactHit

log = structlog.get_logger(__name__)

_BASE = "https://api.batchdata.com/api/v3"
_COST_CENTS = 40  # ~$0.40 per matched property record


def _key() -> str:
    # Read via config (which calls load_dotenv(override=True) on import).
    # Reading os.getenv() directly here was a bug — if .env was loaded after
    # this module imported, the key would be empty.
    return config.BATCHDATA_API_KEY


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {_key()}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


async def skip_trace_property(
    *,
    street: str = "",
    city: str = "",
    state: str = "",
    zip_code: str = "",
    apn: str = "",
    county: str = "",
    county_fips: str = "",
    network_role: str = "building_contact",
    role_category: str = "owner",
) -> list[ContactHit]:
    """
    Call V3 Property Skip Trace and return a ContactHit for each matched person.

    Provide one of:
        • street + city + state  (or street + zip + state)
        • apn + state + county   (or apn + state + countyFipsCode)

    Args:
        network_role:  Set to 'signer' when called from Prong 1,
                       'building_contact' when called from Prong 3.
        role_category: Typically 'owner' for property skip trace results.

    Returns:
        List of ContactHit (empty on miss or error).
        Cost is _COST_CENTS charged only when at least one person is returned.
    """
    k = _key()
    if not k:
        log.debug("batchdata.skip_trace.skipped", reason="no BATCHDATA_API_KEY set")
        return []

    # --- Build the single request object -----------------------------------
    req: dict = {}
    if apn and state and (county or county_fips):
        req["apn"] = apn
        req["state"] = state
        if county_fips:
            req["countyFipsCode"] = county_fips
        else:
            req["county"] = county
    elif street and state and (city or zip_code):
        addr: dict = {"street": street, "state": state}
        if city:
            addr["city"] = city
        if zip_code:
            addr["zip"] = zip_code
        req["propertyAddress"] = addr
    else:
        log.debug(
            "batchdata.skip_trace.skipped",
            reason="insufficient address params",
            street=street, city=city, state=state, zip=zip_code,
        )
        return []

    payload = {
        "requests": [req],
        "options": {
            "includeTCPABlacklistedPhones": True,  # we decide per-phone below
        },
    }

    # --- API call ----------------------------------------------------------
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.post(
                f"{_BASE}/property/skip-trace",
                headers=_headers(),
                json=payload,
            )
            r.raise_for_status()
            data = r.json()
        except httpx.HTTPStatusError as e:
            log.warn("batchdata.skip_trace.http_error",
                     status=e.response.status_code, body=e.response.text[:200])
            return []
        except Exception as e:
            log.warn("batchdata.skip_trace.failed", error=str(e))
            return []

    result_items = (data.get("result", {}).get("data") or [])
    if not result_items:
        log.debug("batchdata.skip_trace.empty_result")
        return []

    item = result_items[0]
    if not (item.get("meta") or {}).get("matched"):
        log.debug("batchdata.skip_trace.no_match", req=req)
        return []

    persons = item.get("persons") or []
    out: list[ContactHit] = []

    for person in persons:
        if person.get("deceased"):
            continue

        name = person.get("name") or {}
        full = (
            name.get("full")
            or f"{name.get('first', '')} {name.get('last', '')}".strip()
        )
        if not full:
            continue

        # Best non-DNC phone — prefer mobile, then by rank
        all_phones = sorted(
            person.get("phones") or [],
            key=lambda p: (
                p.get("dnc", False),          # DNC last
                0 if p.get("type") == "Mobile" else 1,   # mobile first
                p.get("rank", 99),
            ),
        )
        # Skip phones that are on the Do Not Call list
        callable_phones = [p for p in all_phones if not p.get("dnc")]
        best_phone = callable_phones[0] if callable_phones else None

        phone_num = best_phone["number"] if best_phone else None
        phone_type = best_phone.get("type") if best_phone else None
        phone_tcpa = best_phone.get("tcpa", False) if best_phone else False

        # Encode TCPA warning into type string so brokers/UI see it
        if phone_tcpa and phone_type:
            phone_type = f"{phone_type} (TCPA)"
        elif phone_tcpa:
            phone_type = "TCPA"

        # Best email (rank 1 = most likely deliverable)
        all_emails = sorted(person.get("emails") or [], key=lambda e: e.get("rank", 99))
        best_email = all_emails[0]["email"] if all_emails else None

        # Human-readable evidence string
        evidence_parts = ["BatchData V3 skip trace"]
        if best_phone:
            flags = []
            if best_phone.get("reachable"):
                flags.append("reachable")
            if phone_tcpa:
                flags.append("TCPA")
            evidence_parts.append(
                f"phone type={best_phone.get('type', '?')}"
                + (f" [{','.join(flags)}]" if flags else "")
            )
        if best_email and all_emails and all_emails[0].get("tested"):
            evidence_parts.append("email=tested")
        if person.get("litigator"):
            evidence_parts.append("⚠ TCPA litigator")

        out.append(ContactHit(
            full_name=full,
            first_name=name.get("first"),
            last_name=name.get("last"),
            phone=phone_num,
            phone_type=phone_type,
            email=best_email,
            network_role=network_role,
            role_category=role_category,
            confidence=0.80,
            source="batchdata_skip_trace",
            evidence=" | ".join(evidence_parts),
            cost_cents=_COST_CENTS,
            raw=person,
        ))

    # Cost is per-property (one charge regardless of # of persons returned)
    if out:
        # Mark cost on first hit only; orchestrator sums cost_cents across hits
        for i in range(1, len(out)):
            out[i].cost_cents = 0

    return out
