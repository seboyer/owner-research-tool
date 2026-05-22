"""
enrichment/skip_filter.py — Gate-keeper called at queue-insertion time.

Single entry point:
    evaluate(entity, properties) -> SkipDecision | None

Returns None if the entity should be queued for enrichment, or a
SkipDecision if it should be skipped. Every skip records a reason,
a 0.0–1.0 score, and a short human-readable evidence string that
the admin UI can display.

Critical invariant: entities where is_building_llc=True are NEVER
skipped — they are the *input* to the LLC-piercing process.
"""

import re
from dataclasses import dataclass

_LAWYER_RE = re.compile(
    r"\b(esq\.?|attorney|atty\.?|law\s+(office|offices|firm|group))\b",
    re.IGNORECASE,
)

_BANK_RE = re.compile(
    r"\b(BANK|MORTGAGE|LENDER|TRUST\s+CO|FINANCIAL|SAVINGS|CREDIT\s+UNION"
    r"|FANNIE\s+MAE|FREDDIE\s+MAC|MERS|MORTGAGE\s+ELECTRONIC)\b",
    re.IGNORECASE,
)

_DECEASED_RE = re.compile(
    r"\b(ESTATE\s+OF|DECEASED|DCSD|DEC['’]D|LATE\s+OF)\b",
    re.IGNORECASE,
)

_INDIVIDUAL_TYPES = {"individual", "unknown", None}


@dataclass
class SkipDecision:
    reason: str
    score: float
    evidence: str


def is_lawyer_name(name: str) -> bool:
    """Return True if the name looks like an attorney rather than an owner.

    Moved here from database/client.py so skip_filter is the single source
    of truth. database/client.py re-exports this symbol for back-compat.
    """
    if not name:
        return False
    return bool(_LAWYER_RE.search(name))


def evaluate(entity: dict, properties: list[dict] | None = None) -> SkipDecision | None:
    """Return None if the entity should be queued, SkipDecision if it should be skipped.

    entity dict keys consumed:
        name            (str)
        entity_type     (str | None)
        portfolio_size  (int | None)
        is_building_llc (bool, defaults False)
        has_owned_by_parents (bool, defaults False) — set by llc_piercer after
                         writing entity_relationships, so the low_value_score
                         rule can reward confirmed-landlord individuals.

    properties list dict keys consumed:
        unit_count      (int | None)
        hpd_reg_id      (str | None)

    Deviation from plan: has_owned_by_parents is passed on the entity dict
    rather than fetched inside this function, to keep evaluate() free of DB
    calls. Callers are responsible for populating it.
    """
    name = (entity.get("name") or "").strip()
    entity_type = entity.get("entity_type")
    is_building = entity.get("is_building_llc", False)

    # Auto-pass: shell LLCs routed to llc_pierce are never skipped.
    if is_building:
        return None

    # Rule 1 — lawyer_name
    if is_lawyer_name(name):
        m = _LAWYER_RE.search(name)
        token = m.group(0) if m else name
        return SkipDecision(
            reason="lawyer_name",
            score=0.0,
            evidence=f"name matches lawyer pattern: '{token}'",
        )

    # Rule 2 — govt_entity
    from enrichment.contact.filters import is_govt_entity
    if is_govt_entity(name):
        return SkipDecision(
            reason="govt_entity",
            score=0.0,
            evidence=f"name matches government entity pattern: '{name[:60]}'",
        )

    # Rule 3 — bank_or_lender
    m = _BANK_RE.search(name)
    if m:
        return SkipDecision(
            reason="bank_or_lender",
            score=0.0,
            evidence=f"name matches bank/lender pattern: '{m.group(0)}'",
        )

    # Rule 4 — deceased
    m = _DECEASED_RE.search(name)
    if m:
        return SkipDecision(
            reason="deceased",
            score=0.0,
            evidence=f"name matches deceased/estate pattern: '{m.group(0)}'",
        )

    # Rule 5 — low_value_score (individuals and unknowns only)
    if entity_type not in _INDIVIDUAL_TYPES:
        return None

    # If no property context is available yet, auto-pass — brand-new entity.
    if not properties:
        return None

    return _evaluate_low_value(entity, properties)


def _evaluate_low_value(entity: dict, properties: list[dict]) -> SkipDecision | None:
    """Compute the low-value score for an individual/unknown entity.

    Weight table (clamped to [0.0, 1.0]):
        +0.40  any property has hpd_reg_id IS NOT NULL  (3+ unit landlord signal)
        +0.30  any property has unit_count >= 3
        +0.20  portfolio_size >= 2
        +0.15  has entity_relationships.relationship_type='owned_by' parents
        -0.50  individual + no HPD reg + max unit_count <= 2 (single-family/duplex)

    Skips if score < config.SKIP_LOW_VALUE_THRESHOLD.
    """
    from config import config

    portfolio_size = entity.get("portfolio_size") or 0
    has_owned_by_parents = entity.get("has_owned_by_parents", False)

    has_hpd_reg = any(bool(p.get("hpd_reg_id")) for p in properties)
    has_3plus_units = any((p.get("unit_count") or 0) >= 3 for p in properties)
    max_units = max((p.get("unit_count") or 0) for p in properties) if properties else 0

    score = 0.0
    signals: list[str] = []

    if has_hpd_reg:
        score += 0.40
        signals.append("HPD reg")

    if has_3plus_units:
        score += 0.30
        signals.append(f"unit_count>={3}")

    if portfolio_size >= 2:
        score += 0.20
        signals.append(f"portfolio_size={portfolio_size}")

    if has_owned_by_parents:
        score += 0.15
        signals.append("owned_by parent")

    entity_type = entity.get("entity_type")
    if entity_type == "individual" and not has_hpd_reg and max_units <= 2:
        score -= 0.50
        signals.append(f"individual+no_hpd+max_units={max_units}")

    score = max(0.0, min(1.0, score))

    building_count = len(properties)
    evidence_parts = [
        entity_type or "unknown_type",
        f"{building_count} building{'s' if building_count != 1 else ''}",
    ]
    evidence_parts.extend(signals)
    evidence = f"{', '.join(evidence_parts)} (score {score:.2f})"

    if score < config.SKIP_LOW_VALUE_THRESHOLD:
        return SkipDecision(
            reason="low_value_score",
            score=round(score, 3),
            evidence=evidence,
        )

    return None
