"""
filters.py — shared contact filtering utilities.

Centralised so the same logic applies in every prong and every source
without duplication.
"""

import re

# Each tuple is a regex pattern (case-insensitive).  We use flexible patterns
# rather than exact strings because ACRIS records have many misspellings,
# missing spaces, and abbreviations for government entities.
_GOVT_PATTERNS: list[str] = [
    # City of New York — many spelling/spacing variants
    r"CITY\s+OF\s+N",             # matches "CITY OF NY", "CITY OF N Y", "CITY OF NEW YORK"
    r"CITY\s*NY\b",               # "CITY NY", "CITYNY"
    r"THE\s+CITY\s+OF",           # "THE CITY OF NEW YORK", "THE CITY OFNEW YORK"
    r"\bN\.?Y\.?C\.?\b",         # NYC, N.Y.C., N.Y.C (standalone)

    # Commissioner of Finance — many misspellings
    r"COMM(?:ISSIONER|ISSONER|ISSOINER)?\s+OF\s+FIN",  # all the misspellings
    r"COMM\s+OF\s+FINANCE",
    r"COMMISSIONER\s+FINANCE",    # "COMMISSIONER FINANCE OF THE CITY..."
    r"DEPT\s+OF\s+FINANCE",
    r"DEPARTMENT\s+OF\s+FINANCE",
    r"FINANCE\s+ADMI+N",          # "FINANCE ADMINISTRATION", "FINANCE ADMIN", "FINANCE ADMIMINISTRATION"

    # Other NYC agencies
    r"\bNYC\s+HPD\b",
    r"\bNYC\s+DOB\b",
    r"\bNYC\s+DOF\b",
    r"\bNYC\s+DEP\b",
    r"NEW\s+YORK\s+CITY",
    r"DEPARTMENT\s+OF\s+HOUSING",
    r"BOARD\s+OF\s+ESTIMATE",
    r"\bNYCHA\b",
    r"HOUSING\s+AUTHORITY",

    # State / Federal
    r"STATE\s+OF\s+NEW\s+YORK",
    r"NEW\s+YORK\s+STATE",
    r"UNITED\s+STATES\s+OF\s+AMERICA",
    r"U\.S\.\s+DEPARTMENT",
    r"\bHUD\b",
    r"FEDERAL\s+DEPOSIT",
    r"FEDERAL\s+NATIONAL",
    r"\bFANNIE\s+MAE\b",
    r"\bFREDDIE\s+MAC\b",
    r"\bFDIC\b",
]

_COMPILED = re.compile(
    "|".join(f"(?:{p})" for p in _GOVT_PATTERNS),
    re.IGNORECASE,
)


def is_govt_entity(name: str) -> bool:
    """
    Return True if the name looks like a government body or city agency.
    Handles common ACRIS misspellings and abbreviations, e.g.:
      - "CITY OF NEW YORK", "CITY OF N Y", "THE CITY OFNEW YORK"
      - "COMMISSIONER OF FINANCE", "COMMISSOINER OF FINANCE", "COMM OF FINANCE"
      - "NYC HPD", "N.Y.C.", "NYCHA"
      - "FINANCE ADMINISTRATION", "FINANCE ADMIN"
    """
    if not name:
        return False
    return bool(_COMPILED.search(name))
