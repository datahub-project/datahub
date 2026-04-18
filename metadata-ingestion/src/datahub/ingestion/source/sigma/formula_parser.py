import logging
from dataclasses import dataclass
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class BracketRef:
    source: Optional[str]  # part before `/`; None when no slash present
    column: str  # part after `/`, or the whole ref when no slash
    raw: str  # bracket content after unescaping `\/` → `/`


def _split_on_first_unescaped_slash(content: str) -> Tuple[Optional[str], str]:
    """Return (source, column) by splitting on the first `/` not preceded by `\\`."""
    i = 0
    while i < len(content):
        if content[i] == "\\" and i + 1 < len(content) and content[i + 1] == "/":
            i += 2  # `\/` is a literal `/`, not the source/column separator
        elif content[i] == "/":
            return content[:i].replace("\\/", "/"), content[i + 1 :].replace("\\/", "/")
        else:
            i += 1
    return None, content.replace("\\/", "/")


def parse_formula(formula: str) -> List[BracketRef]:
    """Extract all bracket references from a Sigma formula.

    Grammar (live-verified against ~150 real formulas, 2026-04-16):
      - ``[Col]``       → BracketRef(source=None, column="Col")
      - ``[Src/Col]``   → BracketRef(source="Src", column="Col")
      - ``[X \\/ Y]``   → literal ``/`` in name — no source/column split
      - ``[P_*]``       → parameter ref; caller should skip silently
      - Function calls and operators outside brackets are ignored

    Returns [] for empty input. Logs a warning and returns partial results on
    malformed input (e.g. unclosed bracket).
    """
    if not formula or not formula.strip():
        return []

    try:
        refs: List[BracketRef] = []
        i = 0
        while i < len(formula):
            if formula[i] != "[":
                i += 1
                continue
            j = i + 1
            # Scan for closing `]`. Nested brackets and escaped `]` were not
            # observed in the live sample, so a simple linear scan suffices.
            while j < len(formula) and formula[j] != "]":
                j += 1
            if j >= len(formula):
                logger.warning("Malformed formula — unclosed '[': %r", formula)
                return refs  # return what we have so far
            inner = formula[i + 1 : j]
            raw = inner.replace("\\/", "/")
            source, column = _split_on_first_unescaped_slash(inner)
            refs.append(BracketRef(source=source, column=column, raw=raw))
            i = j + 1
        return refs
    except Exception:
        logger.warning("Failed to parse formula: %r", formula, exc_info=True)
        return []
