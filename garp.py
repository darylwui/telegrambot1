"""
GARP (growth-at-a-reasonable-price) quality scoring for portfolio positions.

Runs a 6-criterion fundamental screen against each ticker and returns an X/6
score suitable for surfacing as informational context in the portfolio brief.

Criteria (Option B — relaxed from the original strict version, after
evaluation showed strict thresholds only produced 3 passers out of 117):
    Trailing P/E < 25
    Forward P/E < 20      (relaxed from 15)
    Debt/Equity < 60%     (relaxed from 35%)
    EPS Growth > 15% YoY
    PEG < 2.0
    Market Cap > $5B

Financial-sector exception: yfinance returns None for `debtToEquity` on banks
(JPM, GS, BAC, C, WFC) because bank D/E is structurally meaningless. For
those, the D/E check is marked "N/A" (not a fail) and the score is out of 5
instead of 6.

Cache: 7-day TTL in .garp_cache.json (fundamentals move quarterly, not
daily). Committed to reduce API call volume in CI.
"""
from __future__ import annotations

import datetime
import json
import time
from pathlib import Path
from typing import Optional

import yfinance as yf

REPO = Path(__file__).resolve().parent
CACHE_FILE = REPO / ".garp_cache.json"
CACHE_TTL_SECONDS = 7 * 24 * 60 * 60  # 7 days

CRITERIA = {
    "tPE": 25,
    "fPE": 20,
    "DE": 60,
    "EPSg": 0.15,
    "PEG": 2.0,
    "MCap": 5_000_000_000,
}


def _load_cache() -> dict:
    if not CACHE_FILE.exists():
        return {"entries": {}}
    try:
        return json.loads(CACHE_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return {"entries": {}}


def _save_cache(cache: dict) -> None:
    CACHE_FILE.write_text(json.dumps(cache, indent=2))


def _fetch_fundamentals(ticker: str) -> dict:
    """Pull raw fundamental values from yfinance. None on failure per field."""
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception:
        info = {}
    return {
        "tPE": info.get("trailingPE"),
        "fPE": info.get("forwardPE"),
        "DE": info.get("debtToEquity"),
        "EPSg": info.get("earningsGrowth"),
        "PEG": info.get("pegRatio") or info.get("trailingPegRatio"),
        "MCap": info.get("marketCap"),
        "sector": info.get("sector"),
    }


def _is_financial(vals: dict) -> bool:
    """Heuristic: D/E is None AND sector is a financial one."""
    if vals.get("DE") is not None:
        return False
    sector = (vals.get("sector") or "").lower()
    return "financial" in sector or sector in {"banks", "insurance"}


def score(ticker: str, cache: Optional[dict] = None) -> dict:
    """
    Return {
        "score": int,           # X (numerator)
        "max_score": int,       # 5 or 6 (denominator; 5 when D/E is N/A for a financial)
        "checks": {name: True|False|None},  # None means "not applicable"
        "values": dict of raw metric values,
        "cached": bool,
    }
    """
    if cache is None:
        cache = _load_cache()
    now = int(time.time())
    entry = cache.get("entries", {}).get(ticker)
    cached = bool(entry and (now - entry.get("cached_at", 0)) < CACHE_TTL_SECONDS)
    if cached:
        vals = entry["values"]
    else:
        vals = _fetch_fundamentals(ticker)
        cache.setdefault("entries", {})[ticker] = {
            "values": vals,
            "cached_at": now,
        }
        time.sleep(0.1)  # be polite to yfinance

    is_fin = _is_financial(vals)

    def check(key, op) -> Optional[bool]:
        v = vals.get(key)
        if v is None:
            # For D/E on financials, treat as N/A (not a fail)
            if key == "DE" and is_fin:
                return None
            return False
        return op(v)

    checks = {
        "tPE":  check("tPE",  lambda v: v < CRITERIA["tPE"]),
        "fPE":  check("fPE",  lambda v: v < CRITERIA["fPE"]),
        "DE":   check("DE",   lambda v: v < CRITERIA["DE"]),
        "EPSg": check("EPSg", lambda v: v > CRITERIA["EPSg"]),
        "PEG":  check("PEG",  lambda v: v < CRITERIA["PEG"]),
        "MCap": check("MCap", lambda v: v > CRITERIA["MCap"]),
    }
    max_score = sum(1 for v in checks.values() if v is not None)
    n_pass = sum(1 for v in checks.values() if v is True)

    return {
        "score": n_pass,
        "max_score": max_score,
        "checks": checks,
        "values": vals,
        "cached": cached,
    }


def score_all(tickers: list[str]) -> dict[str, dict]:
    """Score every ticker; persist cache once at the end."""
    cache = _load_cache()
    out = {}
    for t in tickers:
        out[t] = score(t, cache=cache)
    _save_cache(cache)
    return out


def render_line(result: dict) -> str:
    """Compact one-line Telegram-safe rendering: `⭐ Quality: 5/6 (tPE:✓ ...)`."""
    checks = result["checks"]
    parts = []
    for k in ("tPE", "fPE", "DE", "EPSg", "PEG", "MCap"):
        c = checks.get(k)
        if c is True:
            parts.append(f"{k}:✓")
        elif c is None:
            parts.append(f"{k}:—")  # N/A (financial-sector D/E)
        else:
            parts.append(f"{k}:·")
    marks = " ".join(parts)
    return f"⭐ Quality: {result['score']}/{result['max_score']} <i>({marks})</i>"


# --- for a weekly hunt list; called only from Monday-AM sessions ---

DEFAULT_HUNT_UNIVERSE = [
    # Mega tech
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "AVGO", "ORCL", "CRM", "ADBE", "NFLX",
    # Semis
    "AMD", "TSM", "INTC", "MU", "QCOM", "LRCX", "KLAC", "AMAT", "MRVL", "ARM",
    # Cyber / SaaS
    "CRWD", "PANW", "ZS", "SNOW", "DDOG", "NOW", "PLTR",
    # Chinese ADRs
    "BABA", "PDD", "JD", "BIDU",
    # Financials
    "JPM", "GS", "BAC", "MS", "C", "WFC", "BLK", "AXP", "V", "MA",
    # Healthcare
    "LLY", "UNH", "JNJ", "MRK", "ABBV", "PFE", "TMO", "ABT", "DHR", "VRTX", "GILD",
    # Consumer disc
    "HD", "LOW", "NKE", "MCD", "SBUX", "DIS", "BKNG", "CMG",
    # Consumer staples
    "WMT", "COST", "TGT", "PG", "KO", "PEP",
    # Industrials
    "CAT", "BA", "HON", "GE", "UPS", "RTX", "LMT", "DE",
    # Energy
    "XOM", "CVX", "COP", "OXY", "SLB", "EOG",
    # Utilities/REIT
    "NEE", "PLD", "EQIX",
    # Misc high-attention
    "UBER", "ABNB", "SPOT", "MSTR", "COIN",
]


def hunt_list(min_score: int = 5, universe: Optional[list[str]] = None) -> list[dict]:
    """Return tickers scoring >= min_score from the hunt universe.

    Called on Monday-AM sessions only per the portfolio_report cadence.
    """
    tickers = universe or DEFAULT_HUNT_UNIVERSE
    scored = score_all(tickers)
    passers = []
    for t, r in scored.items():
        if r["score"] >= min_score and r["max_score"] >= 5:
            passers.append({"ticker": t, **r})
    passers.sort(key=lambda x: (-x["score"], x["ticker"]))
    return passers
