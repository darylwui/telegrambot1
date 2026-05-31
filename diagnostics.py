"""
Factual diagnostics on the existing portfolio.

Surfaces: position concentration vs. config threshold, drawdown leaderboard
(best/worst P/L %), sector mix, and correlated pairs (60-day return r > 0.85).

Does NOT recommend trims, exits, or adds. Tells you what the math says about
positions you already hold; you decide what to do with the information.

Public API used by portfolio_report.py:
    build_diagnostics_section(portfolio, prices, history_df) -> Optional[str]
"""

from __future__ import annotations

import html
from collections import defaultdict
from typing import Optional

import numpy as np
import pandas as pd
import yfinance as yf

try:
    from clusters import SINGLE_NAME_THRESHOLD
except Exception:
    SINGLE_NAME_THRESHOLD = 10.0  # fallback in case clusters.py unavailable


CORRELATION_WINDOW_DAYS = 60
CORRELATION_THRESHOLD = 0.85
DRAWDOWN_LEADERBOARD_N = 3


# ─────────────────────────────────────────────────────────────────────────────
# DATA HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _sector_for(ticker: str, cache: dict) -> Optional[str]:
    if ticker in cache:
        return cache[ticker]
    try:
        info = yf.Ticker(ticker).info or {}
        sector = info.get("sector")
    except Exception:
        sector = None
    cache[ticker] = sector
    return sector


def _ticker_close_series(history: pd.DataFrame, ticker: str) -> Optional[pd.Series]:
    """Pull a clean close series for one ticker from the multi-index frame."""
    try:
        if isinstance(history.columns, pd.MultiIndex):
            return history[ticker]["Close"].dropna()
        return history["Close"].dropna()
    except (KeyError, AttributeError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# DIAGNOSTICS
# ─────────────────────────────────────────────────────────────────────────────

def _concentration(rows: list, total_value: float, threshold: float) -> list[str]:
    """Return display lines for positions exceeding the single-name threshold."""
    out = []
    for r in rows:
        t, sh, c, px = r["ticker"], r["shares"], r["cost"], r["price"]
        if px is None:
            continue
        weight = sh * px / total_value * 100 if total_value else 0
        if weight > threshold:
            out.append(f"  • <b>{t}</b> {weight:.1f}% of book (>{threshold:.0f}% threshold)")
    return out


def _drawdown_leaderboard(rows: list, n: int = DRAWDOWN_LEADERBOARD_N) -> tuple[list, list]:
    """Return (worst_n, best_n) by P/L %."""
    valid = [r for r in rows if r["price"] is not None and r["cost"] > 0]
    by_pct = sorted(valid, key=lambda r: r["pnl_pct"])
    worst = by_pct[:n]
    best = list(reversed(by_pct[-n:]))
    return worst, best


def _sector_breakdown(rows: list, total_value: float, sector_cache: dict) -> list[tuple]:
    """Return list of (sector, $value, % of book, [tickers]) sorted by $ desc."""
    bucket: dict[str, dict] = defaultdict(lambda: {"value": 0.0, "tickers": []})
    for r in rows:
        if r["price"] is None:
            continue
        sector = _sector_for(r["ticker"], sector_cache) or "Unknown"
        bucket[sector]["value"] += r["shares"] * r["price"]
        bucket[sector]["tickers"].append(r["ticker"])
    out = []
    for sec, data in bucket.items():
        pct = data["value"] / total_value * 100 if total_value else 0
        out.append((sec, data["value"], pct, sorted(data["tickers"])))
    out.sort(key=lambda x: -x[1])
    return out


def _correlation_pairs(
    history: pd.DataFrame,
    tickers: list[str],
    window: int = CORRELATION_WINDOW_DAYS,
    threshold: float = CORRELATION_THRESHOLD,
) -> list[tuple[str, str, float]]:
    """
    Compute pairwise correlation of daily returns over last `window` days.
    Returns pairs with |r| >= threshold, sorted by r desc.
    """
    series_map = {}
    for t in tickers:
        s = _ticker_close_series(history, t)
        if s is None or len(s) < window + 1:
            continue
        series_map[t] = s.iloc[-(window + 1):].pct_change().dropna()
    if len(series_map) < 2:
        return []

    df = pd.DataFrame(series_map)
    df = df.dropna(how="any")
    if len(df) < window // 2:
        return []
    corr = df.corr()

    pairs = []
    cols = list(corr.columns)
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            r = corr.iloc[i, j]
            if pd.notna(r) and abs(r) >= threshold:
                pairs.append((cols[i], cols[j], float(r)))
    pairs.sort(key=lambda x: -x[2])
    return pairs


# ─────────────────────────────────────────────────────────────────────────────
# RENDERING
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_money(n) -> str:
    try:
        n = float(n)
    except Exception:
        return "—"
    if n >= 1e6:
        return f"${n / 1e6:.1f}M"
    if n >= 1e3:
        return f"${n / 1e3:.1f}K"
    return f"${n:.0f}"


def build_diagnostics_section(
    portfolio: dict,
    prices: dict,
    history: Optional[pd.DataFrame] = None,
) -> Optional[str]:
    """
    Render the diagnostics section. portfolio is the parsed portfolio.json,
    prices is {ticker: last_price}, history is the multi-index OHLC frame
    used by portfolio_report.py (optional; correlation skipped if absent).
    """
    positions = portfolio.get("positions") or []
    if not positions:
        return None

    rows = []
    total_value = 0.0
    for p in positions:
        t, sh, c = p["ticker"], p["shares"], p["cost"]
        px = prices.get(t)
        if px is None:
            rows.append({"ticker": t, "shares": sh, "cost": c, "price": None,
                         "pnl": None, "pnl_pct": None})
            continue
        value = sh * px
        pnl = value - sh * c
        pnl_pct = (px - c) / c * 100 if c else 0
        total_value += value
        rows.append({"ticker": t, "shares": sh, "cost": c, "price": px,
                     "pnl": pnl, "pnl_pct": pnl_pct})

    if total_value <= 0:
        return None

    sector_cache: dict = {}
    lines = ["<b>🩺 Portfolio Diagnostics</b>"]

    # Concentration
    conc = _concentration(rows, total_value, SINGLE_NAME_THRESHOLD)
    lines.append("")
    lines.append(f"<b>Concentration (&gt;{SINGLE_NAME_THRESHOLD:.0f}% threshold)</b>")
    if conc:
        lines.extend(conc)
    else:
        lines.append("  <i>No single-name position over threshold.</i>")

    # Drawdown leaderboard
    worst, best = _drawdown_leaderboard(rows)
    lines.append("")
    lines.append("<b>P/L leaders + laggards</b>")
    for r in best:
        lines.append(
            f"  📈 <b>{r['ticker']}</b> {r['pnl_pct']:+.1f}% "
            f"({r['pnl']:+,.0f})"
        )
    for r in worst:
        lines.append(
            f"  📉 <b>{r['ticker']}</b> {r['pnl_pct']:+.1f}% "
            f"({r['pnl']:+,.0f})"
        )

    # Sector breakdown
    sectors = _sector_breakdown(rows, total_value, sector_cache)
    lines.append("")
    lines.append("<b>Sector mix</b>")
    for sec, val, pct, tickers in sectors:
        sec_safe = html.escape(sec)
        lines.append(
            f"  • <b>{sec_safe}:</b> {_fmt_money(val)} ({pct:.1f}%) — "
            f"{' '.join(tickers)}"
        )

    # Correlation pairs
    if history is not None and not history.empty:
        tickers = [r["ticker"] for r in rows if r["price"] is not None]
        pairs = _correlation_pairs(history, tickers)
        lines.append("")
        lines.append(
            f"<b>Correlated pairs</b> "
            f"(|r| ≥ {CORRELATION_THRESHOLD}, last {CORRELATION_WINDOW_DAYS}d)"
        )
        if pairs:
            for a, b, r in pairs[:10]:  # cap at 10 to keep section tight
                lines.append(f"  • <b>{a}</b> / <b>{b}</b>  r = {r:+.2f}")
        else:
            lines.append("  <i>No pairs above threshold.</i>")

    lines.append("")
    lines.append(
        "<i>Diagnostics are factual: concentration vs. your configured "
        "threshold, P/L leaderboard, sector totals, return correlation. "
        "Not trim/exit/add recommendations — you decide what to do with "
        "the information.</i>"
    )
    return "\n".join(lines)
