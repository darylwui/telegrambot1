"""
Factual diagnostics on the existing portfolio.

Surfaces: P/L leaderboard (best/worst P/L %) and correlated pairs (60-day
return |r| >= 0.85).

Intentionally NOT shown here (to avoid duplication with earlier sections):
  - Single-name + cluster concentration → already in At-a-glance alerts
  - Sector grouping → already in Cluster exposure (uses your user-defined
    clusters, smarter than raw yfinance sectors)

Does NOT recommend trims, exits, or adds. Tells you what the math says about
positions you already hold; you decide what to do with the information.

Public API used by portfolio_report.py:
    build_diagnostics_section(portfolio, prices, history_df) -> Optional[str]
"""

from __future__ import annotations

from typing import Optional

import pandas as pd


CORRELATION_WINDOW_DAYS = 60
CORRELATION_THRESHOLD = 0.85
DRAWDOWN_LEADERBOARD_N = 3


# ─────────────────────────────────────────────────────────────────────────────
# DATA HELPERS
# ─────────────────────────────────────────────────────────────────────────────

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

def _drawdown_leaderboard(rows: list, n: int = DRAWDOWN_LEADERBOARD_N) -> tuple[list, list]:
    """Return (worst_n, best_n) by P/L %."""
    valid = [r for r in rows if r["price"] is not None and r["cost"] > 0]
    by_pct = sorted(valid, key=lambda r: r["pnl_pct"])
    worst = by_pct[:n]
    best = list(reversed(by_pct[-n:]))
    return worst, best
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

    lines = ["<b>🩺 Portfolio Diagnostics</b>"]

    # P/L leaders + laggards
    # (Concentration is intentionally NOT shown here — at-a-glance alerts
    # already surface single-name + cluster concentration warnings, and
    # showing them twice is noise.)
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

    # Correlation pairs
    # (Sector mix is intentionally NOT shown here — Cluster exposure
    # earlier in the message already groups your book and uses your
    # user-defined clusters, which are smarter than raw yfinance sectors.)
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
        "<i>Diagnostics: P/L leaderboard + 60-day return correlation. "
        "Not trim/exit/add calls — you decide what to do with the info. "
        "(Concentration alerts at the top; sector grouping is in "
        "Cluster exposure earlier.)</i>"
    )
    return "\n".join(lines)
