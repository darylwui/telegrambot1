"""
User-defined stock screener.

Reads screener.json (filters + universes per screen) and themes.json (theme
→ ticker name mappings). For each screen, expands the universe, fetches
yfinance.info per ticker, applies the user's numeric filters, and returns
matches with raw data.

The bot does NOT rank, recommend, or pre-select tickers. The user writes
the universe and the filters; the screener just executes them.

Public API used by portfolio_report.py:
    build_screener_section() -> Optional[str]
"""

from __future__ import annotations

import html
import json
import os
from typing import Optional

import yfinance as yf


SCREENER_FILE = "screener.json"
THEMES_FILE = "themes.json"


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG LOAD
# ─────────────────────────────────────────────────────────────────────────────

def _load_screens() -> list[dict]:
    if not os.path.exists(SCREENER_FILE):
        return []
    try:
        with open(SCREENER_FILE) as f:
            cfg = json.load(f)
    except Exception as e:
        print(f"[screener] config load failed: {e}")
        return []
    screens = cfg.get("screens") or []
    return [s for s in screens if isinstance(s, dict) and s.get("name")]


def _load_themes() -> dict:
    if not os.path.exists(THEMES_FILE):
        return {}
    try:
        with open(THEMES_FILE) as f:
            cfg = json.load(f)
    except Exception as e:
        print(f"[screener] themes load failed: {e}")
        return {}
    return cfg.get("themes") or {}


# ─────────────────────────────────────────────────────────────────────────────
# UNIVERSE EXPANSION
# ─────────────────────────────────────────────────────────────────────────────

def _expand_universe(universe, themes: dict) -> list[str]:
    """
    Accepts:
      - list of tickers → use as-is
      - {'theme': 'name'} → look up in themes.json
      - {'tickers': [...]} → use the embedded list (alias)
    Returns deduped, uppercased ticker list. Never auto-suggests.
    """
    if isinstance(universe, list):
        tickers = universe
    elif isinstance(universe, dict):
        if "theme" in universe:
            theme_name = universe["theme"]
            tickers = themes.get(theme_name) or []
            if not tickers:
                print(f"[screener] unknown theme: {theme_name}")
        elif "tickers" in universe:
            tickers = universe["tickers"]
        else:
            tickers = []
    else:
        tickers = []
    return sorted({str(t).upper() for t in tickers if t})


# ─────────────────────────────────────────────────────────────────────────────
# FILTERS
# ─────────────────────────────────────────────────────────────────────────────

# Map filter key → (yfinance.info field, comparison direction).
# Direction: "min" means info_value >= threshold passes; "max" means info_value <= threshold passes.
_FILTER_MAP: dict[str, tuple[str, str]] = {
    "marketCapMin":             ("marketCap", "min"),
    "marketCapMax":             ("marketCap", "max"),
    "currentPriceMin":          ("__price__", "min"),
    "currentPriceMax":          ("__price__", "max"),
    "trailingPEMin":            ("trailingPE", "min"),
    "trailingPEMax":            ("trailingPE", "max"),
    "forwardPEMin":             ("forwardPE", "min"),
    "forwardPEMax":             ("forwardPE", "max"),
    "psRatioMin":               ("priceToSalesTrailing12Months", "min"),
    "psRatioMax":               ("priceToSalesTrailing12Months", "max"),
    "pbRatioMin":               ("priceToBook", "min"),
    "pbRatioMax":               ("priceToBook", "max"),
    "betaMin":                  ("beta", "min"),
    "betaMax":                  ("beta", "max"),
    "dividendYieldMin":         ("dividendYield", "min"),
    "dividendYieldMax":         ("dividendYield", "max"),
    "revenueGrowthMin":         ("revenueGrowth", "min"),
    "revenueGrowthMax":         ("revenueGrowth", "max"),
    "profitMarginsMin":         ("profitMargins", "min"),
    "profitMarginsMax":         ("profitMargins", "max"),
    "shortPercentOfFloatMax":   ("shortPercentOfFloat", "max"),
}


def _get_info(ticker: str) -> dict:
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception as e:
        print(f"[screener] info fetch failed for {ticker}: {e}")
        return {}
    info["__price__"] = info.get("regularMarketPrice") or info.get("previousClose")
    return info


def _apply_filters(info: dict, filters: dict) -> tuple[bool, list[str]]:
    """
    Apply all filters. Returns (passed, list_of_fail_reasons).
    Missing info data on a filtered field = fail (we don't pass on absent data).
    """
    fails: list[str] = []
    for fkey, threshold in (filters or {}).items():
        if fkey not in _FILTER_MAP:
            fails.append(f"unknown filter {fkey}")
            continue
        info_key, direction = _FILTER_MAP[fkey]
        val = info.get(info_key)
        if val is None:
            fails.append(f"no data for {fkey}")
            continue
        try:
            v = float(val)
            t = float(threshold)
        except (TypeError, ValueError):
            fails.append(f"non-numeric {fkey}")
            continue
        if direction == "min" and v < t:
            fails.append(f"{fkey}: {v:g} < {t:g}")
        elif direction == "max" and v > t:
            fails.append(f"{fkey}: {v:g} > {t:g}")
    return (len(fails) == 0, fails)


# ─────────────────────────────────────────────────────────────────────────────
# RENDERING
# ─────────────────────────────────────────────────────────────────────────────

def _fmt_money(n) -> str:
    if n is None:
        return "—"
    try:
        n = float(n)
    except Exception:
        return "—"
    sign = "-" if n < 0 else ""
    n = abs(n)
    if n >= 1e12:
        return f"{sign}${n / 1e12:.2f}T"
    if n >= 1e9:
        return f"{sign}${n / 1e9:.2f}B"
    if n >= 1e6:
        return f"{sign}${n / 1e6:.0f}M"
    if n >= 1e3:
        return f"{sign}${n / 1e3:.0f}K"
    return f"{sign}${n:.2f}"


def _match_data_line(ticker: str, info: dict) -> str:
    """Compact one-line summary of the raw data fetched for a match."""
    parts = [f"<b>{ticker}</b>"]
    px = info.get("__price__")
    if px is not None:
        parts.append(f"${float(px):.2f}")
    mc = info.get("marketCap")
    if mc:
        parts.append(f"Mkt {_fmt_money(mc)}")
    pe = info.get("trailingPE")
    if pe is not None:
        parts.append(f"P/E {float(pe):.1f}×")
    ps = info.get("priceToSalesTrailing12Months")
    if ps is not None:
        parts.append(f"P/S {float(ps):.1f}×")
    beta = info.get("beta")
    if beta is not None:
        parts.append(f"β {float(beta):.2f}")
    rg = info.get("revenueGrowth")
    if rg is not None:
        parts.append(f"Rev g {float(rg) * 100:+.0f}%")
    dy = info.get("dividendYield")
    if dy is not None and dy > 0:
        # yfinance has been inconsistent across versions: sometimes decimal
        # (0.025 = 2.5%), sometimes already in percent (2.5 = 2.5%).
        dy_pct = float(dy) if float(dy) > 1 else float(dy) * 100
        # Suppress implausible values — real div yields are <20%. Beyond that
        # something is wrong with the field and showing it just misleads.
        if 0 < dy_pct <= 20:
            parts.append(f"Div {dy_pct:.2f}%")
    return " · ".join(parts)


def _run_screen(screen: dict, themes: dict) -> dict:
    name = screen.get("name", "(unnamed)")
    universe = _expand_universe(screen.get("universe"), themes)
    filters = screen.get("filters") or {}

    matches: list[tuple[str, dict]] = []
    near_misses: list[tuple[str, dict, list[str]]] = []
    errors: list[str] = []

    for t in universe:
        info = _get_info(t)
        if not info:
            errors.append(t)
            continue
        passed, fails = _apply_filters(info, filters)
        if passed:
            matches.append((t, info))
        else:
            near_misses.append((t, info, fails))

    return {
        "name": name,
        "universe_size": len(universe),
        "matches": matches,
        "near_misses": near_misses,
        "errors": errors,
        "filters": filters,
    }


def _render_screen(result: dict) -> list[str]:
    lines: list[str] = []
    n_matches = len(result["matches"])
    n_universe = result["universe_size"]
    lines.append(f"<b>🔍 Screen: {html.escape(result['name'])}</b>")
    lines.append(f"  Universe {n_universe} · {n_matches} pass · "
                 f"{len(result['near_misses'])} fail · "
                 f"{len(result['errors'])} no-data")

    # Filter summary (compact)
    if result["filters"]:
        flt_parts = []
        for k, v in result["filters"].items():
            if isinstance(v, float):
                flt_parts.append(f"{k}={v:g}")
            else:
                flt_parts.append(f"{k}={v}")
        lines.append(f"  Filters: {html.escape(' · '.join(flt_parts))}")

    if not result["matches"]:
        lines.append("  <i>No matches.</i>")
    else:
        lines.append("  Matches:")
        for ticker, info in result["matches"]:
            lines.append(f"    • {_match_data_line(ticker, info)}")

    # Show near-misses by smallest number of failing filters (closest to passing).
    # Cap at 3 to keep the section tight.
    if result["near_misses"]:
        sorted_misses = sorted(result["near_misses"], key=lambda x: len(x[2]))[:3]
        lines.append("  Near misses (closest to passing):")
        for ticker, info, fails in sorted_misses:
            why = "; ".join(fails[:2])
            lines.append(f"    • <b>{ticker}</b> ${float(info.get('__price__') or 0):.2f} "
                         f"— failed: {html.escape(why)}")

    return lines


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────────────────

def build_screener_section() -> Optional[str]:
    """
    Top-level entry called by portfolio_report.py. Reads config, runs all
    enabled screens, renders an HTML section. Returns None if config missing
    or no screens are defined.
    """
    screens = _load_screens()
    if not screens:
        return None
    themes = _load_themes()

    lines: list[str] = ["<b>🧮 Screens</b>"]
    for sc in screens:
        try:
            result = _run_screen(sc, themes)
        except Exception as e:
            print(f"[screener] screen {sc.get('name')!r} failed: {e}")
            continue
        lines.append("")
        lines.extend(_render_screen(result))

    if len(lines) == 1:
        return None  # nothing was actually rendered

    lines.append("")
    lines.append(
        "<i>Screens are yours — universe + filters from screener.json + "
        "themes.json. The bot returns matches with raw data; it does not "
        "rank or recommend.</i>"
    )
    return "\n".join(lines)


if __name__ == "__main__":
    out = build_screener_section()
    if out:
        print(out)
    else:
        print("[screener] no section produced (no screens configured)")
