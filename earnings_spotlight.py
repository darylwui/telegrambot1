"""
Earnings Spotlight — factual cards for notable upcoming earnings.

Scope = S&P 500 ∪ portfolio.json, filtered to:
  - market cap >= MIN_MARKET_CAP
  - reporting in next HORIZON_DAYS

Per ticker: factual data only (EPS consensus, revenue est, sector, 4Q beat
record, historical earnings-day move, revision delta vs 30 days ago).

The revision label ("Bullish revisions" / "Cautious revisions" / "Revisions
flat") is mechanical from the data — current consensus vs 30-day-ago
consensus. Not editorial.

Public API used by portfolio_report.py:
    build_earnings_spotlight_section() -> Optional[str]
"""

from __future__ import annotations

import html
import json
import os
import warnings
from typing import Optional

import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")


HORIZON_DAYS = 7
MIN_MARKET_CAP = 10_000_000_000  # $10B
PORTFOLIO_FILE = "portfolio.json"
UNIVERSE_FILE = "spx500.json"
CACHE_FILE = "earnings_cache.json"

MAX_WORKERS = 12  # parallel yfinance fetches

# Revision-momentum thresholds, mechanical from data
REVISION_BULLISH_THRESHOLD = 0.02   # current consensus > 30d-ago by 2%+
REVISION_BEARISH_THRESHOLD = -0.02


# ─────────────────────────────────────────────────────────────────────────────
# UNIVERSE
# ─────────────────────────────────────────────────────────────────────────────

def _load_universe() -> list[str]:
    """S&P 500 + portfolio, deduped."""
    tickers: set[str] = set()
    if os.path.exists(PORTFOLIO_FILE):
        try:
            p = json.load(open(PORTFOLIO_FILE))
            tickers |= {x["ticker"].upper() for x in p.get("positions", []) if x.get("ticker")}
        except Exception:
            pass
    if os.path.exists(UNIVERSE_FILE):
        try:
            sp = json.load(open(UNIVERSE_FILE))
            tickers |= {t.upper() for t in sp.get("tickers", []) if t}
        except Exception:
            pass
    return sorted(tickers)


# ─────────────────────────────────────────────────────────────────────────────
# PER-TICKER DATA FETCH
# ─────────────────────────────────────────────────────────────────────────────

def _quick_filter_from_info(info: dict, horizon_days: int) -> Optional[dict]:
    """
    Cheap filter using only the .info dict (no extra API calls). Checks
    if the ticker has earnings within the horizon. Returns minimal payload
    or None.
    """
    ts = info.get("earningsTimestamp") or info.get("earningsTimestampStart")
    if not ts:
        return None
    try:
        next_date = pd.Timestamp(int(ts), unit="s", tz="UTC")
    except Exception:
        return None
    today_utc = pd.Timestamp.now(tz="UTC").normalize()
    days_until = (next_date.normalize() - today_utc).days
    if days_until < 0 or days_until > horizon_days:
        return None
    return {"next_date": next_date, "days_until": days_until}


def _load_cache() -> dict:
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_cache(cache: dict) -> None:
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2, default=str)
    except Exception as e:
        print(f"[spotlight] cache save failed: {e}")


def _cache_records_to_df(records: list) -> tuple[pd.DataFrame, list]:
    """Decode cached records into a (past_df, announcement_dates) tuple."""
    if not records:
        return pd.DataFrame(columns=["Reported EPS", "Surprise(%)"]), []
    # utc=True normalizes mixed offsets (-05:00 / -04:00 from DST) cleanly
    dates = pd.to_datetime([r["date"] for r in records], utc=True)
    df = pd.DataFrame({
        "Reported EPS": [r["reported_eps"] for r in records],
        "Surprise(%)": [r["surprise_pct"] for r in records],
    }, index=dates)
    return df, list(dates)


def _df_to_cache_records(df: pd.DataFrame) -> list:
    return [
        {
            "date": idx.isoformat(),
            "reported_eps": float(row["Reported EPS"]),
            "surprise_pct": float(row["Surprise(%)"]),
        }
        for idx, row in df.iterrows()
        if pd.notna(row["Reported EPS"]) and pd.notna(row["Surprise(%)"])
    ]


def _past_performance(ticker_obj: yf.Ticker, ticker: str, cache: dict) -> dict:
    """
    Return {
      'past_df': DataFrame with ['Reported EPS', 'Surprise(%)'],
      'announcement_dates': list of past actual announcement dates OR None
                           when the source only provides fiscal-period-end dates
                           (in which case we cannot compute earnings-day moves).
    }

    Strategy:
      1) Try get_earnings_dates first — has real announcement dates, enabling
         the historical earnings-day move calculation.
      2) Fall back to earnings_history — stable but indexed by fiscal period
         end, not announcement date. We use it for beat/miss stats only and
         flag announcement_dates as None so the move calc is skipped.
    """
    # Path 1: get_earnings_dates — real announcement dates. Updates cache on success.
    try:
        ed = ticker_obj.get_earnings_dates(limit=10)
        if ed is not None and not ed.empty:
            idx_tz = ed.index.tz
            now = pd.Timestamp.now(tz=idx_tz) if idx_tz else pd.Timestamp.now()
            past = ed[ed.index < now].dropna(subset=["Reported EPS"]).sort_index(ascending=False).head(4)
            if not past.empty and "Surprise(%)" in past.columns:
                past_df = past[["Reported EPS", "Surprise(%)"]]
                cache[ticker] = _df_to_cache_records(past_df)
                return {
                    "past_df": past_df,
                    "announcement_dates": list(past_df.index),
                }
    except Exception:
        pass

    # Path 2: cache — built up from past successful runs. Has real announcement dates.
    if ticker in cache and cache[ticker]:
        df, dates = _cache_records_to_df(cache[ticker])
        if not df.empty:
            return {"past_df": df, "announcement_dates": dates}

    # Path 3: earnings_history — beat/miss only, no announcement dates
    try:
        eh = ticker_obj.earnings_history
        if eh is not None and not eh.empty:
            df = eh.sort_index(ascending=False).head(4).copy()
            rep_col = next((c for c in ["epsActual", "Reported EPS"] if c in df.columns), None)
            sur_col = next((c for c in ["surprisePercent", "Surprise(%)"] if c in df.columns), None)
            if rep_col and sur_col:
                out = pd.DataFrame({
                    "Reported EPS": df[rep_col],
                    "Surprise(%)": df[sur_col],
                }, index=df.index).dropna(subset=["Reported EPS"])
                if not out.empty:
                    return {"past_df": out, "announcement_dates": None}
    except Exception:
        pass

    return {"past_df": pd.DataFrame(columns=["Reported EPS", "Surprise(%)"]),
            "announcement_dates": None}


def _next_quarter_eps_estimate(ticker_obj: yf.Ticker) -> Optional[float]:
    """Quarterly EPS consensus for the upcoming report. Uses earnings_estimate DF."""
    try:
        est = ticker_obj.earnings_estimate
        if est is not None and not est.empty and "0q" in est.index:
            v = est.loc["0q", "avg"]
            return float(v) if pd.notna(v) else None
    except Exception:
        pass
    return None


def _historical_moves(hist: pd.DataFrame, past_dates: list) -> list[float]:
    """Compute close[d-1] -> close[d+1] % moves for each past earnings date."""
    if hist is None or hist.empty:
        return []
    hist = hist.copy()
    if hist.index.tz is not None:
        hist.index = hist.index.tz_localize(None)
    moves = []
    for d in past_dates:
        try:
            d_naive = pd.Timestamp(d).tz_localize(None) if pd.Timestamp(d).tzinfo else pd.Timestamp(d)
            pos = hist.index.get_indexer([d_naive], method="nearest")[0]
            if pos > 0 and pos + 1 < len(hist):
                before = float(hist["Close"].iloc[pos - 1])
                after = float(hist["Close"].iloc[pos + 1])
                moves.append((after - before) / before * 100)
        except Exception:
            pass
    return moves


def _rsi_from_hist(hist: pd.DataFrame, period: int = 14) -> Optional[float]:
    """Wilder RSI on daily closes. Returns latest value or None."""
    if hist is None or hist.empty or len(hist) < period + 1:
        return None
    close = hist["Close"].astype(float)
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    try:
        return float(rsi.iloc[-1])
    except Exception:
        return None


def _buy_verdict(d: dict) -> tuple[str, str]:
    """
    Mechanical buy-at-current-levels call. Returns (label, reason).
    Signals scored: target upside, revision momentum, RSI, beat rate.
    """
    score = 0
    reasons: list[str] = []

    upside = d.get("target_upside_pct")
    if upside is not None:
        if upside >= 15:
            score += 2; reasons.append(f"target +{upside:.0f}%")
        elif upside >= 5:
            score += 1; reasons.append(f"target +{upside:.0f}%")
        elif upside <= -5:
            score -= 1; reasons.append(f"target {upside:.0f}%")

    rev = d.get("revision_label") or ""
    if "Bullish" in rev:
        score += 1; reasons.append("↑ revisions")
    elif "Cautious" in rev:
        score -= 1; reasons.append("↓ revisions")

    rsi = d.get("rsi")
    if rsi is not None:
        if rsi < 30:
            score += 1; reasons.append(f"RSI {rsi:.0f} oversold")
        elif rsi > 70:
            score -= 1; reasons.append(f"RSI {rsi:.0f} overbought")

    n = d.get("past_n") or 0
    if n >= 3:
        beat_rate = (d.get("beats") or 0) / n
        if beat_rate >= 0.75:
            score += 1; reasons.append(f"beats {d['beats']}/{n}")
        elif beat_rate <= 0.25:
            score -= 1; reasons.append(f"misses {d.get('misses',0)}/{n}")

    if score >= 2:
        label = "🟢 BUY"
    elif score == 1:
        label = "🟡 Nibble"
    elif score == 0:
        label = "⚪ Hold / wait"
    else:
        label = "🔴 Avoid"
    return label, ", ".join(reasons) if reasons else "no clear signal"


def _earnings_play_verdict(d: dict) -> tuple[str, str]:
    """
    Mechanical earnings-play call. Returns (label, reason).
    Rewards proven movers with bullish setup; punishes flat or bearish.
    """
    score = 0
    reasons: list[str] = []

    avg_move = d.get("avg_abs_move")
    if avg_move is not None:
        if avg_move >= 8:
            score += 2; reasons.append(f"|move| {avg_move:.1f}%")
        elif avg_move >= 5:
            score += 1; reasons.append(f"|move| {avg_move:.1f}%")
        elif avg_move < 3:
            score -= 1; reasons.append(f"|move| {avg_move:.1f}% quiet")

    n = d.get("past_n") or 0
    beat_rate = (d.get("beats") or 0) / n if n else 0
    rev = d.get("revision_label") or ""
    if n >= 3 and beat_rate >= 0.75 and "Bullish" in rev:
        score += 2; reasons.append("beats + ↑ revs")
    elif n >= 3 and (beat_rate <= 0.25 or "Cautious" in rev):
        score -= 1
        if beat_rate <= 0.25:
            reasons.append(f"weak beat record")
        if "Cautious" in rev:
            reasons.append("↓ revs")

    du = d.get("days_until")
    if du is not None and 1 <= du <= 5:
        score += 1

    if score >= 3:
        label = "🎯 Strong ER play"
    elif score >= 1:
        label = "🎲 Lottery — small size"
    else:
        label = "⏭️  Skip"
    return label, ", ".join(reasons) if reasons else "no edge"


def _revision_label(ticker_obj: yf.Ticker) -> tuple[Optional[str], Optional[float]]:
    """
    Compare current quarter EPS estimate vs 30-days-ago estimate.
    Returns (label, delta_pct). Mechanical from data — not editorial.
    """
    try:
        est = ticker_obj.earnings_estimate
        if est is None or est.empty or "0q" not in est.index:
            return (None, None)
        row = est.loc["0q"]
        current = row.get("avg")
        d30 = row.get("growth")  # not the right field
        # earnings_estimate has columns: avg, low, high, yearAgoEps, numberOfAnalysts, growth
        # No direct 30-days-ago. Use eps_trend instead.
        trend = ticker_obj.eps_trend
        if trend is None or trend.empty or "0q" not in trend.index:
            return (None, None)
        trow = trend.loc["0q"]
        current = trow.get("current")
        d30_ago = trow.get("30daysAgo")
        if current is None or d30_ago is None or pd.isna(current) or pd.isna(d30_ago) or d30_ago == 0:
            return (None, None)
        delta = (current - d30_ago) / abs(d30_ago)
        if delta > REVISION_BULLISH_THRESHOLD:
            return ("↑ Bullish revisions", delta * 100)
        if delta < REVISION_BEARISH_THRESHOLD:
            return ("↓ Cautious revisions", delta * 100)
        return ("· Revisions flat", delta * 100)
    except Exception:
        return (None, None)


def _dossier_for_ticker(tk: str, cache: dict) -> Optional[dict]:
    """Build the full dossier dict for one ticker, or None if it doesn't qualify."""
    import logging
    # yfinance logs 404s for delisted/renamed tickers — suppress to keep output clean
    yf_logger = logging.getLogger("yfinance")
    prev_level = yf_logger.level
    yf_logger.setLevel(logging.CRITICAL)
    try:
        t = yf.Ticker(tk)
        try:
            info = t.info or {}
        except Exception:
            return None
        market_cap = info.get("marketCap") or 0
        if market_cap < MIN_MARKET_CAP:
            return None
    finally:
        yf_logger.setLevel(prev_level)

    # Cheap filter first — no extra API calls beyond the .info we already have
    quick = _quick_filter_from_info(info, HORIZON_DAYS)
    if not quick:
        return None

    # Qualifying — fetch past performance + quarterly EPS estimate from stable DFs
    perf = _past_performance(t, tk, cache)
    past = perf["past_df"]
    announcement_dates = perf["announcement_dates"]
    eps_estimate_next = _next_quarter_eps_estimate(t)

    earnings = {
        "next_date": quick["next_date"],
        "days_until": quick["days_until"],
        "eps_estimate_next": eps_estimate_next,
        "past": past,
        "announcement_dates": announcement_dates,
    }
    beats = int((past["Surprise(%)"] > 0).sum()) if "Surprise(%)" in past.columns else 0
    misses = int((past["Surprise(%)"] < 0).sum()) if "Surprise(%)" in past.columns else 0
    ins = int((past["Surprise(%)"] == 0).sum()) if "Surprise(%)" in past.columns else 0

    # Always pull recent history — needed for RSI regardless of move calc.
    try:
        hist = t.history(period="3y", auto_adjust=True)
    except Exception:
        hist = pd.DataFrame()

    # Only compute earnings-day moves when we have ACTUAL announcement dates
    # (Path 1 of _past_performance). Fiscal-period-end fallback would measure
    # unrelated price action, so skip it rather than report wrong numbers.
    moves: list[float] = []
    avg_abs_move = None
    if announcement_dates:
        moves = _historical_moves(hist, list(announcement_dates[:4]))
        avg_abs_move = (sum(abs(m) for m in moves) / len(moves)) if moves else None

    rsi = _rsi_from_hist(hist)
    price = info.get("currentPrice") or info.get("regularMarketPrice")
    target = info.get("targetMeanPrice")
    target_upside_pct = None
    if isinstance(price, (int, float)) and isinstance(target, (int, float)) and price:
        target_upside_pct = (target - price) / price * 100

    # Earnings call timing — BMO vs AMC heuristic from the timestamp hour
    next_dt = earnings["next_date"]
    hour_et = next_dt.tz_convert("America/New_York").hour if next_dt.tz else None
    if hour_et is not None:
        bmo_amc = "BMO" if hour_et < 12 else "AMC"
    else:
        bmo_amc = "—"

    # Revenue + EPS analyst count from the structured estimate DataFrames
    rev_avg = None
    eps_count = info.get("numberOfAnalystOpinions")
    try:
        rev_df = t.revenue_estimate
        if rev_df is not None and not rev_df.empty and "0q" in rev_df.index:
            rev_avg = rev_df.loc["0q", "avg"]
    except Exception:
        pass
    try:
        est_df = t.earnings_estimate
        if est_df is not None and not est_df.empty and "0q" in est_df.index:
            n = est_df.loc["0q", "numberOfAnalysts"]
            if pd.notna(n):
                eps_count = int(n)
    except Exception:
        pass

    rev_label, rev_delta = _revision_label(t)

    d = {
        "ticker": tk,
        "name": info.get("shortName") or info.get("longName") or tk,
        "next_date": next_dt.date(),
        "days_until": earnings["days_until"],
        "bmo_amc": bmo_amc,
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "market_cap": market_cap,
        "price": price,
        "target_mean": target,
        "target_upside_pct": target_upside_pct,
        "rsi": rsi,
        "eps_est": earnings["eps_estimate_next"],
        "rev_est": rev_avg,
        "analyst_count": eps_count,
        "beats": beats,
        "misses": misses,
        "in_lines": ins,
        "past_n": len(past),
        "moves": moves,
        "avg_abs_move": avg_abs_move,
        "revision_label": rev_label,
        "revision_delta_pct": rev_delta,
    }
    d["buy_verdict"], d["buy_reason"] = _buy_verdict(d)
    d["play_verdict"], d["play_reason"] = _earnings_play_verdict(d)
    return d


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
    if n >= 1e12:
        return f"${n / 1e12:.2f}T"
    if n >= 1e9:
        return f"${n / 1e9:.1f}B"
    if n >= 1e6:
        return f"${n / 1e6:.0f}M"
    return f"${n:.0f}"


def _fmt_date(d) -> str:
    months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    return f"{months[d.month - 1]} {d.day} ({d.strftime('%a')})"


def _render_card(d: dict) -> list[str]:
    name = html.escape(d["name"])
    sector = html.escape(d["sector"] or "—")
    eps_s = f"${d['eps_est']:.2f}" if isinstance(d["eps_est"], (int, float)) and pd.notna(d["eps_est"]) else "—"
    rev_s = _fmt_money(d["rev_est"])
    analyst_s = f"{d['analyst_count']} analysts" if d.get("analyst_count") else ""
    mkt_s = _fmt_money(d["market_cap"])
    bm_s = f"{d['beats']}/{d['misses']}/{d['in_lines']} of {d['past_n']}"
    avg_s = f"{d['avg_abs_move']:.1f}%" if d["avg_abs_move"] is not None else "—"
    moves_s = ", ".join(f"{m:+.1f}%" for m in d["moves"]) if d["moves"] else "—"

    lines = []
    lines.append(f"<b>{d['ticker']}</b> — {name} · {d['bmo_amc']}")
    parts1 = [f"EPS est <b>{eps_s}</b>"]
    if analyst_s:
        parts1.append(analyst_s)
    parts1.append(f"Rev est {rev_s}")
    parts1.append(f"Mkt {mkt_s}")
    parts1.append(sector)
    lines.append("  " + " · ".join(parts1))

    # Price / target / RSI line — only rendered when we have data
    px_bits: list[str] = []
    if isinstance(d.get("price"), (int, float)):
        px_bits.append(f"Px ${d['price']:.2f}")
    if isinstance(d.get("target_mean"), (int, float)):
        up = d.get("target_upside_pct")
        up_s = f" ({up:+.0f}%)" if up is not None else ""
        px_bits.append(f"Tgt ${d['target_mean']:.2f}{up_s}")
    if isinstance(d.get("rsi"), (int, float)):
        px_bits.append(f"RSI {d['rsi']:.0f}")
    if px_bits:
        lines.append("  " + " · ".join(px_bits))

    lines.append(f"  Beat record: <b>{bm_s}</b> · Avg ⎮move⎮ {avg_s}")
    if d["moves"]:
        lines.append(f"  Past 4 moves: {moves_s}")
    if d["revision_label"]:
        rev_delta = d["revision_delta_pct"]
        rev_pct_s = f" ({rev_delta:+.1f}% vs 30d ago)" if rev_delta is not None else ""
        lines.append(f"  {d['revision_label']}{rev_pct_s}")

    # Verdicts — mechanical, from data
    lines.append(f"  <b>{d['buy_verdict']}</b> at current — <i>{html.escape(d['buy_reason'])}</i>")
    lines.append(f"  <b>{d['play_verdict']}</b> — <i>{html.escape(d['play_reason'])}</i>")
    return lines


def build_earnings_spotlight_section() -> Optional[str]:
    """Top-level entry. Returns rendered HTML section or None if nothing qualifies."""
    universe = _load_universe()
    if not universe:
        print("[spotlight] empty universe; section skipped")
        return None

    cache = _load_cache()
    cache_size_before = sum(1 for v in cache.values() if v)

    print(f"[spotlight] scanning {len(universe)} tickers "
          f"(cache: {cache_size_before} entries, workers: {MAX_WORKERS})...")
    dossiers: list[dict] = []
    errors = 0
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_dossier_for_ticker, tk, cache): tk for tk in universe}
        for fut in as_completed(futures):
            tk = futures[fut]
            try:
                d = fut.result()
                if d:
                    dossiers.append(d)
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"[spotlight] {tk}: {type(e).__name__}: {str(e)[:80]}")

    cache_size_after = sum(1 for v in cache.values() if v)
    if cache_size_after != cache_size_before:
        _save_cache(cache)
        print(f"[spotlight] cache updated: {cache_size_before} → {cache_size_after} entries")

    print(f"[spotlight] {len(dossiers)} qualified for next {HORIZON_DAYS}d window "
          f"(scanned {len(universe)}, errors {errors})")
    if not dossiers:
        return None

    dossiers.sort(key=lambda x: (x["next_date"], x["ticker"]))

    lines = [f"<b>📈 Earnings Spotlight — next {HORIZON_DAYS} days</b>"]
    lines.append(f"  <i>S&amp;P 500 ∪ portfolio, market cap ≥ {_fmt_money(MIN_MARKET_CAP)}</i>")

    cur_date = None
    for d in dossiers:
        if d["next_date"] != cur_date:
            cur_date = d["next_date"]
            lines.append("")
            lines.append(f"<b>📅 {_fmt_date(cur_date)}</b>")
        lines.append("")
        lines.extend(_render_card(d))

    lines.append("")
    lines.append(
        "<i>Verdicts are mechanical from data (target upside, revisions, RSI, "
        "beat rate, historical earnings-day move) — not editorial. Sanity-check before acting.</i>"
    )
    return "\n".join(lines)


if __name__ == "__main__":
    out = build_earnings_spotlight_section()
    print(out or "[no spotlight — no qualifying earnings in horizon]")
