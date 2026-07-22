#!/usr/bin/env python3
"""
Portfolio report: reads portfolio.json, fetches live prices + analyst data
+ recent news + technical indicators + per-ticker reads from the
trading-research daily brief, computes P&L (and 7-day delta), and posts
a bucketed action-oriented summary to the Portfolio Telegram chat.

Runs twice daily via GitHub Actions:
  - Tue-Sat 08:23 SGT (00:23 UTC)  → Morning pre-market read
  - Mon-Fri 20:00 SGT (12:00 UTC)  → Evening post-close review

Required env vars:
  PORTFOLIO_BOT_TOKEN  — Telegram bot token (sender)
  PORTFOLIO_CHAT_ID    — Telegram chat id (destination)

Optional env vars:
  BRIEF_REPO_TOKEN     — fine-grained PAT with `contents:read` on
                         darylwui/trading-research. Enables embedding
                         your daily brief's per-ticker read into each
                         position block. If unset, that section is skipped.
  PORTFOLIO_DRY_RUN    — set to "1" to print to stdout instead of posting.
"""
import datetime
import html
import json
import os
import sys
import time
from typing import Optional

import pandas as pd
import pytz
import requests
import yfinance as yf

from clusters import CLUSTERS, SINGLE_NAME_THRESHOLD, CLUSTER_THRESHOLD, classify
import garp

try:
    from watchlist_signals import build_watchlist_section
except Exception as _e:
    build_watchlist_section = None
    print(f"watchlist_signals unavailable: {_e}")

try:
    from diagnostics import build_diagnostics_section
except Exception as _e:
    build_diagnostics_section = None
    print(f"diagnostics unavailable: {_e}")

try:
    from earnings_spotlight import build_earnings_spotlight_section
except Exception as _e:
    build_earnings_spotlight_section = None
    print(f"earnings_spotlight unavailable: {_e}")

BOT_TOKEN = os.environ.get("PORTFOLIO_BOT_TOKEN", "")
CHAT_ID = os.environ.get("PORTFOLIO_CHAT_ID", "")
BRIEF_REPO_TOKEN = os.environ.get("BRIEF_REPO_TOKEN", "")
DRY_RUN = os.environ.get("PORTFOLIO_DRY_RUN") == "1"

PORTFOLIO_FILE = "portfolio.json"
HISTORY_FILE = "portfolio_history.json"
HISTORY_MAX = 30  # keep ~15 trading days of AM+PM snapshots

BRIEF_REPO = "darylwui/trading-research"


# ─────────────────────────────────────────────────────────────────────────────
# DATA LAYER
# ─────────────────────────────────────────────────────────────────────────────

def load_portfolio() -> dict:
    with open(PORTFOLIO_FILE) as f:
        return json.load(f)


def get_history(tickers: list[str], period: str = "250d") -> pd.DataFrame:
    """Download ~1y of daily history for indicator computation."""
    try:
        return yf.download(
            tickers,
            period=period,
            auto_adjust=True,
            progress=False,
            group_by="ticker",
        )
    except Exception as e:
        print(f"history download failed: {e}")
        return pd.DataFrame()


def _ticker_close_series(history: pd.DataFrame, ticker: str) -> Optional[pd.Series]:
    """Pull a clean close series for one ticker from the multi-index frame."""
    try:
        if isinstance(history.columns, pd.MultiIndex):
            return history[ticker]["Close"].dropna()
        return history["Close"].dropna()
    except (KeyError, AttributeError):
        return None


def compute_indicators(history: pd.DataFrame, ticker: str) -> dict:
    """Return RSI(14, Wilder), SMA20/50/200, ATR14, 52w high, and last close."""
    out = {"rsi": None, "sma20": None, "sma50": None, "sma200": None, "last": None,
           "atr14": None, "high52w": None}
    closes = _ticker_close_series(history, ticker)
    if closes is None or len(closes) < 20:
        return out

    out["last"] = round(float(closes.iloc[-1]), 2)
    out["high52w"] = round(float(closes.tail(252).max()), 2)
    if len(closes) >= 20:
        out["sma20"] = round(float(closes.rolling(20).mean().iloc[-1]), 2)
    if len(closes) >= 50:
        out["sma50"] = round(float(closes.rolling(50).mean().iloc[-1]), 2)
    if len(closes) >= 200:
        out["sma200"] = round(float(closes.rolling(200).mean().iloc[-1]), 2)

    # Wilder RSI(14)
    if len(closes) >= 15:
        diff = closes.diff()
        up = diff.clip(lower=0)
        down = -diff.clip(upper=0)
        alpha = 1 / 14
        avg_up = up.ewm(alpha=alpha, adjust=False).mean()
        avg_down = down.ewm(alpha=alpha, adjust=False).mean()
        rs = avg_up / avg_down.replace(0, float("nan"))
        rsi = 100 - (100 / (1 + rs))
        last_rsi = rsi.iloc[-1]
        if pd.notna(last_rsi):
            out["rsi"] = round(float(last_rsi), 1)

    # ATR(14) — needs High/Low from history
    try:
        if isinstance(history.columns, pd.MultiIndex):
            hi = history[ticker]["High"].dropna().astype(float)
            lo = history[ticker]["Low"].dropna().astype(float)
            cl = history[ticker]["Close"].dropna().astype(float)
        else:
            hi = history["High"].dropna().astype(float)
            lo = history["Low"].dropna().astype(float)
            cl = history["Close"].dropna().astype(float)
        tr = pd.concat([hi - lo, (hi - cl.shift(1)).abs(), (lo - cl.shift(1)).abs()], axis=1).max(axis=1)
        out["atr14"] = round(float(tr.ewm(alpha=1/14, adjust=False).mean().iloc[-1]), 4)
    except Exception:
        pass

    return out


def compute_entry_levels(px: float, ind: dict, analyst_target: Optional[float]) -> dict:
    """Derive entry zone, stop, and take-profit from pre-computed indicators."""
    sma20  = ind.get("sma20")
    sma50  = ind.get("sma50")
    sma200 = ind.get("sma200")
    rsi    = ind.get("rsi") or 50
    atr    = ind.get("atr14") or px * 0.02   # fallback: 2% of price
    high52 = ind.get("high52w") or px

    screen = el = eh = st = None

    if sma200 and px > sma200 and rsi < 35 and sma50 and abs(px / sma50 - 1) <= 0.05:
        screen = "oversold pullback"
        el, eh = px - 0.25 * atr, px + 0.25 * atr
        st = sma200 * 0.98
    elif sma200 and sma20 and sma50 and sma20 > sma50 > sma200 and 55 <= rsi <= 70 and px > sma20:
        screen = "momentum"
        el, eh = sma20 * 0.99, sma20 * 1.015
        st = sma50 * 0.98
    elif px >= high52 * 0.99:
        screen = "breakout"
        el, eh = high52 * 0.99, high52 * 1.01
        st = high52 * 0.95
    elif sma200 and px > sma200 and 40 < rsi < 55 and sma50:
        screen = "uptrend cooling"
        el, eh = sma50 * 0.99, sma50 * 1.01
        st = sma200 * 0.98
    elif rsi > 75 and sma20 and px > sma20 * 1.10:
        screen = "extended"
        el, eh = px - 0.25 * atr, px + 0.25 * atr
        st = px + 1.5 * atr   # short stop above price
    else:
        screen = "no clean setup"
        el, eh = px - 0.5 * atr, px + 0.5 * atr
        st = px - 1.5 * atr

    entry_mid = (el + eh) / 2
    risk = abs(entry_mid - st)

    # Take profit: analyst target if above entry zone, else 2R
    tp = tp_source = None
    if analyst_target and analyst_target > eh and screen != "extended":
        tp = round(float(analyst_target), 2)
        tp_source = "analyst PT"
    elif risk > 0 and screen != "extended":
        tp = round(entry_mid + 2 * risk, 2)
        tp_source = "2R"

    upside = round((tp / px - 1) * 100, 1) if tp else None

    return {
        "screen": screen,
        "entry_low":  round(el, 2),
        "entry_high": round(eh, 2),
        "stop":       round(st, 2),
        "tp":         tp,
        "tp_source":  tp_source,
        "upside":     upside,
    }


def _pick_news_fields(item: dict) -> tuple:
    title = item.get("title")
    publisher = item.get("publisher")
    url = item.get("link")
    if not title and isinstance(item.get("content"), dict):
        c = item["content"]
        title = c.get("title")
        provider = c.get("provider") or {}
        publisher = provider.get("displayName")
        click = c.get("clickThroughUrl") or {}
        canonical = c.get("canonicalUrl") or {}
        url = click.get("url") or canonical.get("url") or url
    return title, publisher, url


def fetch_snapshot(ticker: str, current_px: Optional[float]) -> dict:
    """Pull analyst targets, earnings date, and one news headline."""
    out = {
        "analyst": None,
        "rating_label": None,
        "rating_score": 0,
        "earnings": None,
        "earnings_date": None,
        "earnings_days": None,
        "news": None,
        "target_mean": None,
        "target_low": None,
        "target_high": None,
    }
    t = yf.Ticker(ticker)
    try:
        info = t.info or {}
    except Exception:
        info = {}

    rating = info.get("recommendationKey") or ""
    count = info.get("numberOfAnalystOpinions")
    target_mean = info.get("targetMeanPrice")
    target_high = info.get("targetHighPrice")
    target_low = info.get("targetLowPrice")

    parts = []
    if rating:
        out["rating_label"] = str(rating).replace("_", " ").title()
        r = rating.lower()
        if "strong" in r and "buy" in r:
            out["rating_score"] = 3
        elif "buy" in r:
            out["rating_score"] = 2
        elif "hold" in r:
            out["rating_score"] = 0
        elif "sell" in r:
            out["rating_score"] = -2 if "strong" in r else -1
        parts.append(out["rating_label"])
    if count:
        parts.append(f"{count} analysts")
    if target_mean and current_px:
        upside = (target_mean - current_px) / current_px * 100
        sign = "+" if upside >= 0 else ""
        parts.append(f"PT ${target_mean:.0f} ({sign}{upside:.0f}%)")
    elif target_mean:
        parts.append(f"PT ${target_mean:.0f}")
    if parts:
        out["analyst"] = " | ".join(parts)
    out["target_mean"] = target_mean
    out["target_low"] = target_low
    out["target_high"] = target_high

    ts = info.get("earningsTimestamp") or info.get("earningsTimestampStart")
    if ts:
        try:
            ed = datetime.datetime.fromtimestamp(int(ts), tz=datetime.timezone.utc).date()
            days = (ed - datetime.date.today()).days
            if -2 <= days <= 60:
                out["earnings"] = f"Earnings {ed.strftime('%b %d')} ({days:+d}d)"
                out["earnings_date"] = ed
                out["earnings_days"] = days
        except Exception:
            pass

    try:
        raw_news = t.news or []
    except Exception:
        raw_news = []
    for item in raw_news:
        title, publisher, url = _pick_news_fields(item)
        if not title:
            continue
        out["news"] = {"title": title, "publisher": publisher, "url": url}
        break

    return out


def fetch_brief_lines(date_str: str, tickers: list[str]) -> dict[str, str]:
    """Fetch the daily trade brief and extract the per-ticker line for each portfolio ticker.

    Tries today's brief first; falls back one day. Returns {} if PAT not set
    or repo unreachable. Matches lines like '- **NVDA $216.61** — ...' or
    '### N. NVDA — ...' from briefs/YYYY-MM-DD.md in BRIEF_REPO.
    """
    if not BRIEF_REPO_TOKEN:
        return {}
    headers = {
        "Authorization": f"token {BRIEF_REPO_TOKEN}",
        "Accept": "application/vnd.github.v3.raw",
    }
    text = None
    for offset in (0, 1):
        d = (datetime.date.fromisoformat(date_str) - datetime.timedelta(days=offset)).isoformat()
        url = f"https://api.github.com/repos/{BRIEF_REPO}/contents/briefs/{d}.md"
        try:
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code == 200:
                text = r.text
                break
        except Exception as e:
            print(f"brief fetch failed for {d}: {e}")
            continue
    if not text:
        return {}

    out = {}
    for raw in text.splitlines():
        line = raw.strip()
        for tkr in tickers:
            if tkr in out:
                continue
            # Match common brief shapes
            patterns = [f"**{tkr} ", f"**{tkr}**", f"**{tkr}—", f"**{tkr}/", f"# {tkr} ", f"# {tkr}—"]
            if any(p in line for p in patterns):
                # Strip markdown bullet prefix and bold markers
                cleaned = line.lstrip("- ").lstrip("# ").strip()
                # Truncate runaway-long lines
                if len(cleaned) > 280:
                    cleaned = cleaned[:277] + "..."
                out[tkr] = cleaned
    return out


def load_state() -> dict:
    if not os.path.exists(HISTORY_FILE):
        return {"snapshots": []}
    try:
        with open(HISTORY_FILE) as f:
            return json.load(f)
    except Exception:
        return {"snapshots": []}


def save_state(state: dict) -> None:
    # Trim to most-recent HISTORY_MAX
    state["snapshots"] = state["snapshots"][-HISTORY_MAX:]
    with open(HISTORY_FILE, "w") as f:
        json.dump(state, f, indent=2)


def append_snapshot(state: dict, date_str: str, session: str, prices: dict, total_value: float) -> None:
    state["snapshots"].append({
        "date": date_str,
        "session": session,
        "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "prices": {k: v for k, v in prices.items() if v is not None},
        "total_value": round(total_value, 2),
    })


def compute_streak_and_delta(state: dict, ticker: str, current_px: float, shares: int) -> dict:
    """7-day P/L delta and consecutive up/down session streak.

    Walks the snapshot history (PM-preferred per date), finds the entry from
    closest to 7 calendar days ago, and counts consecutive up/down sessions
    going backwards from today.
    """
    out = {"delta_7d": None, "delta_pct_7d": None, "streak": None, "streak_dir": None}
    snaps = state.get("snapshots", [])
    if not snaps:
        return out

    # Build daily series (one entry per date, prefer PM)
    daily = {}
    for s in snaps:
        d = s.get("date")
        px = s.get("prices", {}).get(ticker)
        if px is None:
            continue
        if d not in daily or s.get("session") == "PM":
            daily[d] = px
    sorted_dates = sorted(daily.keys())
    if not sorted_dates:
        return out

    # 7-day delta
    today = datetime.date.today()
    target = today - datetime.timedelta(days=7)
    chosen = None
    for d in reversed(sorted_dates):
        try:
            dd = datetime.date.fromisoformat(d)
        except Exception:
            continue
        if dd <= target:
            chosen = d
            break
    if chosen is None and sorted_dates:
        chosen = sorted_dates[0]
    if chosen:
        old_px = daily[chosen]
        out["delta_7d"] = round((current_px - old_px) * shares, 0)
        if old_px:
            out["delta_pct_7d"] = round((current_px - old_px) / old_px * 100, 1)

    # Streak — walk backward through close-to-close daily prices
    if len(sorted_dates) >= 2:
        last_dir = None
        streak = 0
        prev_px = current_px
        for d in reversed(sorted_dates):
            px = daily[d]
            if px is None:
                continue
            if px == prev_px:
                break
            this_dir = "up" if prev_px > px else "down"
            if last_dir is None:
                last_dir = this_dir
                streak = 1
            elif this_dir == last_dir:
                streak += 1
            else:
                break
            prev_px = px
        if last_dir and streak >= 2:
            out["streak"] = streak
            out["streak_dir"] = last_dir
    return out


# ─────────────────────────────────────────────────────────────────────────────
# ANALYSIS LAYER
# ─────────────────────────────────────────────────────────────────────────────

def cluster_breakdown(rows: list, total_value: float) -> list[tuple]:
    """Return [(cluster, dollars, pct_of_book, [tickers])] sorted desc by $."""
    bucket = {name: {"value": 0.0, "tickers": []} for name in list(CLUSTERS.keys()) + ["Other"]}
    for r in rows:
        t, sh, _c, px, _pnl, _pct = r
        if px is None:
            continue
        cl = classify(t)
        bucket[cl]["value"] += sh * px
        bucket[cl]["tickers"].append(t)
    out = []
    for cl, data in bucket.items():
        if data["value"] <= 0:
            continue
        pct = (data["value"] / total_value * 100) if total_value else 0
        out.append((cl, data["value"], pct, data["tickers"]))
    out.sort(key=lambda x: -x[1])
    return out


def concentration_alerts(rows: list, total_value: float, breakdown: list) -> list[str]:
    alerts = []
    for r in rows:
        t, sh, _c, px, _pnl, _pct = r
        if px is None:
            continue
        weight = (sh * px / total_value * 100) if total_value else 0
        if weight > SINGLE_NAME_THRESHOLD:
            alerts.append(f"{t} {weight:.1f}% of book (>{SINGLE_NAME_THRESHOLD:.0f}% threshold)")
    for cl, _val, pct, tickers in breakdown:
        if pct > CLUSTER_THRESHOLD:
            alerts.append(f"{cl} cluster {pct:.1f}% of book (>{CLUSTER_THRESHOLD:.0f}% threshold)")
    return alerts


def build_earnings_calendar(snapshots: dict, days_ahead: int = 14) -> list[tuple]:
    """Return [(date, [tickers])] sorted ascending, within next N days."""
    by_date = {}
    today = datetime.date.today()
    for tkr, snap in snapshots.items():
        ed = snap.get("earnings_date")
        if not ed:
            continue
        delta = (ed - today).days
        if -1 <= delta <= days_ahead:
            by_date.setdefault(ed, []).append(tkr)
    return sorted(by_date.items(), key=lambda x: x[0])


def filter_time_sensitive(snapshots: dict, session: str) -> list[tuple]:
    """Positions reporting within the next 1 trading day.

    AM session: include today (next 0–1 days). PM session: include today and tomorrow.
    Returns [(ticker, days_until, label)].
    """
    out = []
    today = datetime.date.today()
    for tkr, snap in snapshots.items():
        days = snap.get("earnings_days")
        ed = snap.get("earnings_date")
        if days is None or ed is None:
            continue
        # Reporting "tonight" if same day; "tomorrow" if days==1
        if session == "AM" and days in (0, 1):
            label = "reports today AMC" if days == 0 else "reports tomorrow"
            out.append((tkr, days, label))
        elif session == "PM" and days in (0, 1):
            label = "reports tonight AMC" if days == 0 else "reports tomorrow"
            out.append((tkr, days, label))
    out.sort(key=lambda x: x[1])
    return out


def score_position(px, target_mean, pnl_pct, rating_score, portfolio_pct, rsi, earnings_days=None) -> tuple:
    """Return (bucket, reason). Buckets: TRIM, BUY, HOLD, EXIT.

    Adds RSI + ER-binary awareness on top of the prior rule engine.
    """
    if px is None or target_mean is None:
        return "HOLD", "Incomplete data"

    upside = (target_mean - px) / px * 100

    # TRIM signals
    if earnings_days is not None and 0 <= earnings_days <= 1 and pnl_pct >= 15:
        return "TRIM", f"ER in {earnings_days}d + green {pnl_pct:.0f}% — protect gain"
    if portfolio_pct > SINGLE_NAME_THRESHOLD and pnl_pct > 15:
        return "TRIM", f"Oversized ({portfolio_pct:.0f}% of book) + +{pnl_pct:.0f}% gain"
    if rsi is not None and rsi > 80 and pnl_pct > 10:
        return "TRIM", f"RSI {rsi:.0f} extended + green position — bank some"
    if pnl_pct >= 40 and rsi is not None and rsi >= 65:
        return "TRIM", f"+{pnl_pct:.0f}% gain + RSI {rsi:.0f} — extended"
    if px >= target_mean and portfolio_pct > 10:
        return "TRIM", f"At/above PT (${target_mean:.0f}) + sized {portfolio_pct:.0f}%"
    if pnl_pct >= 30 and portfolio_pct > 10:
        return "TRIM", f"+{pnl_pct:.0f}% gain on a {portfolio_pct:.0f}% position"

    # EXIT signals (worse than SELL — full exit candidate)
    if upside <= -15 and rating_score <= -1:
        return "EXIT", f"PT ${target_mean:.0f} below px + bearish ({rating_score})"

    # BUY signals
    if pnl_pct <= -25 and rating_score >= 2 and (rsi is None or rsi < 40):
        return "BUY", f"Underwater {pnl_pct:.0f}% + buy rating + RSI {rsi or 'na'} = avg-down zone"
    if upside >= 25 and rating_score >= 2:
        return "BUY", f"+{upside:.0f}% upside + buy rating"
    if upside >= 15 and rating_score >= 2 and portfolio_pct < 3:
        return "BUY", f"+{upside:.0f}% upside, micro position ({portfolio_pct:.1f}%)"

    # HOLD
    if px >= target_mean:
        return "HOLD", f"At PT ${target_mean:.0f} — monitor"
    if 5 <= upside < 15:
        return "HOLD", f"Moderate upside (+{upside:.0f}%)"
    if upside < -5:
        return "HOLD", f"Below PT ({upside:.0f}%) but no exit trigger"
    return "HOLD", "Balanced"


def synthesize_read(ticker: str, px: float, ind: dict, snap: dict, brief_line: Optional[str]) -> list[str]:
    """1–3 line read combining technicals, analyst view, and brief mention."""
    lines = []

    tech_parts = []
    if ind.get("rsi") is not None:
        rsi = ind["rsi"]
        tag = ""
        if rsi >= 75:
            tag = " (overbought)"
        elif rsi <= 30:
            tag = " (oversold)"
        tech_parts.append(f"RSI {rsi}{tag}")
    if ind.get("sma20") and px:
        diff = (px - ind["sma20"]) / ind["sma20"] * 100
        sign = "+" if diff >= 0 else ""
        tech_parts.append(f"SMA20 ${ind['sma20']:.0f} ({sign}{diff:.0f}%)")
    if ind.get("sma200") and px and ind.get("sma20") and ind.get("sma50"):
        if ind["sma20"] > ind["sma50"] > ind["sma200"]:
            tech_parts.append("trend up")
        elif ind["sma20"] < ind["sma50"] < ind["sma200"]:
            tech_parts.append("trend down")
    if tech_parts:
        lines.append("📖 " + " · ".join(tech_parts))

    if snap.get("analyst"):
        lines.append("🧠 " + snap["analyst"])

    if brief_line:
        # Compact and HTML-escape
        safe = html.escape(brief_line)
        lines.append("📝 Brief: " + safe)

    return lines


def synthesize_playbook(
    ticker: str, bucket: str, reason: str, px: float, cost: float, ind: dict, snap: dict
) -> str:
    """1-line playbook action with computed stop level."""
    target = snap.get("target_mean")
    lv = compute_entry_levels(px, ind, target)
    stop = lv.get("stop")

    if bucket == "TRIM":
        if stop:
            buf = (stop - cost) / cost * 100
            buf_sign = "+" if buf >= 0 else ""
            return f"▶ Trim 25% · trail stop ${stop:.2f} ({buf_sign}{buf:.0f}% vs ${cost:.2f} cost)"
        return f"▶ Trim 25% — {reason}"

    if bucket == "BUY":
        if target and px:
            return f"▶ Add on weakness · PT ${target:.0f} ({((target-px)/px*100):+.0f}%) · stop ${stop:.2f}" if stop else f"▶ Add on weakness · PT ${target:.0f}"
        return f"▶ Accumulate — {reason}"

    if bucket == "EXIT":
        return f"▶ Re-validate thesis or exit — {reason}"

    # HOLD
    if cost and px:
        gap = (px - cost) / cost * 100
        sign = "+" if gap >= 0 else ""
        stop_str = f" · stop ${stop:.2f}" if stop else ""
        return f"▶ Hold{stop_str} · cost ${cost:.2f} ({sign}{gap:.0f}% from px)"
    return f"▶ Hold — {reason}"


# ─────────────────────────────────────────────────────────────────────────────
# MESSAGE LAYER
# ─────────────────────────────────────────────────────────────────────────────

BUCKET_META = {
    "TRIM": ("🔻", "Trim / risk-down"),
    "EXIT": ("🚫", "Exit / re-validate"),
    "BUY": ("🟢", "Add-down candidates"),
    "HOLD": ("⏸", "Hold steady"),
}


def fetch_vix() -> Optional[dict]:
    """Fetch VIX level, prior close, delta, regime label, and (at extremes) an
    interpretation note. Returns None on failure (informational, non-blocking)."""
    try:
        hist = yf.download("^VIX", period="5d", auto_adjust=False, progress=False)
        closes = hist["Close"].dropna()
        if len(closes) < 2:
            return None
        # yfinance may return single-column DataFrame; squeeze safely
        try:
            last = float(closes.iloc[-1].iloc[0]) if hasattr(closes.iloc[-1], 'iloc') else float(closes.iloc[-1])
            prev = float(closes.iloc[-2].iloc[0]) if hasattr(closes.iloc[-2], 'iloc') else float(closes.iloc[-2])
        except (AttributeError, TypeError):
            last = float(closes.iloc[-1]); prev = float(closes.iloc[-2])
    except Exception as e:
        print(f"VIX fetch failed: {e}")
        return None

    delta = last - prev
    delta_pct = (delta / prev * 100) if prev else 0.0

    if last < 12:
        regime = "Ultra-complacent"
        extra = "complacency stretched — don't chase or oversize longs; expect a vol spike soon"
    elif last < 15:
        regime = "Complacent"
        extra = "calm tape; momentum & breakouts work well; standard position sizes fit"
    elif last < 20:
        regime = "Normal"
        extra = "baseline market conditions; no size or stop adjustments needed"
    elif last < 25:
        regime = "Elevated"
        extra = "expect larger daily P/L swings; tighten stops on high-beta names; avoid new stretched entries"
    elif last < 30:
        regime = "High fear"
        extra = "risk-off tape; trim discretionary positions, hold conviction plays; wait for confirmation before adding"
    else:
        regime = "Panic/stressed"
        extra = "preservation mode; VIX ≥30 spikes often mark short-term bottoms — watch for reversal cues, don't panic-sell"

    return {"level": last, "prev": prev, "delta": delta, "delta_pct": delta_pct,
            "regime": regime, "extra": extra}


def _safe_close_pair(hist) -> Optional[tuple]:
    """Return (last, prev) close as floats, handling single- or multi-column DataFrame."""
    try:
        closes = hist["Close"].dropna()
    except (KeyError, TypeError):
        return None
    if len(closes) < 2:
        return None
    def unwrap(x):
        return float(x.iloc[0]) if hasattr(x, "iloc") else float(x)
    try:
        return unwrap(closes.iloc[-1]), unwrap(closes.iloc[-2])
    except (AttributeError, TypeError, IndexError):
        return None


def fetch_yield_10y() -> Optional[dict]:
    """10-year Treasury yield with delta in basis points and regime interpretation."""
    try:
        hist = yf.download("^TNX", period="5d", auto_adjust=False, progress=False)
    except Exception as e:
        print(f"10Y fetch failed: {e}"); return None
    pair = _safe_close_pair(hist)
    if not pair: return None
    last, prev = pair
    delta_bps = (last - prev) * 100  # yield is in percent already; ×100 → bps

    if delta_bps > 5:
        extra = "significant rate spike — headwind for growth (NVDA/MSFT/GOOG); watch small caps"
    elif delta_bps > 2:
        extra = "modest rate rise — mild headwind for high-multiple growth"
    elif delta_bps < -5:
        extra = "significant rate drop — bullish for growth, REITs, gold; watch TLT for continuation"
    elif delta_bps < -2:
        extra = "modest rate decline — mild tailwind for growth and rate-sensitives"
    else:
        extra = "rates stable — no rate-driven adjustment needed"

    return {"level": last, "delta_bps": delta_bps, "extra": extra}


def render_yield_line(y: Optional[dict]) -> Optional[str]:
    if not y: return None
    sd = "+" if y["delta_bps"] >= 0 else ""
    return f"💰 <b>10Y</b> {y['level']:.2f}% ({sd}{y['delta_bps']:.0f}bps) · {y['extra']}"


def fetch_dxy() -> Optional[dict]:
    """DXY dollar index with delta % and regime interpretation."""
    try:
        hist = yf.download("DX-Y.NYB", period="5d", auto_adjust=False, progress=False)
    except Exception as e:
        print(f"DXY fetch failed: {e}"); return None
    pair = _safe_close_pair(hist)
    if not pair: return None
    last, prev = pair
    delta = last - prev
    delta_pct = (delta / prev * 100) if prev else 0.0

    if delta_pct > 0.5:
        extra = "dollar strengthening sharply — headwind for BABA, commodities, USD-earning multinationals"
    elif delta_pct > 0.1:
        extra = "dollar firming — mild EM/commodity headwind"
    elif delta_pct < -0.5:
        extra = "dollar weakening sharply — tailwind for BABA, GLD, gold miners, EM equities"
    elif delta_pct < -0.1:
        extra = "dollar softening — mild tailwind for BABA, gold, USD-heavy multinationals"
    else:
        extra = "dollar stable — no FX-driven adjustment needed"

    return {"level": last, "delta": delta, "delta_pct": delta_pct, "extra": extra}


def render_dxy_line(d: Optional[dict]) -> Optional[str]:
    if not d: return None
    sd = "+" if d["delta_pct"] >= 0 else ""
    return f"💵 <b>DXY</b> {d['level']:.2f} ({sd}{d['delta_pct']:.2f}%) · {d['extra']}"


def fetch_spy_trend() -> Optional[dict]:
    """SPY vs 200-day SMA — market regime read."""
    try:
        hist = yf.download("SPY", period="1y", auto_adjust=True, progress=False)
    except Exception as e:
        print(f"SPY fetch failed: {e}"); return None
    try:
        closes = hist["Close"].dropna()
        if len(closes) < 200:
            return None
        def unwrap(x):
            return float(x.iloc[0]) if hasattr(x, "iloc") else float(x)
        last = unwrap(closes.iloc[-1])
        sma200 = float(closes.rolling(200).mean().iloc[-1].iloc[0]) if hasattr(closes.rolling(200).mean().iloc[-1], 'iloc') else float(closes.rolling(200).mean().iloc[-1])
    except Exception as e:
        print(f"SPY trend calc failed: {e}"); return None

    pct_above = (last - sma200) / sma200 * 100

    if pct_above > 10:
        regime = "Strong bull"
        extra = "extended above SMA200 — momentum & breakouts favored, but stretched readings warrant profit-taking on winners"
    elif pct_above > 0:
        regime = "Bull trend intact"
        extra = "favor momentum/continuation setups; standard sizing fits"
    elif pct_above > -5:
        regime = "Trend under pressure"
        extra = "just below SMA200 — watch for reclaim vs failure; reduce new adds"
    elif pct_above > -10:
        regime = "Confirmed downtrend"
        extra = "reduce discretionary risk; defensive positioning preferred"
    else:
        regime = "Bear market"
        extra = "preservation mode — cash and defensives favored; avoid new long adds"

    return {"level": last, "sma200": sma200, "pct_above": pct_above,
            "regime": regime, "extra": extra}


def render_trend_line(t: Optional[dict]) -> Optional[str]:
    if not t: return None
    sign = "+" if t["pct_above"] >= 0 else ""
    return (f"📈 <b>SPY</b> ${t['level']:.0f} ({sign}{t['pct_above']:.1f}% vs SMA200) "
            f"— {t['regime']} · {t['extra']}")


def render_vix_line(vix: Optional[dict]) -> Optional[str]:
    """Format the VIX line. Returns None if fetch failed."""
    if not vix:
        return None
    d = vix["delta"]; dp = vix["delta_pct"]
    ds = "+" if d >= 0 else ""
    suffix = f" · {vix['extra']}" if vix.get("extra") else ""
    return (f"📊 <b>VIX</b> {vix['level']:.1f} ({ds}{d:.1f}, {ds}{dp:.1f}%) "
            f"— {vix['regime']}{suffix}")


def render_at_a_glance(total_value, total_cost, total_pnl, total_pct, alerts, weekly_delta,
                       vix=None, yield_10y=None, dxy=None, spy_trend=None):
    sign_tot = "+" if total_pnl >= 0 else ""
    lines = [
        f"<b>Total:</b> ${total_value:,.0f}  |  "
        f"Cost ${total_cost:,.0f}  |  "
        f"P&amp;L {sign_tot}${total_pnl:,.0f} ({sign_tot}{total_pct:.2f}%)"
    ]
    if weekly_delta is not None:
        sd = "+" if weekly_delta >= 0 else ""
        lines.append(f"<b>7-day Δ:</b> {sd}${weekly_delta:,.0f}")
    for renderer, data in (
        (render_vix_line, vix),
        (render_yield_line, yield_10y),
        (render_dxy_line, dxy),
        (render_trend_line, spy_trend),
    ):
        line = renderer(data)
        if line:
            lines.append(line)
    for a in alerts:
        lines.append(f"🚨 {a}")
    return "\n".join(lines)


def render_time_sensitive(items, snapshots, rows_by_ticker, brief_lines):
    if not items:
        return None
    out = ["<b>🚨 Time-sensitive (next 24h)</b>"]
    for tkr, days, label in items:
        row = rows_by_ticker.get(tkr)
        if not row:
            continue
        sh, c, px, pnl, pct = row
        sign = "+" if pnl >= 0 else ""
        out.append(f"• <b>{tkr}</b> — {label} · {sign}{pct:.0f}% ({sign}${pnl:,.0f})")
        bl = brief_lines.get(tkr)
        if bl and "HARD EXIT" in bl.upper():
            out.append(f"  ⚠️ Brief: HARD EXIT before close")
    return "\n".join(out)


def render_earnings_calendar(calendar):
    if not calendar:
        return None
    out = ["<b>📅 Earnings — next 14 days</b>"]
    for date, tickers in calendar:
        d_label = date.strftime("%a %b %d")
        out.append(f"• {d_label} — {', '.join(tickers)}")
    return "\n".join(out)


def render_cluster_exposure(breakdown):
    if not breakdown:
        return None
    out = ["<b>🎯 Cluster exposure</b>"]
    for cl, val, pct, tickers in breakdown:
        out.append(f"• <b>{cl}:</b> ${val:,.0f} ({pct:.1f}%) — {' '.join(tickers)}")
    return "\n".join(out)


def render_position_block(tkr, sh, c, px, pnl, pct, ind, snap, brief_line, streak, weight, garp_score=None):
    if px is None:
        return f"<b>{tkr}</b>  {sh}@${c:.2f}  |  Last: unavailable"

    sign = "+" if pnl >= 0 else ""
    lines = [
        f"<b>{tkr}</b>  {sh}@${c:.2f}",
        f"💵 Px ${px:.2f} · {sign}{pct:.1f}% ({sign}${pnl:,.0f}) · {weight:.1f}% of book"
    ]

    # Streak / 7d delta as a single status line
    extras = []
    if streak.get("delta_7d") is not None:
        d7 = streak["delta_7d"]
        sd = "+" if d7 >= 0 else ""
        d7p = streak.get("delta_pct_7d")
        pp = f" ({sd}{d7p:.1f}%)" if d7p is not None else ""
        extras.append(f"7d Δ {sd}${d7:,.0f}{pp}")
    if streak.get("streak"):
        arrow = "📈" if streak["streak_dir"] == "up" else "📉"
        extras.append(f"{arrow} {streak['streak']} sessions {streak['streak_dir']}")
    if extras:
        lines.append("📊 " + " · ".join(extras))

    # Entry / stop / take-profit levels
    lv = compute_entry_levels(px, ind, snap.get("target_mean"))
    tp_str = f"  |  PT ${lv['tp']:.2f} ({lv['upside']:+.1f}%)" if lv.get("tp") else ""
    lines.append(
        f"🎯 Entry ${lv['entry_low']:.2f}–${lv['entry_high']:.2f}"
        f"  |  Stop ${lv['stop']:.2f}"
        f"{tp_str}"
        f"  <i>({lv['screen']})</i>"
    )

    # Read lines
    for line in synthesize_read(tkr, px, ind, snap, brief_line):
        lines.append(line)

    # Earnings if present
    if snap.get("earnings"):
        lines.append("📅 " + snap["earnings"])

    # GARP quality score (informational — not a buy/sell trigger)
    if garp_score is not None:
        lines.append(garp.render_line(garp_score))

    return "\n".join(lines)


def render_action_plan(scored_rows, prices):
    """Concrete share-count actions for Trim / Buy positions."""
    actions = []
    for sr in scored_rows:
        tkr, bucket, reason, sh, c, px, pnl, pct, weight, snap, ind = sr
        if px is None:
            continue
        if bucket == "TRIM":
            trim = max(1, int(sh * 0.25))
            cash = trim * px
            actions.append(
                f"<b>⬇ {tkr}:</b> trim {trim} (${cash:,.0f} proceeds) → {sh - trim} remain · {reason}"
            )
        elif bucket == "BUY":
            add = max(1, int(sh * 0.15)) if sh > 0 else 1
            cap = add * px
            actions.append(
                f"<b>🟢 {tkr}:</b> add {add} (${cap:,.0f} capital) → {sh + add} total · {reason}"
            )
        elif bucket == "EXIT":
            cash = sh * px
            actions.append(
                f"<b>🚫 {tkr}:</b> exit {sh} (${cash:,.0f} proceeds) · {reason}"
            )
    if not actions:
        return None
    return "<b>📑 Auto Action Plan</b>\n" + "\n".join(actions)


def build_message(portfolio, prices, snapshots, indicators, history_state, brief_lines, date_str, session_label, history=None, garp_scores=None, vix=None, yield_10y=None, dxy=None, spy_trend=None):
    sgt = pytz.timezone("Asia/Singapore")
    now_sgt = datetime.datetime.now(sgt)
    weekday = now_sgt.strftime("%a")

    if session_label == "AM":
        tagline = "Pre-market read"
    else:
        tagline = "Post-close review"

    lines = [f"<b>💼 Portfolio Watch — {date_str} {session_label} ({weekday})</b>",
             f"<i>{tagline}</i>", ""]

    positions = portfolio["positions"]
    total_cost = 0.0
    total_value = 0.0
    rows = []
    rows_by_ticker = {}
    for p in positions:
        tkr, sh, c = p["ticker"], p["shares"], p["cost"]
        px = prices.get(tkr)
        cost_basis = sh * c
        total_cost += cost_basis
        if px is None:
            rows.append((tkr, sh, c, None, None, None))
            continue
        value = sh * px
        pnl = value - cost_basis
        pnl_pct = pnl / cost_basis * 100 if cost_basis else 0.0
        total_value += value
        rows.append((tkr, sh, c, px, pnl, pnl_pct))
        rows_by_ticker[tkr] = (sh, c, px, pnl, pnl_pct)

    total_pnl = total_value - total_cost
    total_pct = (total_pnl / total_cost * 100) if total_cost else 0.0

    # 7-day total-value delta
    weekly_delta = None
    snaps = history_state.get("snapshots", [])
    if snaps:
        target_date = datetime.date.today() - datetime.timedelta(days=7)
        chosen_total = None
        for s in reversed(snaps):
            try:
                d = datetime.date.fromisoformat(s["date"])
            except Exception:
                continue
            if d <= target_date and s.get("total_value"):
                chosen_total = s["total_value"]
                break
        if chosen_total is None and snaps:
            chosen_total = snaps[0].get("total_value")
        if chosen_total:
            weekly_delta = round(total_value - chosen_total, 0)

    breakdown = cluster_breakdown(rows, total_value)
    alerts = concentration_alerts(rows, total_value, breakdown)

    # ── At a glance ──
    lines.append(render_at_a_glance(total_value, total_cost, total_pnl, total_pct, alerts, weekly_delta,
                                     vix=vix, yield_10y=yield_10y, dxy=dxy, spy_trend=spy_trend))

    # ── Time-sensitive ──
    ts_items = filter_time_sensitive(snapshots, session_label)
    block = render_time_sensitive(ts_items, snapshots, rows_by_ticker, brief_lines)
    if block:
        lines.append("")
        lines.append(block)

    # ── Earnings calendar ──
    calendar = build_earnings_calendar(snapshots)
    block = render_earnings_calendar(calendar)
    if block:
        lines.append("")
        lines.append(block)

    # ── Earnings Spotlight (broader S&P 100 awareness, factual cards) ──
    if build_earnings_spotlight_section is not None:
        try:
            spotlight = build_earnings_spotlight_section()
            if spotlight:
                lines.append("")
                lines.append(spotlight)
        except Exception as e:
            print(f"earnings spotlight section failed: {e}")

    # ── Cluster exposure ──
    block = render_cluster_exposure(breakdown)
    if block:
        lines.append("")
        lines.append(block)

    # ── Score and bucket positions ──
    scored = []
    for tkr, sh, c, px, pnl, pct in rows:
        weight = (sh * px / total_value * 100) if (px and total_value) else 0
        snap = snapshots.get(tkr) or {}
        ind = indicators.get(tkr) or {}
        bucket, reason = score_position(
            px, snap.get("target_mean"), pct, snap.get("rating_score", 0),
            weight, ind.get("rsi"), snap.get("earnings_days")
        )
        scored.append((tkr, bucket, reason, sh, c, px, pnl, pct, weight, snap, ind))

    # Group by bucket
    by_bucket = {"TRIM": [], "EXIT": [], "BUY": [], "HOLD": []}
    for sr in scored:
        by_bucket.setdefault(sr[1], []).append(sr)

    # ── Position breakdown by bucket ──
    lines.append("")
    lines.append("<b>📋 Position breakdown</b>")
    for bucket in ["TRIM", "EXIT", "BUY", "HOLD"]:
        items = by_bucket.get(bucket, [])
        if not items:
            continue
        emoji, label = BUCKET_META[bucket]
        lines.append("")
        lines.append(f"<b>{emoji} {label}</b>")
        for sr in items:
            tkr, _b, reason, sh, c, px, pnl, pct, weight, snap, ind = sr
            streak = compute_streak_and_delta(history_state, tkr, px, sh) if px else {}
            block = render_position_block(
                tkr, sh, c, px, pnl, pct, ind, snap,
                brief_lines.get(tkr), streak or {}, weight,
                garp_score=(garp_scores or {}).get(tkr),
            )
            playbook = synthesize_playbook(tkr, bucket, reason, px, c, ind, snap) if px else ""
            lines.append("")
            lines.append(block)
            if playbook:
                lines.append(playbook)

    # ── Action plan (summary of position breakdown — keep adjacent) ──
    plan = render_action_plan(scored, prices)
    if plan:
        lines.append("")
        lines.append(plan)

    # ── Weekly GARP hunt list (Monday AM only — fundamentals move quarterly, not daily) ──
    if now_sgt.weekday() == 0 and session_label == "AM":
        try:
            passers = garp.hunt_list(min_score=5)
            if passers:
                lines.append("")
                lines.append("<b>⭐ Weekly GARP hunt list (score ≥ 5/6)</b>")
                lines.append("<i>Long-horizon quality-value candidates from a curated large-cap universe. Informational — not a buy signal.</i>")
                for p in passers[:15]:
                    v = p["values"]
                    fpe = f"fPE {v.get('fPE'):.1f}" if v.get("fPE") is not None else "fPE na"
                    peg = f"PEG {v.get('PEG'):.1f}" if v.get("PEG") is not None else "PEG na"
                    de = f"D/E {v.get('DE'):.0f}" if v.get("DE") is not None else "D/E —"
                    lines.append(f"• <b>{p['ticker']}</b> {p['score']}/{p['max_score']}  ({fpe} · {de} · {peg})")
        except Exception as e:
            print(f"GARP hunt list skipped: {e}")

    # ── Portfolio diagnostics (factual, not trim/exit calls) ──
    if build_diagnostics_section is not None:
        try:
            diag_section = build_diagnostics_section(portfolio, prices, history)
            if diag_section:
                lines.append("")
                lines.append(diag_section)
        except Exception as e:
            print(f"diagnostics section failed: {e}")

    # ── Strategy watchlist (rule-state only, separate from portfolio) ──
    if build_watchlist_section is not None:
        try:
            watchlist_section = build_watchlist_section()
            if watchlist_section:
                lines.append("")
                lines.append(watchlist_section)
        except Exception as e:
            print(f"watchlist section failed: {e}")

    lines.append("")
    lines.append("<i>⚠️ Not financial advice — your call, your risk.</i>")

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# DELIVERY LAYER
# ─────────────────────────────────────────────────────────────────────────────

def _explain_tg_error(result):
    if not result:
        return "No response from Telegram API."
    desc = (result.get("description") or "").lower()
    code = result.get("error_code")
    if "chat not found" in desc:
        return (
            f"PORTFOLIO_CHAT_ID={CHAT_ID!r} is wrong, or the bot is not a member "
            f"of that chat."
        )
    if "unauthorized" in desc or code == 401:
        return "PORTFOLIO_BOT_TOKEN is invalid or revoked."
    if "bot was kicked" in desc or "bot is not a member" in desc:
        return "Bot was removed from the chat."
    return f"Telegram returned: {result}"


def post(text):
    if DRY_RUN:
        print("=" * 60)
        print("DRY RUN — would post:")
        print("=" * 60)
        print(text)
        return {"ok": True, "dry_run": True}

    MAX = 4000
    if len(text) <= MAX:
        chunks = [text]
    else:
        chunks, cur = [], ""
        for block in text.split("\n\n"):
            if len(cur) + len(block) + 2 > MAX and cur:
                chunks.append(cur)
                cur = block
            else:
                cur = f"{cur}\n\n{block}" if cur else block
        if cur:
            chunks.append(cur)

    total = len(chunks)
    last = None
    for i, chunk in enumerate(chunks, 1):
        body = f"<i>Page {i}/{total}</i>\n\n{chunk}" if total > 1 else chunk
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "parse_mode": "HTML", "text": body, "disable_web_page_preview": "true"},
            timeout=30,
        )
        last = resp.json()
        if not last.get("ok"):
            return last
    return last


def preflight():
    if DRY_RUN:
        print("DRY_RUN=1 — skipping Telegram preflight.")
        return
    try:
        me = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getMe", timeout=30).json()
    except Exception as e:
        print(f"FATAL: cannot reach Telegram API: {e}")
        sys.exit(1)
    if not me.get("ok"):
        print(f"FATAL: PORTFOLIO_BOT_TOKEN rejected: {me}")
        sys.exit(1)
    print(f"Bot OK: @{me['result'].get('username')}")
    chat = requests.get(
        f"https://api.telegram.org/bot{BOT_TOKEN}/getChat",
        params={"chat_id": CHAT_ID},
        timeout=30,
    ).json()
    if not chat.get("ok"):
        print(f"FATAL: chat unreachable: {chat}")
        print(_explain_tg_error(chat))
        sys.exit(1)
    title = chat["result"].get("title") or chat["result"].get("username") or CHAT_ID
    print(f"Chat OK: {title} (id={CHAT_ID})")


def main():
    sgt = pytz.timezone("Asia/Singapore")
    now = datetime.datetime.now(sgt)
    date_str = now.strftime("%Y-%m-%d")
    session_label = "AM" if now.hour < 12 else "PM"

    preflight()

    portfolio = load_portfolio()
    tickers = [p["ticker"] for p in portfolio["positions"]]

    # Live close
    print("Fetching live prices…")
    live = yf.download(tickers, period="2d", auto_adjust=True, progress=False)
    prices = {}
    for t in tickers:
        try:
            if len(tickers) == 1:
                prices[t] = round(float(live["Close"].dropna().iloc[-1]), 2)
            else:
                prices[t] = round(float(live["Close"][t].dropna().iloc[-1]), 2)
        except Exception:
            prices[t] = None

    # Indicators
    print("Computing indicators…")
    history = get_history(tickers)
    indicators = {t: compute_indicators(history, t) for t in tickers}

    # Snapshots (analyst/earnings/news)
    print("Fetching snapshots…")
    snapshots = {}
    for t in tickers:
        try:
            snapshots[t] = fetch_snapshot(t, prices.get(t))
        except Exception as e:
            print(f"snapshot failed for {t}: {e}")
            snapshots[t] = {"analyst": None, "earnings": None, "news": None,
                            "target_mean": None, "rating_score": 0,
                            "earnings_date": None, "earnings_days": None}

    # Brief lines (optional)
    print("Fetching trade-research brief…")
    brief_lines = fetch_brief_lines(date_str, tickers)
    print(f"Brief embed: {len(brief_lines)} ticker(s) matched")

    # GARP quality scores (cached 7d; ~free after first-of-week miss)
    print("Scoring GARP fundamentals…")
    try:
        garp_scores = garp.score_all(tickers)
        cached_hits = sum(1 for r in garp_scores.values() if r.get("cached"))
        print(f"  GARP: {len(garp_scores)} scored, {cached_hits} cache hits")
    except Exception as e:
        print(f"GARP scoring failed: {e}")
        garp_scores = {}

    # Macro context (informational — VIX + 10Y yield + DXY + SPY trend)
    print("Fetching macro context…")
    vix = fetch_vix()
    yield_10y = fetch_yield_10y()
    dxy = fetch_dxy()
    spy_trend = fetch_spy_trend()
    if vix:       print(f"  VIX {vix['level']:.1f} ({vix['regime']})")
    if yield_10y: print(f"  10Y {yield_10y['level']:.2f}% ({yield_10y['delta_bps']:+.0f}bps)")
    if dxy:       print(f"  DXY {dxy['level']:.2f} ({dxy['delta_pct']:+.2f}%)")
    if spy_trend: print(f"  SPY {spy_trend['level']:.0f} ({spy_trend['pct_above']:+.1f}% vs SMA200 · {spy_trend['regime']})")

    # State (history)
    state = load_state()

    # Build message before saving state, so streaks reflect prior state
    msg = build_message(portfolio, prices, snapshots, indicators, state, brief_lines, date_str, session_label, history=history, garp_scores=garp_scores, vix=vix, yield_10y=yield_10y, dxy=dxy, spy_trend=spy_trend)

    # Append today's snapshot AFTER building (don't pollute streaks with current)
    total_value = sum((p["shares"] * prices[p["ticker"]]) for p in portfolio["positions"] if prices.get(p["ticker"]))
    append_snapshot(state, date_str, session_label, prices, total_value)
    save_state(state)

    result = post(msg)
    if result and result.get("ok"):
        print("Posted successfully.")
        return
    print(f"First attempt failed: {result}")
    print(_explain_tg_error(result))
    time.sleep(5)
    result2 = post(msg)
    if result2 and result2.get("ok"):
        print("Posted on retry.")
    else:
        print(f"Retry failed: {result2}")
        print(_explain_tg_error(result2))
        sys.exit(1)


if __name__ == "__main__":
    main()
