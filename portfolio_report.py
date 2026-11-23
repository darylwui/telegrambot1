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
    from diagnostics import build_diagnostics_section
except Exception as _e:
    build_diagnostics_section = None
    print(f"diagnostics unavailable: {_e}")

try:
    from earnings_spotlight import build_earnings_tiers_section
except Exception as _e:
    build_earnings_tiers_section = None
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
        parts.append(f"Street PT ${target_mean:.0f} ({sign}{upside:.0f}%)")
    elif target_mean:
        parts.append(f"Street PT ${target_mean:.0f}")
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
        print("Brief embed skipped: BRIEF_REPO_TOKEN not set")
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
        except Exception as e:
            print(f"brief fetch failed for {d}: {e}")
            continue
        if r.status_code == 200:
            text = r.text
            break
        if r.status_code in (401, 403):
            print(f"Brief embed: HTTP {r.status_code} — BRIEF_REPO_TOKEN expired or lacks "
                  f"contents:read on {BRIEF_REPO}; rotate the secret")
            return {}
        print(f"Brief embed: HTTP {r.status_code} for briefs/{d}.md")
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
                # Lines can list several tickers separated by " / " — keep only this ticker's segment.
                segments = [s for s in line.split(" / ") if any(p in s for p in patterns)]
                chosen = segments[0] if segments else line
                cleaned = chosen.lstrip("- ").lstrip("# ").replace("**", "").replace("`", "").strip()
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


ADD_BLOCK_ER_DAYS = 5      # no adds this close to an earnings print
ADD_BLOCK_RSI = 65         # no adds into overbought names
ADD_BLOCK_SMA20_EXT = 8.0  # no adds when px is this % above SMA20


def _fmt_gain(pnl_pct: float) -> str:
    """Gain label; cost basis <= 0 (house money) has no meaningful %."""
    if pnl_pct == float("inf"):
        return "house money"
    return f"{pnl_pct:+.0f}%"


def score_position(px, target_mean, pnl_pct, rating_score, portfolio_pct, rsi, earnings_days=None,
                   cluster_over: Optional[str] = None, sma20_ext: Optional[float] = None) -> tuple:
    """Return (bucket, reason). Buckets: TRIM, BUY, HOLD, EXIT.

    pnl_pct is +inf for house-money positions (cost basis <= 0).
    cluster_over names the position's cluster when it already breaches CLUSTER_THRESHOLD.
    sma20_ext is % distance of px above SMA20.
    """
    if px is None or target_mean is None:
        return "HOLD", "Incomplete data"

    upside = (target_mean - px) / px * 100
    gain = _fmt_gain(pnl_pct)

    # TRIM signals
    if earnings_days is not None and 0 <= earnings_days <= 1 and pnl_pct >= 15:
        return "TRIM", f"ER in {earnings_days}d + {gain} — protect gain"
    if portfolio_pct > SINGLE_NAME_THRESHOLD and pnl_pct > 15:
        return "TRIM", f"Oversized ({portfolio_pct:.0f}% of book), {gain} gain"
    if rsi is not None and rsi > 80 and pnl_pct > 10:
        return "TRIM", f"RSI {rsi:.0f} extended + green position — bank some"
    if pnl_pct >= 40 and rsi is not None and rsi >= 65:
        return "TRIM", f"{gain} gain + RSI {rsi:.0f} — extended"
    if px >= target_mean and portfolio_pct > 10:
        return "TRIM", f"At/above PT (${target_mean:.0f}) + sized {portfolio_pct:.0f}%"
    if pnl_pct >= 30 and portfolio_pct > 10:
        return "TRIM", f"{gain} gain on a {portfolio_pct:.0f}% position"

    # EXIT signals (worse than SELL — full exit candidate)
    if upside <= -15 and rating_score <= -1:
        return "EXIT", f"PT ${target_mean:.0f} below px + bearish ({rating_score})"

    # BUY signals
    buy_reason = None
    if pnl_pct <= -25 and rating_score >= 2 and (rsi is None or rsi < 40):
        buy_reason = f"Underwater {pnl_pct:.0f}% + buy rating + RSI {rsi or 'na'} = avg-down zone"
    elif upside >= 25 and rating_score >= 2:
        buy_reason = f"+{upside:.0f}% upside + buy rating"
    elif upside >= 15 and rating_score >= 2 and portfolio_pct < 3:
        buy_reason = f"+{upside:.0f}% upside, micro position ({portfolio_pct:.1f}%)"

    if buy_reason:
        if cluster_over:
            return "HOLD", f"Add blocked — {cluster_over} cluster over {CLUSTER_THRESHOLD:.0f}%"
        if earnings_days is not None and 0 <= earnings_days <= ADD_BLOCK_ER_DAYS:
            return "HOLD", f"Add blocked — earnings in {earnings_days}d, wait for the print"
        if rsi is not None and rsi >= ADD_BLOCK_RSI:
            return "HOLD", f"Add blocked — RSI {rsi:.0f}, wait for pullback"
        if sma20_ext is not None and sma20_ext >= ADD_BLOCK_SMA20_EXT:
            return "HOLD", f"Add blocked — {sma20_ext:.0f}% above SMA20, wait for pullback"
        return "BUY", buy_reason

    # HOLD
    if px >= target_mean:
        return "HOLD", f"At PT ${target_mean:.0f} — monitor"
    if 5 <= upside < 15:
        return "HOLD", f"Moderate upside (+{upside:.0f}%)"
    if upside < -5:
        return "HOLD", f"Below PT ({upside:.0f}%) but no exit trigger"
    return "HOLD", "Balanced"


# ─────────────────────────────────────────────────────────────────────────────
# MESSAGE LAYER
# ─────────────────────────────────────────────────────────────────────────────

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
        move = "significant rate spike — headwind for growth (NVDA/MSFT/GOOG); watch small caps"
    elif delta_bps > 2:
        move = "modest rate rise — mild headwind for high-multiple growth"
    elif delta_bps < -5:
        move = "significant rate drop — bullish for growth, REITs, gold; watch TLT for continuation"
    elif delta_bps < -2:
        move = "modest rate decline — mild tailwind for growth and rate-sensitives"
    else:
        move = "stable today"

    # The level matters as much as the daily move for a growth-heavy book.
    if last >= 5.0:
        level = "restrictive level — pressure on long-duration growth multiples; keep growth sizing tight"
    elif last >= 4.5:
        level = "elevated level — growth multiples capped; favor earnings-backed names"
    elif last < 3.5:
        level = "accommodative level — supportive for growth"
    else:
        level = None

    if level and move == "stable today":
        extra = f"stable today, but {level}"
    elif level:
        extra = f"{move}; {level}"
    elif move == "stable today":
        extra = "rates stable — no rate-driven adjustment needed"
    else:
        extra = move

    return {"level": last, "delta_bps": delta_bps, "extra": extra}


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
        pct_s = "house money" if c <= 0 else f"{sign}{pct:.0f}%"
        out.append(f"• <b>{tkr}</b> — {label} · {pct_s} ({sign}${pnl:,.0f})")
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


# ── Market regime → how hard the action plan may add ──

ADD_FRACTION = 0.15  # base add size, as a fraction of current shares

REGIME_NORMAL = "normal"
REGIME_CAUTION = "caution"
REGIME_DEFENSIVE = "defensive"
REGIME_BEAR = "bear"


def classify_regime(vix: Optional[dict], spy_trend: Optional[dict]) -> tuple[str, str]:
    """Combine SPY-vs-SMA200 and VIX into one regime that gates new adds.

    normal    — adds at full size
    caution   — adds at half size          (SPY just below SMA200, or VIX 25–30)
    defensive — avg-down adds paused, other adds at half size
                                           (SPY ≥5% below SMA200, or VIX ≥30)
    bear      — all adds paused            (SPY ≥10% below SMA200)
    """
    pct = spy_trend["pct_above"] if spy_trend else None
    v = vix["level"] if vix else None
    why = []
    if pct is not None and pct < 0:
        why.append(f"S&amp;P 500 {abs(pct):.1f}% below its 200-day average")
    if v is not None and v >= 25:
        why.append(f"VIX {v:.0f}")
    reason = ", ".join(why)

    if pct is not None and pct <= -10:
        return REGIME_BEAR, reason
    if (pct is not None and pct <= -5) or (v is not None and v >= 30):
        return REGIME_DEFENSIVE, reason
    if (pct is not None and pct < 0) or (v is not None and v >= 25):
        return REGIME_CAUTION, reason
    return REGIME_NORMAL, reason


REGIME_RULES = {
    REGIME_NORMAL: None,
    REGIME_CAUTION: "🟡 <b>Caution mode</b> ({why}): adds at half size.",
    REGIME_DEFENSIVE: "🛡️ <b>Defensive mode</b> ({why}): avg-down adds paused, other adds at half size.",
    REGIME_BEAR: "🐻 <b>Bear-market mode</b> ({why}): all new adds paused.",
}


def _greeting(now_sgt: datetime.datetime) -> str:
    h = now_sgt.hour
    part = "morning" if h < 12 else ("afternoon" if h < 18 else "evening")
    emoji = {"morning": "🌅", "afternoon": "☀️", "evening": "🌆"}[part]
    day = f"{now_sgt.strftime('%a')} {now_sgt.day} {now_sgt.strftime('%b')}"
    return (f"{emoji} <b>Good {part} sir</b> — here's your portfolio update for {day}. "
            f"But before that, here's what changed in markets.")


def render_markets_section(vix, yield_10y, dxy, spy_trend, regime) -> str:
    lines = ["📊 <b><u>Markets</u></b>"]
    if vix:
        v, dp = vix["level"], vix["delta_pct"]
        if v < 15:
            mood = "fear is low"
        elif v < 20:
            mood = "volatility is normal"
        elif v < 25:
            mood = "volatility is elevated"
        elif v < 30:
            mood = "fear is high"
        else:
            mood = "fear is very high"
        if dp <= -5:
            mood += ", but easing"
        elif dp >= 5:
            mood += " and rising"
        emoji = "😰" if v >= 25 else ("😐" if v >= 20 else "😌")
        lines.append(f"• {emoji} <b>VIX</b> {v:.1f} ({dp:+.0f}%) — {mood}")
    if yield_10y:
        bps = round(yield_10y["delta_bps"]) + 0
        if bps <= -5:
            note = "rates falling, a tailwind for growth stocks"
        elif bps >= 5:
            note = "rates rising, a headwind for growth stocks"
        else:
            note = "little changed"
        lines.append(f"• 💰 <b>10Y yield</b> {yield_10y['level']:.2f}% ({bps:+d}bps) — {note}")
    if dxy:
        dp = dxy["delta_pct"]
        note = "flat" if abs(dp) < 0.1 else ("stronger" if dp > 0 else "weaker")
        lines.append(f"• 💵 <b>Dollar (DXY)</b> {dxy['level']:.2f} ({dp:+.2f}%) — {note}")
    if spy_trend:
        pct = spy_trend["pct_above"]
        side = "above" if pct >= 0 else "below"
        emoji = "📈" if pct >= 0 else "📉"
        lines.append(f"• {emoji} <b>S&amp;P 500 (SPY)</b> ${spy_trend['level']:.0f}, "
                     f"{abs(pct):.1f}% {side} its 200-day average — {spy_trend['regime'].lower()}")

    # One-line summary built from the same readings — no free text.
    parts = []
    if spy_trend:
        r = spy_trend["regime"]
        parts.append({
            "Strong bull": "stocks are in a strong uptrend",
            "Bull trend intact": "stocks are in an uptrend",
            "Trend under pressure": "stocks are testing their long-term trend",
            "Confirmed downtrend": "stocks are in a downtrend",
            "Bear market": "stocks are in a bear market",
        }.get(r, "the trend is unclear"))
    if vix:
        v = vix["level"]
        if v >= 25:
            parts.append("fear elevated" + (", though it eased today" if vix["delta_pct"] <= -5 else ""))
        elif v < 15:
            parts.append("volatility unusually calm")
    if len(parts) == 2:
        summary = f"{parts[0]}, with {parts[1]}"
    else:
        summary = parts[0] if parts else "markets are mixed"
    summary = summary[0].upper() + summary[1:] + "."
    if yield_10y:
        bps = yield_10y["delta_bps"]
        if bps <= -5:
            summary += " Falling yields are the one bright spot."
        elif bps >= 5:
            summary += " Rising yields add pressure on growth names."
    summary += {
        REGIME_NORMAL: " Conditions support standard sizing.",
        REGIME_CAUTION: " It's a time to size new adds smaller.",
        REGIME_DEFENSIVE: " The regime favours protecting capital over adding risk.",
        REGIME_BEAR: " Preservation mode — no new adds.",
    }[regime]
    lines.append("")
    lines.append(f"🧭 <b>Summary:</b> {summary}")
    return "\n".join(lines)


def render_portfolio_update(rows, total_value, total_cost, total_pnl, total_pct, weekly_delta,
                            alerts, breakdown, time_sensitive_block) -> str:
    def money(x):
        return f"{'+' if x >= 0 else '-'}${abs(x):,.0f}"
    lines = ["💼 <b><u>Portfolio Update</u></b>",
             f"<b>${total_value:,.0f}</b> · P&amp;L {money(total_pnl)} ({total_pct:+.1f}%)"
             + (f" · 7-day {money(weekly_delta)}" if weekly_delta is not None else "")]
    for a in alerts:
        lines.append(f"🚨 {a}")
    if time_sensitive_block:
        lines.append("")
        lines.append(time_sensitive_block)

    priced = [r for r in rows if r[3] is not None]
    ranked = [r for r in priced if r[5] != float("inf")]
    ranked.sort(key=lambda r: r[5], reverse=True)
    if len(ranked) >= 2:
        n = min(3, len(ranked) // 2)
        lines.append("")
        lines.append("📈 <b>Leaders:</b> " + " · ".join(f"{r[0]} {r[5]:+.0f}%" for r in ranked[:n]))
        lines.append("📉 <b>Laggards:</b> " + " · ".join(f"{r[0]} {r[5]:+.0f}%" for r in ranked[::-1][:n]))

    by_size = sorted(priced, key=lambda r: r[1] * r[3], reverse=True)
    top, rest = by_size[:8], by_size[8:]
    lines.append("")
    lines.append("<i>By size of position</i>")
    table = []
    for tkr, sh, c, px, pnl, pct in top:
        w = sh * px / total_value * 100 if total_value else 0
        g = "house money" if pct == float("inf") else f"{pct:+.1f}%"
        table.append(f"{tkr:<5} {w:5.1f}%  ${px:>8.2f}  {g}")
    lines.append("<pre>" + html.escape("\n".join(table)) + "</pre>")
    if rest:
        lines.append(f"<i>+ {len(rest)} smaller ({', '.join(r[0] for r in rest)})</i>")

    if breakdown:
        lines.append("")
        lines.append("<i>By cluster</i>")
        for cl, val, pct, tickers in breakdown:
            lines.append(f"• <b>{cl}:</b> ${val:,.0f} ({pct:.1f}%) — {' '.join(tickers)}")
    return "\n".join(lines)


def render_action_plan(scored_rows, regime: str, regime_why: str, rows, total_value: float):
    """Share-count actions, gated by market regime and post-add concentration."""
    rule = REGIME_RULES[regime]
    trims, exits, avg_down, adds = [], [], [], []
    paused, blocked, holds = [], [], []

    # Running values so each add is checked against the book *after* earlier adds.
    tkr_value = {r[0]: r[1] * r[3] for r in rows if r[3] is not None}
    cl_value: dict[str, float] = {}
    for t, v in tkr_value.items():
        cl_value[classify(t)] = cl_value.get(classify(t), 0) + v
    book = total_value

    for sr in scored_rows:
        tkr, bucket, reason, sh, c, px, pnl, pct, weight, snap, ind = sr
        if px is None:
            continue
        if bucket == "TRIM":
            trim = max(1, int(sh * 0.25))
            trims.append(f"{tkr} −{trim} (${trim * px:,.0f}) · {reason}")
        elif bucket == "EXIT":
            exits.append(f"{tkr} all {sh} (${sh * px:,.0f}) · {reason}")
        elif bucket == "BUY":
            is_avg_down = reason.startswith("Underwater")
            if regime == REGIME_BEAR or (regime == REGIME_DEFENSIVE and is_avg_down):
                paused.append(tkr)
                continue
            frac = ADD_FRACTION if regime == REGIME_NORMAL else ADD_FRACTION / 2
            add = max(1, int(sh * frac)) if sh > 0 else 1
            cap = add * px
            new_book = book + cap
            t_after = (tkr_value.get(tkr, 0) + cap) / new_book * 100
            cl = classify(tkr)
            c_after = (cl_value.get(cl, 0) + cap) / new_book * 100
            if t_after > SINGLE_NAME_THRESHOLD:
                blocked.append(f"{tkr} (would be {t_after:.0f}% of book, limit {SINGLE_NAME_THRESHOLD:.0f}%)")
                continue
            if cl != "Other" and c_after > CLUSTER_THRESHOLD:
                blocked.append(f"{tkr} (would take {cl} to {c_after:.0f}%, limit {CLUSTER_THRESHOLD:.0f}%)")
                continue
            tkr_value[tkr] = tkr_value.get(tkr, 0) + cap
            cl_value[cl] = cl_value.get(cl, 0) + cap
            book = new_book
            (avg_down if is_avg_down else adds).append((tkr, add, cap))
        else:
            holds.append(f"{tkr} ({reason[len('Add blocked — '):]})" if reason.startswith("Add blocked") else tkr)

    lines = ["📑 <b><u>Action Plan</u></b>"]
    if rule:
        lines.append(rule.format(why=regime_why or "risk-off signals"))
    lines.append("⬇ <b>Trim:</b> " + (" · ".join(trims) if trims else "none"))
    if exits:
        lines.append("🚫 <b>Exit / re-validate:</b> " + " · ".join(exits))

    def fmt(items):
        return " · ".join(f"{t} +{n} (${cap:,.0f})" for t, n, cap in sorted(items, key=lambda x: -x[2]))

    if avg_down:
        lines.append("🟢 <b>Avg-down candidates</b> <i>(underwater + buy rating + oversold)</i>")
        lines.append(f"   → {fmt(avg_down)}")
    if adds:
        lines.append("🟢 <b>Add on weakness</b> <i>(analyst upside + buy rating)</i>")
        lines.append(f"   → {fmt(adds)}")
    if paused:
        lines.append(f"⏸ <b>Paused by market regime:</b> {', '.join(paused)}")
    if blocked:
        lines.append("🚧 <b>Blocked by concentration limits:</b> " + " · ".join(blocked))
    if holds:
        lines.append("⏸ <b>Hold:</b> " + ", ".join(holds))
    total = sum(cap for _t, _n, cap in avg_down + adds)
    lines.append(f"<i>Total if all executed: ~${total:,.0f} of new capital</i>" if total
                 else "<i>No new capital suggested today.</i>")
    return "\n".join(lines)


def build_message(portfolio, prices, snapshots, indicators, history_state, brief_lines, date_str, session_label, history=None, garp_scores=None, vix=None, yield_10y=None, dxy=None, spy_trend=None):
    sgt = pytz.timezone("Asia/Singapore")
    now_sgt = datetime.datetime.now(sgt)

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
        pnl_pct = pnl / cost_basis * 100 if cost_basis > 0 else float("inf")
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
    regime, regime_why = classify_regime(vix, spy_trend)

    lines = [_greeting(now_sgt), ""]

    # ── Markets ──
    lines.append(render_markets_section(vix, yield_10y, dxy, spy_trend, regime))

    # ── Portfolio update ──
    ts_items = filter_time_sensitive(snapshots, session_label)
    ts_block = render_time_sensitive(ts_items, snapshots, rows_by_ticker, brief_lines)
    lines.append("")
    lines.append(render_portfolio_update(rows, total_value, total_cost, total_pnl, total_pct,
                                         weekly_delta, alerts, breakdown, ts_block))

    # ── Earnings, grouped by play quality ──
    calendar = build_earnings_calendar(snapshots)
    if build_earnings_tiers_section is not None:
        try:
            block = build_earnings_tiers_section(calendar)
            if block:
                lines.append("")
                lines.append(block)
        except Exception as e:
            print(f"earnings section failed: {e}")
            block = render_earnings_calendar(calendar)
            if block:
                lines.append("")
                lines.append(block)

    # ── Score positions → action plan ──
    over_clusters = {cl for cl, _v, pct, _t in breakdown if pct > CLUSTER_THRESHOLD}
    scored = []
    for tkr, sh, c, px, pnl, pct in rows:
        weight = (sh * px / total_value * 100) if (px and total_value) else 0
        snap = snapshots.get(tkr) or {}
        ind = indicators.get(tkr) or {}
        cl = classify(tkr)
        sma20 = ind.get("sma20")
        sma20_ext = (px / sma20 - 1) * 100 if (px and sma20) else None
        bucket, reason = score_position(
            px, snap.get("target_mean"), pct, snap.get("rating_score", 0),
            weight, ind.get("rsi"), snap.get("earnings_days"),
            cluster_over=cl if cl in over_clusters else None,
            sma20_ext=sma20_ext,
        )
        scored.append((tkr, bucket, reason, sh, c, px, pnl, pct, weight, snap, ind))

    lines.append("")
    lines.append(render_action_plan(scored, regime, regime_why, rows, total_value))

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

    # ── Correlated pairs (leaders/laggards already shown in Portfolio Update) ──
    if build_diagnostics_section is not None:
        try:
            diag_section = build_diagnostics_section(portfolio, prices, history, include_leaderboard=False)
            if diag_section:
                lines.append("")
                lines.append(diag_section)
        except Exception as e:
            print(f"diagnostics section failed: {e}")

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
