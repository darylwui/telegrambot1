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
from macro_config import MACRO

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
    """Return RSI(14, Wilder), SMA20/50/200, and last close. None on failure."""
    out = {"rsi": None, "sma20": None, "sma50": None, "sma200": None, "last": None}
    closes = _ticker_close_series(history, ticker)
    if closes is None or len(closes) < 20:
        return out

    out["last"] = round(float(closes.iloc[-1]), 2)
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
    """1-line playbook with cost-basis-relative levels where relevant."""
    target = snap.get("target_mean")

    if bucket == "TRIM":
        # Suggest trim 25% with trail-stop framed against cost basis
        sma20 = ind.get("sma20")
        stop = None
        if sma20 and sma20 > cost:
            stop = sma20
        elif sma20:
            stop = max(round(cost * 1.05, 2), sma20)
        if stop:
            buf = (stop - cost) / cost * 100
            buf_sign = "+" if buf >= 0 else ""
            return f"🎯 Trim 25% + trail stop ${stop:.0f} ({buf_sign}{buf:.0f}% vs ${cost:.2f} cost)"
        return f"🎯 Trim 25% — {reason}"

    if bucket == "BUY":
        if target and px:
            return f"🎯 Add on weakness · PT ${target:.0f} ({((target-px)/px*100):+.0f}%)"
        return f"🎯 Accumulate — {reason}"

    if bucket == "EXIT":
        return f"🎯 Re-validate thesis or exit — {reason}"

    # HOLD
    if cost and px:
        gap = (px - cost) / cost * 100
        sign = "+" if gap >= 0 else ""
        return f"🎯 Hold · stop {cost:.2f} cost ({sign}{gap:.0f}% from px)"
    return f"🎯 Hold — {reason}"


# ─────────────────────────────────────────────────────────────────────────────
# MESSAGE LAYER
# ─────────────────────────────────────────────────────────────────────────────

BUCKET_META = {
    "TRIM": ("🔻", "Trim / risk-down"),
    "EXIT": ("🚫", "Exit / re-validate"),
    "BUY": ("🟢", "Add-down candidates"),
    "HOLD": ("⏸", "Hold steady"),
}


def render_at_a_glance(total_value, total_cost, total_pnl, total_pct, alerts, weekly_delta):
    sign_tot = "+" if total_pnl >= 0 else ""
    lines = [
        f"<b>Total:</b> ${total_value:,.0f}  |  "
        f"Cost ${total_cost:,.0f}  |  "
        f"P&amp;L {sign_tot}${total_pnl:,.0f} ({sign_tot}{total_pct:.2f}%)"
    ]
    if weekly_delta is not None:
        sd = "+" if weekly_delta >= 0 else ""
        lines.append(f"<b>7-day Δ:</b> {sd}${weekly_delta:,.0f}")
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


def render_position_block(tkr, sh, c, px, pnl, pct, ind, snap, brief_line, streak, weight):
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

    # Read lines
    for line in synthesize_read(tkr, px, ind, snap, brief_line):
        lines.append(line)

    # Earnings if present
    if snap.get("earnings"):
        lines.append("📅 " + snap["earnings"])

    return "\n".join(lines)


def render_macro():
    lines = ["<b>🌐 Macro Outlook</b>", "<b>Key risks</b>"]
    for r in MACRO["risks"]:
        lines.append(f"  ⚠️ {r}")
    lines.append("<b>Watch</b>")
    for w in MACRO["watch"]:
        lines.append(f"  📌 {w}")
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


def build_message(portfolio, prices, snapshots, indicators, history_state, brief_lines, date_str, session_label):
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
    lines.append(render_at_a_glance(total_value, total_cost, total_pnl, total_pct, alerts, weekly_delta))

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
                brief_lines.get(tkr), streak or {}, weight
            )
            playbook = synthesize_playbook(tkr, bucket, reason, px, c, ind, snap) if px else ""
            lines.append("")
            lines.append(block)
            if playbook:
                lines.append(playbook)

    # ── Macro ──
    lines.append("")
    lines.append(render_macro())

    # ── Action plan ──
    plan = render_action_plan(scored, prices)
    if plan:
        lines.append("")
        lines.append(plan)

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

    # State (history)
    state = load_state()

    # Build message before saving state, so streaks reflect prior state
    msg = build_message(portfolio, prices, snapshots, indicators, state, brief_lines, date_str, session_label)

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
