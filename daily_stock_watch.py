#!/usr/bin/env python3
"""
Daily stock watch: fetches live prices, computes deltas vs strikes,
and posts a formatted bear/bull summary to Telegram.

Update THESES quarterly as earnings dates, capex guidance, and
macro catalysts evolve.
"""
import html
import os
import sys
import time
import datetime
import pytz
import yfinance as yf
import requests

# ── Config ──────────────────────────────────────────────────────────────────

STRIKES = {
    "AMZN": 198.79,
    "GOOG": 302.02,
    "META": 639.29,
    "NVDA": 171.88,
}

# Update each quarter as catalysts change
THESES = {
    "AMZN": {
        "bull": (
            "AWS re-accelerated for a fifth straight quarter to +37% YoY ($42.2B) in Q2 2026, "
            "with backlog at $496B and growing triple digits; group revenue +20% to $200.6B, "
            "operating income +43%. Q3 2026 earnings Oct 29 (guided $197–202B revenue)."
        ),
        "bear": (
            "2026 capex raised to ~$220B (from ~$200B) as memory costs climb — FCF stays "
            "under pressure; AWS is capacity-constrained, so growth depends on getting new "
            "data centres online; retail still exposed to tariffs and consumer softness."
        ),
    },
    "GOOG": {
        "bull": (
            "Google Cloud revenue +82% YoY to $24.8B in Q2 2026 with a 35.6% operating margin "
            "and $514B backlog; group revenue +24% to $119.8B. Q3 2026 earnings Oct 28 — "
            "watch whether Cloud growth holds above 70%."
        ),
        "bear": (
            "2026 capex guidance lifted to $195–205B and management expects a significant "
            "further increase in 2027; Q2 free cash flow turned negative (−$5.9B); "
            "AI chat assistants keep chipping at traditional Search usage."
        ),
    },
    "META": {
        "bull": (
            "Q3 2026 (reported Oct 21) revenue +26% YoY to $64.6B, and Q4 guided to "
            "$73–76B, above consensus — AI-driven ad targeting is still lifting pricing "
            "and volume. Ex-legal charge, 2026 operating income tracking above 2025."
        ),
        "bear": (
            "A $9.9B youth-mental-health settlement hit Q3 profit; 2026 capex narrowed up "
            "to $135–145B with Q3 alone at $34.9B; Reality Labs still losing $18–19B "
            "this year. Next earnings late January."
        ),
    },
    "NVDA": {
        "bull": (
            "Q2 FY2027 revenue +106% YoY to $96.2B, data centre +117% to $89.0B, gross "
            "margin 75%; Q3 guided to $108B (above ~$104B consensus) and management is "
            "targeting ~70% growth in FY2028. Q3 earnings expected Nov 17."
        ),
        "bear": (
            "Guidance assumes zero China data-centre revenue, so policy remains a swing "
            "factor; hyperscalers are ~55% of data-centre sales — any capex digestion "
            "hits directly; rising memory costs are a margin watch-point."
        ),
    },
}

from macro_config import MACRO

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]

# ── Earnings + analyst fetch ───────────────────────────────────────────────────

def fetch_earnings_data(ticker):
    """Return earnings date, days away, EPS estimate, and analyst consensus."""
    out = {"date": None, "days": None, "eps_est": None, "recommendation": None, "target": None,
           "target_low": None, "target_high": None, "analyst_count": None}
    try:
        t = yf.Ticker(ticker)
        info = t.info or {}

        earnings_ts = info.get("earningsTimestamp") or info.get("earningsTimestampStart")
        if earnings_ts:
            ed = datetime.datetime.fromtimestamp(int(earnings_ts), tz=datetime.timezone.utc).date()
            days = (ed - datetime.date.today()).days
            if -3 <= days <= 90:
                out["date"] = ed
                out["days"] = days

        out["recommendation"] = info.get("recommendationKey") or None
        out["target"] = info.get("targetMeanPrice") or None
        out["target_low"] = info.get("targetLowPrice") or None
        out["target_high"] = info.get("targetHighPrice") or None
        out["analyst_count"] = info.get("numberOfAnalystOpinions") or None

        try:
            trend = t.eps_trend
            if trend is not None and not trend.empty and "0q" in trend.index:
                est = trend.loc["0q", "current"]
                if est and str(est) not in ("nan", "None"):
                    out["eps_est"] = float(est)
        except Exception:
            pass
    except Exception:
        pass
    return out

# ── Entry / stop level computation ────────────────────────────────────────────

def _atr(df, window=14):
    high = df["High"].astype(float)
    low  = df["Low"].astype(float)
    prev = df["Close"].astype(float).shift(1)
    tr = pandas_concat([high - low, (high - prev).abs(), (low - prev).abs()], axis=1).max(axis=1)
    return float(tr.ewm(alpha=1 / window, adjust=False).mean().iloc[-1])

def _rsi(series, window=14):
    import pandas as pd
    delta = series.diff()
    gain = delta.clip(lower=0).ewm(alpha=1 / window, adjust=False).mean()
    loss = (-delta.clip(upper=0)).ewm(alpha=1 / window, adjust=False).mean()
    rs = gain / loss.replace(0, pd.NA)
    return float((100 - 100 / (1 + rs)).iloc[-1])

def fetch_levels(ticker, analyst_target=None):
    """
    Compute entry_low, entry_high, stop for a ticker using price history.
    Returns dict with keys: entry_low, entry_high, stop, target, screen.
    """
    import pandas as pd
    try:
        df = yf.download(ticker, period="1y", auto_adjust=True, progress=False)
        if df is None or df.empty or len(df) < 60:
            return None
        df = df.dropna(subset=["Close"])
        close  = df["Close"].astype(float)
        last   = float(close.iloc[-1])
        sma20  = float(close.rolling(20).mean().iloc[-1])
        sma50  = float(close.rolling(50).mean().iloc[-1])
        sma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else None
        high52 = float(close.tail(252).max())
        rsi14  = _rsi(close)
        atr14  = _atr(df)

        # Pick the most relevant screen and derive levels
        screen = None
        entry_low = entry_high = stop = None

        if sma200 and last > sma200 and rsi14 < 35 and abs(last / sma50 - 1) <= 0.05:
            screen = "oversold_pullback"
            entry_low  = round(last - 0.25 * atr14, 2)
            entry_high = round(last + 0.25 * atr14, 2)
            stop       = round(sma200 * 0.98, 2)

        elif sma200 and sma20 > sma50 > sma200 and 55 <= rsi14 <= 70 and last > sma20:
            screen = "momentum_continuation"
            entry_low  = round(sma20 * 0.99, 2)
            entry_high = round(sma20 * 1.015, 2)
            stop       = round(sma50 * 0.98, 2)

        elif last >= high52 * 0.99:
            screen = "breakout"
            entry_low  = round(high52 * 0.99, 2)
            entry_high = round(high52 * 1.01, 2)
            stop       = round(high52 * 0.95, 2)

        elif sma200 and last > sma200 and 40 < rsi14 < 55:
            screen = "uptrend_cooling"
            entry_low  = round(sma50 * 0.99, 2)
            entry_high = round(sma50 * 1.01, 2)
            stop       = round(sma200 * 0.98, 2)

        else:
            # Fallback: bracket current price by ±0.5 ATR; stop 1.5 ATR below
            screen = "current_level"
            entry_low  = round(last - 0.5 * atr14, 2)
            entry_high = round(last + 0.5 * atr14, 2)
            stop       = round(last - 1.5 * atr14, 2)

        # Use analyst mean target when it's above the entry zone, else None
        target = None
        if analyst_target and analyst_target > entry_high:
            target = round(float(analyst_target), 2)

        return {
            "screen": screen,
            "entry_low": entry_low,
            "entry_high": entry_high,
            "stop": stop,
            "target": target,
        }
    except Exception as e:
        print(f"fetch_levels failed for {ticker}: {e}")
        return None

# helper alias (avoids importing pandas at module level just for concat)
def pandas_concat(frames, axis=1):
    import pandas as pd
    return pd.concat(frames, axis=axis)

# ── Price fetch ───────────────────────────────────────────────────────────────────────

def get_prices():
    tickers = list(STRIKES.keys())
    data = yf.download(tickers, period="2d", auto_adjust=True, progress=False)
    prices = {}
    for t in tickers:
        try:
            prices[t] = round(float(data["Close"][t].dropna().iloc[-1]), 2)
        except Exception:
            prices[t] = None
    return prices

# ── Message builder ──────────────────────────────────────────────────────────────────

def build_message(prices, earnings_map, levels_map, date_str):
    lines = [f"<b>\U0001f4ca Daily Stock Watch \u2014 {date_str}</b>"]

    lines.append("")
    lines.append("<b>\U0001f310 Macro Outlook</b>")
    lines.append("<b>Key risks</b>")
    for r in MACRO["risks"]:
        lines.append(f"  \u26a0\ufe0f {r}")
    lines.append("<b>Watch</b>")
    for w in MACRO["watch"]:
        lines.append(f"  \U0001f4cc {w}")

    # Upcoming earnings summary (sorted by proximity)
    upcoming = sorted(
        [(t, e) for t, e in earnings_map.items() if e["days"] is not None],
        key=lambda x: x[1]["days"],
    )
    if upcoming:
        lines.append("")
        lines.append("<b>\U0001f4c5 Upcoming earnings</b>")
        for ticker, e in upcoming:
            days = e["days"]
            label = e["date"].strftime("%b %d")
            if days <= 0:
                tag = " \u26a0\ufe0f reporting now/just reported"
            elif days <= 7:
                tag = f" \u26a0\ufe0f <b>this week</b>"
            elif days <= 14:
                tag = " \u23f0 next 2 weeks"
            else:
                tag = ""
            rec = (e["recommendation"] or "").replace("_", " ").title()
            target = f" | PT ${e['target']:.0f}" if e["target"] else ""
            eps = f" | EPS est ${e['eps_est']:.2f}" if e["eps_est"] is not None else ""
            lines.append(f"  {ticker}: {label} ({days:+d}d){tag}  {rec}{target}{eps}")

    pct = {}
    for ticker, strike in STRIKES.items():
        price = prices.get(ticker)
        lines.append("")
        if price is None:
            lines.append(f"<b>{ticker}</b>  Strike: ${strike:.2f}")
            lines.append("Last: unavailable")
            pct[ticker] = None
            continue
        delta = price - strike
        p = delta / strike * 100
        pct[ticker] = p
        sign = "+" if delta >= 0 else ""
        lines.append(f"<b>{ticker}</b>  Strike: ${strike:.2f}")
        lines.append(f"Last: ${price:.2f}  |  \u0394 ${sign}{delta:.2f} ({sign}{p:.2f}%)")
        e = earnings_map.get(ticker, {})
        if e.get("days") is not None:
            lines.append(f"\U0001f4c5 Earnings {e['date'].strftime('%b %d')} ({e['days']:+d}d)")
        lv = levels_map.get(ticker)
        if lv:
            screen_label = lv["screen"].replace("_", " ").title()
            lines.append(
                f"\U0001f3af Entry: ${lv['entry_low']:.2f} – ${lv['entry_high']:.2f}"
                f"  |  Stop: ${lv['stop']:.2f}"
                + (f"  |  PT: ${lv['target']:.2f}" if lv.get("target") else "")
                + f"  <i>({screen_label})</i>"
            )

    valid = {t: v for t, v in pct.items() if v is not None}
    if valid:
        worst = min(valid, key=valid.get)
        sign = "+" if valid[worst] >= 0 else ""
        lines.append("")
        lines.append(
            f"<b>\U0001f53b Worst performer:</b> {worst} ({sign}{valid[worst]:.2f}%)"
        )

    lines.append("")
    lines.append("<b>Bear \U0001f43b / Bull \U0001f402 \u2014 earnings outlook</b>")
    for ticker in STRIKES:
        lines.append("")
        lines.append(f"<b>{ticker}</b>")
        e = earnings_map.get(ticker, {})
        if e.get("days") is not None:
            eps = f" | EPS est ${e['eps_est']:.2f}" if e["eps_est"] is not None else ""
            lines.append(f"\U0001f4c5 Earnings {e['date'].strftime('%b %d')} ({e['days']:+d}d){eps}")
        lines.append(f"\U0001f402 {THESES[ticker]['bull']}")
        lines.append(f"\U0001f43b {THESES[ticker]['bear']}")

    return "\n".join(lines)

# ── Telegram post ────────────────────────────────────────────────────────────────────

def post(text):
    resp = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        data={"chat_id": CHAT_ID, "parse_mode": "HTML", "text": text},
        timeout=15,
    )
    return resp.json()

# ── Main ──────────────────────────────────────────────────────────────────────────────

def main():
    sgt = pytz.timezone("Asia/Singapore")
    date_str = datetime.datetime.now(sgt).strftime("%Y-%m-%d")

    prices = get_prices()
    earnings_map = {}
    for ticker in STRIKES:
        try:
            earnings_map[ticker] = fetch_earnings_data(ticker)
        except Exception as e:
            print(f"earnings fetch failed for {ticker}: {e}")
            earnings_map[ticker] = {}

    levels_map = {}
    for ticker in STRIKES:
        try:
            analyst_target = (earnings_map.get(ticker) or {}).get("target")
            levels_map[ticker] = fetch_levels(ticker, analyst_target)
        except Exception as e:
            print(f"levels fetch failed for {ticker}: {e}")
            levels_map[ticker] = None

    msg = build_message(prices, earnings_map, levels_map, date_str)

    result = post(msg)
    if result.get("ok"):
        print("Posted successfully.")
        return

    print(f"First attempt failed: {result}")
    time.sleep(5)
    result2 = post(msg)
    if result2.get("ok"):
        print("Posted successfully on retry.")
    else:
        print(f"Retry failed: {result2}")
        sys.exit(1)


if __name__ == "__main__":
    main()
