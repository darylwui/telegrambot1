#!/usr/bin/env python3
"""
Daily stock watch: fetches live prices, computes deltas vs strikes,
and posts a formatted bear/bull summary to Telegram.

Update THESES quarterly as earnings dates, capex guidance, and
macro catalysts evolve.
"""
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
            "AWS growing 24% YoY on a $142B annualized run rate with a $244B backlog "
            "(up 40% YoY); Bedrock AI spend grew 60% QoQ. Q1 2026 earnings April 29, "
            "operating income guided $16.5–21.5B."
        ),
        "bear": (
            "$200B capex plan pressures near-term FCF; tariffs on imported goods and "
            "consumer spending softness threaten the retail segment; elevated valuation "
            "leaves limited downside cushion."
        ),
        "take": (
            "Best-in-basket momentum; AWS AI ramp is a multi-year structural tailwind "
            "— hold and reassess post Q1 earnings."
        ),
        "verdict": (
            "Hold — strong earnings momentum and AWS AI tailwinds support the position; "
            "trim only if Q1 operating income guidance disappoints below the $16.5B floor."
        ),
    },
    "GOOG": {
        "bull": (
            "Alphabet crossed $400B in annual revenue in 2025; Google Cloud growing "
            "~48% YoY; Q1 2026 earnings April 29 expected to beat as AI monetization "
            "scales across Search and Cloud."
        ),
        "bear": (
            "DOJ antitrust remedies could unwind lucrative default-search distribution "
            "deals; $175–$185B 2026 capex more than doubles prior-year spend; "
            "OpenAI and Perplexity intensifying competition in AI-driven search."
        ),
        "take": (
            "Solid cash generation and Cloud acceleration offset antitrust noise "
            "— hold with attention on DOJ outcome timeline and Q1 Cloud growth print."
        ),
        "verdict": (
            "Hold — Cloud growth and Search cash flows justify the position; "
            "antitrust resolution is the key re-rating event to watch."
        ),
    },
    "META": {
        "bull": (
            "Q1 2026 revenue guidance $53.5–$56.5B reflects AI-powered ad growth "
            "accelerating to ~24% YoY; Advantage+ automation and Reels continue to "
            "lift advertiser ROI; PayPal partnership expands commerce footprint."
        ),
        "bear": (
            "$115–$135B 2026 capex nearly doubles 2025 spend and will pressure FCF "
            "for multiple quarters; Reality Labs operating losses persist; "
            "macro headwinds from tariffs could soften H2 ad budgets."
        ),
        "take": (
            "Laggard in this basket; Q1 earnings on Apr 29 is the key catalyst — "
            "the AI capex narrative needs validation before the position deserves expansion."
        ),
        "verdict": (
            "Hold — position is modestly in profit with a clear near-term catalyst; "
            "consider partial profit-taking only on a 15%+ gap expansion post-earnings, "
            "or cut if Q1 revenue misses the low end of $53.5B guidance."
        ),
    },
    "NVDA": {
        "bull": (
            "Blackwell GPUs drove ~70% of data center compute revenue with 69% YoY "
            "total revenue growth; sovereign AI demand and hyperscaler capex buildouts "
            "sustaining structural GPU demand."
        ),
        "bear": (
            "Export restrictions expected to cost ~$8B in H20 revenue in Q2 FY2027; "
            "prior $4.5B H20 charge already compressed gross margin to 61%; "
            "further escalation of chip export controls to China is a direct earnings risk."
        ),
        "take": (
            "China revenue permanently impaired but Blackwell demand from the rest of world "
            "more than compensates — momentum favors holding into the next earnings print."
        ),
        "verdict": (
            "Hold — export control headwinds are now priced in; Blackwell ramp and "
            "sovereign AI demand sustain the bull case; trim only on gross margin "
            "compression below 60% in the next quarterly print."
        ),
    },
}

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]

# ── Earnings + analyst fetch ───────────────────────────────────────────────────

def fetch_earnings_data(ticker):
    """Return earnings date, days away, EPS estimate, and analyst consensus."""
    out = {"date": None, "days": None, "eps_est": None, "recommendation": None, "target": None}
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

def build_message(prices, earnings_map, date_str):
    lines = [f"<b>\U0001f4ca Daily Stock Watch \u2014 {date_str}</b>"]

    # Price table
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

    # Worst performer
    valid = {t: v for t, v in pct.items() if v is not None}
    worst = None
    if valid:
        worst = min(valid, key=valid.get)
        sign = "+" if valid[worst] >= 0 else ""
        lines.append("")
        lines.append(
            f"<b>\U0001f53b Worst performer:</b> {worst} ({sign}{valid[worst]:.2f}%)"
        )

    # Bear/Bull theses
    lines.append("")
    lines.append("<b>Bear \U0001f43b / Bull \U0001f402 \u2014 earnings outlook</b>")
    for ticker in STRIKES:
        lines.append("")
        lines.append(f"<b>{ticker}</b>")
        lines.append(f"\U0001f402 {THESES[ticker]['bull']}")
        lines.append(f"\U0001f43b {THESES[ticker]['bear']}")

    # Overall take
    lines.append("")
    lines.append("<b>\U0001f4a1 Overall take</b>")
    for ticker in STRIKES:
        lines.append(f"<b>{ticker}</b> {THESES[ticker]['take']}")

    # Profit/loss structured note for worst performer
    if worst is not None:
        price = prices.get(worst)
        strike = STRIKES[worst]
        delta = price - strike
        p = valid[worst]
        sign = "+" if delta >= 0 else ""
        territory = "profit" if delta >= 0 else "loss"
        e = earnings_map.get(worst, {})

        lines.append("")
        lines.append(f"<b>\U0001f4cb P&amp;L Note \u2014 {worst} (worst performer)</b>")
        lines.append(
            f"\u0394 vs strike: {sign}{p:.2f}% | ${sign}{delta:.2f} \u2014 in {territory}"
        )
        lines.append(THESES[worst]["verdict"])
        if e.get("days") is not None:
            days = e["days"]
            date_label = e["date"].strftime("%b %d")
            lines.append(f"\U0001f4c5 Near-term trigger: earnings {date_label} ({days:+d}d)")

    # Upcoming earnings (sorted by proximity)
    upcoming = sorted(
        [(t, e) for t, e in earnings_map.items() if e.get("days") is not None],
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
            rec = (e.get("recommendation") or "").replace("_", " ").title()
            target = f" | PT ${e['target']:.0f}" if e.get("target") else ""
            eps = f" | EPS est ${e['eps_est']:.2f}" if e.get("eps_est") is not None else ""
            lines.append(f"  {ticker}: {label} ({days:+d}d){tag}  {rec}{target}{eps}")

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

    msg = build_message(prices, earnings_map, date_str)

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
