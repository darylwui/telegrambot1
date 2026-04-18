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

# ── Config ────────────────────────────────────────────────────────────────────

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
            "operating income guided $16.5–$21.5B."
        ),
        "bear": (
            "$200B capex plan pressures near-term FCF; tariffs on imported goods and "
            "consumer spending softness threaten the retail segment; elevated valuation "
            "leaves limited downside cushion."
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
    },
}

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]

# ── Price fetch ───────────────────────────────────────────────────────────────

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

# ── Message builder ───────────────────────────────────────────────────────────

def build_message(prices, date_str):
    lines = [f"<b>\U0001f4ca Daily Stock Watch \u2014 {date_str}</b>"]

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

    valid = {t: v for t, v in pct.items() if v is not None}
    if valid:
        worst = min(valid, key=valid.get)
        sign = "+" if valid[worst] >= 0 else ""
        lines.append("")
        lines.append(
            f"<b>\U0001f53b Worst performer:</b> {worst} ({sign}{valid[worst]:.2f}%)"
        )

    lines.append("")
    lines.append("<b>Bear / Bull \u2014 next quarter</b>")
    for ticker in STRIKES:
        lines.append("")
        lines.append(f"<b>{ticker}</b>")
        lines.append(f"\U0001f402 {THESES[ticker]['bull']}")
        lines.append(f"\U0001f43b {THESES[ticker]['bear']}")

    return "\n".join(lines)

# ── Telegram post ─────────────────────────────────────────────────────────────

def post(text):
    resp = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        data={"chat_id": CHAT_ID, "parse_mode": "HTML", "text": text},
        timeout=15,
    )
    return resp.json()

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    sgt = pytz.timezone("Asia/Singapore")
    date_str = datetime.datetime.now(sgt).strftime("%Y-%m-%d")

    prices = get_prices()
    msg = build_message(prices, date_str)

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
