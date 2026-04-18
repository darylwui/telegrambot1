#!/usr/bin/env python3
"""
Portfolio report: reads portfolio.json, fetches live prices, computes P&L,
generates fresh bull/bear/analyst theses via Claude, and posts a
formatted summary to the Portfolio Telegram chat.

Runs twice daily via GitHub Actions:
  - Tue–Sat 08:00 SGT (00:00 UTC)
  - Mon–Fri 20:00 SGT (12:00 UTC)
"""
import json
import os
import sys
import time
import datetime
import pytz
import yfinance as yf
import requests
import anthropic

# ── Config ────────────────────────────────────────────────────────────────────

BOT_TOKEN     = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID       = os.environ["PORTFOLIO_CHAT_ID"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]

PORTFOLIO_FILE = "portfolio.json"
CLAUDE_MODEL   = "claude-opus-4-7"

# ── Portfolio + prices ────────────────────────────────────────────────────────

def load_portfolio():
    with open(PORTFOLIO_FILE) as f:
        return json.load(f)

def get_prices(tickers):
    data = yf.download(tickers, period="2d", auto_adjust=True, progress=False)
    prices = {}
    for t in tickers:
        try:
            if len(tickers) == 1:
                prices[t] = round(float(data["Close"].dropna().iloc[-1]), 2)
            else:
                prices[t] = round(float(data["Close"][t].dropna().iloc[-1]), 2)
        except Exception:
            prices[t] = None
    return prices

# ── Claude-generated theses ───────────────────────────────────────────────────

def generate_theses(tickers):
    """Ask Claude for fresh per-ticker theses. Returns {ticker: {analyst, bull, bear}}."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    tickers_str = ", ".join(tickers)
    prompt = (
        f"For each of these tickers: {tickers_str}\n\n"
        "Provide three concise items per ticker, grounded in the most recent "
        "views of top Wall Street analysts (Morgan Stanley, Goldman Sachs, JPM, "
        "Barclays, Bernstein, Evercore, etc.):\n"
        "  1. analyst — consensus analyst view (1–2 sentences, include recent "
        "     rating/price-target signal if notable)\n"
        "  2. bull — strongest bull case for next 1–2 quarters (1–2 sentences)\n"
        "  3. bear — strongest bear case / key risk for next 1–2 quarters (1–2 sentences)\n\n"
        "Return ONLY valid JSON, no prose, in this exact shape:\n"
        '{"TICKER": {"analyst": "...", "bull": "...", "bear": "..."}, ...}\n'
        "Keep each field under 280 characters. No markdown, no backticks."
    )
    msg = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}],
    )
    text = msg.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    return json.loads(text)

# ── Message builder ───────────────────────────────────────────────────────────

def build_message(portfolio, prices, theses, date_str, session_label):
    positions = portfolio["positions"]
    lines = [f"<b>\U0001f4bc Portfolio Watch \u2014 {date_str} {session_label}</b>"]

    total_cost = 0.0
    total_value = 0.0
    rows = []
    for p in positions:
        t, sh, c = p["ticker"], p["shares"], p["cost"]
        px = prices.get(t)
        cost_basis = sh * c
        total_cost += cost_basis
        if px is None:
            rows.append((t, sh, c, None, None, None))
            continue
        value = sh * px
        pnl = value - cost_basis
        pnl_pct = pnl / cost_basis * 100 if cost_basis else 0.0
        total_value += value
        rows.append((t, sh, c, px, pnl, pnl_pct))

    total_pnl = total_value - total_cost
    total_pct = (total_pnl / total_cost * 100) if total_cost else 0.0
    sign_tot = "+" if total_pnl >= 0 else ""
    lines.append("")
    lines.append(
        f"<b>Total:</b> ${total_value:,.0f}  |  "
        f"Cost ${total_cost:,.0f}  |  "
        f"P&amp;L {sign_tot}${total_pnl:,.0f} ({sign_tot}{total_pct:.2f}%)"
    )

    lines.append("")
    lines.append("<b>Positions</b>")
    for t, sh, c, px, pnl, pct in rows:
        if px is None:
            lines.append(f"<b>{t}</b>  {sh}@${c:.2f}  |  Last: unavailable")
            continue
        sign = "+" if pnl >= 0 else ""
        lines.append(
            f"<b>{t}</b>  {sh}@${c:.2f}  |  Px ${px:.2f}  |  "
            f"P&amp;L {sign}${pnl:,.0f} ({sign}{pct:.2f}%)"
        )

    lines.append("")
    lines.append("<b>Analyst / Bull / Bear \u2014 per ticker</b>")
    for p in positions:
        t = p["ticker"]
        th = theses.get(t, {})
        lines.append("")
        lines.append(f"<b>{t}</b>")
        if th.get("analyst"):
            lines.append(f"\U0001f9e0 {th['analyst']}")
        if th.get("bull"):
            lines.append(f"\U0001f402 {th['bull']}")
        if th.get("bear"):
            lines.append(f"\U0001f43b {th['bear']}")

    return "\n".join(lines)

# ── Telegram post ─────────────────────────────────────────────────────────────

def post(text):
    # Telegram limit is 4096 chars; split by double-newline blocks if needed.
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

    last = None
    for chunk in chunks:
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "parse_mode": "HTML", "text": chunk},
            timeout=30,
        )
        last = resp.json()
        if not last.get("ok"):
            return last
    return last

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    sgt = pytz.timezone("Asia/Singapore")
    now = datetime.datetime.now(sgt)
    date_str = now.strftime("%Y-%m-%d")
    session_label = "AM" if now.hour < 12 else "PM"

    portfolio = load_portfolio()
    tickers = [p["ticker"] for p in portfolio["positions"]]

    prices = get_prices(tickers)
    try:
        theses = generate_theses(tickers)
    except Exception as e:
        print(f"Thesis generation failed: {e}")
        theses = {}

    msg = build_message(portfolio, prices, theses, date_str, session_label)

    result = post(msg)
    if result and result.get("ok"):
        print("Posted successfully.")
        return
    print(f"First attempt failed: {result}")
    time.sleep(5)
    result2 = post(msg)
    if result2 and result2.get("ok"):
        print("Posted on retry.")
    else:
        print(f"Retry failed: {result2}")
        sys.exit(1)


if __name__ == "__main__":
    main()
