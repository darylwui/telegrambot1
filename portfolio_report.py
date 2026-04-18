#!/usr/bin/env python3
"""
Portfolio report: reads portfolio.json, fetches live prices + analyst data
+ recent news from Yahoo Finance, computes P&L, and posts a formatted
summary to the Portfolio Telegram chat. No hand-maintained files.

Runs twice daily via GitHub Actions:
  - Tue-Sat 08:00 SGT (00:00 UTC)
  - Mon-Fri 20:00 SGT (12:00 UTC)
"""
import datetime
import html
import json
import os
import sys
import time

import pytz
import requests
import yfinance as yf

from news_sources import fetch_all_feeds, filter_news

BOT_TOKEN = os.environ["PORTFOLIO_BOT_TOKEN"]
CHAT_ID   = os.environ["PORTFOLIO_CHAT_ID"]

PORTFOLIO_FILE = "portfolio.json"


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


def fetch_snapshot(ticker, current_px, all_news):
    """Pull live analyst + earnings signals and filtered US news for one ticker."""
    out = {"analyst": None, "earnings": None, "news": []}
    t = yf.Ticker(ticker)

    info = {}
    try:
        info = t.info or {}
    except Exception:
        info = {}

    rating = info.get("recommendationKey") or ""
    count = info.get("numberOfAnalystOpinions")
    target_mean = info.get("targetMeanPrice")
    target_high = info.get("targetHighPrice")
    target_low  = info.get("targetLowPrice")

    parts = []
    if rating:
        parts.append(str(rating).replace("_", " ").title())
    if count:
        parts.append(f"{count} analysts")
    if target_mean and current_px:
        upside = (target_mean - current_px) / current_px * 100
        sign = "+" if upside >= 0 else ""
        parts.append(f"PT ${target_mean:.0f} ({sign}{upside:.0f}%)")
    elif target_mean:
        parts.append(f"PT ${target_mean:.0f}")
    if target_low and target_high and target_mean:
        parts.append(f"range ${target_low:.0f}-${target_high:.0f}")
    if parts:
        out["analyst"] = " | ".join(parts)
    out["target_mean"] = target_mean

    earnings_ts = info.get("earningsTimestamp") or info.get("earningsTimestampStart")
    if earnings_ts:
        try:
            ed = datetime.datetime.fromtimestamp(int(earnings_ts), tz=datetime.timezone.utc).date()
            days = (ed - datetime.date.today()).days
            if -2 <= days <= 60:
                out["earnings"] = f"Earnings {ed.strftime('%b %d')} ({days:+d}d)"
        except Exception:
            pass

    company_name = info.get("shortName") or info.get("longName") or ""
    out["news"] = filter_news(all_news, ticker, company_name)

    return out


def build_message(portfolio, prices, snapshots, date_str, session_label):
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

    for t, sh, c, px, pnl, pct in rows:
        lines.append("")
        if px is None:
            lines.append(f"<b>{t}</b>  {sh}@${c:.2f}  |  Last: unavailable")
        else:
            sign = "+" if pnl >= 0 else ""
            lines.append(
                f"<b>{t}</b>  {sh}@${c:.2f}  |  Px ${px:.2f}  |  "
                f"P&amp;L {sign}${pnl:,.0f} ({sign}{pct:.2f}%)"
            )
        snap = snapshots.get(t) or {}
        pt = snap.get("target_mean")
        if pt and px:
            dist = (pt - px) / px * 100
            sign = "+" if dist >= 0 else ""
            if px >= pt:
                lines.append(f"\u2705 PT ${pt:.0f} reached  |  now {sign}{dist:.0f}% above target")
            else:
                dollar_away = pt - px
                lines.append(f"\u27a1\ufe0f PT ${pt:.0f}  |  ${dollar_away:.2f} away ({sign}{dist:.0f}%)")
        if snap.get("analyst"):
            lines.append(f"\U0001f9e0 {snap['analyst']}")
        if snap.get("earnings"):
            lines.append(f"\U0001f4c5 {snap['earnings']}")
        for n in snap.get("news", []):
            title = html.escape(n["title"])
            pub = f" ({html.escape(n['publisher'])})" if n.get("publisher") else ""
            if n.get("url"):
                url = html.escape(n["url"], quote=True)
                lines.append(f"\U0001f4f0 <a href=\"{url}\">{title}</a>{pub}")
            else:
                lines.append(f"\U0001f4f0 {title}{pub}")

    return "\n".join(lines)


def _explain_tg_error(result):
    """Turn a Telegram API error into actionable guidance."""
    if not result:
        return "No response from Telegram API."
    desc = (result.get("description") or "").lower()
    code = result.get("error_code")
    if "chat not found" in desc:
        return (
            f"PORTFOLIO_CHAT_ID={CHAT_ID!r} is wrong, or the bot is not a member "
            f"of that chat. Fix: add the bot to the chat, send one message, then "
            f"visit https://api.telegram.org/bot<TOKEN>/getUpdates to read the "
            f"correct chat.id (groups are negative, supergroups start with -100)."
        )
    if "unauthorized" in desc or code == 401:
        return "TELEGRAM_BOT_TOKEN is invalid or revoked. Regenerate via @BotFather."
    if "bot was kicked" in desc or "bot is not a member" in desc:
        return "Bot was removed from the chat. Re-add it and try again."
    return f"Telegram returned: {result}"


def post(text):
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
        text = f"<i>Page {i}/{total}</i>\n\n{chunk}" if total > 1 else chunk
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": CHAT_ID, "parse_mode": "HTML", "text": text},
            timeout=30,
        )
        last = resp.json()
        if not last.get("ok"):
            return last
    return last


def preflight():
    """Verify bot token and chat before doing any work."""
    try:
        me = requests.get(f"https://api.telegram.org/bot{BOT_TOKEN}/getMe", timeout=30).json()
    except Exception as e:
        print(f"FATAL: cannot reach Telegram API: {e}")
        sys.exit(1)
    if not me.get("ok"):
        print(f"FATAL: TELEGRAM_BOT_TOKEN rejected by Telegram: {me}")
        print("Fix: regenerate the token via @BotFather and update the GitHub Actions secret.")
        sys.exit(1)
    bot_username = me["result"].get("username")
    print(f"Bot OK: @{bot_username}")

    chat = requests.get(
        f"https://api.telegram.org/bot{BOT_TOKEN}/getChat",
        params={"chat_id": CHAT_ID},
        timeout=30,
    ).json()
    if not chat.get("ok"):
        print(f"FATAL: PORTFOLIO_CHAT_ID={CHAT_ID!r} not reachable by @{bot_username}.")
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

    prices = get_prices(tickers)
    print("Fetching US news feeds...")
    all_news = fetch_all_feeds()
    print(f"  {len(all_news)} articles loaded across all feeds.")
    snapshots = {}
    for t in tickers:
        try:
            snapshots[t] = fetch_snapshot(t, prices.get(t), all_news)
        except Exception as e:
            print(f"snapshot failed for {t}: {e}")
            snapshots[t] = {"analyst": None, "earnings": None, "news": []}

    msg = build_message(portfolio, prices, snapshots, date_str, session_label)

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
