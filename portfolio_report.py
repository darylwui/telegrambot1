#!/usr/bin/env python3
"""
Portfolio report: reads portfolio.json, fetches live prices + analyst data
+ recent news from Yahoo Finance, computes P&L, and posts a formatted
summary to the Portfolio Telegram chat. No hand-maintained files.

Runs twice daily via GitHub Actions:
  - Tue-Sat 08:23 SGT (00:23 UTC)
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

BOT_TOKEN = os.environ["PORTFOLIO_BOT_TOKEN"]
CHAT_ID   = os.environ["PORTFOLIO_CHAT_ID"]

PORTFOLIO_FILE = "portfolio.json"
ACTIONS_FILE = "portfolio_actions.json"

from macro_config import MACRO


def load_portfolio():
    with open(PORTFOLIO_FILE) as f:
        return json.load(f)


def load_actions():
    """Load recommended portfolio actions."""
    if not os.path.exists(ACTIONS_FILE):
        return {"actions": []}
    try:
        with open(ACTIONS_FILE) as f:
            return json.load(f)
    except Exception:
        return {"actions": []}


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


def _pick_news_fields(item):
    """yfinance returns two shapes depending on version. Normalize them."""
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


def fetch_snapshot(ticker, current_px):
    """Pull live analyst + earnings + news signals for one ticker."""
    out = {"analyst": None, "earnings": None, "news": [], "target_mean": None}
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

    try:
        raw_news = t.news or []
    except Exception:
        raw_news = []
    for item in raw_news:
        title, publisher, url = _pick_news_fields(item)
        if not title:
            continue
        out["news"].append({"title": title, "publisher": publisher, "url": url})
        if len(out["news"]) >= 2:
            break

    return out


def score_position(ticker, px, pnl_pct, target_mean, analyst_rating, portfolio_pct):
    """
    Assign Buy/Hold/Sell recommendation based on multiple signals.
    Returns (action, reasoning)
    """
    if px is None or target_mean is None:
        return "HOLD", "Incomplete data"
    
    # Upside/downside to target
    upside = (target_mean - px) / px * 100
    
    # Normalize analyst rating (Yahoo keys: strong buy, buy, hold, sell, strong sell)
    rating_score = 0  # 0 = hold/unknown, +2 = buy, +3 = strong buy, -1 = sell, -2 = strong sell
    if analyst_rating:
        r = analyst_rating.lower()
        if "strong buy" in r:
            rating_score = 3
        elif "buy" in r:
            rating_score = 2
        elif "hold" in r:
            rating_score = 0
        elif "sell" in r:
            rating_score = -2 if "strong" in r else -1
    
    # Decision logic
    reasons = []
    
    # SELL signals
    if upside <= -15 and rating_score < 0:
        return "SELL", "Below target + bearish analyst"
    if px >= target_mean and portfolio_pct > 15:
        return "TRIM", f"At target + oversized ({portfolio_pct:.0f}% of portfolio)"
    if pnl_pct >= 25 and portfolio_pct > 15:
        return "TRIM", f"Significant profit + oversized ({portfolio_pct:.0f}% of portfolio)"
    
    # BUY signals
    if upside >= 20 and rating_score >= 1:
        return "BUY", f"Strong upside ({upside:.0f}%) + bullish analyst"
    if upside >= 15 and rating_score >= 2:
        return "BUY", f"Upside ({upside:.0f}%) + buy rating"
    if pnl_pct <= -20 and rating_score >= 1:
        return "BUY", "Deeply underwater + bullish thesis (avg down)"
    if upside >= 15:
        return "BUY", f"Meaningful upside ({upside:.0f}%) to target"
    
    # HOLD is default
    if px >= target_mean:
        return "HOLD", "At/above target — monitor for exit"
    if 5 <= upside < 15:
        return "HOLD", f"Moderate upside ({upside:.0f}%) — on track"
    if upside < 0 and rating_score >= 1:
        return "HOLD", f"Slight downside but bullish thesis ({upside:.0f}%)"
    
    return "HOLD", "Balanced thesis"


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

    lines.append("")
    lines.append("<b>\U0001f310 Macro Outlook</b>")
    lines.append("<b>Key risks</b>")
    for r in MACRO["risks"]:
        lines.append(f"  \u26a0\ufe0f {r}")
    lines.append("<b>Watch</b>")
    for w in MACRO["watch"]:
        lines.append(f"  \U0001f4cc {w}")

    # === Buy/Hold/Sell recommendations ===
    recommendations = {"BUY": [], "HOLD": [], "TRIM": [], "SELL": []}
    
    for t, sh, c, px, pnl, pct in rows:
        if px is None:
            continue
        portfolio_pct = (sh * px / total_value * 100) if total_value else 0
        snap = snapshots.get(t) or {}
        target = snap.get("target_mean")
        analyst = snap.get("analyst")
        # Extract rating from analyst string (e.g., "Buy | 15 analysts | PT $150 (+20%)")
        analyst_rating = analyst.split("|")[0].strip() if analyst else ""
        
        action, reason = score_position(t, px, pct, target, analyst_rating, portfolio_pct)
        recommendations[action].append((t, action, reason, portfolio_pct))
    
    # Print recommendations
    if any(recommendations.values()):
        lines.append("")
        lines.append("<b>\U0001f3af Buy/Hold/Sell</b>")
        
        for action in ["BUY", "TRIM", "SELL", "HOLD"]:
            if recommendations[action]:
                emoji = {"BUY": "\U0001f310", "HOLD": "⏸", "TRIM": "\u2b07\ufe0f", "SELL": "\U0001f4a5"}[action]
                lines.append(f"\n<b>{emoji} {action}</b>")
                for t, _, reason, pct in recommendations[action]:
                    lines.append(f"  \u2022 <b>{t}</b> ({pct:.1f}% of portfolio) — {reason}")

    # === Position details ===
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

    # === NEW: Action plan section ===
    actions_config = load_actions()
    if actions_config.get("actions"):
        lines.append("")
        lines.append("<b>\U0001f4d1 Action Plan</b>")
        
        action_lines = []
        for action in actions_config["actions"]:
            t = action["ticker"]
            act_type = action["action"]  # TRIM, BUY, SELL
            sh = action["shares"]
            thesis = action["thesis"]
            
            # Find current position
            current_px = prices.get(t)
            current_shares = None
            for p in portfolio["positions"]:
                if p["ticker"] == t:
                    current_shares = p["shares"]
                    break
            
            if current_px and current_shares is not None:
                if act_type == "TRIM":
                    new_shares = current_shares - sh
                    cash_raised = sh * current_px
                    action_lines.append(
                        f"<b>\u2b07\ufe0f {t}:</b> Trim {sh} shares (${cash_raised:,.0f} proceeds) "
                        f"→ {new_shares} shares remain. {thesis}"
                    )
                elif act_type == "BUY":
                    new_shares = current_shares + sh
                    capital_needed = sh * current_px
                    action_lines.append(
                        f"<b>\U0001f310 {t}:</b> Add {sh} shares (${capital_needed:,.0f} capital) "
                        f"→ {new_shares} shares total. {thesis}"
                    )
                elif act_type == "SELL":
                    cash_raised = sh * current_px
                    new_shares = current_shares - sh
                    action_lines.append(
                        f"<b>\U0001f4a5 {t}:</b> Exit {sh} shares (${cash_raised:,.0f} proceeds) "
                        f"→ {new_shares} shares remain. {thesis}"
                    )
        
        for line in action_lines:
            lines.append(line)

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
        return "PORTFOLIO_BOT_TOKEN is invalid or revoked. Regenerate via @BotFather."
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
        print(f"FATAL: PORTFOLIO_BOT_TOKEN rejected by Telegram: {me}")
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
    snapshots = {}
    for t in tickers:
        try:
            snapshots[t] = fetch_snapshot(t, prices.get(t))
        except Exception as e:
            print(f"snapshot failed for {t}: {e}")
            snapshots[t] = {"analyst": None, "earnings": None, "news": [], "target_mean": None}

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
