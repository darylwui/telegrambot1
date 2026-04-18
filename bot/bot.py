#!/usr/bin/env python3
"""
Telegram bot for portfolio uploads.

- Receives a photo (portfolio screenshot) or a CSV document.
- Photos: parsed with Claude Vision into structured positions.
- CSVs: parsed directly (expects columns: ticker, shares, cost).
- Commits the resulting portfolio.json to the GitHub repo on the default branch.
- Replies in-chat with a confirmation summary.

Runs as a long-lived poller on Railway.
"""
import base64
import csv
import datetime
import io
import json
import logging
import os
import sys
import time

import requests
import anthropic

# ── Config ──────────────────────────────────────────────────────────────────

BOT_TOKEN      = os.environ["TELEGRAM_BOT_TOKEN"]
ANTHROPIC_KEY  = os.environ["ANTHROPIC_API_KEY"]
GITHUB_TOKEN   = os.environ["GITHUB_TOKEN"]
GITHUB_REPO    = os.environ["GITHUB_REPO"]               # e.g. darylwui/telegrambot1
GITHUB_BRANCH  = os.environ.get("GITHUB_BRANCH", "master")
PORTFOLIO_PATH = os.environ.get("PORTFOLIO_PATH", "portfolio.json")
CLAUDE_MODEL   = os.environ.get("CLAUDE_MODEL", "claude-opus-4-7")

TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
GH_API = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{PORTFOLIO_PATH}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("portfolio-bot")

# ── Telegram helpers ───────────────────────────────────────────────────────────────

def tg_get(method, **params):
    r = requests.get(f"{TG_API}/{method}", params=params, timeout=60)
    return r.json()

def tg_send(chat_id, text):
    requests.post(
        f"{TG_API}/sendMessage",
        data={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
        timeout=30,
    )

def tg_download_file(file_id):
    info = tg_get("getFile", file_id=file_id)
    path = info["result"]["file_path"]
    url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{path}"
    return requests.get(url, timeout=60).content

# ── Parsers ─────────────────────────────────────────────────────────────────────

def parse_photo(image_bytes, mime="image/jpeg"):
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    b64 = base64.standard_b64encode(image_bytes).decode("utf-8")
    prompt = (
        "Extract every portfolio position from this screenshot.\n"
        "For each row, return ticker symbol, shares held, and average cost per share.\n"
        "Return ONLY valid JSON matching this exact shape:\n"
        '{"positions": [{"ticker": "AAPL", "shares": 10, "cost": 150.25}, ...]}\n'
        "Rules:\n"
        "- Use the ticker symbol only (e.g. AMZN, not Amazon).\n"
        "- shares must be a number (integer or float).\n"
        "- cost must be the average cost per share as a number.\n"
        "- Ignore total rows, headers, cash rows.\n"
        "- No markdown, no prose, no backticks."
    )
    msg = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=4000,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image", "source": {
                    "type": "base64", "media_type": mime, "data": b64,
                }},
                {"type": "text", "text": prompt},
            ],
        }],
    )
    text = msg.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    return json.loads(text)

def parse_csv(csv_bytes):
    text = csv_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    positions = []
    for row in reader:
        norm = {k.strip().lower(): v for k, v in row.items() if k}
        t = (norm.get("ticker") or norm.get("symbol") or "").strip().upper()
        if not t:
            continue
        try:
            shares = float(norm.get("shares") or norm.get("quantity") or 0)
            cost = float(norm.get("cost") or norm.get("avg cost") or norm.get("price") or 0)
        except ValueError:
            continue
        if shares <= 0:
            continue
        positions.append({"ticker": t, "shares": shares, "cost": cost})
    return {"positions": positions}

# ── GitHub commit ────────────────────────────────────────────────────────────────────

def commit_portfolio(portfolio):
    portfolio["updated"] = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    body = json.dumps(portfolio, indent=2) + "\n"
    b64 = base64.standard_b64encode(body.encode("utf-8")).decode("utf-8")

    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    r = requests.get(GH_API, headers=headers, params={"ref": GITHUB_BRANCH}, timeout=30)
    sha = r.json().get("sha") if r.status_code == 200 else None

    payload = {
        "message": f"Update portfolio.json via bot ({portfolio['updated']})",
        "content": b64,
        "branch": GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha

    r = requests.put(GH_API, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()

# ── Message handler ────────────────────────────────────────────────────────────────

def summarize(portfolio):
    ps = portfolio["positions"]
    lines = [f"\u2705 Portfolio updated ({len(ps)} positions)"]
    for p in ps[:30]:
        sh = p["shares"]
        sh_s = f"{int(sh)}" if float(sh).is_integer() else f"{sh}"
        lines.append(f"• <b>{p['ticker']}</b>  {sh_s}@${p['cost']:.2f}")
    if len(ps) > 30:
        lines.append(f"… and {len(ps) - 30} more")
    return "\n".join(lines)

def handle_update(update):
    msg = update.get("message") or update.get("channel_post")
    if not msg:
        return
    chat_id = msg["chat"]["id"]

    try:
        portfolio = None

        if "photo" in msg:
            file_id = msg["photo"][-1]["file_id"]
            img = tg_download_file(file_id)
            tg_send(chat_id, "\u23f3 Parsing screenshot with Claude…")
            portfolio = parse_photo(img, mime="image/jpeg")

        elif "document" in msg:
            doc = msg["document"]
            name = (doc.get("file_name") or "").lower()
            mime = doc.get("mime_type") or ""
            data = tg_download_file(doc["file_id"])
            if name.endswith(".csv") or "csv" in mime:
                tg_send(chat_id, "\u23f3 Parsing CSV…")
                portfolio = parse_csv(data)
            elif mime.startswith("image/"):
                tg_send(chat_id, "\u23f3 Parsing image with Claude…")
                portfolio = parse_photo(data, mime=mime)
            else:
                tg_send(chat_id, "Please send a portfolio screenshot (photo) or a CSV.")
                return

        elif "text" in msg:
            text = msg["text"].strip().lower()
            if text in ("/start", "/help"):
                tg_send(
                    chat_id,
                    "Send a portfolio screenshot or a CSV with columns "
                    "<code>ticker,shares,cost</code>. I will update "
                    "<code>portfolio.json</code> in the repo.",
                )
            return
        else:
            return

        if not portfolio or not portfolio.get("positions"):
            tg_send(chat_id, "\u26a0\ufe0f Couldn't extract any positions. Try a clearer image or a CSV.")
            return

        commit_portfolio(portfolio)
        tg_send(chat_id, summarize(portfolio))

    except Exception as e:
        log.exception("handler error")
        tg_send(chat_id, f"\u274c Error: {e}")

# ── Poll loop ───────────────────────────────────────────────────────────────────

def main():
    log.info("Portfolio bot starting; repo=%s branch=%s", GITHUB_REPO, GITHUB_BRANCH)
    offset = None
    while True:
        try:
            params = {"timeout": 50}
            if offset is not None:
                params["offset"] = offset
            r = requests.get(f"{TG_API}/getUpdates", params=params, timeout=60)
            data = r.json()
            if not data.get("ok"):
                log.warning("getUpdates not ok: %s", data)
                time.sleep(5)
                continue
            for update in data["result"]:
                offset = update["update_id"] + 1
                handle_update(update)
        except requests.RequestException as e:
            log.warning("network error: %s", e)
            time.sleep(5)
        except Exception:
            log.exception("loop error")
            time.sleep(5)


if __name__ == "__main__":
    sys.exit(main())
