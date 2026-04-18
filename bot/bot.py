#!/usr/bin/env python3
"""
Telegram bot for portfolio uploads (CSV-only, zero third-party AI cost).

- Receives a CSV document with columns: ticker, shares, cost.
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

BOT_TOKEN      = os.environ["TELEGRAM_BOT_TOKEN"]
GITHUB_TOKEN   = os.environ["GITHUB_TOKEN"]
GITHUB_REPO    = os.environ["GITHUB_REPO"]
GITHUB_BRANCH  = os.environ.get("GITHUB_BRANCH", "master")
PORTFOLIO_PATH = os.environ.get("PORTFOLIO_PATH", "portfolio.json")

TG_API = f"https://api.telegram.org/bot{BOT_TOKEN}"
GH_API = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{PORTFOLIO_PATH}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("portfolio-bot")

CSV_HELP = (
    "Send a CSV file with columns <code>ticker,shares,cost</code>.\n\n"
    "Example:\n"
    "<code>ticker,shares,cost\n"
    "AMZN,50,183.38\n"
    "NVDA,400,187.36</code>"
)


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


def summarize(portfolio):
    ps = portfolio["positions"]
    lines = [f"\u2705 Portfolio updated ({len(ps)} positions)"]
    for p in ps[:30]:
        sh = p["shares"]
        sh_s = f"{int(sh)}" if float(sh).is_integer() else f"{sh}"
        lines.append(f"\u2022 <b>{p['ticker']}</b>  {sh_s}@${p['cost']:.2f}")
    if len(ps) > 30:
        lines.append(f"\u2026 and {len(ps) - 30} more")
    return "\n".join(lines)


def handle_update(update):
    msg = update.get("message") or update.get("channel_post")
    if not msg:
        return
    chat_id = msg["chat"]["id"]

    try:
        if "photo" in msg:
            tg_send(chat_id, "\u26a0\ufe0f Screenshots are disabled. " + CSV_HELP)
            return

        if "document" in msg:
            doc = msg["document"]
            name = (doc.get("file_name") or "").lower()
            mime = doc.get("mime_type") or ""
            if not (name.endswith(".csv") or "csv" in mime):
                tg_send(chat_id, "\u26a0\ufe0f Only CSV files are supported. " + CSV_HELP)
                return
            tg_send(chat_id, "\u23f3 Parsing CSV\u2026")
            data = tg_download_file(doc["file_id"])
            portfolio = parse_csv(data)
            if not portfolio.get("positions"):
                tg_send(chat_id, "\u26a0\ufe0f Couldn't parse any positions. " + CSV_HELP)
                return
            commit_portfolio(portfolio)
            tg_send(chat_id, summarize(portfolio))
            return

        if "text" in msg:
            text = msg["text"].strip().lower()
            if text in ("/start", "/help"):
                tg_send(chat_id, CSV_HELP)
            return

    except Exception as e:
        log.exception("handler error")
        tg_send(chat_id, f"\u274c Error: {e}")


def main():
    log.info("Portfolio bot starting; repo=%s branch=%s", GITHUB_REPO, GITHUB_BRANCH)
    try:
        requests.post(
            f"{TG_API}/deleteWebhook",
            params={"drop_pending_updates": "true"},
            timeout=30,
        )
    except Exception as e:
        log.warning("deleteWebhook failed: %s", e)
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
