"""
US news feed aggregator. Fetches RSS feeds once and filters per ticker.
Sources: Seeking Alpha, Semiconductor Engineering, TechSpot.
"""
import re

import feedparser
import requests

US_FEEDS = [
    ("Seeking Alpha",           "https://seekingalpha.com/market_currents.xml"),
    ("Semiconductor Engineering", "https://semiengineering.com/feed/"),
    ("TechSpot",                "https://www.techspot.com/backend.xml"),
]

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; portfolio-bot/1.0)"}

# Words too generic to match on
_STOPWORDS = {
    "inc", "corp", "ltd", "llc", "group", "holdings", "technologies",
    "technology", "systems", "solutions", "the", "and", "for",
}


def fetch_all_feeds(timeout=15):
    """Fetch all US RSS feeds and return a flat list of (title, url, source)."""
    entries = []
    for name, url in US_FEEDS:
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=timeout)
            resp.raise_for_status()
            feed = feedparser.parse(resp.content)
            for e in feed.entries[:60]:
                title = (e.get("title") or "").strip()
                link = e.get("link") or ""
                if title:
                    entries.append((title, link, name))
        except Exception as exc:
            print(f"feed fetch failed [{name}]: {exc}")
    return entries


def _keywords(ticker, company_name):
    """Build a set of match keywords for a ticker."""
    kws = {ticker.upper()}
    for part in re.split(r"[\s,\-\.]+", company_name or ""):
        part = part.strip().rstrip(".")
        if len(part) > 3 and part.lower() not in _STOPWORDS:
            kws.add(part)
    return kws


def filter_news(all_entries, ticker, company_name="", max_results=2):
    """Return up to max_results articles mentioning ticker or company."""
    kws = _keywords(ticker, company_name)
    results = []
    for title, url, source in all_entries:
        title_lower = title.lower()
        if any(kw.lower() in title_lower for kw in kws):
            results.append({"title": title, "url": url, "publisher": source})
            if len(results) >= max_results:
                break
    return results
