"""
Strategy-based watchlist monitor for tickers OUTSIDE the main portfolio.

Reads watchlist.json, fetches daily history, runs registered strategies,
reports current state per (ticker, strategy), and renders an HTML section
for the existing Telegram report. State persists at watchlist_state.json
so LONG↔FLAT flips can be highlighted across runs.

This is NOT a buy/sell recommender. It reflects the state of rules the
user authored. Whether to follow your own rules is your call.

Public API (used by portfolio_report.py):
    build_watchlist_section() -> Optional[str]
"""

from __future__ import annotations

import json
import os
import html
from typing import Callable, Optional

import numpy as np
import pandas as pd
import yfinance as yf


CONFIG_FILE = "watchlist.json"
STATE_FILE = "watchlist_state.json"


# ─────────────────────────────────────────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────────────────────────────────────────

def _sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n).mean()


def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()


def _rsi(s: pd.Series, n: int = 14) -> pd.Series:
    diff = s.diff()
    gain = diff.clip(lower=0)
    loss = -diff.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / n, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / n, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - 100 / (1 + rs)


def _macd(s: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    line = _ema(s, fast) - _ema(s, slow)
    sig = _ema(line, signal)
    return line, sig


def _atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    h, l, c = df["High"], df["Low"], df["Close"]
    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / n, adjust=False).mean()


def _donchian(df: pd.DataFrame, n: int):
    return df["Low"].rolling(n).min(), df["High"].rolling(n).max()


# ─────────────────────────────────────────────────────────────────────────────
# STRATEGIES — each returns Series of {0, 1} (long-only, no shorting)
# ─────────────────────────────────────────────────────────────────────────────

def _donchian_breakout(n: int = 20, exit_n: int = 10) -> Callable:
    def strat(df):
        _, upper = _donchian(df, n)
        lower_exit, _ = _donchian(df, exit_n)
        long_on = (df["Close"] > upper.shift(1)).fillna(False)
        long_off = (df["Close"] < lower_exit.shift(1)).fillna(False)
        pos, out = 0, []
        for v_on, v_off in zip(long_on, long_off):
            if pos == 0 and v_on:
                pos = 1
            elif pos == 1 and v_off:
                pos = 0
            out.append(pos)
        return pd.Series(out, index=df.index)
    return strat


def _sma_cross(fast: int, slow: int) -> Callable:
    def strat(df):
        return (_sma(df["Close"], fast) > _sma(df["Close"], slow)).astype(int)
    return strat


def _ema_cross(fast: int, slow: int) -> Callable:
    def strat(df):
        return (_ema(df["Close"], fast) > _ema(df["Close"], slow)).astype(int)
    return strat


def _macd_signal() -> Callable:
    def strat(df):
        line, sig = _macd(df["Close"])
        return (line > sig).astype(int)
    return strat


def _trend_with_filter(fast: int = 20, slow: int = 50, regime: int = 200) -> Callable:
    def strat(df):
        f = _sma(df["Close"], fast)
        s = _sma(df["Close"], slow)
        regime_filter = df["Close"] > _sma(df["Close"], regime)
        return ((f > s) & regime_filter).astype(int)
    return strat


def _atr_trailing_stop(fast: int = 20, slow: int = 50, atr_n: int = 14, mult: float = 3.0) -> Callable:
    def strat(df):
        f = _sma(df["Close"], fast)
        s = _sma(df["Close"], slow)
        a = _atr(df, atr_n)
        long_signal = (f > s).fillna(False)
        pos, hwm = 0, 0.0
        out = []
        for i in range(len(df)):
            close = float(df["Close"].iloc[i])
            atr_v = float(a.iloc[i]) if pd.notna(a.iloc[i]) else 0.0
            if pos == 0:
                if long_signal.iloc[i]:
                    pos, hwm = 1, close
            else:
                hwm = max(hwm, close)
                stop = hwm - mult * atr_v
                if close < stop or not long_signal.iloc[i]:
                    pos, hwm = 0, 0.0
            out.append(pos)
        return pd.Series(out, index=df.index)
    return strat


STRATEGIES: dict[str, Callable[[], Callable]] = {
    "donchian_20_10": lambda: _donchian_breakout(20, 10),
    "donchian_55_20": lambda: _donchian_breakout(55, 20),
    "sma_10_30":      lambda: _sma_cross(10, 30),
    "sma_20_50":      lambda: _sma_cross(20, 50),
    "sma_50_200":     lambda: _sma_cross(50, 200),
    "ema_12_26":      lambda: _ema_cross(12, 26),
    "macd":           lambda: _macd_signal(),
    "trend_filter":   lambda: _trend_with_filter(),
    "atr_trail":      lambda: _atr_trailing_stop(),
}

# Plain-English display names for the Telegram message.
STRATEGY_DISPLAY: dict[str, str] = {
    "donchian_20_10": "20-day breakout",
    "donchian_55_20": "55-day breakout",
    "sma_10_30":      "Fast trend (10/30 MA)",
    "sma_20_50":      "Mid trend (20/50 MA)",
    "sma_50_200":     "Long trend (50/200 MA)",
    "ema_12_26":      "EMA cross (12/26)",
    "macd":           "MACD momentum",
    "trend_filter":   "Confirmed trend (20/50 + 200)",
    "atr_trail":      "Trend + ATR trailing stop",
}


# ─────────────────────────────────────────────────────────────────────────────
# STATE — persisted across runs so we can detect flips
# ─────────────────────────────────────────────────────────────────────────────

def _load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {"tickers": {}}
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {"tickers": {}}


def _save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)


# ─────────────────────────────────────────────────────────────────────────────
# DATA + EVALUATION
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_history(ticker: str, period: str) -> Optional[pd.DataFrame]:
    try:
        df = yf.download(ticker, period=period, auto_adjust=True, progress=False)
    except Exception as e:
        print(f"[watchlist] fetch failed for {ticker}: {e}")
        return None
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
    return df


def _fetch_fundamentals(ticker: str) -> dict:
    """Best-effort fundamental + analyst snapshot from yfinance.info. Pure facts, no opinion."""
    out = {
        "market_cap": None, "fifty_two_high": None, "fifty_two_low": None,
        "beta": None, "revenue_ttm": None, "cash": None, "debt": None,
        "ps_ratio": None, "short_pct_float": None,
        # Analyst consensus — third-party data point, not editorial
        "rec_key": None, "rec_label": None, "analyst_count": None,
        "target_mean": None, "target_high": None, "target_low": None,
    }
    try:
        info = yf.Ticker(ticker).info or {}
    except Exception as e:
        print(f"[watchlist] info fetch failed for {ticker}: {e}")
        return out
    out["market_cap"] = info.get("marketCap")
    out["fifty_two_high"] = info.get("fiftyTwoWeekHigh")
    out["fifty_two_low"] = info.get("fiftyTwoWeekLow")
    out["beta"] = info.get("beta")
    out["revenue_ttm"] = info.get("totalRevenue")
    out["cash"] = info.get("totalCash")
    out["debt"] = info.get("totalDebt")
    out["ps_ratio"] = info.get("priceToSalesTrailing12Months")
    out["short_pct_float"] = info.get("shortPercentOfFloat")
    rec = info.get("recommendationKey") or ""
    if rec:
        out["rec_key"] = rec
        out["rec_label"] = str(rec).replace("_", " ").title()
    out["analyst_count"] = info.get("numberOfAnalystOpinions")
    out["target_mean"] = info.get("targetMeanPrice")
    out["target_high"] = info.get("targetHighPrice")
    out["target_low"] = info.get("targetLowPrice")
    return out


def _fmt_money(n) -> str:
    """Compact dollar formatting: 1234567890 -> '$1.23B'."""
    if n is None:
        return "—"
    try:
        n = float(n)
    except Exception:
        return "—"
    sign = "-" if n < 0 else ""
    n = abs(n)
    if n >= 1e12:
        return f"{sign}${n / 1e12:.2f}T"
    if n >= 1e9:
        return f"{sign}${n / 1e9:.2f}B"
    if n >= 1e6:
        return f"{sign}${n / 1e6:.0f}M"
    if n >= 1e3:
        return f"{sign}${n / 1e3:.0f}K"
    return f"{sign}${n:.0f}"


def _trigger_detail(df: pd.DataFrame, sname: str, state: str) -> str:
    """
    Compute a compact 'where would your rule flip' string per strategy.

    Mechanical computation from the rule's own definitions — not a buy
    recommendation. For Donchian: the literal entry/exit trigger price.
    For MA-cross rules: current fast/slow values + gap, so the reader can
    see how close the rule is to flipping. For ATR-trail when ON: the
    current trailing stop level (HWM × 3 ATR).
    """
    close = df["Close"]
    n_bars = len(df)

    def _pct(a, b):
        return (a - b) / b * 100 if b else 0.0

    if sname in ("donchian_20_10", "donchian_55_20"):
        n_in, n_out = (20, 10) if sname == "donchian_20_10" else (55, 20)
        if n_bars < n_in + 2:
            return ""
        prior_high = float(df["High"].iloc[-(n_in + 1):-1].max())
        prior_low = float(df["Low"].iloc[-(n_out + 1):-1].min())
        if state == "LONG":
            cur = float(close.iloc[-1])
            return f"exit ↓ ${prior_low:.2f} ({_pct(prior_low, cur):+.1f}%)"
        cur = float(close.iloc[-1])
        return f"trigger ↑ ${prior_high:.2f} ({_pct(prior_high, cur):+.1f}%)"

    if sname in ("sma_10_30", "sma_20_50", "sma_50_200"):
        fast, slow = {"sma_10_30": (10, 30), "sma_20_50": (20, 50),
                      "sma_50_200": (50, 200)}[sname]
        if n_bars < slow:
            return ""
        f_v = float(_sma(close, fast).iloc[-1])
        s_v = float(_sma(close, slow).iloc[-1])
        return f"SMA{fast} ${f_v:.2f} · SMA{slow} ${s_v:.2f} · gap {_pct(f_v, s_v):+.1f}%"

    if sname == "ema_12_26":
        f_v = float(_ema(close, 12).iloc[-1])
        s_v = float(_ema(close, 26).iloc[-1])
        return f"EMA12 ${f_v:.2f} · EMA26 ${s_v:.2f} · gap {_pct(f_v, s_v):+.1f}%"

    if sname == "macd":
        line, sig = _macd(close)
        l_v = float(line.iloc[-1])
        s_v = float(sig.iloc[-1])
        return f"MACD {l_v:+.3f} · signal {s_v:+.3f} · gap {l_v - s_v:+.3f}"

    if sname == "trend_filter":
        if n_bars < 200:
            return ""
        f_v = float(_sma(close, 20).iloc[-1])
        s_v = float(_sma(close, 50).iloc[-1])
        r_v = float(_sma(close, 200).iloc[-1])
        cur = float(close.iloc[-1])
        return (f"SMA20/50/200 ${f_v:.2f}/${s_v:.2f}/${r_v:.2f} · "
                f"regime {'✓ above' if cur > r_v else '✗ below'} 200")

    if sname == "atr_trail":
        f_ser = _sma(close, 20)
        s_ser = _sma(close, 50)
        a_ser = _atr(df, 14)
        if state == "LONG":
            # Walk forward to find HWM since the active entry
            pos, hwm = 0, 0.0
            for i in range(n_bars):
                c = float(close.iloc[i])
                if pd.isna(f_ser.iloc[i]) or pd.isna(s_ser.iloc[i]):
                    continue
                long_sig = f_ser.iloc[i] > s_ser.iloc[i]
                atr_v = float(a_ser.iloc[i]) if pd.notna(a_ser.iloc[i]) else 0.0
                if pos == 0 and long_sig:
                    pos, hwm = 1, c
                elif pos == 1:
                    hwm = max(hwm, c)
                    stop = hwm - 3.0 * atr_v
                    if c < stop or not long_sig:
                        pos, hwm = 0, 0.0
            if pos == 1:
                cur_atr = float(a_ser.iloc[-1])
                stop = hwm - 3.0 * cur_atr
                cur = float(close.iloc[-1])
                return f"HWM ${hwm:.2f} · trailing stop ${stop:.2f} ({_pct(stop, cur):+.1f}%)"
        # OFF — show the SMA cross gap (what would flip the entry)
        f_v = float(f_ser.iloc[-1])
        s_v = float(s_ser.iloc[-1])
        return f"SMA20 ${f_v:.2f} · SMA50 ${s_v:.2f} · gap {_pct(f_v, s_v):+.1f}%"

    return ""


def _evaluate_one(df: pd.DataFrame, sname: str) -> dict:
    strat = STRATEGIES[sname]()
    sig = strat(df).reindex(df.index).fillna(0).clip(lower=0).astype(int)
    cur = int(sig.iloc[-1])
    flips = sig[sig != sig.shift()].index
    if len(flips):
        flip_idx = df.index.get_loc(flips[-1])
        exec_idx = min(flip_idx + 1, len(df) - 1)
        entry_px = float(df["Open"].iloc[exec_idx])
        flip_date = flips[-1]
    else:
        entry_px = float(df["Close"].iloc[0])
        flip_date = df.index[0]

    state = "LONG" if cur == 1 else "FLAT"
    unrealized = None
    if cur == 1:
        unrealized = (float(df["Close"].iloc[-1]) / entry_px - 1) * 100

    try:
        trigger = _trigger_detail(df, sname, state)
    except Exception as e:
        print(f"[watchlist] trigger detail failed for {sname}: {e}")
        trigger = ""

    return {
        "state": state,
        "last_flip_date": flip_date.strftime("%Y-%m-%d"),
        "entry_price_if_long": round(entry_px, 4) if cur == 1 else None,
        "unrealized_pct": round(unrealized, 2) if unrealized is not None else None,
        "trigger_detail": trigger,
    }


def _evaluate_ticker(ticker: str, df: pd.DataFrame, strategies: list[str]) -> dict:
    last = df.iloc[-1]
    close = float(last["Close"])
    atr_v = float(_atr(df, 14).iloc[-1])
    rsi_v = float(_rsi(df["Close"], 14).iloc[-1])

    strat_state = {}
    for s in strategies:
        if s not in STRATEGIES:
            continue
        try:
            strat_state[s] = _evaluate_one(df, s)
        except Exception as e:
            print(f"[watchlist] strategy {s} failed for {ticker}: {e}")

    return {
        "as_of": df.index[-1].strftime("%Y-%m-%d"),
        "close": round(close, 2),
        "atr14": round(atr_v, 3),
        "rsi14": round(rsi_v, 1),
        "strategies": strat_state,
        "fundamentals": _fetch_fundamentals(ticker),
    }


# ─────────────────────────────────────────────────────────────────────────────
# RENDERING
# ─────────────────────────────────────────────────────────────────────────────

_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _friendly_date(date_str: str) -> str:
    """'2026-04-14' -> 'Apr 14'. Adds year suffix if not the current year."""
    try:
        from datetime import date
        d = date.fromisoformat(date_str)
        today = date.today()
        if d.year == today.year:
            return f"{_MONTHS[d.month - 1]} {d.day}"
        return f"{_MONTHS[d.month - 1]} {d.day} '{str(d.year)[2:]}"
    except Exception:
        return date_str


def _consensus_summary(strategies: dict) -> tuple[str, str]:
    """Return (emoji, label) summarizing how many rules are ON."""
    total = len(strategies)
    on = sum(1 for s in strategies.values() if s.get("state") == "LONG")
    if total == 0:
        return "⚪", f"{on}/{total} rules ON"
    pct = on / total
    if pct == 1.0:
        return "🟢", f"{on}/{total} rules ON · all aligned"
    if pct >= 0.8:
        return "🟢", f"{on}/{total} rules ON · mostly aligned"
    if pct >= 0.5:
        return "🟡", f"{on}/{total} rules ON · mixed"
    if pct > 0:
        return "🟠", f"{on}/{total} rules ON · mostly quiet"
    return "🔴", f"{on}/{total} rules ON · all quiet"


def _fundamentals_lines(fund: dict, close: Optional[float] = None) -> list[str]:
    """Compact lines of factual fundamentals. Skips when nothing is available."""
    if not fund:
        return []
    out = []
    parts1 = []
    if fund.get("market_cap"):
        parts1.append(f"Mkt cap {_fmt_money(fund['market_cap'])}")
    if fund.get("fifty_two_low") and fund.get("fifty_two_high"):
        rng = f"52w ${fund['fifty_two_low']:.2f}–${fund['fifty_two_high']:.2f}"
        if close and fund.get("fifty_two_high"):
            pct_off_high = (close - fund["fifty_two_high"]) / fund["fifty_two_high"] * 100
            rng += f" ({pct_off_high:+.0f}% from high)"
        parts1.append(rng)
    if fund.get("beta") is not None:
        parts1.append(f"β {fund['beta']:.2f}")
    if parts1:
        out.append(f"  📊 {' · '.join(parts1)}")

    parts2 = []
    if fund.get("revenue_ttm"):
        parts2.append(f"Rev (TTM) {_fmt_money(fund['revenue_ttm'])}")
    if fund.get("cash") is not None:
        parts2.append(f"Cash {_fmt_money(fund['cash'])}")
    if fund.get("debt") is not None:
        parts2.append(f"Debt {_fmt_money(fund['debt'])}")
    if fund.get("ps_ratio") is not None:
        parts2.append(f"P/S {fund['ps_ratio']:.1f}×")
    if parts2:
        out.append(f"  💰 {' · '.join(parts2)}")

    # Analyst consensus — third-party data point (sell-side aggregate), not editorial
    parts3 = []
    if fund.get("rec_label"):
        parts3.append(fund["rec_label"])
    if fund.get("analyst_count"):
        parts3.append(f"{fund['analyst_count']} analysts")
    if fund.get("target_mean") and close:
        upside = (fund["target_mean"] - close) / close * 100
        parts3.append(f"PT ${fund['target_mean']:.2f} ({upside:+.0f}%)")
    elif fund.get("target_mean"):
        parts3.append(f"PT ${fund['target_mean']:.2f}")
    if parts3:
        out.append(f"  🧠 Analyst: {' · '.join(parts3)}")

    return out


def _thesis_lines(thesis: dict) -> list[str]:
    """Bull/bear lines if present and non-empty."""
    if not thesis:
        return []
    out = []
    bull = (thesis.get("bull") or "").strip()
    bear = (thesis.get("bear") or "").strip()
    if bull:
        out.append(f"  🐂 <b>Bull:</b> {html.escape(bull)}")
    if bear:
        out.append(f"  🐻 <b>Bear:</b> {html.escape(bear)}")
    return out


def _render(report: dict, flips: list[str], thesis_cfg: dict) -> str:
    lines = ["<b>📡 Strategy Watchlist</b>"]

    if flips:
        lines.append("")
        lines.append("<b>⚡ Changed since last run</b>")
        for f in flips:
            lines.append(f"  • {html.escape(f)}")

    for ticker, data in report["tickers"].items():
        if "error" in data:
            lines.append(f"\n<b>{ticker}</b> — error: {html.escape(str(data['error']))}")
            continue

        emoji, summary = _consensus_summary(data["strategies"])
        lines.append(
            f"\n<b>{ticker}</b> ${data['close']:.2f} · "
            f"RSI {data['rsi14']:.0f} · ATR(14) {data['atr14']:.2f}"
        )
        lines.append(f"  {emoji} {summary}")

        # Fundamentals snapshot + analyst consensus
        for fline in _fundamentals_lines(data.get("fundamentals") or {}, close=data.get("close")):
            lines.append(fline)

        # User-authored bull/bear
        for tline in _thesis_lines(thesis_cfg.get(ticker) or {}):
            lines.append(tline)

        # Strategy state, with trigger detail per rule (where mechanically computable)
        for sname, s in data["strategies"].items():
            display = STRATEGY_DISPLAY.get(sname, sname)
            flip_date = _friendly_date(s["last_flip_date"])
            trig = s.get("trigger_detail") or ""
            trig_suffix = f" · {trig}" if trig else ""
            if s["state"] == "LONG":
                entry = s["entry_price_if_long"]
                ur = s["unrealized_pct"]
                ur_sign = "+" if (ur is not None and ur >= 0) else ""
                lines.append(
                    f"    ✅ <b>{display}</b> — ON since {flip_date} "
                    f"(entry ${entry:.2f}, {ur_sign}{ur:.1f}%){trig_suffix}"
                )
            else:
                lines.append(
                    f"    ⏸ {display} — OFF since {flip_date}{trig_suffix}"
                )

    lines.append("")
    lines.append(
        "<i>How to read: ✅ ON = your rule signals long; entry price + "
        "unrealized return shown. ⏸ OFF = rule is quiet. Trigger detail "
        "after each rule (Donchian = literal trigger price; MA/MACD = "
        "current indicator gap; ATR-trail when ON = trailing stop level) "
        "is mechanical from your rules — not a buy call. 🧠 Analyst is "
        "sell-side aggregate. Bull/Bear is your own text in watchlist.json.</i>"
    )
    return "\n".join(lines)


def _diff_flips(prev: dict, cur_report: dict) -> list[str]:
    flips = []
    prev_tickers = prev.get("tickers", {})
    for ticker, data in cur_report["tickers"].items():
        if "error" in data:
            continue
        prev_t = prev_tickers.get(ticker, {})
        prev_strats = prev_t.get("strategies", {})
        for sname, s in data["strategies"].items():
            old = prev_strats.get(sname)
            if old and old.get("state") != s["state"]:
                flips.append(
                    f"{ticker} {sname}: {old['state']} → {s['state']} "
                    f"(close ${data['close']:.2f}, {data['as_of']})"
                )
    return flips


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────────────────

def build_watchlist_section() -> Optional[str]:
    """
    Top-level entry called by portfolio_report.py. Reads config, evaluates,
    renders an HTML section. Returns None if config missing or no tickers
    succeed (so the caller can omit cleanly). Persists state for diffing
    across runs.
    """
    if not os.path.exists(CONFIG_FILE):
        return None
    try:
        with open(CONFIG_FILE) as f:
            cfg = json.load(f)
    except Exception as e:
        print(f"[watchlist] config load failed: {e}")
        return None

    tickers = cfg.get("tickers") or []
    strategies = cfg.get("strategies") or []
    period = cfg.get("data_period", "400d")
    if not tickers or not strategies:
        return None

    report: dict = {"tickers": {}}
    for t in tickers:
        df = _fetch_history(t, period)
        if df is None or len(df) < 50:
            report["tickers"][t] = {"error": "insufficient data"}
            continue
        try:
            report["tickers"][t] = _evaluate_ticker(t, df, strategies)
        except Exception as e:
            report["tickers"][t] = {"error": str(e)}

    if not any("strategies" in v for v in report["tickers"].values()):
        return None

    prev = _load_state()
    flips = _diff_flips(prev, report)

    thesis_cfg = cfg.get("thesis") or {}
    section = _render(report, flips, thesis_cfg)

    try:
        _save_state(report)
    except Exception as e:
        print(f"[watchlist] state save failed: {e}")

    return section


if __name__ == "__main__":
    out = build_watchlist_section()
    if out:
        print(out)
    else:
        print("[watchlist] no section produced")
