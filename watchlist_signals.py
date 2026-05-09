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

    return {
        "state": state,
        "last_flip_date": flip_date.strftime("%Y-%m-%d"),
        "entry_price_if_long": round(entry_px, 4) if cur == 1 else None,
        "unrealized_pct": round(unrealized, 2) if unrealized is not None else None,
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
    }


# ─────────────────────────────────────────────────────────────────────────────
# RENDERING
# ─────────────────────────────────────────────────────────────────────────────

def _render(report: dict, flips: list[str]) -> str:
    lines = ["<b>📡 Strategy Watchlist</b>"]

    for ticker, data in report["tickers"].items():
        if "error" in data:
            lines.append(f"\n<b>{ticker}</b> — error: {html.escape(str(data['error']))}")
            continue
        lines.append(
            f"\n<b>{ticker}</b> ${data['close']:.2f} · "
            f"ATR(14) {data['atr14']} · RSI {data['rsi14']:.1f} · {data['as_of']}"
        )
        for sname, s in data["strategies"].items():
            if s["state"] == "LONG":
                entry = s["entry_price_if_long"]
                ur = s["unrealized_pct"]
                ur_sign = "+" if (ur is not None and ur >= 0) else ""
                lines.append(
                    f"  ⚙️ {sname}: <b>LONG</b> since {s['last_flip_date']} "
                    f"(entry ${entry:.2f}, {ur_sign}{ur:.1f}%)"
                )
            else:
                lines.append(
                    f"  ⚙️ {sname}: FLAT since {s['last_flip_date']}"
                )

    if flips:
        lines.append("")
        lines.append("<b>⚡ Recent flips since last run</b>")
        for f in flips:
            lines.append(f"  • {html.escape(f)}")

    lines.append("")
    lines.append(
        "<i>Watchlist signals reflect rules in watchlist_signals.py — "
        "not buy/sell calls. Edit watchlist.json to change tickers/strategies.</i>"
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

    section = _render(report, flips)

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
