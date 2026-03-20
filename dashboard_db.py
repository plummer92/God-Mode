#!/usr/bin/env python3
"""
dashboard_gui.py — God Mode Trading Dashboard
Run: streamlit run dashboard_gui.py --server.port 8501
"""
import streamlit as st
import sqlite3
import json
import pandas as pd
from datetime import datetime
import pytz

DB_PATH      = "/home/theplummer92/wolfe_signals.db"
REGIME_PATH  = "/home/theplummer92/regime_snapshot.json"
SNIPER_LOG   = "/home/theplummer92/sniper.log"
APPROVED     = "/home/theplummer92/approved_symbols.json"
CST          = pytz.timezone("America/Chicago")

st.set_page_config(page_title="GOD MODE", page_icon="🦅", layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Orbitron:wght@400;700;900&display=swap');
* { box-sizing: border-box; }
html, body, [data-testid="stAppViewContainer"] { background: #020408 !important; color: #e0ffe0 !important; font-family: 'Share Tech Mono', monospace !important; }
[data-testid="stAppViewContainer"] { background: repeating-linear-gradient(0deg, transparent, transparent 39px, #0a1a0a 39px, #0a1a0a 40px), repeating-linear-gradient(90deg, transparent, transparent 39px, #0a1a0a 39px, #0a1a0a 40px), #020408 !important; }
h1, h2, h3 { font-family: 'Orbitron', monospace !important; }
.block-container { padding: 1.5rem 2rem !important; max-width: 100% !important; }
[data-testid="metric-container"] { background: #000d00 !important; border: 1px solid #1a4a1a !important; border-radius: 4px !important; padding: 1rem !important; box-shadow: 0 0 12px #00ff0022 inset !important; }
[data-testid="stMetricLabel"] { color: #4aff4a !important; font-size: 0.65rem !important; letter-spacing: 0.15em !important; text-transform: uppercase !important; }
[data-testid="stMetricValue"] { color: #00ff41 !important; font-family: 'Orbitron', monospace !important; font-size: 1.4rem !important; }
.section-header { font-family: 'Orbitron', monospace; font-size: 0.7rem; letter-spacing: 0.3em; color: #4aff4a; text-transform: uppercase; border-bottom: 1px solid #1a4a1a; padding-bottom: 0.4rem; margin-bottom: 1rem; margin-top: 1.5rem; }
.regime-open { color: #00ff41; font-weight: bold; font-size: 1.1rem; text-shadow: 0 0 10px #00ff41; }
.regime-sellonly { color: #ffaa00; font-weight: bold; font-size: 1.1rem; text-shadow: 0 0 10px #ffaa00; }
.regime-blocked { color: #ff3333; font-weight: bold; font-size: 1.1rem; text-shadow: 0 0 10px #ff3333; }
.log-box { background: #000d00; border: 1px solid #1a4a1a; border-radius: 4px; padding: 1rem; font-size: 0.72rem; line-height: 1.6; max-height: 320px; overflow-y: auto; color: #7aff7a; }
.god-title { font-family: 'Orbitron', monospace; font-size: 2.2rem; font-weight: 900; color: #00ff41; text-shadow: 0 0 30px #00ff4166; letter-spacing: 0.2em; margin: 0; }
.god-sub { font-size: 0.65rem; color: #2a7a2a; letter-spacing: 0.4em; text-transform: uppercase; margin-top: 0.2rem; }
hr { border-color: #1a4a1a !important; }
.refresh-note { font-size: 0.6rem; color: #2a5a2a; text-align: right; margin-top: -0.5rem; }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=15)
def load_regime():
    try:
        with open(REGIME_PATH) as f:
            return json.load(f)
    except:
        return {"regime": "UNKNOWN", "vix": 0, "tnx": 0, "dxy": 0, "timestamp": ""}

@st.cache_data(ttl=15)
def load_sniper_status():
    try:
        con = sqlite3.connect(DB_PATH)
        df  = pd.read_sql("SELECT * FROM sniper_status ORDER BY ts_utc DESC LIMIT 1", con)
        con.close()
        return df.iloc[0].to_dict() if len(df) else {}
    except:
        return {}

@st.cache_data(ttl=15)
def load_recent_signals(n=30):
    try:
        con = sqlite3.connect(DB_PATH)
        df  = pd.read_sql(f"SELECT timestamp, symbol, signal_type, price, rvol, change_pct FROM signals ORDER BY timestamp DESC LIMIT {n}", con)
        con.close()
        return df
    except:
        return pd.DataFrame()

@st.cache_data(ttl=30)
def load_signal_counts():
    try:
        con = sqlite3.connect(DB_PATH)
        df  = pd.read_sql("SELECT signal_type, COUNT(*) as count FROM signals WHERE timestamp > datetime('now', '-24 hours') GROUP BY signal_type ORDER BY count DESC", con)
        con.close()
        return df
    except:
        return pd.DataFrame()

@st.cache_data(ttl=15)
def load_sniper_log(lines=35):
    try:
        with open(SNIPER_LOG) as f:
            all_lines = f.readlines()
        return all_lines[-lines:]
    except:
        return []

@st.cache_data(ttl=60)
def load_approved():
    try:
        with open(APPROVED) as f:
            return json.load(f)
    except:
        return {}

@st.cache_data(ttl=30)
def load_top_movers():
    try:
        con = sqlite3.connect(DB_PATH)
        df  = pd.read_sql("SELECT symbol, signal_type, price, rvol, change_pct, timestamp FROM signals WHERE timestamp > datetime('now', '-4 hours') AND (signal_type LIKE '%STRONG%' OR signal_type LIKE '%ABSORPTION%') ORDER BY ABS(change_pct) DESC LIMIT 10", con)
        con.close()
        return df
    except:
        return pd.DataFrame()

def regime_mode(regime, vix):
    if vix >= 30:
        return "BLOCKED", "regime-blocked", "🚫"
    if regime == "RISK_OFF_VOLATILITY" and vix >= 25:
        return "SELL-ONLY", "regime-sellonly", "📉"
    return "OPEN", "regime-open", "🟢"

# Title
col_title, col_time = st.columns([3, 1])
with col_title:
    st.markdown('<p class="god-title">🦅 GOD MODE</p>', unsafe_allow_html=True)
    st.markdown('<p class="god-sub">Autonomous Signal Intelligence — v8.0</p>', unsafe_allow_html=True)
with col_time:
    now_cst = datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S CST")
    st.markdown(f'<p class="refresh-note">🕐 {now_cst}<br>Auto-refresh every 15s</p>', unsafe_allow_html=True)

st.markdown("---")

# IBKR Gateway Status
st.markdown('<p class="section-header">◈ IBKR Gateway</p>', unsafe_allow_html=True)

def check_ibkr():
    try:
        import requests, urllib3
        urllib3.disable_warnings()
        r = requests.get("https://71.239.227.167:5000/v1/api/iserver/auth/status",
                        verify=False, timeout=5)
        data = r.json()
        return data.get("authenticated", False)
    except:
        return False

ibkr_ok = check_ibkr()
if ibkr_ok:
    st.markdown('''<div style="display:flex;align-items:center;gap:12px;padding:1rem;
    background:#000d00;border:1px solid #1a4a1a;border-radius:4px;">
    <div style="width:16px;height:16px;border-radius:50%;background:#00ff41;
    box-shadow:0 0 12px #00ff41;animation:pulse 2s infinite;"></div>
    <div style="font-family:Orbitron;color:#00ff41;font-size:0.9rem;">GATEWAY ONLINE</div>
    <div style="color:#2a6a2a;font-size:0.7rem;margin-left:auto;">IBKR Connected ✓</div>
    </div>
    <style>@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.5}}</style>''',
    unsafe_allow_html=True)
else:
    st.markdown('''<div style="display:flex;align-items:center;gap:12px;padding:1rem;
    background:#1a0000;border:1px solid #4a1a1a;border-radius:4px;">
    <div style="width:16px;height:16px;border-radius:50%;background:#ff3333;
    box-shadow:0 0 12px #ff3333;animation:pulse 2s infinite;"></div>
    <div style="font-family:Orbitron;color:#ff3333;font-size:0.9rem;">GATEWAY OFFLINE</div>
    <div style="color:#6a2a2a;font-size:0.7rem;margin-left:auto;">Login at localhost:5000</div>
    </div>
    <style>@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.5}}</style>''',
    unsafe_allow_html=True)



# Regime
regime_data = load_regime()
r   = regime_data.get("regime", "UNKNOWN")
vix = float(regime_data.get("vix", 0))
tnx = float(regime_data.get("tnx", 0))
dxy = float(regime_data.get("dxy", 0))
mode, css, icon = regime_mode(r, vix)

st.markdown('<p class="section-header">◈ Market Regime</p>', unsafe_allow_html=True)
c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    st.markdown(f'<div style="text-align:center;padding:1rem;background:#000d00;border:1px solid #1a4a1a;border-radius:4px;"><div style="font-size:0.6rem;color:#4aff4a;letter-spacing:0.2em;">TRADING MODE</div><div class="{css}" style="margin-top:0.5rem;">{icon} {mode}</div></div>', unsafe_allow_html=True)
with c2:
    st.metric("VIX", f"{vix:.1f}")
with c3:
    st.metric("10Y YIELD", f"{tnx:.3f}%")
with c4:
    st.metric("DXY", f"{dxy:.2f}")
with c5:
    ts = regime_data.get("timestamp", "")
    st.metric("LAST SCAN", ts[11:19] if len(ts) > 11 else "—")

# Sniper status
st.markdown('<p class="section-header">◈ Sniper Status</p>', unsafe_allow_html=True)
status   = load_sniper_status()
approved = load_approved()
c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    s = status.get("status", "—")
    color = "#00ff41" if s == "OK" else "#ff3333"
    st.markdown(f'<div style="text-align:center;padding:1rem;background:#000d00;border:1px solid #1a4a1a;border-radius:4px;"><div style="font-size:0.6rem;color:#4aff4a;letter-spacing:0.2em;">BOT STATUS</div><div style="color:{color};font-family:Orbitron;font-size:1.2rem;margin-top:0.5rem;">● {s}</div></div>', unsafe_allow_html=True)
with c2:
    st.metric("VERSION", "V8.0")
with c3:
    # Get real positions from Alpaca
    try:
        from alpaca.trading.client import TradingClient
        import os
        _client = TradingClient(os.getenv("APCA_API_KEY_ID"), os.getenv("APCA_API_SECRET_KEY"), paper=False)
        _positions = _client.get_all_positions()
        _acct = _client.get_account()
        _equity = float(_acct.equity)
        _n_pos = len(_positions)
    except:
        _n_pos = int(status.get("in_position", 0))
        _equity = 0.0
    st.metric("OPEN POSITIONS", _n_pos)
with c4:
    st.metric("LONG SYMBOLS", len(approved.get("buy", [])))
with c5:
    st.metric("SHORT SYMBOLS", len(approved.get("sell", [])))

# Signals + log
st.markdown('<p class="section-header">◈ Recent Signals</p>', unsafe_allow_html=True)
col_sig, col_log = st.columns([3, 2])

with col_sig:
    signals_df = load_recent_signals(30)
    if not signals_df.empty:
        display = signals_df.copy()
        display["timestamp"]  = display["timestamp"].str[11:19]
        display["price"]      = display["price"].apply(lambda x: f"${x:.2f}")
        display["rvol"]       = display["rvol"].apply(lambda x: f"{x:.2f}x")
        display["change_pct"] = display["change_pct"].apply(lambda x: f"{x*100:+.2f}%")
        display.columns       = ["TIME","SYMBOL","SIGNAL","PRICE","RVOL","CHG%"]
        st.dataframe(display, use_container_width=True, height=320, hide_index=True)
    else:
        st.markdown('<div class="log-box">No signals yet.</div>', unsafe_allow_html=True)

with col_log:
    st.markdown('<p style="font-size:0.65rem;color:#4aff4a;letter-spacing:0.2em;text-transform:uppercase;">Sniper Log</p>', unsafe_allow_html=True)
    log_lines = load_sniper_log(35)
    colored = []
    for line in log_lines:
        line = line.strip()
        if "🚀" in line or "✅" in line:
            colored.append(f'<span style="color:#00ff41">{line}</span>')
        elif "🛑" in line or "❌" in line or "💀" in line:
            colored.append(f'<span style="color:#ff4444">{line}</span>')
        elif "💰" in line:
            colored.append(f'<span style="color:#ffdd00">{line}</span>')
        elif "📉" in line:
            colored.append(f'<span style="color:#ff9900">{line}</span>')
        elif "🫀" in line:
            colored.append(f'<span style="color:#2a6a2a">{line}</span>')
        else:
            colored.append(f'<span style="color:#4a8a4a">{line}</span>')
    st.markdown(f'<div class="log-box">{"<br>".join(colored)}</div>', unsafe_allow_html=True)

# Hot signals + counts
st.markdown('<p class="section-header">◈ Hot Signals (Last 4h)</p>', unsafe_allow_html=True)
col_hot, col_counts = st.columns([3, 2])

with col_hot:
    movers = load_top_movers()
    if not movers.empty:
        display = movers.copy()
        display["timestamp"]  = display["timestamp"].str[11:19]
        display["price"]      = display["price"].apply(lambda x: f"${x:.2f}")
        display["rvol"]       = display["rvol"].apply(lambda x: f"{x:.2f}x")
        display["change_pct"] = display["change_pct"].apply(lambda x: f"{x*100:+.2f}%")
        display.columns       = ["SYMBOL","SIGNAL","PRICE","RVOL","CHG%","TIME"]
        st.dataframe(display, use_container_width=True, height=240, hide_index=True)
    else:
        st.markdown('<div style="color:#2a5a2a;padding:1rem;">No strong signals in last 4h</div>', unsafe_allow_html=True)

with col_counts:
    counts = load_signal_counts()
    if not counts.empty:
        st.markdown('<p style="font-size:0.65rem;color:#4aff4a;letter-spacing:0.2em;text-transform:uppercase;margin-bottom:0.5rem;">Signal Types — 24h</p>', unsafe_allow_html=True)
        for _, row in counts.iterrows():
            sig   = str(row["signal_type"])
            cnt   = int(row["count"])
            color = "#00ff41" if "BUY" in sig else "#ff6666" if "SELL" in sig or "ABSORPTION" in sig else "#888"
            bar_w = min(100, int(cnt * 3))
            st.markdown(f'<div style="margin-bottom:0.5rem;"><div style="font-size:0.65rem;color:{color};margin-bottom:2px;">{sig[:35]}</div><div style="display:flex;align-items:center;gap:8px;"><div style="background:{color}33;border:1px solid {color}55;height:6px;width:{bar_w}px;border-radius:2px;"></div><span style="font-size:0.65rem;color:{color};">{cnt}</span></div></div>', unsafe_allow_html=True)

# Approved symbols
st.markdown('<p class="section-header">◈ Approved Symbols</p>', unsafe_allow_html=True)
col_buy, col_sell = st.columns(2)
with col_buy:
    st.markdown('<p style="font-size:0.65rem;color:#00ff41;letter-spacing:0.2em;margin-bottom:0.5rem;">▲ LONG LIST</p>', unsafe_allow_html=True)
    pills = " ".join([f'<span style="background:#00ff4111;border:1px solid #00ff4133;color:#00ff41;padding:3px 10px;border-radius:3px;margin:2px;display:inline-block;font-size:0.75rem;">{s}</span>' for s in approved.get("buy",[])])
    st.markdown(pills, unsafe_allow_html=True)
with col_sell:
    st.markdown('<p style="font-size:0.65rem;color:#ff6666;letter-spacing:0.2em;margin-bottom:0.5rem;">▼ SHORT LIST</p>', unsafe_allow_html=True)
    pills = " ".join([f'<span style="background:#ff444411;border:1px solid #ff444433;color:#ff6666;padding:3px 10px;border-radius:3px;margin:2px;display:inline-block;font-size:0.75rem;">{s}</span>' for s in approved.get("sell",[])])
    st.markdown(pills, unsafe_allow_html=True)

# Footer
st.markdown("---")
st.markdown(f'<div style="text-align:center;font-size:0.6rem;color:#1a4a1a;letter-spacing:0.2em;">GOD MODE TRADING SYSTEM · SNIPER V8.0 · REGIME: {r} · VIX: {vix:.1f} · {now_cst}</div>', unsafe_allow_html=True)
st.markdown("<script>setTimeout(function(){window.location.reload();},15000);</script>", unsafe_allow_html=True)
