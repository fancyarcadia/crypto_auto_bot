
import time
import json
import os
import tempfile
import logging
from collections import deque
from datetime import datetime, timezone
import requests
import pandas as pd
from web3 import Web3
import ta
from threading import Thread, Lock
from flask import Flask, jsonify, request
from cryptography.fernet import Fernet, InvalidToken
import base64
import hashlib
import sys
from waitress import serve
from dotenv import load_dotenv

load_dotenv()
import threading
import traceback

# ---------------- CONFIG ----------------
RPC = os.getenv("RPC_URL", "https://bsc-dataseed.binance.org/")
RPC_URLS = os.getenv("RPC_URLS", "").strip()
RPC_TIMEOUT = int(os.getenv("RPC_TIMEOUT", "8"))
DASH_TOKEN = os.getenv("DASH_TOKEN", "")
DASH_HOST = os.getenv("DASH_HOST", "0.0.0.0")
DASH_PORT = int(os.getenv("PORT", os.getenv("DASH_PORT", "8000")))
AUTO_LOOP = os.getenv("AUTO_LOOP", "1") == "1"
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")
WALLET_ADDRESS = os.getenv("WALLET_ADDRESS", "")
WALLET_SECRET = os.getenv("WALLET_SECRET", "")
WALLET_KDF_ITERATIONS = int(os.getenv("WALLET_KDF_ITERATIONS", "200000"))
ROUTER_ADDRESS = os.getenv("ROUTER_ADDRESS", "0x10ED43C718714eb63d5aA57B78B54704E256024E")
WBNB_ADDRESS = os.getenv("WBNB_ADDRESS", "0xBB4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c")
USDT_ADDRESS = os.getenv("USDT_ADDRESS", "0x55d398326f99059fF775485246999027B3197955")
USDC_ADDRESS = os.getenv("USDC_ADDRESS", "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d")
BUSD_ADDRESS = os.getenv("BUSD_ADDRESS", "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56")
FOLLOW_IGNORE_TOKENS = os.getenv("FOLLOW_IGNORE_TOKENS", "").strip()
SLIPPAGE_BPS = int(os.getenv("SLIPPAGE_BPS", "100"))
GAS_LIMIT = int(os.getenv("GAS_LIMIT", "350000"))
GAS_PRICE_GWEI = os.getenv("GAS_PRICE_GWEI", "")
CHAIN_ID = int(os.getenv("CHAIN_ID", "56"))
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")
ETHERSCAN_API_KEYS = os.getenv("ETHERSCAN_API_KEYS", "")
ETHERSCAN_API_URL = os.getenv("ETHERSCAN_API_URL", "https://api.etherscan.io/v2/api")
FOLLOW_POLL_SEC = int(os.getenv("FOLLOW_POLL_SEC", "30"))
FOLLOW_DEDUPE_MAX = int(os.getenv("FOLLOW_DEDUPE_MAX", "500"))
FOLLOW_MAX_AGE_SEC = int(os.getenv("FOLLOW_MAX_AGE_SEC", "60"))

RSI_PERIOD = 14
RSI_ENTRY = 20
SL_BUFFER = 0.995
MIN_CANDLES = int(os.getenv("MIN_CANDLES", "20"))
FETCH_RETRY_SEC = int(os.getenv("FETCH_RETRY_SEC", "120"))
WALLET_BALANCE_REFRESH_SEC = int(os.getenv("WALLET_BALANCE_REFRESH_SEC", "30"))
BNB_PRICE_REFRESH_SEC = int(os.getenv("BNB_PRICE_REFRESH_SEC", "30"))

STATE_FILE = "state.json"

web3 = Web3(Web3.HTTPProvider(RPC, request_kwargs={"timeout": RPC_TIMEOUT}))
read_rpc_urls = [RPC]
if RPC_URLS:
    read_rpc_urls = [u.strip() for u in RPC_URLS.split(",") if u.strip()]
read_web3s = [Web3(Web3.HTTPProvider(u, request_kwargs={"timeout": RPC_TIMEOUT})) for u in read_rpc_urls]
read_rpc_map = {u: read_web3s[i] for i, u in enumerate(read_rpc_urls)}

def _normalize_addr(addr):
    try:
        return Web3.to_checksum_address(addr).lower()
    except Exception:
        return (addr or "").lower()

if FOLLOW_IGNORE_TOKENS:
    FOLLOW_IGNORE_SET = {_normalize_addr(a.strip()) for a in FOLLOW_IGNORE_TOKENS.split(",") if a.strip()}
else:
    FOLLOW_IGNORE_SET = {
        _normalize_addr(WBNB_ADDRESS),
        _normalize_addr(USDT_ADDRESS),
        _normalize_addr(USDC_ADDRESS),
        _normalize_addr(BUSD_ADDRESS),
    }
ROUTER_ADDR_LC = _normalize_addr(ROUTER_ADDRESS)
state_lock = Lock()
tx_lock = Lock()
session = requests.Session()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("doji-rsi-bot")
events = deque(maxlen=200)
state_cache = {"tokens": {}, "events": [], "followers": [], "wallet_error": "", "wallet_balance_ts": 0}
warnings_cache = []
wallet_balance_cache = None
wallet_balance_ts_cache = 0
bnb_price_cache = None
bnb_price_ts = 0
tx_cache = {}

dash = Flask(__name__)

ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "_owner", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "balance", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    },
    {
        "constant": True,
        "inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}],
        "name": "allowance",
        "outputs": [{"name": "", "type": "uint256"}],
        "type": "function",
    },
    {
        "constant": False,
        "inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}],
        "name": "approve",
        "outputs": [{"name": "", "type": "bool"}],
        "type": "function",
    },
]

ROUTER_ABI = [
    {
        "name": "getAmountsOut",
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
        "inputs": [{"name": "amountIn", "type": "uint256"}, {"name": "path", "type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "name": "swapExactETHForTokens",
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
        "inputs": [
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"},
        ],
        "stateMutability": "payable",
        "type": "function",
    },
    {
        "name": "swapExactETHForTokensSupportingFeeOnTransferTokens",
        "outputs": [],
        "inputs": [
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"},
        ],
        "stateMutability": "payable",
        "type": "function",
    },
    {
        "name": "swapExactTokensForETH",
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"},
        ],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "name": "swapExactTokensForETHSupportingFeeOnTransferTokens",
        "outputs": [],
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"},
        ],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]

router = web3.eth.contract(address=Web3.to_checksum_address(ROUTER_ADDRESS), abi=ROUTER_ABI)
wallet_cfg = {
    "address": WALLET_ADDRESS,
    "private_key": PRIVATE_KEY,
}
wallet_lock = Lock()

def trading_enabled():
    with wallet_lock:
        return bool(wallet_cfg.get("address") and wallet_cfg.get("private_key"))

def _fernet_legacy():
    if not WALLET_SECRET:
        return None
    key = hashlib.sha256(WALLET_SECRET.encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(key))

def _derive_key(secret, salt, iterations):
    return hashlib.pbkdf2_hmac("sha256", secret.encode("utf-8"), salt, iterations, dklen=32)

def _fernet_kdf(salt, iterations):
    if not WALLET_SECRET:
        return None
    key = _derive_key(WALLET_SECRET, salt, iterations)
    return Fernet(base64.urlsafe_b64encode(key))

def _encrypt_wallet_value(text, salt, iterations):
    f = _fernet_kdf(salt, iterations)
    if not f:
        return None
    return f.encrypt(text.encode("utf-8")).decode("utf-8")

def _decrypt_wallet_value(token, salt, iterations):
    f = _fernet_kdf(salt, iterations)
    if not f:
        return None
    return f.decrypt(token.encode("utf-8")).decode("utf-8")

def _decrypt_wallet_legacy(token):
    f = _fernet_legacy()
    if not f:
        return None
    return f.decrypt(token.encode("utf-8")).decode("utf-8")

def log_event(kind, token=None, detail=None):
    evt = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "kind": kind,
        "token": token,
        "detail": detail,
    }
    with state_lock:
        events.appendleft(evt)

def snapshot_state():
    acquired = state_lock.acquire(timeout=1)
    if not acquired:
        return {}, list(events)
    try:
        tokens = {}
        for t, d in state["tokens"].items():
            tokens[t] = dict(d)
        evts = list(events)
        return tokens, evts
    finally:
        state_lock.release()

def update_wallet_balance(force=False):
    global wallet_balance_cache
    global wallet_balance_ts_cache
    now_ts = int(time.time())
    with state_lock:
        last = state.get("wallet_balance_ts", 0)
    if not force and (now_ts - int(last) < WALLET_BALANCE_REFRESH_SEC):
        return
    with wallet_lock:
        addr = wallet_cfg.get("address")
    if not addr:
        with state_lock:
            state["wallet_balance"] = None
            state["wallet_balance_ts"] = now_ts
            save_state_unlocked(state)
        wallet_balance_cache = None
        wallet_balance_ts_cache = now_ts
        return
    try:
        bal = None
        for w in read_web3s:
            try:
                bal = w.from_wei(w.eth.get_balance(Web3.to_checksum_address(addr)), "ether")
                break
            except Exception:
                continue
        if bal is None:
            raise Exception("RPC_FAIL")
        with state_lock:
            state["wallet_balance"] = float(bal)
            state["wallet_balance_ts"] = now_ts
            save_state_unlocked(state)
        wallet_balance_cache = float(bal)
        wallet_balance_ts_cache = now_ts
    except Exception:
        with state_lock:
            state["wallet_balance"] = None
            state["wallet_balance_ts"] = now_ts
            save_state_unlocked(state)
        wallet_balance_cache = None
        wallet_balance_ts_cache = now_ts

def snapshot_loop():
    global state_cache, warnings_cache
    while True:
        tokens, evts = snapshot_state()
        wallet_error = ""
        wallet_balance_ts = 0
        followers = []
        acquired = state_lock.acquire(timeout=1)
        if acquired:
            try:
                wallet_error = state.get("wallet_error", "")
                wallet_balance_ts = state.get("wallet_balance_ts", 0)
                followers = list(state.get("followers", []))
            finally:
                state_lock.release()
        state_cache = {
            "tokens": tokens,
            "events": evts,
            "followers": followers,
            "wallet_error": wallet_error,
            "wallet_balance_ts": wallet_balance_ts,
        }
        try:
            warnings_cache = _warnings()
        except Exception:
            warnings_cache = []
        time.sleep(2)

def _auth_ok(req, read_only=False):
    if DASH_TOKEN:
        return req.args.get("token") == DASH_TOKEN
    if read_only and not trading_enabled():
        return True
    return False

def _warnings():
    warns = []
    if trading_enabled() and not DASH_TOKEN:
        warns.append("DASH_TOKEN is not set; dashboard is public while trading is enabled.")
    if trading_enabled() and not WALLET_SECRET:
        warns.append("WALLET_SECRET is not set; wallet encryption disabled.")
    if not _ensure_chain():
        warns.append("RPC chain id does not match CHAIN_ID.")
    return warns

@dash.get("/api/state")
def api_state():
    if not _auth_ok(request, read_only=True):
        return jsonify({"error": "unauthorized"}), 401
    tokens = state_cache.get("tokens", {})
    evts = state_cache.get("events", [])
    followers = state_cache.get("followers", [])
    wallet_error = state_cache.get("wallet_error", "")
    wallet_balance_ts = state_cache.get("wallet_balance_ts", 0)
    if not wallet_balance_ts:
        wallet_balance_ts = wallet_balance_ts_cache
    bnb = wallet_balance_cache
    with wallet_lock:
        address = wallet_cfg.get("address") or ""
    return jsonify({
        "server_time": datetime.now(timezone.utc).isoformat(),
        "config": {
            "rsi_period": RSI_PERIOD,
            "rsi_entry": RSI_ENTRY,
            "sl_buffer": SL_BUFFER,
            "trading_enabled": trading_enabled(),
            "chain_id": CHAIN_ID,
            "wallet_secret_set": bool(WALLET_SECRET),
            "dash_token_set": bool(DASH_TOKEN),
            "warnings": warnings_cache,
        },
        "wallet": {
            "address": address,
            "bnb_balance": float(bnb) if bnb is not None else None,
            "error": wallet_error,
        },
        "followers": followers,
        "follow_history": _follow_history_list(),
        "wallet_balance_ts": wallet_balance_ts,
        "tokens": tokens,
        "events": evts,
    })

@dash.get("/health")
def health():
    return jsonify({"ok": True})

@dash.get("/debug/threads")
def debug_threads():
    if not _auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    frames = sys._current_frames()
    dump = {}
    for t in threading.enumerate():
        frame = frames.get(t.ident)
        if not frame:
            dump[t.name] = "no frame"
            continue
        dump[t.name] = "".join(traceback.format_stack(frame)[-20:])
    return jsonify(dump)

@dash.post("/api/token/update")
def api_update_token():
    if not _auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    token_raw = (data.get("token") or "").strip()
    if not token_raw:
        return jsonify({"error": "token required"}), 400
    try:
        token = Web3.to_checksum_address(token_raw)
    except Exception:
        return jsonify({"error": "invalid token"}), 400
    try:
        sell_rsi = float(data.get("sell_rsi"))
        sell_use_doji = bool(data.get("sell_use_doji"))
        sell_size_pct = float(data.get("sell_size_pct"))
        buy_rsi = float(data.get("buy_rsi"))
        buy_use_doji = bool(data.get("buy_use_doji"))
        name = (data.get("name") or "").strip()
    except Exception:
        return jsonify({"error": "invalid values"}), 400
    if sell_rsi <= 0 or sell_rsi > 100:
        return jsonify({"error": "sell_rsi must be 0-100"}), 400
    if buy_rsi <= 0 or buy_rsi > 100:
        return jsonify({"error": "buy_rsi must be 0-100"}), 400
    if sell_size_pct <= 0 or sell_size_pct > 100:
        return jsonify({"error": "sell_size_pct must be 0-100"}), 400

    with state_lock:
        if token not in state["tokens"]:
            return jsonify({"error": "token not found"}), 404
        if name:
            state["tokens"][token]["name"] = name
        state["tokens"][token]["sell_rsi"] = sell_rsi
        state["tokens"][token]["sell_use_doji"] = sell_use_doji
        state["tokens"][token]["sell_size_pct"] = sell_size_pct
        state["tokens"][token]["buy_rsi"] = buy_rsi
        state["tokens"][token]["buy_use_doji"] = buy_use_doji
        state["tokens"][token]["last_update"] = datetime.now(timezone.utc).isoformat()
        save_state_unlocked(state)
    log_event("RULE_UPDATE", token, f"buy_rsi={buy_rsi} buy_doji={buy_use_doji} sell_rsi={sell_rsi} sell_doji={sell_use_doji} pct={sell_size_pct}")
    return jsonify({"ok": True})

@dash.post("/api/wallet/update")
def api_wallet_update():
    if not _auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    addr = (data.get("address") or "").strip().replace(" ", "")
    key = (data.get("private_key") or "").strip().replace(" ", "")
    if not key:
        return jsonify({"error": "private_key required"}), 400
    if not WALLET_SECRET:
        return jsonify({"error": "WALLET_SECRET not set"}), 400
    try:
        if not addr:
            addr = web3.eth.account.from_key(key).address
        addr = Web3.to_checksum_address(addr)
    except Exception:
        return jsonify({"error": "invalid address"}), 400
    with wallet_lock:
        wallet_cfg["address"] = addr
        wallet_cfg["private_key"] = key
    salt = os.urandom(16)
    iterations = WALLET_KDF_ITERATIONS
    addr_enc = _encrypt_wallet_value(addr, salt, iterations)
    key_enc = _encrypt_wallet_value(key, salt, iterations)
    with state_lock:
        state["wallet"] = {
            "kdf": "pbkdf2",
            "kdf_salt": base64.b64encode(salt).decode("utf-8"),
            "kdf_iter": iterations,
            "address_enc": addr_enc,
            "key_enc": key_enc,
        }
        state.pop("wallet_error", None)
        save_state_unlocked(state)
    log_event("WALLET_SET", addr, None)
    return jsonify({"ok": True})

@dash.post("/api/wallet/clear")
def api_wallet_clear():
    if not _auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    with wallet_lock:
        wallet_cfg["address"] = ""
        wallet_cfg["private_key"] = ""
    with state_lock:
        state["wallet"] = {}
        save_state_unlocked(state)
    log_event("WALLET_CLEAR", None, None)
    return jsonify({"ok": True})

@dash.post("/api/wallet/refresh")
def api_wallet_refresh():
    if not _auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    update_wallet_balance(force=True)
    return jsonify({"ok": True, "bnb_balance": wallet_balance_cache})

@dash.get("/api/rpc_test")
def api_rpc_test():
    if not _auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    out = {}
    with wallet_lock:
        addr = wallet_cfg.get("address")
    for url, w in read_rpc_map.items():
        try:
            chain = w.eth.chain_id
            bal = None
            if addr:
                bal = w.from_wei(w.eth.get_balance(Web3.to_checksum_address(addr)), "ether")
            out[url] = {"ok": True, "chain_id": chain, "bnb_balance": float(bal) if bal is not None else None}
        except Exception as e:
            out[url] = {"ok": False, "error": str(e)}
    return jsonify(out)

@dash.get("/")
def dashboard():
    if not _auth_ok(request):
        return "unauthorized", 401
    return """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Doji RSI Bot</title>
  <style>
    :root { color-scheme: light; }
    body { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace; margin: 0; color: #111; background: #f7f6f2; }
    h1 { font-size: 20px; margin: 0 0 8px; }
    .sub { color: #555; font-size: 12px; margin-bottom: 16px; }
    .container { max-width: 1200px; width: 100%; margin: 0 auto; padding: 16px; box-sizing: border-box; }
    .header-section { margin-bottom: 16px; }
    .footer-section { margin-top: 16px; }
    .main-grid { display: grid; grid-template-columns: 1fr; gap: 16px; }
    .card { background: #fff; border: 1px solid #e5e5e5; border-radius: 8px; padding: 12px; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; table-layout: fixed; }
    th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid #eee; word-break: break-word; }
    th { color: #666; font-weight: 600; }
    .badge { display: inline-block; padding: 2px 6px; border-radius: 6px; border: 1px solid #ddd; font-size: 11px; }
    .ok { background: #e7f7ec; border-color: #cfe9d7; }
    .warn { background: #fff4e5; border-color: #f1d5b8; }
    .err { background: #fde8e8; border-color: #f1c2c2; }
    .mono { font-variant-numeric: tabular-nums; }
    .events { max-height: 260px; overflow: auto; }
    .table-wrap { overflow-x: hidden; }
    body { overflow-x: hidden; }
    .tokens-table { display: none; }
    .tokens-list { display: block; }
    .token-card { border: 1px solid #e5e5e5; border-radius: 8px; padding: 10px; margin-bottom: 10px; background: #fff; }
    .token-row { display: flex; justify-content: space-between; gap: 8px; font-size: 12px; padding: 2px 0; }
    .token-row .label { color: #666; }
    .token-actions { display: flex; gap: 8px; margin-top: 6px; }
    .token-name { font-weight: 600; }
    .token-addr { font-size: 11px; color: #777; }
    .token-more { display: none; margin-top: 6px; border-top: 1px dashed #eee; padding-top: 6px; }
    .toggle-link { color: #333; text-decoration: underline; font-size: 11px; cursor: pointer; }
    .mobile-only { display: none; }
    .controls { display: flex; gap: 8px; flex-wrap: wrap; }
    .controls input, .controls select { padding: 6px; border: 1px solid #ddd; border-radius: 6px; }
    .btn { padding: 6px 10px; border: 1px solid #222; background: #111; color: #fff; border-radius: 6px; }
    .btn-ghost { padding: 3px 6px; border: 1px solid #ddd; border-radius: 6px; background: #f7f7f7; }
    .btn-danger { padding: 3px 6px; border: 1px solid #f0c1c1; border-radius: 6px; background: #fff0f0; }
    @media (min-width: 1100px) { .main-grid { grid-template-columns: 1fr; } }
    @media (max-width: 720px) {
      .container { padding: 10px; }
      h1 { font-size: 18px; }
      table { font-size: 11px; }
      th, td { padding: 5px 6px; }
      .desktop-table { display: none; }
      .mobile-list { display: block; }
      .mobile-only { display: block; }
    }
    @media (min-width: 721px) {
      .desktop-table { display: block; }
      .mobile-list { display: none; }
      .mobile-only { display: none; }
    }
  </style>
</head>
<body>
  <div class="container">
    <h1>Doji RSI Bot</h1>
    <div class="sub">Live dashboard • auto refresh every 5s • <span id="tradeStatus"></span></div>

      <header class="header-section">
      <div class="card">
        <div class="sub">Wallet</div>
        <div style="margin-bottom:6px;">
          <button id="walletToggle" class="btn-ghost">Edit wallet</button>
        </div>
        <div id="walletForm" class="controls" style="margin-bottom:6px;">
          <input id="walletAddr" placeholder="Wallet address (optional)" style="flex:2;">
          <input id="walletKey" type="password" placeholder="Private key" style="flex:2;">
          <label style="display:flex; align-items:center; gap:6px; font-size:12px;">
            <input id="showKey" type="checkbox"> Show key
          </label>
          <button id="walletSave" class="btn">Save</button>
          <button id="walletClear" class="btn-ghost">Clear</button>
        </div>
        <div id="walletMsg" class="sub" style="margin-bottom:8px;"></div>
        <div id="warningBox" class="sub" style="margin-bottom:8px;"></div>
        <div class="token-row"><span class="label">Address</span><span id="walletAddrView" class="mono">-</span></div>
        <div class="token-row"><span class="label">BNB Balance</span><span id="walletBnb" class="mono">-</span></div>
        <div class="token-row"><span class="label">Balance updated</span><span id="walletBnbTs" class="mono">-</span></div>
        <div style="margin-top:6px;">
          <button id="walletRefresh" class="btn-ghost">Refresh balance</button>
        </div>
      </div>
      </header>

      <main class="main-grid">
      <div class="card">
        <div class="sub">Tokens</div>
        <div style="margin-bottom:6px; display:flex; gap:8px; flex-wrap:wrap;">
          <button id="tokenFormToggle" class="btn-ghost">Add token</button>
          <button id="rulesToggle" class="btn-ghost">Edit rules</button>
        </div>
        <div id="tokenForm" class="controls" style="margin-bottom:10px;">
          <input id="nameInput" placeholder="Token name" style="flex:1;">
          <input id="tokenInput" placeholder="Token address" style="flex:2;">
          <input id="buyInput" placeholder="Buy BNB" style="flex:1;">
          <input id="buyRsi" placeholder="Buy RSI" style="flex:1;">
          <label style="display:flex; align-items:center; gap:6px; font-size:12px;">
            <input id="buyDoji" type="checkbox"> Buy Doji
          </label>
          <button id="addBtn" class="btn">Add</button>
          <button id="runOnceBtn" class="btn-ghost">Run once</button>
        </div>
        <div id="rulesForm" class="controls" style="margin-bottom:12px;">
          <select id="ruleToken" style="flex:2;"></select>
          <input id="nameEdit" placeholder="Token name" style="flex:1;">
          <input id="buyRsiEdit" placeholder="Buy RSI" style="flex:1;">
          <label style="display:flex; align-items:center; gap:6px; font-size:12px;">
            <input id="buyDojiEdit" type="checkbox"> Buy Doji
          </label>
          <input id="sellRsi" placeholder="Sell RSI" style="flex:1;">
          <input id="sellPct" placeholder="Sell % (0-100)" style="flex:1;">
          <label style="display:flex; align-items:center; gap:6px; font-size:12px;">
            <input id="sellDoji" type="checkbox"> Doji
          </label>
          <button id="ruleSave" class="btn">Save</button>
        </div>
        <div class="table-wrap tokens-table">
        <table>
        <thead>
          <tr>
            <th>Name</th>
            <th>Token</th>
            <th>Status</th>
            <th>Active</th>
            <th>Holding</th>
            <th>Buy Size</th>
            <th>Hold Size</th>
            <th>Last Price</th>
            <th>RSI</th>
            <th>Doji</th>
            <th>Buy Condition</th>
            <th>Entry</th>
            <th>SL</th>
            <th>Buy RSI</th>
            <th>Buy Doji</th>
            <th>Sell RSI</th>
            <th>Sell Doji</th>
            <th>Sell %</th>
            <th>Next Fetch</th>
            <th>Updated</th>
            <th>Auto</th>
            <th>Last Action</th>
            <th>Last Token</th>
            <th>Last USD</th>
            <th>Last Time</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody id="tokens"></tbody>
      </table>
      </div>
      <div id="tokensList" class="tokens-list"></div>
      </div>

      <div class="card">
        <div class="sub">Follow Wallets</div>
        <div style="margin-bottom:6px; display:flex; gap:8px; flex-wrap:wrap;">
          <button id="followToggle" class="btn-ghost">Add / Edit Follow</button>
        </div>
        <div id="followForm" class="controls" style="margin-bottom:10px;">
          <input id="followName" placeholder="Wallet name" style="flex:1;">
          <input id="followAddress" placeholder="Wallet address" style="flex:2;">
          <input id="followMinBuy" placeholder="Buy > USDT" style="flex:1;">
          <input id="followMinSell" placeholder="Sell > USDT" style="flex:1;">
          <input id="followMyBuy" placeholder="My buy BNB" style="flex:1;">
          <input id="followMySell" placeholder="My sell %" style="flex:1;">
          <button id="followSave" class="btn">Save</button>
          <button id="followNew" class="btn-ghost">New</button>
        </div>
        <div class="table-wrap desktop-table">
          <table>
            <thead>
              <tr>
                <th>Name</th>
                <th>Address</th>
            <th>Active</th>
            <th>Buy > USDT</th>
            <th>Sell > USDT</th>
            <th>My Buy BNB</th>
            <th>My Sell %</th>
            <th>Actions</th>
              </tr>
            </thead>
            <tbody id="followers"></tbody>
          </table>
        </div>
        <div id="followersMobile" class="mobile-list"></div>
      </div>

      <div class="card">
        <div class="sub">Follow Activity (last 10)</div>
        <div id="followHistory" class="events"></div>
      </div>
      </main>

      <footer class="footer-section">
        <div class="card">
          <div class="sub">Recent events</div>
          <div class="mobile-only" style="margin-bottom:6px;">
            <span id="eventsToggle" class="toggle-link">Show events</span>
          </div>
          <div id="events" class="events"></div>
        </div>
      </footer>
  </div>

  <script>
    const fmt = (v) => (v === null || v === undefined) ? "-" : v;
    const fmtNum = (v) => (v === null || v === undefined) ? "-" : Number(v).toFixed(6);
    const fmtTs = (v) => v ? new Date(v).toLocaleTimeString() : "-";
    const badge = (txt, cls) => `<span class="badge ${cls}">${txt}</span>`;
    function esc(v) {
      const s = String((v === null || v === undefined) ? "" : v);
      let out = "";
      for (let i = 0; i < s.length; i++) {
        const code = s.charCodeAt(i);
        if (code === 38) out += "&amp;";
        else if (code === 60) out += "&lt;";
        else if (code === 62) out += "&gt;";
        else if (code === 34) out += "&quot;";
        else if (code === 39) out += "&#39;";
        else out += s.charAt(i);
      }
      return out;
    }

    function statusBadge(s) {
      if (!s) return badge("UNKNOWN", "warn");
      if (s === "OK") return badge(s, "ok");
      if (s.includes("ERROR") || s.includes("NO_DATA")) return badge(s, "err");
      return badge(s, "warn");
    }

    function apiUrl(path) {
      const token = new URLSearchParams(location.search).get("token");
      return token ? `${path}?token=${token}` : path;
    }

    function fillRuleForm(token, d) {
      if (!token || !d) return;
      document.getElementById("nameEdit").value = (d.name === null || d.name === undefined) ? "" : d.name;
      document.getElementById("buyRsiEdit").value = (d.buy_rsi === null || d.buy_rsi === undefined) ? "" : d.buy_rsi;
      document.getElementById("buyDojiEdit").checked = !!d.buy_use_doji;
      document.getElementById("sellRsi").value = (d.sell_rsi === null || d.sell_rsi === undefined) ? "" : d.sell_rsi;
      document.getElementById("sellPct").value = (d.sell_size_pct === null || d.sell_size_pct === undefined) ? "" : d.sell_size_pct;
      document.getElementById("sellDoji").checked = !!d.sell_use_doji;
    }

    async function refresh() {
      try {
        const token = new URLSearchParams(location.search).get("token");
        const url = token ? `/api/state?token=${token}` : "/api/state";
        const res = await fetch(url);
        if (!res.ok) {
          const warn = document.getElementById("warningBox");
          if (warn) warn.textContent = `API error: ${res.status}`;
          return;
        }
        const data = await res.json();
        if (!data || !data.config) {
          const warn = document.getElementById("warningBox");
          if (warn) warn.textContent = "API error: bad response";
          return;
        }
        const tokensObj = data.tokens || {};
        const tbody = document.getElementById("tokens");
        if (tbody) tbody.innerHTML = "";
        const tokensList = document.getElementById("tokensList");
        if (tokensList) tokensList.innerHTML = "";

        const ruleSelect = document.getElementById("ruleToken");
        if (ruleSelect) ruleSelect.innerHTML = "";
        let firstToken = null;
        Object.entries(tokensObj).forEach(([token, d]) => {
          if (!firstToken) firstToken = token;
        if (ruleSelect) {
          const opt = document.createElement("option");
          opt.value = token;
          const name = d.name || token.slice(0, 6) + "..." + token.slice(-4);
          opt.textContent = name;
          ruleSelect.appendChild(opt);
        }

        const buyCond = (d.last_rsi !== undefined && d.last_doji !== undefined)
          ? (d.last_rsi < data.config.rsi_entry && d.last_doji)
          : false;
        const row = document.createElement("tr");
        const displayName = d.name ? d.name : "-";
        const showAddr = !d.name;
        row.innerHTML = `
          <td>${esc(displayName)}</td>
          <td class="mono">${showAddr ? esc(token) : ""}</td>
          <td>${statusBadge(d.status)}</td>
          <td>${d.active ? "yes" : "no"}</td>
          <td>${d.holding ? "yes" : "no"}</td>
          <td class="mono">${fmt(d.buy_bnb)} BNB</td>
          <td class="mono">${fmtNum(d.holding_size)}</td>
          <td class="mono">${fmtNum(d.last_price)}</td>
          <td class="mono">${fmtNum(d.last_rsi)}</td>
          <td>${d.last_doji ? "yes" : "no"}</td>
          <td>${buyCond ? badge("TRUE", "ok") : badge("false", "warn")}</td>
          <td class="mono">${fmtNum(d.entry)}</td>
          <td class="mono">${fmtNum(d.sl)}</td>
          <td class="mono">${fmtNum(d.buy_rsi)}</td>
          <td>${d.buy_use_doji ? "yes" : "no"}</td>
          <td class="mono">${fmtNum(d.sell_rsi)}</td>
          <td>${d.sell_use_doji ? "yes" : "no"}</td>
          <td class="mono">${fmtNum(d.sell_size_pct)}</td>
          <td>${fmtTs(d.next_fetch)}</td>
          <td>${fmtTs(d.last_update)}</td>
          <td>
            <span class="badge ${d.active ? "ok" : "warn"}">${d.active ? "ON" : "OFF"}</span>
          </td>
          <td>
            <button data-toggle="${token}" class="btn-ghost">${d.active ? "stop" : "start"}</button>
            <button data-remove="${token}" class="btn-danger">remove</button>
          </td>
        `;
        if (tbody) tbody.appendChild(row);

        const card = document.createElement("div");
        card.className = "token-card";
        const addrLine = d.name ? "" : `<div class="token-addr mono">${esc(token)}</div>`;
        card.innerHTML = `
          <div class="token-name">${esc(displayName)}</div>
          ${addrLine}
          <div class="token-row"><span class="label">Status</span><span>${statusBadge(d.status)}</span></div>
          <div class="token-row"><span class="label">Auto</span><span class="badge ${d.active ? "ok" : "warn"}">${d.active ? "ON" : "OFF"}</span></div>
          <div class="token-row"><span class="label">Holding</span><span>${d.holding ? "yes" : "no"}</span></div>
          <div class="token-row"><span class="label">Price</span><span class="mono">${fmtNum(d.last_price)}</span></div>
          <div class="token-row"><span class="label">RSI</span><span class="mono">${fmtNum(d.last_rsi)}</span></div>
          <div class="token-row"><span class="label">Doji</span><span>${d.last_doji ? "yes" : "no"}</span></div>
          <div class="token-row"><span class="label">Buy cond</span><span>${buyCond ? badge("TRUE", "ok") : badge("false", "warn")}</span></div>
          <div class="token-row"><span class="label"><span class="toggle-link" data-more="${token}">Details</span></span><span></span></div>
          <div class="token-more" data-more-box="${token}">
            <div class="token-row"><span class="label">Buy size</span><span class="mono">${fmt(d.buy_bnb)} BNB</span></div>
            <div class="token-row"><span class="label">Hold size</span><span class="mono">${fmtNum(d.holding_size)}</span></div>
            <div class="token-row"><span class="label">SL</span><span class="mono">${fmtNum(d.sl)}</span></div>
            <div class="token-row"><span class="label">Buy RSI</span><span class="mono">${fmtNum(d.buy_rsi)}</span></div>
            <div class="token-row"><span class="label">Sell RSI</span><span class="mono">${fmtNum(d.sell_rsi)}</span></div>
            <div class="token-row"><span class="label">Sell %</span><span class="mono">${fmtNum(d.sell_size_pct)}</span></div>
            <div class="token-row"><span class="label">Next fetch</span><span>${fmtTs(d.next_fetch)}</span></div>
          </div>
          <div class="token-actions">
            <button data-toggle="${token}" class="btn-ghost">${d.active ? "stop" : "start"}</button>
            <button data-remove="${token}" class="btn-danger">remove</button>
          </div>
        `;
        if (tokensList) tokensList.appendChild(card);
      });

      const selectedToken = (ruleSelect && ruleSelect.value) || firstToken;
      if (ruleSelect && selectedToken && tokensObj[selectedToken]) {
        ruleSelect.value = selectedToken;
        fillRuleForm(selectedToken, tokensObj[selectedToken]);
      }

      document.querySelectorAll("button[data-remove]").forEach(btn => {
        btn.onclick = async () => {
          await fetch(apiUrl("/api/token/remove"), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ token: btn.dataset.remove })
          });
          refresh();
        };
      });
      document.querySelectorAll("button[data-toggle]").forEach(btn => {
        btn.onclick = async () => {
          await fetch(apiUrl("/api/token/toggle"), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ token: btn.dataset.toggle })
          });
          refresh();
        };
      });
      document.querySelectorAll("[data-more]").forEach(el => {
        el.onclick = () => {
          const box = document.querySelector(`[data-more-box="${el.dataset.more}"]`);
          if (!box) return;
          box.style.display = box.style.display === "block" ? "none" : "block";
        };
      });

      const events = document.getElementById("events");
      if (events) events.innerHTML = (data.events || []).map(e => {
        const t = new Date(e.ts).toLocaleTimeString();
        const token = e.token ? ` ${esc(e.token)}` : "";
        const detail = e.detail ? ` - ${esc(e.detail)}` : "";
        return `<div class="mono">${esc(t)} ${esc(e.kind)}${token}${detail}</div>`;
      }).join("");

      const followersBody = document.getElementById("followers");
      const followersMobile = document.getElementById("followersMobile");
      if (followersBody) followersBody.innerHTML = "";
      if (followersMobile) followersMobile.innerHTML = "";
      (data.followers || []).forEach(f => {
        const row = document.createElement("tr");
        row.innerHTML = `
          <td>${esc(f.name || "-")}</td>
          <td class="mono">${esc(f.address || "")}</td>
          <td>${f.active ? "yes" : "no"}</td>
          <td>${esc(fmt(f.min_buy_usd))}</td>
          <td>${esc(fmt(f.min_sell_usd))}</td>
          <td>${esc(fmt(f.my_buy_bnb))}</td>
          <td>${esc(fmt(f.my_sell_pct))}</td>
          <td>${esc(f.last_action || "-")}</td>
          <td class="mono">${esc(f.last_token || "-")}</td>
          <td>${esc(fmt(f.last_usd))}</td>
          <td>${esc(f.last_action_ts ? new Date(f.last_action_ts).toLocaleTimeString() : "-")}</td>
          <td>
            <button data-follow-edit="${f.address}" class="btn-ghost">edit</button>
            <button data-follow-toggle="${f.address}" class="btn-ghost">${f.active ? "pause" : "resume"}</button>
            <button data-follow-remove="${f.address}" class="btn-danger">remove</button>
          </td>
        `;
        if (followersBody) followersBody.appendChild(row);

        const card = document.createElement("div");
        card.className = "token-card";
        card.innerHTML = `
          <div class="token-name">${esc(f.name || "Follow wallet")}</div>
          <div class="token-addr mono">${esc(f.address || "")}</div>
          <div class="token-row"><span class="label">Active</span><span>${f.active ? "yes" : "no"}</span></div>
          <div class="token-row"><span class="label">Buy > USDT</span><span>${esc(fmt(f.min_buy_usd))}</span></div>
          <div class="token-row"><span class="label">Sell > USDT</span><span>${esc(fmt(f.min_sell_usd))}</span></div>
          <div class="token-row"><span class="label">My buy BNB</span><span>${esc(fmt(f.my_buy_bnb))}</span></div>
          <div class="token-row"><span class="label">My sell %</span><span>${esc(fmt(f.my_sell_pct))}</span></div>
          <div class="token-row"><span class="label">Last action</span><span>${esc(f.last_action || "-")}</span></div>
          <div class="token-row"><span class="label">Last token</span><span class="mono">${esc(f.last_token || "-")}</span></div>
          <div class="token-row"><span class="label">Last USD</span><span>${esc(fmt(f.last_usd))}</span></div>
          <div class="token-row"><span class="label">Last time</span><span>${esc(f.last_action_ts ? new Date(f.last_action_ts).toLocaleTimeString() : "-")}</span></div>
          <div class="token-actions">
            <button data-follow-edit="${f.address}" class="btn-ghost">edit</button>
            <button data-follow-toggle="${f.address}" class="btn-ghost">${f.active ? "pause" : "resume"}</button>
            <button data-follow-remove="${f.address}" class="btn-danger">remove</button>
          </div>
        `;
        if (followersMobile) followersMobile.appendChild(card);
      });

      const followHistory = document.getElementById("followHistory");
      if (followHistory) followHistory.innerHTML = (data.follow_history || []).slice(-10).reverse().map(h => {
        const t = h.ts ? new Date(h.ts).toLocaleTimeString() : "";
        return `<div class="mono">${esc(t)} ${esc(h.action)} ${esc(h.token)} ${esc(h.usd ? h.usd.toFixed(2) : "")}</div>`;
      }).join("");
      document.querySelectorAll("[data-follow-remove]").forEach(btn => {
        btn.onclick = async () => {
          await fetch(apiUrl("/api/follow/remove"), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ address: btn.dataset.followRemove })
          });
          refresh();
        };
      });
      document.querySelectorAll("[data-follow-edit]").forEach(btn => {
        btn.onclick = () => {
          const addr = btn.dataset.followEdit;
          const f = (data.followers || []).find(x => x.address === addr);
          if (!f) return;
          document.getElementById("followName").value = f.name || "";
          document.getElementById("followAddress").value = f.address || "";
          document.getElementById("followMinBuy").value = f.min_buy_usd || "";
          document.getElementById("followMinSell").value = f.min_sell_usd || "";
          document.getElementById("followMyBuy").value = f.my_buy_bnb || "";
          document.getElementById("followMySell").value = f.my_sell_pct || "";
          document.getElementById("followForm").dataset.mode = "update";
          const form = document.getElementById("followForm");
          if (form) form.style.display = "flex";
          const followToggle = document.getElementById("followToggle");
          if (followToggle) followToggle.textContent = "Hide follow";
        };
      });
      document.querySelectorAll("[data-follow-toggle]").forEach(btn => {
        btn.onclick = async () => {
          await fetch(apiUrl("/api/follow/toggle"), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ address: btn.dataset.followToggle })
          });
          refresh();
        };
      });

      const eventsToggle = document.getElementById("eventsToggle");
      if (eventsToggle && !eventsToggle.dataset.bound && events) {
        eventsToggle.dataset.bound = "1";
        eventsToggle.onclick = () => {
          const show = events.style.display === "none";
          events.style.display = show ? "block" : "none";
          eventsToggle.textContent = show ? "Hide events" : "Show events";
        };
        events.style.display = "none";
      }

      const tradeStatus = document.getElementById("tradeStatus");
      tradeStatus.innerHTML = data.config.trading_enabled ? badge("TRADING ON", "ok") : badge("SIGNAL ONLY", "warn");
      const warn = document.getElementById("warningBox");
      warn.innerHTML = (data.config.warnings || []).map(w => `<div>${w}</div>`).join("");

    const walletAddrView = document.getElementById("walletAddrView");
    if (walletAddrView) walletAddrView.textContent = data.wallet.address || "-";
    const walletBnb = document.getElementById("walletBnb");
    if (walletBnb) walletBnb.textContent = data.wallet.bnb_balance === null ? "-" : Number(data.wallet.bnb_balance).toFixed(6);
    const walletBnbTs = document.getElementById("walletBnbTs");
    if (walletBnbTs) walletBnbTs.textContent = data.wallet_balance_ts ? fmtTs(data.wallet_balance_ts * 1000) : "-";
      if (data.wallet.error) {
        document.getElementById("walletMsg").textContent = `Wallet error: ${data.wallet.error}`;
      }
      const walletAddrInput = document.getElementById("walletAddr");
      if (walletAddrInput && !walletAddrInput.value && data.wallet.address) {
        walletAddrInput.value = data.wallet.address;
      }
      const walletMsg = document.getElementById("walletMsg");
      if (walletMsg && data.config.trading_enabled) {
        walletMsg.textContent = "Wallet active";
      }
      } catch (err) {
        console.error("Refresh error", err);
        const warn = document.getElementById("warningBox");
        if (warn) warn.textContent = "Dashboard error: check console";
      }
    }

    refresh();
    setInterval(refresh, 5000);

    document.getElementById("ruleToken").onchange = async () => {
      const res = await fetch(apiUrl("/api/state"));
      const data = await res.json();
      const token = document.getElementById("ruleToken").value;
      fillRuleForm(token, data.tokens[token]);
    };

    document.getElementById("addBtn").onclick = async () => {
      const name = document.getElementById("nameInput").value.trim();
      const token = document.getElementById("tokenInput").value.trim();
      const buy = document.getElementById("buyInput").value.trim();
      const buy_rsi = document.getElementById("buyRsi").value.trim() || "20";
      const buy_use_doji = document.getElementById("buyDoji").checked;
      if (!token || !buy) return;
      await fetch(apiUrl("/api/token/add"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name, token, buy_bnb: buy, buy_rsi, buy_use_doji })
      });
      document.getElementById("nameInput").value = "";
      document.getElementById("tokenInput").value = "";
      document.getElementById("buyInput").value = "";
      document.getElementById("buyRsi").value = "";
      document.getElementById("buyDoji").checked = false;
      refresh();
    };

    document.getElementById("runOnceBtn").onclick = async () => {
      await fetch(apiUrl("/api/run_once"), { method: "POST" });
      refresh();
    };

    document.getElementById("walletSave").onclick = async () => {
      const address = document.getElementById("walletAddr").value.trim();
      const private_key = document.getElementById("walletKey").value.trim().replace(/\\s+/g, "");
      if (!private_key) return;
      const res = await fetch(apiUrl("/api/wallet/update"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ address, private_key })
      });
      const msg = document.getElementById("walletMsg");
      const out = await res.json();
      msg.textContent = out.error ? `Wallet error: ${out.error}` : "Wallet saved";
      document.getElementById("walletKey").value = "";
      const walletForm = document.getElementById("walletForm");
      if (walletForm && window.innerWidth <= 720) {
        walletForm.style.display = "none";
      }
      refresh();
    };
    document.getElementById("walletClear").onclick = async () => {
      await fetch(apiUrl("/api/wallet/clear"), { method: "POST" });
      document.getElementById("walletMsg").textContent = "Wallet cleared";
      refresh();
    };

    document.getElementById("walletRefresh").onclick = async () => {
      await fetch(apiUrl("/api/wallet/refresh"), { method: "POST" });
      refresh();
    };

    document.getElementById("showKey").onchange = (e) => {
      document.getElementById("walletKey").type = e.target.checked ? "text" : "password";
    };

    const walletToggle = document.getElementById("walletToggle");
    if (walletToggle && !walletToggle.dataset.bound) {
      walletToggle.dataset.bound = "1";
      walletToggle.onclick = () => {
        const form = document.getElementById("walletForm");
        if (!form) return;
        const show = form.style.display === "none";
        form.style.display = show ? "flex" : "none";
        walletToggle.textContent = show ? "Hide wallet" : "Edit wallet";
        localStorage.setItem("walletFormOpen", show ? "1" : "0");
      };
      const open = localStorage.getItem("walletFormOpen") === "1";
      document.getElementById("walletForm").style.display = open ? "flex" : "none";
      walletToggle.textContent = open ? "Hide wallet" : "Edit wallet";
    }

    const tokenFormToggle = document.getElementById("tokenFormToggle");
    if (tokenFormToggle && !tokenFormToggle.dataset.bound) {
      tokenFormToggle.dataset.bound = "1";
      tokenFormToggle.onclick = () => {
        const form = document.getElementById("tokenForm");
        if (!form) return;
        const show = form.style.display === "none";
        form.style.display = show ? "flex" : "none";
        tokenFormToggle.textContent = show ? "Hide add" : "Add token";
        localStorage.setItem("tokenFormOpen", show ? "1" : "0");
      };
      const open = localStorage.getItem("tokenFormOpen") === "1";
      document.getElementById("tokenForm").style.display = open ? "flex" : "none";
      tokenFormToggle.textContent = open ? "Hide add" : "Add token";
    }

    const rulesToggle = document.getElementById("rulesToggle");
    if (rulesToggle && !rulesToggle.dataset.bound) {
      rulesToggle.dataset.bound = "1";
      rulesToggle.onclick = () => {
        const form = document.getElementById("rulesForm");
        if (!form) return;
        const show = form.style.display === "none";
        form.style.display = show ? "flex" : "none";
        rulesToggle.textContent = show ? "Hide rules" : "Edit rules";
        localStorage.setItem("rulesFormOpen", show ? "1" : "0");
      };
      const open = localStorage.getItem("rulesFormOpen") === "1";
      document.getElementById("rulesForm").style.display = open ? "flex" : "none";
      rulesToggle.textContent = open ? "Hide rules" : "Edit rules";
    }

    document.getElementById("ruleSave").onclick = async () => {
      const token = document.getElementById("ruleToken").value;
      const name = document.getElementById("nameEdit").value.trim();
      const buy_rsi = document.getElementById("buyRsiEdit").value.trim();
      const buy_use_doji = document.getElementById("buyDojiEdit").checked;
      const sell_rsi = document.getElementById("sellRsi").value.trim();
      const sell_size_pct = document.getElementById("sellPct").value.trim();
      const sell_use_doji = document.getElementById("sellDoji").checked;
      if (!token || !buy_rsi || !sell_rsi || !sell_size_pct) return;
      await fetch(apiUrl("/api/token/update"), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ token, name, buy_rsi, buy_use_doji, sell_rsi, sell_size_pct, sell_use_doji })
      });
      refresh();
    };

    const followToggle = document.getElementById("followToggle");
    if (followToggle && !followToggle.dataset.bound) {
      followToggle.dataset.bound = "1";
      followToggle.onclick = () => {
        const form = document.getElementById("followForm");
        if (!form) return;
        const show = form.style.display === "none";
        form.style.display = show ? "flex" : "none";
        followToggle.textContent = show ? "Hide follow" : "Add / Edit Follow";
        localStorage.setItem("followFormOpen", show ? "1" : "0");
      };
      const open = localStorage.getItem("followFormOpen") === "1";
      document.getElementById("followForm").style.display = open ? "flex" : "none";
      followToggle.textContent = open ? "Hide follow" : "Add / Edit Follow";
    }

    document.getElementById("followSave").onclick = async () => {
      const name = document.getElementById("followName").value.trim();
      const address = document.getElementById("followAddress").value.trim();
      const min_buy_usd = document.getElementById("followMinBuy").value.trim() || "0";
      const min_sell_usd = document.getElementById("followMinSell").value.trim() || "0";
      const my_buy_bnb = document.getElementById("followMyBuy").value.trim() || "0";
      const my_sell_pct = document.getElementById("followMySell").value.trim() || "0";
      if (!address) return;
      const mode = document.getElementById("followForm").dataset.mode || "add";
      const endpoint = mode === "update" ? "/api/follow/update" : "/api/follow/add";
      await fetch(apiUrl(endpoint), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name, address, min_buy_usd, min_sell_usd, my_buy_bnb, my_sell_pct })
      });
      document.getElementById("followName").value = "";
      document.getElementById("followAddress").value = "";
      document.getElementById("followMinBuy").value = "";
      document.getElementById("followMinSell").value = "";
      document.getElementById("followMyBuy").value = "";
      document.getElementById("followMySell").value = "";
      document.getElementById("followForm").dataset.mode = "add";
      refresh();
    };

    document.getElementById("followNew").onclick = () => {
      document.getElementById("followName").value = "";
      document.getElementById("followAddress").value = "";
      document.getElementById("followMinBuy").value = "";
      document.getElementById("followMinSell").value = "";
      document.getElementById("followMyBuy").value = "";
      document.getElementById("followMySell").value = "";
      document.getElementById("followForm").dataset.mode = "add";
    };
  </script>
</body>
</html>
"""

# ---------------- STATE ----------------
def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {"tokens": {}}

def save_state_unlocked(state):
    dirpath = os.path.dirname(os.path.abspath(STATE_FILE)) or "."
    tmp = None
    try:
        with tempfile.NamedTemporaryFile("w", dir=dirpath, delete=False) as f:
            tmp = f.name
            json.dump(state, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, STATE_FILE)
    finally:
        if tmp and os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass

def save_state(state):
    with state_lock:
        save_state_unlocked(state)

state = load_state()

def _load_wallet_from_state():
    try:
        w = state.get("wallet", {})
        addr_enc = w.get("address_enc")
        key_enc = w.get("key_enc")
        if not addr_enc or not key_enc:
            return
        if not WALLET_SECRET:
            return
        kdf = w.get("kdf")
        if kdf == "pbkdf2":
            salt_b64 = w.get("kdf_salt", "")
            iterations = int(w.get("kdf_iter", WALLET_KDF_ITERATIONS))
            if not salt_b64:
                return
            salt = base64.b64decode(salt_b64)
            addr = _decrypt_wallet_value(addr_enc, salt, iterations)
            key = _decrypt_wallet_value(key_enc, salt, iterations)
        else:
            addr = _decrypt_wallet_legacy(addr_enc)
            key = _decrypt_wallet_legacy(key_enc)
        if not addr or not key:
            return
        with wallet_lock:
            wallet_cfg["address"] = Web3.to_checksum_address(addr)
            wallet_cfg["private_key"] = key
        log_event("WALLET_LOADED", wallet_cfg["address"], None)
        with state_lock:
            state.pop("wallet_error", None)
            save_state_unlocked(state)
    except (InvalidToken, Exception):
        logger.warning("Failed to decrypt wallet from state.json")
        with state_lock:
            state["wallet_error"] = "WALLET_DECRYPT_FAIL"
            save_state_unlocked(state)

_load_wallet_from_state()

def _migrate_state():
    changed = False
    with state_lock:
        for t, d in state.get("tokens", {}).items():
            if "tp" in d:
                d.pop("tp", None)
                changed = True
        if changed:
            save_state_unlocked(state)

_migrate_state()

# ---------------- DATA ----------------
def _pair_score(pair):
    try:
        liq = float((pair.get("liquidity") or {}).get("usd") or 0)
        vol = float((pair.get("volume") or {}).get("h24") or 0)
        return liq, vol
    except Exception:
        return 0.0, 0.0

def _dexscreener_pairs(token):
    try:
        url=f"https://api.dexscreener.com/latest/dex/tokens/{token}"
        r = session.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        pairs = [p for p in (data.get("pairs") or []) if p.get("chainId") == "bsc"]
        pairs.sort(key=_pair_score, reverse=True)
        return pairs
    except Exception as e:
        logger.warning("pairs lookup failed for %s: %s", token, e)
        return []

def _fetch_ohlc(pair_or_token):
    url=f"https://api.dexscreener.com/latest/dex/ohlc/bsc/{pair_or_token}?interval=15m&limit=100"
    try:
        r = session.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        candles = data.get("candles", [])
        if len(candles) < MIN_CANDLES:
            return None, "INSUFFICIENT_CANDLES"
        parsed = []
        for c in candles:
            if len(c) < 5:
                continue
            parsed.append([float(c[1]), float(c[2]), float(c[3]), float(c[4])])
        if len(parsed) < MIN_CANDLES:
            return None, "INSUFFICIENT_CANDLES"
        return parsed, None
    except Exception as e:
        logger.warning("fetch_candles failed for %s: %s", pair_or_token, e)
        return None, "FETCH_ERROR"

def fetch_candles(token):
    pairs = _dexscreener_pairs(token)
    last_err = None
    if pairs:
        for p in pairs:
            pair_addr = p.get("pairAddress")
            if not pair_addr:
                continue
            parsed, err = _fetch_ohlc(pair_addr)
            if parsed is not None:
                return parsed, None
            last_err = err
        return None, last_err or "INSUFFICIENT_CANDLES"
    parsed, err = _fetch_ohlc(token)
    if parsed is not None:
        return parsed, None
    return None, err or "NO_PAIR"

def get_price_usd(token):
    pairs = _dexscreener_pairs(token)
    for p in pairs:
        price = p.get("priceUsd")
        if price is None:
            continue
        try:
            return float(price)
        except Exception:
            continue
    return None

def get_bnb_price_usd():
    global bnb_price_cache, bnb_price_ts
    now_ts = int(time.time())
    if bnb_price_cache is not None and (now_ts - bnb_price_ts) < BNB_PRICE_REFRESH_SEC:
        return bnb_price_cache
    price = get_price_usd(WBNB_ADDRESS)
    if price is None:
        return None
    bnb_price_cache = price
    bnb_price_ts = now_ts
    return price
def get_price(token):
    try:
        return get_price_usd(token)
    except Exception as e:
        logger.warning("get_price failed for %s: %s", token, e)
        return None

# ---------------- RSI ----------------
def safe_rsi(candles):
    try:
        closes=[c[3] for c in candles]
        series=pd.Series(closes)
        rsi=ta.momentum.RSIIndicator(series,window=RSI_PERIOD).rsi()
        value=float(rsi.iloc[-1])
        if value!=value:
            return None
        return value
    except Exception as e:
        logger.warning("RSI calc failed: %s", e)
        return None

# ---------------- DOJI ----------------
def is_perfect_doji(c):
    o,h,l,c2=c
    body=abs(c2-o)
    rng=h-l
    if rng==0:
        return False
    upper=h-max(o,c2)
    lower=min(o,c2)-l
    if body/rng>0.05:
        return False
    if upper<body*3:
        return False
    if lower<body*3:
        return False
    return True

# ---------------- TRAILING ----------------
def update_trailing(token,candles):
    with state_lock:
        pos=state["tokens"].get(token)
        if not pos:
            return
    last=candles[-2]
    if not is_perfect_doji(last):
        return
    new_sl=last[2]*SL_BUFFER
    if new_sl>pos["sl"]:
        pos["sl"]=new_sl
        save_state_unlocked(state)
        logger.info("Trailing SL moved: %s %.8f", token, new_sl)

def set_status(token, status):
    with state_lock:
        if token in state["tokens"]:
            state["tokens"][token]["status"] = status
            state["tokens"][token]["last_update"] = datetime.now(timezone.utc).isoformat()
            save_state_unlocked(state)

def update_metrics(token, price, rsi, last_doji):
    with state_lock:
        if token in state["tokens"]:
            state["tokens"][token]["last_price"] = price
            state["tokens"][token]["last_rsi"] = rsi
            state["tokens"][token]["last_doji"] = last_doji
            state["tokens"][token]["last_update"] = datetime.now(timezone.utc).isoformat()
            save_state_unlocked(state)

def set_next_fetch(token, ts):
    with state_lock:
        if token in state["tokens"]:
            state["tokens"][token]["next_fetch"] = ts
            save_state_unlocked(state)

def resync_holding(token):
    try:
        dec = _token_decimals(token)
        bal = _token_balance(token) / (10 ** dec)
        with state_lock:
            if token in state["tokens"]:
                state["tokens"][token]["holding_size"] = bal
                if bal <= 0:
                    state["tokens"][token]["holding"] = False
                state["tokens"][token]["last_update"] = datetime.now(timezone.utc).isoformat()
                save_state_unlocked(state)
    except Exception:
        pass

def _ensure_follow_state():
    with state_lock:
        if "followers" not in state:
            state["followers"] = []
        if "follow_seen" not in state:
            state["follow_seen"] = []
        if "follow_history" not in state:
            state["follow_history"] = []
        save_state_unlocked(state)

def _get_followers():
    _ensure_follow_state()
    with state_lock:
        return list(state.get("followers", []))

def _save_followers(followers):
    with state_lock:
        state["followers"] = followers
        save_state_unlocked(state)

def _merge_follow_updates(updates):
    if not updates:
        return
    with state_lock:
        for f in state.get("followers", []):
            addr = f.get("address")
            if addr in updates:
                f.update(updates[addr])
        save_state_unlocked(state)

def _follow_history_add(entry):
    with state_lock:
        hist = state.get("follow_history", [])
        hist.append(entry)
        if len(hist) > 200:
            hist = hist[-200:]
        state["follow_history"] = hist
        save_state_unlocked(state)

def _follow_history_list():
    with state_lock:
        return list(state.get("follow_history", []))


etherscan_keys = []
if ETHERSCAN_API_KEYS:
    etherscan_keys = [k.strip() for k in ETHERSCAN_API_KEYS.split(",") if k.strip()]
elif ETHERSCAN_API_KEY:
    etherscan_keys = [ETHERSCAN_API_KEY]

def _etherscan_tokentx(address, startblock):
    if not etherscan_keys:
        return []
    key = etherscan_keys[int(time.time()) % len(etherscan_keys)]
    params = {
        "chainid": CHAIN_ID,
        "module": "account",
        "action": "tokentx",
        "address": address,
        "startblock": startblock,
        "sort": "asc",
        "apikey": key,
    }
    try:
        r = session.get(ETHERSCAN_API_URL, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("status") != "1":
            return []
        return data.get("result", [])
    except Exception as e:
        logger.warning("Etherscan tokentx failed for %s: %s", address, e)
    return []

def _etherscan_tx_by_hash(txhash):
    if not etherscan_keys:
        return None
    if txhash in tx_cache:
        return tx_cache[txhash]
    key = etherscan_keys[int(time.time()) % len(etherscan_keys)]
    params = {
        "chainid": CHAIN_ID,
        "module": "proxy",
        "action": "eth_getTransactionByHash",
        "txhash": txhash,
        "apikey": key,
    }
    try:
        r = session.get(ETHERSCAN_API_URL, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        tx = data.get("result")
        tx_cache[txhash] = tx
        if len(tx_cache) > 200:
            tx_cache.pop(next(iter(tx_cache)))
        return tx
    except Exception as e:
        logger.warning("Etherscan tx failed for %s: %s", txhash, e)
        return None

def _follow_seen_key(txhash, token, direction):
    return f"{txhash}:{token}:{direction}"

def _follow_seen_has(key):
    with state_lock:
        return key in state.get("follow_seen", [])

def _follow_seen_add(key):
    with state_lock:
        seen = state.get("follow_seen", [])
        seen.append(key)
        if len(seen) > FOLLOW_DEDUPE_MAX:
            seen = seen[-FOLLOW_DEDUPE_MAX:]
        state["follow_seen"] = seen
        save_state_unlocked(state)

def _handle_follow_buy(token, amount_usd, my_buy_bnb):
    if amount_usd <= 0:
        return
    if not trading_enabled():
        log_event("FOLLOW_BUY_SKIP", token, "TRADING_DISABLED")
        return
    if my_buy_bnb <= 0:
        log_event("FOLLOW_BUY_SKIP", token, "BUY_BNB_ZERO")
        return
    with tx_lock:
        ok, txid = _buy_token(token, my_buy_bnb)
    if ok:
        log_event("FOLLOW_BUY", token, f"usd={amount_usd:.2f} tx={txid}")
        _follow_history_add({"ts": datetime.now(timezone.utc).isoformat(), "action": "BUY", "token": token, "usd": float(amount_usd), "tx": txid})
        resync_holding(token)
        return True, txid
    else:
        log_event("FOLLOW_BUY_FAIL", token, f"{txid}")
        _follow_history_add({"ts": datetime.now(timezone.utc).isoformat(), "action": "BUY_FAIL", "token": token, "usd": float(amount_usd), "tx": txid})
        return False, txid

def _handle_follow_sell(token, amount_usd, sell_pct):
    if amount_usd <= 0:
        return
    if not trading_enabled():
        log_event("FOLLOW_SELL_SKIP", token, "TRADING_DISABLED")
        return
    if sell_pct <= 0:
        log_event("FOLLOW_SELL_SKIP", token, "SELL_PCT_ZERO")
        return
    with tx_lock:
        dec = _token_decimals(token)
        bal_raw = _token_balance(token)
        amount_raw = int(bal_raw * (sell_pct / 100.0))
        if amount_raw <= 0:
            return
        ok, txid = _sell_token(token, amount_raw)
    if ok:
        log_event("FOLLOW_SELL", token, f"usd={amount_usd:.2f} pct={sell_pct:.2f} tx={txid}")
        _follow_history_add({"ts": datetime.now(timezone.utc).isoformat(), "action": "SELL", "token": token, "usd": float(amount_usd), "tx": txid})
        resync_holding(token)
        return True, txid
    else:
        log_event("FOLLOW_SELL_FAIL", token, f"{txid}")
        _follow_history_add({"ts": datetime.now(timezone.utc).isoformat(), "action": "SELL_FAIL", "token": token, "usd": float(amount_usd), "tx": txid})
        return False, txid

def follow_loop():
    while True:
        followers = _get_followers()
        updates = {}
        for f in followers:
            if not f.get("active", True):
                continue
            address = f.get("address")
            if not address:
                continue
            changes = {}
            startblock = int(f.get("last_block", 0))
            txs = _etherscan_tokentx(address, startblock)
            max_block = startblock
            for tx in txs:
                try:
                    txhash = tx.get("hash", "") if isinstance(tx, dict) else ""
                    block = int(tx.get("blockNumber", "0"))
                    if block > max_block:
                        max_block = block
                    token = Web3.to_checksum_address(tx.get("contractAddress"))
                    if token.lower() in FOLLOW_IGNORE_SET:
                        continue
                    decimals = int(tx.get("tokenDecimal", "18"))
                    value = float(tx.get("value", "0")) / (10 ** decimals)
                    if value <= 0:
                        continue
                    price = get_price_usd(token)
                    if price is None:
                        continue
                    usd = value * price
                    frm = tx.get("from", "").lower()
                    to = tx.get("to", "").lower()
                    addr = address.lower()
                    ts = tx.get("timeStamp")
                    tx_time = None
                    if ts:
                        try:
                            tx_time = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
                        except Exception:
                            tx_time = None
                    if ts:
                        try:
                            age = int(time.time()) - int(ts)
                            if age > FOLLOW_MAX_AGE_SEC:
                                continue
                        except Exception:
                            pass
                    txinfo = _etherscan_tx_by_hash(txhash) if txhash else None
                    if not txinfo:
                        continue
                    tx_to = (txinfo.get("to") or "").lower()
                    if tx_to != ROUTER_ADDR_LC:
                        continue
                    tx_from = (txinfo.get("from", "").lower() if txinfo else "")
                    if tx_from and tx_from != addr:
                        continue
                    bnb_value_usd = None
                    if txinfo and txinfo.get("value"):
                        try:
                            bnb_val = int(txinfo.get("value", "0x0"), 16) / 1e18
                            bnb_price = get_bnb_price_usd()
                            if bnb_price is not None:
                                bnb_value_usd = bnb_val * bnb_price
                        except Exception:
                            bnb_value_usd = None
                    buy_value = bnb_value_usd if bnb_value_usd is not None and bnb_value_usd > 0 else usd
                    sell_value = usd
                    if tx_from and tx_from == addr and bnb_value_usd is not None and bnb_value_usd > 0:
                        sell_value = bnb_value_usd
                    if to == addr and buy_value >= float(f.get("min_buy_usd", 0)):
                        key = _follow_seen_key(txhash, token, "BUY")
                        if not _follow_seen_has(key):
                            _follow_seen_add(key)
                            ok, _ = _handle_follow_buy(token, buy_value, float(f.get("my_buy_bnb", 0)))
                            changes["last_action"] = "BUY" if ok else "BUY_FAIL"
                            changes["last_token"] = token
                            changes["last_usd"] = float(buy_value)
                            changes["last_action_ts"] = tx_time or datetime.now(timezone.utc).isoformat()
                    if frm == addr and sell_value >= float(f.get("min_sell_usd", 0)):
                        key = _follow_seen_key(txhash, token, "SELL")
                        if not _follow_seen_has(key):
                            _follow_seen_add(key)
                            ok, _ = _handle_follow_sell(token, sell_value, float(f.get("my_sell_pct", 0)))
                            changes["last_action"] = "SELL" if ok else "SELL_FAIL"
                            changes["last_token"] = token
                            changes["last_usd"] = float(sell_value)
                            changes["last_action_ts"] = tx_time or datetime.now(timezone.utc).isoformat()
                except Exception as e:
                    logger.warning("Follow loop error for %s tx=%s: %s", address, txhash, e, exc_info=True)
                    continue
            if max_block > startblock:
                changes["last_block"] = max_block
            if changes:
                updates[address] = changes
        if updates:
            _merge_follow_updates(updates)
        time.sleep(FOLLOW_POLL_SEC)

def _gas_price():
    if GAS_PRICE_GWEI:
            return web3.to_wei(float(GAS_PRICE_GWEI), "gwei")
    return web3.eth.gas_price

def _ensure_chain():
    try:
        return int(web3.eth.chain_id) == CHAIN_ID
    except Exception:
        return False

def _erc20(token):
    return web3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)

def _token_decimals(token):
    return int(_erc20(token).functions.decimals().call())

def _token_balance(token):
    with wallet_lock:
        addr = wallet_cfg["address"]
    return int(_erc20(token).functions.balanceOf(Web3.to_checksum_address(addr)).call())

def _allowance(token):
    with wallet_lock:
        addr = wallet_cfg["address"]
    return int(_erc20(token).functions.allowance(Web3.to_checksum_address(addr), Web3.to_checksum_address(ROUTER_ADDRESS)).call())

def _approve_if_needed(token, amount):
    with wallet_lock:
        addr = wallet_cfg["address"]
    if _allowance(token) >= amount:
        return True
    tx = _erc20(token).functions.approve(Web3.to_checksum_address(ROUTER_ADDRESS), amount).build_transaction({
        "from": Web3.to_checksum_address(addr),
        "nonce": web3.eth.get_transaction_count(Web3.to_checksum_address(addr), "pending"),
        "gas": 80000,
        "gasPrice": _gas_price(),
        "chainId": CHAIN_ID,
    })
    with wallet_lock:
        pk = wallet_cfg["private_key"]
    signed = web3.eth.account.sign_transaction(tx, pk)
    tx_hash = web3.eth.send_raw_transaction(signed.rawTransaction)
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    return receipt.status == 1

def _min_out(amount_in, path):
    amounts = router.functions.getAmountsOut(amount_in, path).call()
    out = int(amounts[-1])
    return int(out * (10000 - SLIPPAGE_BPS) / 10000)

def _best_path(amount_in, token, is_buy):
    wbnb = Web3.to_checksum_address(WBNB_ADDRESS)
    usdt = Web3.to_checksum_address(USDT_ADDRESS)
    usdc = Web3.to_checksum_address(USDC_ADDRESS)
    busd = Web3.to_checksum_address(BUSD_ADDRESS)
    t = Web3.to_checksum_address(token)
    candidates = []
    if is_buy:
        candidates = [
            [wbnb, t],
            [wbnb, busd, t],
            [wbnb, usdt, t],
            [wbnb, usdc, t],
        ]
    else:
        candidates = [
            [t, wbnb],
            [t, busd, wbnb],
            [t, usdt, wbnb],
            [t, usdc, wbnb],
        ]
    best = None
    best_out = 0
    for path in candidates:
        try:
            amounts = router.functions.getAmountsOut(amount_in, path).call()
            out = int(amounts[-1])
            if out > best_out:
                best_out = out
                best = path
        except Exception:
            continue
    return best, best_out

def _buy_token(token, buy_bnb):
    if not trading_enabled():
        return False, "TRADING_DISABLED"
    if not _ensure_chain():
        return False, "WRONG_CHAIN"
    try:
        with wallet_lock:
            addr = wallet_cfg["address"]
        bal = web3.from_wei(web3.eth.get_balance(Web3.to_checksum_address(addr)), "ether")
        if float(bal) < float(buy_bnb):
            return False, "INSUFFICIENT_BNB"
    except Exception:
        pass
    value = web3.to_wei(float(buy_bnb), "ether")
    try:
        path, out = _best_path(value, token, True)
        if not path:
            return False, "NO_ROUTE"
        amount_out_min = int(out * (10000 - SLIPPAGE_BPS) / 10000)
    except Exception as e:
        return False, f"NO_LIQUIDITY: {e}"
    tx = router.functions.swapExactETHForTokensSupportingFeeOnTransferTokens(
        amount_out_min,
        path,
        Web3.to_checksum_address(addr),
        int(time.time()) + 120,
    ).build_transaction({
        "from": Web3.to_checksum_address(addr),
        "value": value,
        "nonce": web3.eth.get_transaction_count(Web3.to_checksum_address(addr), "pending"),
        "gas": GAS_LIMIT,
        "gasPrice": _gas_price(),
        "chainId": CHAIN_ID,
    })
    with wallet_lock:
        pk = wallet_cfg["private_key"]
    signed = web3.eth.account.sign_transaction(tx, pk)
    tx_hash = web3.eth.send_raw_transaction(signed.rawTransaction)
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
    return receipt.status == 1, receipt.transactionHash.hex()

def _sell_token(token, amount_tokens):
    if not trading_enabled():
        return False, "TRADING_DISABLED"
    if not _ensure_chain():
        return False, "WRONG_CHAIN"
    try:
        path, out = _best_path(amount_tokens, token, False)
        if not path:
            return False, "NO_ROUTE"
        amount_out_min = int(out * (10000 - SLIPPAGE_BPS) / 10000)
    except Exception as e:
        return False, f"NO_LIQUIDITY: {e}"
    if not _approve_if_needed(token, amount_tokens):
        return False, "APPROVE_FAIL"
    tx = router.functions.swapExactTokensForETHSupportingFeeOnTransferTokens(
        amount_tokens,
        amount_out_min,
        path,
        Web3.to_checksum_address(addr),
        int(time.time()) + 120,
    ).build_transaction({
        "from": Web3.to_checksum_address(addr),
        "nonce": web3.eth.get_transaction_count(Web3.to_checksum_address(addr), "pending"),
        "gas": GAS_LIMIT,
        "gasPrice": _gas_price(),
        "chainId": CHAIN_ID,
    })
    with wallet_lock:
        pk = wallet_cfg["private_key"]
    signed = web3.eth.account.sign_transaction(tx, pk)
    tx_hash = web3.eth.send_raw_transaction(signed.rawTransaction)
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
    return receipt.status == 1, receipt.transactionHash.hex()

# ---------------- TRADE LOGIC ----------------
def evaluate(token):
    with state_lock:
        pos=state["tokens"].get(token)
        if not pos:
            return
        if "name" not in pos:
            pos["name"] = ""
        if "holding_size" not in pos:
            pos["holding_size"] = pos.get("buy_bnb", 0) if pos.get("holding") else 0
        if "buy_rsi" not in pos:
            pos["buy_rsi"] = float(RSI_ENTRY)
        if "buy_use_doji" not in pos:
            pos["buy_use_doji"] = True
        if "sell_rsi" not in pos:
            pos["sell_rsi"] = 70.0
        if "sell_use_doji" not in pos:
            pos["sell_use_doji"] = False
        if "sell_size_pct" not in pos:
            pos["sell_size_pct"] = 100.0
        last_fetch = int(pos.get("last_fetch", 0))
        holding = bool(pos.get("holding"))

    now_ts = int(time.time())
    if now_ts - last_fetch < 900:
        next_ts = last_fetch + 900
        set_next_fetch(token, next_ts)
        if holding:
            # Still allow price-based exits while waiting for next candle.
            price = get_price(token)
            if price is None:
                set_status(token, "PRICE_ERROR")
                return
            with state_lock:
                current = state["tokens"].get(token)
                if not current:
                    return
                if price <= current["sl"]:
                    logger.info("SELL SL: %s", token)
                    if trading_enabled():
                        with tx_lock:
                            bal_raw = _token_balance(token)
                            ok, txid = _sell_token(token, bal_raw)
                        if ok:
                            current["holding"] = False
                            current["holding_size"] = 0
                            current["last_update"] = datetime.now(timezone.utc).isoformat()
                            save_state_unlocked(state)
                            log_event("SELL_SL", token, f"tx={txid}")
                            resync_holding(token)
                        else:
                            set_status(token, "TRADE_ERROR")
                            log_event("SELL_FAIL", token, f"{txid}")
                            resync_holding(token)
                    else:
                        current["holding"] = False
                        current["holding_size"] = 0
                        current["last_update"] = datetime.now(timezone.utc).isoformat()
                        save_state_unlocked(state)
                        log_event("SELL_SL", token, "NO_TRADE")
                        resync_holding(token)
            set_status(token, "WAIT_NEXT_15M")
            return
        set_status(token, "WAIT_NEXT_15M")
        return
    candles, err = fetch_candles(token)
    if candles is None:
        # Retry sooner on failures instead of waiting full 15m.
        with state_lock:
            if token in state["tokens"]:
                state["tokens"][token]["last_fetch"] = int(time.time())
                state["tokens"][token]["next_fetch"] = int(time.time()) + FETCH_RETRY_SEC
                save_state_unlocked(state)
        set_status(token, err or "NO_DATA")
        return
    with state_lock:
        if token in state["tokens"]:
            state["tokens"][token]["last_fetch"] = int(time.time())
            state["tokens"][token]["next_fetch"] = int(time.time()) + 900
            save_state_unlocked(state)

    rsi=safe_rsi(candles)

    if rsi is None:
        set_status(token, "RSI_ERROR")
        return

    set_status(token, "OK")

    price=get_price(token)
    if price is None:
        set_status(token, "PRICE_ERROR")
        return

    last=candles[-2]
    update_metrics(token, price, rsi, is_perfect_doji(last))

    if not pos["holding"]:
        buy_rsi = pos.get("buy_rsi", float(RSI_ENTRY))
        buy_use_doji = pos.get("buy_use_doji", True)
        buy_ok = rsi < buy_rsi and (not buy_use_doji or is_perfect_doji(last))
        if buy_ok:

            entry=price
            sl=last[2]*SL_BUFFER
            if entry <= sl:
                set_status(token, "SKIP_BAD_RISK")
                return
            logger.info("BUY SIGNAL: %s %.8f", token, entry)
            log_event("BUY_SIGNAL", token, f"entry={entry:.8f} sl={sl:.8f}")
            if trading_enabled():
                with tx_lock:
                    ok, txid = _buy_token(token, pos.get("buy_bnb", 0))
                if ok:
                    token_dec = _token_decimals(token)
                    bal = _token_balance(token)
                    with state_lock:
                        if token not in state["tokens"]:
                            return
                        state["tokens"][token]["holding"]=True
                        state["tokens"][token]["entry"]=entry
                        state["tokens"][token]["sl"]=sl
                        state["tokens"][token]["holding_size"]=bal / (10 ** token_dec)
                        state["tokens"][token]["last_update"]=datetime.now(timezone.utc).isoformat()
                        save_state_unlocked(state)
                    log_event("BUY_EXECUTED", token, f"tx={txid}")
                    resync_holding(token)
                else:
                    set_status(token, "TRADE_ERROR")
                    log_event("BUY_FAIL", token, f"{txid}")
                    resync_holding(token)
            else:
                set_status(token, "SIGNAL_ONLY")

    else:

        update_trailing(token,candles)

        with state_lock:
            current=state["tokens"].get(token)
            if not current:
                return
            sell_rule = (rsi >= current.get("sell_rsi", 70.0)) and (not current.get("sell_use_doji", False) or is_perfect_doji(last))
            if price<=current["sl"]:
                logger.info("SELL SL: %s", token)
                if trading_enabled():
                    with tx_lock:
                        token_dec = _token_decimals(token)
                        bal_raw = _token_balance(token)
                        ok, txid = _sell_token(token, bal_raw)
                    if ok:
                        current["holding"]=False
                        current["holding_size"]=0
                        current["last_update"]=datetime.now(timezone.utc).isoformat()
                        save_state_unlocked(state)
                        log_event("SELL_SL", token, f"tx={txid}")
                        resync_holding(token)
                    else:
                        set_status(token, "TRADE_ERROR")
                        log_event("SELL_FAIL", token, f"{txid}")
                        resync_holding(token)
                else:
                    current["holding"]=False
                    current["holding_size"]=0
                    current["last_update"]=datetime.now(timezone.utc).isoformat()
                    save_state_unlocked(state)
                    log_event("SELL_SL", token, "NO_TRADE")
                    resync_holding(token)
            elif sell_rule:
                pct = float(current.get("sell_size_pct", 100.0))
                size = float(current.get("holding_size", 0))
                sell_amt = size * (pct / 100.0)
                remaining = max(size - sell_amt, 0)
                if trading_enabled():
                    with tx_lock:
                        dec = _token_decimals(token)
                        amount_raw = int(sell_amt * (10 ** dec))
                        ok, txid = _sell_token(token, amount_raw)
                    if ok:
                        current["holding_size"] = remaining
                        if remaining <= 0:
                            current["holding"] = False
                        current["last_update"] = datetime.now(timezone.utc).isoformat()
                        save_state_unlocked(state)
                        log_event("SELL_SIGNAL", token, f"pct={pct:.2f} size={sell_amt:.6f} tx={txid}")
                        resync_holding(token)
                    else:
                        set_status(token, "TRADE_ERROR")
                        log_event("SELL_FAIL", token, f"{txid}")
                        resync_holding(token)
                else:
                    current["holding_size"] = remaining
                    if remaining <= 0:
                        current["holding"] = False
                    current["last_update"] = datetime.now(timezone.utc).isoformat()
                    save_state_unlocked(state)
                    log_event("SELL_SIGNAL", token, f"pct={pct:.2f} size={sell_amt:.6f}")

# ---------------- LOOP ----------------
def auto_loop():
    while True:
        with state_lock:
            tokens = list(state["tokens"].keys())
        for token in tokens:
            with state_lock:
                if token not in state["tokens"]:
                    continue
                if not state["tokens"][token]["active"]:
                    continue
            try:
                evaluate(token)
            except Exception as e:
                logger.exception("Loop error for %s: %s", token, e)
                log_event("LOOP_ERROR", token, str(e))
        time.sleep(60)

def wallet_balance_loop():
    while True:
        update_wallet_balance()
        time.sleep(WALLET_BALANCE_REFRESH_SEC)

# ---------------- DASHBOARD API ----------------
@dash.post("/api/token/add")
def api_add_token():
    if not _auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    token_raw = (data.get("token") or "").strip()
    name_raw = (data.get("name") or "").strip()
    buy_raw = data.get("buy_bnb")
    buy_rsi_raw = data.get("buy_rsi")
    buy_use_doji = bool(data.get("buy_use_doji"))
    if not token_raw or buy_raw is None:
        return jsonify({"error": "token and buy_bnb required"}), 400
    try:
        token = Web3.to_checksum_address(token_raw)
        buy = float(buy_raw)
        buy_rsi = float(buy_rsi_raw) if buy_rsi_raw is not None else float(RSI_ENTRY)
    except Exception:
        return jsonify({"error": "invalid token or buy_bnb"}), 400
    if buy_rsi <= 0 or buy_rsi > 100:
        return jsonify({"error": "buy_rsi must be 0-100"}), 400

    with state_lock:
        state["tokens"][token] = {
            "name": name_raw,
            "active": True,
            "holding": False,
            "entry": 0,
            "sl": 0,
            "buy_bnb": buy,
            "holding_size": 0,
            "status": "OK",
            "last_price": None,
            "last_rsi": None,
            "last_doji": None,
            "buy_rsi": buy_rsi,
            "buy_use_doji": buy_use_doji,
            "sell_rsi": 70.0,
            "sell_use_doji": False,
            "sell_size_pct": 100.0,
            "last_update": datetime.now(timezone.utc).isoformat()
        }
        save_state_unlocked(state)
    log_event("TOKEN_ADDED", token, f"buy_bnb={buy}")
    return jsonify({"ok": True})

@dash.post("/api/token/remove")
def api_remove_token():
    if not _auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    token_raw = (data.get("token") or "").strip()
    if not token_raw:
        return jsonify({"error": "token required"}), 400
    try:
        token = Web3.to_checksum_address(token_raw)
    except Exception:
        return jsonify({"error": "invalid token"}), 400
    with state_lock:
        state["tokens"].pop(token, None)
        save_state_unlocked(state)
    log_event("TOKEN_REMOVED", token, None)
    return jsonify({"ok": True})

@dash.post("/api/token/toggle")
def api_toggle_token():
    if not _auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    token_raw = (data.get("token") or "").strip()
    if not token_raw:
        return jsonify({"error": "token required"}), 400
    try:
        token = Web3.to_checksum_address(token_raw)
    except Exception:
        return jsonify({"error": "invalid token"}), 400
    with state_lock:
        if token in state["tokens"]:
            state["tokens"][token]["active"] = not state["tokens"][token]["active"]
            state["tokens"][token]["last_update"] = datetime.now(timezone.utc).isoformat()
            save_state_unlocked(state)
            log_event("TOKEN_TOGGLE", token, f"active={state['tokens'][token]['active']}")
            return jsonify({"ok": True, "active": state["tokens"][token]["active"]})
    return jsonify({"error": "token not found"}), 404

@dash.post("/api/follow/add")
def api_follow_add():
    if not _auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    address = (data.get("address") or "").strip()
    if not address:
        return jsonify({"error": "address required"}), 400
    try:
        address = Web3.to_checksum_address(address)
    except Exception:
        return jsonify({"error": "invalid address"}), 400
    # Initialize last_block to current block to avoid replaying history.
    last_block = int(data.get("last_block", 0))
    if last_block <= 0:
        try:
            last_block = int(web3.eth.block_number)
        except Exception:
            last_block = 0
    follower = {
        "name": name,
        "address": address,
        "active": True,
        "min_buy_usd": float(data.get("min_buy_usd", 0)),
        "min_sell_usd": float(data.get("min_sell_usd", 0)),
        "my_buy_bnb": float(data.get("my_buy_bnb", 0)),
        "my_sell_pct": float(data.get("my_sell_pct", 0)),
        "last_block": last_block,
    }
    followers = _get_followers()
    followers = [f for f in followers if f.get("address") != address]
    followers.append(follower)
    _save_followers(followers)
    return jsonify({"ok": True})

@dash.post("/api/follow/update")
def api_follow_update():
    if not _auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    address = (data.get("address") or "").strip()
    if not address:
        return jsonify({"error": "address required"}), 400
    try:
        address = Web3.to_checksum_address(address)
    except Exception:
        return jsonify({"error": "invalid address"}), 400
    followers = _get_followers()
    for f in followers:
        if f.get("address") == address:
            f["name"] = (data.get("name") or f.get("name") or "").strip()
            if "active" in data:
                f["active"] = bool(data.get("active"))
            f["min_buy_usd"] = float(data.get("min_buy_usd", f.get("min_buy_usd", 0)))
            f["min_sell_usd"] = float(data.get("min_sell_usd", f.get("min_sell_usd", 0)))
            f["my_buy_bnb"] = float(data.get("my_buy_bnb", f.get("my_buy_bnb", 0)))
            f["my_sell_pct"] = float(data.get("my_sell_pct", f.get("my_sell_pct", 0)))
    _save_followers(followers)
    return jsonify({"ok": True})

@dash.post("/api/follow/remove")
def api_follow_remove():
    if not _auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    address = (data.get("address") or "").strip()
    if not address:
        return jsonify({"error": "address required"}), 400
    try:
        address = Web3.to_checksum_address(address)
    except Exception:
        return jsonify({"error": "invalid address"}), 400
    followers = _get_followers()
    followers = [f for f in followers if f.get("address") != address]
    _save_followers(followers)
    return jsonify({"ok": True})

@dash.post("/api/follow/toggle")
def api_follow_toggle():
    if not _auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    data = request.get_json(silent=True) or {}
    address = (data.get("address") or "").strip()
    if not address:
        return jsonify({"error": "address required"}), 400
    try:
        address = Web3.to_checksum_address(address)
    except Exception:
        return jsonify({"error": "invalid address"}), 400
    followers = _get_followers()
    for f in followers:
        if f.get("address") == address:
            f["active"] = not f.get("active", True)
    _save_followers(followers)
    return jsonify({"ok": True})

@dash.post("/api/run_once")
def api_run_once():
    if not _auth_ok(request):
        return jsonify({"error": "unauthorized"}), 401
    with state_lock:
        tokens = list(state["tokens"].keys())
    ran = 0
    for token in tokens:
        with state_lock:
            if token not in state["tokens"]:
                continue
            if not state["tokens"][token]["active"]:
                continue
        try:
            evaluate(token)
            ran += 1
        except Exception as e:
            logger.exception("Manual run error for %s: %s", token, e)
            log_event("RUN_ONCE_ERROR", token, str(e))
    log_event("RUN_ONCE", None, f"tokens={ran}")
    return jsonify({"ok": True, "tokens": ran})

# ---------------- MAIN ----------------
def main():
    update_wallet_balance(force=True)
    if AUTO_LOOP:
        Thread(target=auto_loop, daemon=True).start()
    Thread(target=wallet_balance_loop, daemon=True).start()
    Thread(target=snapshot_loop, daemon=True).start()
    Thread(target=follow_loop, daemon=True).start()
    if "--serve" in sys.argv:
        serve(dash, host=DASH_HOST, port=DASH_PORT)
    else:
        dash.run(host=DASH_HOST, port=DASH_PORT, debug=False, use_reloader=False)

if __name__=="__main__":
    main()
