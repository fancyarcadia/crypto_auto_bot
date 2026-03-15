Manual Doji RSI Bot
===================

Features
--------
- Manual token trading (signals only)
- Multiple tokens supported
- Only ONE trade per token
- 15-minute RSI analysis
- Perfect doji entry
- Trailing stop based on new doji
- Restart-safe state memory
- Minimal web dashboard

Example
-------
Use the dashboard to add tokens and set buy size.

Setup (Local)
-------------
1. Install Python 3.10+
2. Install dependencies

pip install -r requirements.txt

3. Set environment variables (recommended):

RPC_URL="https://bsc-dataseed.binance.org/"
DASH_PORT="8000"
DASH_TOKEN="optional_secret"

4. Run

python bot.py

Render Deployment
-----------------
1. Upload files to a GitHub repository.

2. Go to Render.com
3. Create new Web Service
4. Connect your repository

Build command:

pip install -r requirements.txt

Start command:

python bot.py --serve

Environment Variables
---------------------
RPC_URL
RPC_URLS
DASH_PORT
DASH_TOKEN
AUTO_LOOP
PRIVATE_KEY
WALLET_ADDRESS
WALLET_SECRET
WALLET_KDF_ITERATIONS
ROUTER_ADDRESS
WBNB_ADDRESS
USDT_ADDRESS
USDC_ADDRESS
BUSD_ADDRESS
SLIPPAGE_BPS
GAS_LIMIT
GAS_PRICE_GWEI
CHAIN_ID
MIN_CANDLES
FETCH_RETRY_SEC
ETHERSCAN_API_KEY
ETHERSCAN_API_KEYS
ETHERSCAN_API_URL
FOLLOW_POLL_SEC
FOLLOW_DEDUPE_MAX
BNB_PRICE_REFRESH_SEC
FOLLOW_MAX_AGE_SEC
FOLLOW_IGNORE_TOKENS

Health
------
GET /health returns {"ok": true}.

Dashboard
---------
Open http://localhost:8000 to view the live dashboard.
To protect the UI, set DASH_TOKEN and open http://localhost:8000?token=your_secret
When DASH_TOKEN is not set, only read-only access is allowed while trading is disabled. Set DASH_TOKEN to edit tokens, wallet, or follow settings.
You can set a token name on add and update it later in the rule editor.
Set AUTO_LOOP=0 to disable continuous running and use the "Run once" button or the /api/run_once endpoint.

Trading
-------
Set PRIVATE_KEY and WALLET_ADDRESS to enable real trades on BSC. If not set, the bot runs in signal-only mode.
Default router is PancakeSwap V2. You can override ROUTER_ADDRESS and WBNB_ADDRESS if needed.
You can also set or clear the wallet from the dashboard.

Follow Wallets
--------------
Set ETHERSCAN_API_KEY to enable follow-wallet copy trading (Etherscan API V2 supports BSC with chainid=56).
If needed, set ETHERSCAN_API_URL (default https://api.etherscan.io/v2/api).
The bot will mirror buys/sells above your USD thresholds.
By default, quote tokens (WBNB/USDT/USDC/BUSD) are ignored to avoid false trades; override with FOLLOW_IGNORE_TOKENS (comma-separated).

Sell Rules (per token)
----------------------
You can set:
- Sell RSI level
- Sell Doji on/off
- Sell size percentage of current holding

Buy Rules (per token)
---------------------
You can set:
- Buy RSI level
- Buy Doji on/off

Bot will start automatically and keep state in state.json.
