# -*- coding: utf-8 -*-
"""Central configuration — environment variables, constants, paths."""

import os
import logging
import warnings
from dotenv import load_dotenv



BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("bot")

# ── Tokens & IDs ──────────────────────────────────────────────────────────
MAIN_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
LOG_BOT_TOKEN = os.environ.get("LOG_BOT_TOKEN", "").strip()
OWNER_ID = int(os.environ.get("BOT_OWNER_ID", "0") or "0")
ADMIN_LOG_ID = int(os.environ.get("ADMIN_LOG_ID", "0") or "0")

# ── Activation API ────────────────────────────────────────────────────────
IQLESS_API_KEY = os.environ.get(
    "IQLESS_API_KEY", "ak_ZPZS-M5BS-224H-FCXA-VA3Q-UXPK-ESMV-NRWH"
).strip()
IQLESS_BASE_URL = "https://a8yx0rez5w.localto.net"

# ── Channel & support ────────────────────────────────────────────────────
REQUIRED_CHANNEL = os.environ.get("REQUIRED_CHANNEL", "@toolssheerid")
SUPPORT_USER = os.environ.get("SUPPORT_USER", "@r5llc3")
MY_BOT_USERNAME = os.environ.get("MY_BOT_USERNAME", "ToolsSheerid_bot")

# ── Pricing defaults ─────────────────────────────────────────────────────
DEFAULT_ACTIVATE_PRICE = float(os.environ.get("DEFAULT_ACTIVATE_PRICE", "2.5") or "2.5")
DEFAULT_RESELLER_PROFIT = float(os.environ.get("DEFAULT_RESELLER_PROFIT", "0.5") or "0.5")

# ── Deposit ───────────────────────────────────────────────────────────────
MIN_DEPOSIT = float(os.environ.get("MIN_DEPOSIT", "1.0") or "1.0")
MY_TRC20_ADDRESS = os.environ.get("MY_TRC20_ADDRESS", "TD3Y2TGzVRc5nHJbRRUUGQ9XuEdYXL5Red").strip()
MY_BEP20_ADDRESS = os.environ.get("MY_BEP20_ADDRESS", "0x81bd1a65c2f697025e7cff3ee73ef7c0aee0c7f7").strip()
MY_BARIDIMOB_RIB = os.environ.get("MY_BARIDIMOB_RIB", "00799999001866682562").strip()
BSCSCAN_API_KEY = os.environ.get("BSCSCAN_API_KEY", "").strip()

# ── Rewards ───────────────────────────────────────────────────────────────
CHECKIN_REWARD = float(os.environ.get("CHECKIN_REWARD", "0.1") or "0.1")
REFERRAL_REWARD = float(os.environ.get("REFERRAL_REWARD", "0.1") or "0.1")

# ── Paths ─────────────────────────────────────────────────────────────────
def _resolve(path_value: str, default_name: str) -> str:
    raw = (path_value or "").strip()
    if not raw:
        return os.path.join(BASE_DIR, default_name)
    return raw if os.path.isabs(raw) else os.path.join(BASE_DIR, raw)


DB_PATH = _resolve(os.environ.get("DB_PATH", ""), "bot.db")
PERSISTENCE_PATH = _resolve(os.environ.get("PERSISTENCE_PATH", ""), "user_data.pkl")

# ── Blockchain constants ──────────────────────────────────────────────────
USDT_TRC20_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
USDT_BEP20_CONTRACT = "0x55d398326f99059ff775485246999027b3197955"
BSC_RPC_URL = os.environ.get("BSC_RPC_URL", "https://bsc-dataseed.binance.org").strip()
TRANSFER_EVENT_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
NETWORK_AMOUNT_DECIMALS = {"TRC20": 6, "BEP20": 8}
NETWORK_CHAIN_DECIMALS = {"TRC20": 6, "BEP20": 18}



# ── Mutable global state ─────────────────────────────────────────────────
global_log_bot = None
MAINTENANCE_MODE = False

# Runtime registries for external shops
EXTERNAL_USER_APPS: dict = {}
EXTERNAL_ADMIN_APPS: dict = {}

# Active activation jobs: job_id -> {uid, email, msg_obj, cost, reseller_id, tx_id}
active_jobs: dict = {}

# ── Backup config ─────────────────────────────────────────────────────────
BACKUP_IMPORTANT_ENV = [
    "TELEGRAM_BOT_TOKEN", "LOG_BOT_TOKEN", "IQLESS_API_KEY",
    "SESSION_SECRET", "BOT_OWNER_ID", "ADMIN_LOG_ID",
    "REQUIRED_CHANNEL", "SUPPORT_USER", "MY_BOT_USERNAME",
    "DEFAULT_ACTIVATE_PRICE", "DEFAULT_RESELLER_PROFIT",
    "MIN_DEPOSIT", "MY_TRC20_ADDRESS", "MY_BEP20_ADDRESS", "BSCSCAN_API_KEY",
    "CHECKIN_REWARD", "REFERRAL_REWARD", "DB_PATH",
]

BACKUP_FILES = ["config.py", "database.py", "main.py", "pyproject.toml",
                "localization.py", "utils.py", "app_builder.py"]
