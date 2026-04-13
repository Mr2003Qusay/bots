# -*- coding: utf-8 -*-
import os
import re
import time
import sqlite3
import asyncio
import logging
import datetime
import zipfile
import io
import random
import base64
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from zoneinfo import ZoneInfo
import html
import warnings
import secrets
try:
    import pyotp as _pyotp
except ImportError:
    _pyotp = None

import httpx
from dotenv import load_dotenv
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Bot,
    BotCommand,
    BotCommandScopeChat,
)
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ConversationHandler,
    PicklePersistence,
)

warnings.filterwarnings("ignore", module="telegram.ext")

# =========================
# CONFIG
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

MAIN_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
LOG_BOT_TOKEN = os.environ.get("LOG_BOT_TOKEN", "").strip()
OWNER_ID = int(os.environ.get("BOT_OWNER_ID", "0") or "0")
ADMIN_LOG_ID = int(os.environ.get("ADMIN_LOG_ID", "0") or "0")

IQLESS_API_KEY = os.environ.get("IQLESS_API_KEY", "ak_ZPZS-M5BS-224H-FCXA-VA3Q-UXPK-ESMV-NRWH").strip()
IQLESS_BASE_URL = "https://a8yx0rez5w.localto.net"

# ChatGPT login proxy API (Replit API server — bypasses Cloudflare from user server)
WS_LOGIN_API_URL = os.environ.get(
    "WS_LOGIN_API_URL",
    "https://f5d793b0-5aa2-48a0-86e7-c087d7a92973-00-2xzqw929nddw6.spock.replit.dev"
).rstrip("/")

REQUIRED_CHANNEL = os.environ.get("REQUIRED_CHANNEL", "@toolssheerid")
SUPPORT_USER = os.environ.get("SUPPORT_USER", "@r5llc3")
MY_BOT_USERNAME = os.environ.get("MY_BOT_USERNAME", "ToolsSheerid_bot")

DEFAULT_ACTIVATE_PRICE = float(os.environ.get("DEFAULT_ACTIVATE_PRICE", "2.5") or "2.5")
DEFAULT_RESELLER_PROFIT = float(os.environ.get("DEFAULT_RESELLER_PROFIT", "0.5") or "0.5")
DEFAULT_WS_SEAT_PRICE = float(os.environ.get("DEFAULT_WS_SEAT_PRICE", "10.0") or "10.0")
DEFAULT_WS_MONTHLY_PRICE = float(os.environ.get("DEFAULT_WS_MONTHLY_PRICE", "10.0") or "10.0")
MIN_DEPOSIT = float(os.environ.get("MIN_DEPOSIT", "1.0") or "1.0")
MY_TRC20_ADDRESS = os.environ.get("MY_TRC20_ADDRESS", "TD3Y2TGzVRc5nHJbRRUUGQ9XuEdYXL5Red").strip()
MY_BEP20_ADDRESS = os.environ.get("MY_BEP20_ADDRESS", "0x81bd1a65c2f697025e7cff3ee73ef7c0aee0c7f7").strip()
MY_BARIDIMOB_RIB = os.environ.get("MY_BARIDIMOB_RIB", "00799999001866682562").strip()
BSCSCAN_API_KEY = os.environ.get("BSCSCAN_API_KEY", "").strip()

CHECKIN_REWARD = float(os.environ.get("CHECKIN_REWARD", "0.1") or "0.1")
REFERRAL_REWARD = float(os.environ.get("REFERRAL_REWARD", "0.1") or "0.1")


def resolve_project_path(path_value: str, default_name: str) -> str:
    raw = (path_value or "").strip()
    if not raw:
        return os.path.join(BASE_DIR, default_name)
    return raw if os.path.isabs(raw) else os.path.join(BASE_DIR, raw)


DB_PATH = resolve_project_path(os.environ.get("DB_PATH", ""), "bot.db")
PERSISTENCE_PATH = resolve_project_path(os.environ.get("PERSISTENCE_PATH", ""), "user_data.pkl")
USDT_TRC20_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
USDT_BEP20_CONTRACT = "0x55d398326f99059ff775485246999027b3197955"
BSC_RPC_URL = os.environ.get("BSC_RPC_URL", "https://bsc-dataseed.binance.org").strip()
TRANSFER_EVENT_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
NETWORK_AMOUNT_DECIMALS = {"TRC20": 6, "BEP20": 8}
NETWORK_CHAIN_DECIMALS = {"TRC20": 6, "BEP20": 18}

global_log_bot = None
MAINTENANCE_MODE = False

EXTERNAL_USER_APPS = {}
EXTERNAL_ADMIN_APPS = {}

ADD_NAME, ADD_PRICE, ADD_STOCK, ADD_CAT, ADD_DESC, ADD_IMG, ADD_FILE = range(7)
SUPPORT_CHAT = 99

# Active activation jobs: job_id -> {uid, email, msg_obj, cost, reseller_id, tx_id}
active_jobs = {}


def normalize_network_name(network: str) -> str:
    name = (network or "TRC20").strip().upper()
    if "BEP" in name or "BSC" in name:
        return "BEP20"
    return "TRC20"


def network_amount_decimals(network: str) -> int:
    return NETWORK_AMOUNT_DECIMALS.get(normalize_network_name(network), 6)


def network_chain_decimals(network: str) -> int:
    return NETWORK_CHAIN_DECIMALS.get(normalize_network_name(network), 6)


def network_quantizer(network: str) -> Decimal:
    return Decimal("1").scaleb(-network_amount_decimals(network))


def parse_amount_decimal(value) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value).replace(",", ".").strip())
    except (InvalidOperation, ValueError, TypeError):
        return None


def normalize_amount_decimal(value, network: str) -> Decimal | None:
    dec = parse_amount_decimal(value)
    if dec is None:
        return None
    if dec < 0:
        dec = Decimal("0")
    return dec.quantize(network_quantizer(network), rounding=ROUND_DOWN)


def decimal_to_display(value, trim: bool = True) -> str:
    dec = parse_amount_decimal(value)
    if dec is None:
        return "0"
    text = format(dec, "f")
    return text.rstrip("0").rstrip(".") if trim else text


def format_amount_for_network(value, network: str, trim: bool = False) -> str:
    dec = normalize_amount_decimal(value, network)
    if dec is None:
        dec = Decimal("0").quantize(network_quantizer(network))
    text = format(dec, f".{network_amount_decimals(network)}f")
    return text.rstrip("0").rstrip(".") if trim else text


def pending_expected_amount_str(deposit: dict) -> str:
    stored = (deposit.get("expected_amount_str") or "").strip()
    if stored:
        return stored
    return format_amount_for_network(deposit.get("expected_amount", 0), deposit.get("network", "TRC20"), trim=False)


def pending_expected_amount_decimal(deposit: dict) -> Decimal:
    return normalize_amount_decimal(pending_expected_amount_str(deposit), deposit.get("network", "TRC20")) or Decimal("0")


def canonicalize_txid(txid: str, network: str = "") -> str:
    raw = (txid or "").strip()
    if raw.lower().startswith("/claim"):
        parts = raw.split(maxsplit=1)
        raw = parts[1].strip() if len(parts) > 1 else ""
    if not raw:
        return ""
    raw_hex = raw[2:] if raw.lower().startswith("0x") else raw
    if not re.fullmatch(r"[0-9a-fA-F]{64}", raw_hex):
        return ""
    raw_hex = raw_hex.lower()
    net = normalize_network_name(network) if network else ""
    if net == "BEP20":
        return "0x" + raw_hex
    if net == "TRC20":
        return raw_hex
    return raw.lower() if raw.lower().startswith("0x") else raw_hex


def is_txid_like(text: str) -> bool:
    return bool(canonicalize_txid(text))


def parse_db_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime.datetime):
        return value
    text = str(value).strip()
    try:
        return datetime.datetime.fromisoformat(text)
    except Exception:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.datetime.strptime(text, fmt)
        except Exception:
            pass
    return None


def db_save_job(job_id: str, uid: int, email: str, cost: float, reseller_id: int, tx_id: str, submitted_at: float,
                status_msg_id: int = 0, estimated_wait: float = 0.0):
    try:
        conn = db_connect()
        conn.execute(
            "INSERT OR REPLACE INTO active_jobs_db "
            "(job_id, uid, email, cost, reseller_id, tx_id, submitted_at, status_msg_id, estimated_wait) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (job_id, uid, email, cost, reseller_id, tx_id, submitted_at, status_msg_id, estimated_wait)
        )
        conn.commit()
        conn.close()
        logger.info(f"JOB_SAVED uid={uid} email={email} job_id={job_id} tx_id={tx_id}")
    except Exception as e:
        logger.error(f"db_save_job error: {e}")


def db_update_job_msg(job_id: str, status_msg_id: int):
    try:
        conn = db_connect()
        conn.execute("UPDATE active_jobs_db SET status_msg_id=? WHERE job_id=?", (status_msg_id, job_id))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"db_update_job_msg error: {e}")


def db_remove_job(job_id: str) -> bool:
    """Delete job from DB. Returns True only if THIS call actually deleted it (atomic gate)."""
    try:
        conn = db_connect()
        cur = conn.execute("DELETE FROM active_jobs_db WHERE job_id=?", (job_id,))
        affected = cur.rowcount
        conn.commit()
        conn.close()
        return affected > 0
    except Exception as e:
        logger.error(f"db_remove_job error: {e}")
        return False


def db_load_jobs() -> list:
    try:
        conn = db_connect()
        rows = conn.execute(
            "SELECT job_id, uid, email, cost, reseller_id, tx_id, submitted_at, "
            "COALESCE(status_msg_id,0), COALESCE(estimated_wait,0) FROM active_jobs_db"
        ).fetchall()
        conn.close()
        return [
            {
                "job_id": r[0],
                "uid": r[1],
                "email": r[2],
                "cost": r[3],
                "reseller_id": r[4],
                "tx_id": r[5],
                "submitted_at": r[6],
                "status_msg_id": r[7],
                "estimated_wait": r[8],
            }
            for r in rows
        ]
    except Exception as e:
        logger.error(f"db_load_jobs error: {e}")
        return []


# =========================
# LOCALIZATION
# =========================
def create_lang_dict(base, updates):
    d = base.copy()
    d.update(updates)
    return d

EN_TEXTS = {
    "lang_set": "✅ Language set to <b>English</b>",
    "welcome": (
        "👋 <b>Welcome!</b>\n\n"
        "🆔 ID: <code>{uid}</code>\n"
        "👥 <b>Users:</b> {users_count}\n"
        "💰 <b>Balance:</b> ${balance}\n"
        "💵 <b>Activate Price:</b> ${price}\n\n"
        "{service_line}"
        "<b>Status:</b> {status}\n\n"
        "👇 <b>Select an option below:</b>"
    ),
    "welcome_external": (
        "🛒 <b>{shop_title}</b>\n\n"
        "🆔 ID: <code>{uid}</code>\n"
        "👥 <b>Users:</b> {users_count}\n"
        "💰 <b>Balance:</b> ${balance}\n"
        "📦 <b>Products:</b> {products}\n\n"
        "🛍️ <b>This bot is store-only.</b>\n\n"
        "👇 <b>Select an option below:</b>"
    ),
    "welcome_admin": "👋 <b>Admin Panel</b>\n\nSelect a category to manage:",
    "welcome_ext_admin": "🌐 <b>{shop_title} - Control Panel</b>\n\nSelect a section:",
    "status_active": "🟢 <b>Online</b>",
    "status_maint": "🚧 <b>Maintenance</b>",
    "maint_msg": "⚠️ <b>System is under maintenance.</b>\nPlease try again later.",
    "maint_start_broadcast": "🚧 <b>Maintenance Alert</b>\n\nThe bot is currently under maintenance.",
    "maint_end_broadcast": "✅ <b>System Online</b>\n\nThe bot is back online.",
    "must_join": "⚠️ <b>Access Denied!</b>\n\nYou must join our updates channel to use this bot.",
    "btn_join_ch": "📢 Join Channel",
    "btn_i_joined": "✅ I have Joined",
    "join_success": "✅ <b>Thanks for joining!</b>",
    "still_not_joined": "⚠️ You still have not joined the channel.",
    "lang_select": "🌐 <b>Select Language:</b>",
    "banned": "🚫 <b>BANNED.</b>",
    "send_activate_prompt": (
        "⚡ <b>Google One Activation</b>\n\n"
        "📧 Please enter your <b>Gmail address</b>:"
    ),
    "act_ask_password": "🔑 Now enter your <b>Gmail password</b>:",
    "act_ask_totp": (
        "🔐 Now enter your <b>TOTP Secret</b>:\n\n"
        "📌 This is your 2FA secret key (Base32 encoded, e.g. <code>JBSWY3DPEHPK3PXP</code>)"
    ),
    "act_bad_email": "❌ Invalid email address. Please enter a valid Gmail address:",
    "activate_not_available": "⚠️ Activation is not available in this bot.",
    "bad_credentials": "❌ Invalid format. Please send:\n<code>email\npassword\ntotp_secret</code>",
    "activate_cost": "💵 <b>Activation Cost:</b> ${price}\n\nYour Balance: ${balance}\n\n✅ Confirm to proceed.",
    "activate_no_bal": "❌ Insufficient balance! You need ${price} to activate.\n\nYour balance: ${balance}",
    "activate_queued": (
        "⏳ <b>Job Submitted!</b>\n\n"
        "📧 Email: <code>{email}</code>\n"
        "🆔 Job ID: <code>{job_id}</code>\n"
        "🧾 TX: <code>{tx_id}</code>\n"
        "🔢 Queue Position: {pos}\n"
        "⏱ Est. Wait: ~{wait}s\n\n"
        "I will notify you when done."
    ),
    "activate_success": (
        "✅ <b>Google One Activation Successful!</b>\n\n"
        "📧 Email: <code>{email}</code>\n"
        "🆔 Job ID: <code>{job_id}</code>\n"
        "🔗 Link: {url}\n\n"
        "🧾 Transaction: <code>#{tx}</code>"
    ),
    "activate_failed": (
        "❌ <b>Activation Failed</b>\n\n"
        "📧 Email: <code>{email}</code>\n"
        "Reason: <b>{reason}</b>\n\n"
        "💰 <b>${cost} refunded to your balance.</b>"
    ),
    "activate_already_queued": "⚠️ This email is already in the queue or being processed.",
    "activate_already_done": "⚠️ This email has already been successfully activated.",
    "activate_no_devices": "⚠️ No devices available right now. Please try again in a few minutes.",
    "activate_service_paused": "⚠️ Activation service is temporarily paused. Please try again later.",
    "activate_api_error": "❌ API Error: {error}",
    "checkin_success": "📅 <b>Daily Check-in:</b>\n✅ You received +${amount}!",
    "checkin_fail": "⏳ <b>Already Checked-in!</b>\nCome back tomorrow.",
    "referral_bonus": "🎉 <b>New Referral!</b>\nYou got +${amount} for inviting a friend.",
    "deposit_menu": (
        "💰 <b>Deposit</b>\n\n"
        "🔹 <b>USDT TRC20:</b>\n<code>{trc20}</code>\n\n"
        "🔹 <b>USDT BEP20:</b>\n<code>{bep20}</code>\n\n"
        "🇩🇿 <b>BaridiMob (Algeria):</b>\n<code>{baridimob}</code>\n"
        "📌 Rate: 1000 DA = $4 | 630 DA = $2.5\n\n"
        "🆘 <b>Support:</b> Use /support\n"
        "👇 <b>After payment, contact admin.</b>"
    ),
    "deposit_choose_network": (
        "💰 <b>Deposit — Choose Payment Method</b>\n\n"
        "Select the network you will use to send USDT,\n"
        "or choose BaridiMob for Algeria:"
    ),
    "deposit_ask_amount": (
        "💵 <b>Enter the amount you want to deposit (USD):</b>\n\n"
        "Example: <code>10</code> or <code>5.5</code>\n"
        "Minimum: $1"
    ),
    "deposit_pending_trc20": (
        "✅ <b>Deposit Request Created!</b>\n\n"
        "🔹 Network: <b>USDT TRC20 (TRON)</b>\n\n"
        "💰 Send EXACTLY:\n"
        "<code>{amount}</code> USDT\n\n"
        "📬 To this address:\n"
        "<code>{wallet}</code>\n\n"
        "⚠️ <b>Important:</b> Send exactly this amount — even a tiny difference will prevent confirmation.\n\n"
        "⏰ This request expires in <b>30 minutes</b>.\n"
        "📨 After payment, send <code>/claim TXID</code> or just send the TXID alone."
    ),
    "deposit_pending_bep20": (
        "✅ <b>Deposit Request Created!</b>\n\n"
        "🔹 Network: <b>USDT BEP20 (BSC)</b>\n\n"
        "💰 Send EXACTLY:\n"
        "<code>{amount}</code> USDT\n\n"
        "📬 To this address:\n"
        "<code>{wallet}</code>\n\n"
        "⚠️ <b>Important:</b> Send exactly this amount — even a tiny difference will prevent confirmation.\n\n"
        "⏰ This request expires in <b>30 minutes</b>.\n"
        "📨 After payment, send <code>/claim TXID</code> or just send the TXID alone."
    ),
    "deposit_confirmed": (
        "🎉 <b>Deposit Confirmed!</b>\n\n"
        "✅ <b>+${amount} USDT</b> added to your balance.\n"
        "🔗 TX: <code>{txhash}</code>"
    ),
    "deposit_already_pending": (
        "⚠️ <b>You already have a pending deposit!</b>\n\n"
        "💰 Amount: <code>{amount}</code> USDT\n"
        "🔹 Network: {network}\n"
        "📬 Address: <code>{wallet}</code>\n\n"
        "⏰ Expires: {expires}\n\n"
        "After payment, send <code>/claim TXID</code> or just send the TXID."
    ),
    "deposit_no_pending": "⚠️ You do not have any active deposit request. Use /deposit first.",
    "claim_usage": "❌ Usage: <code>/claim TXID</code>\nYou can also send the TXID alone.",
    "claim_invalid_txid": "❌ Invalid TXID format. Send the full blockchain transaction hash.",
    "claim_checking": "🔎 Checking your transaction...",
    "claim_expired": "⚠️ This deposit request expired. Please create a new one with /deposit.",
    "claim_already_used": "⚠️ This transaction has already been claimed.",
    "claim_not_found": "❌ I could not find this TXID for your pending {network} deposit to our wallet.",
    "claim_not_confirmed": "⏳ This transaction is not confirmed yet. Try again in a moment.",
    "claim_amount_mismatch": "❌ Amount mismatch.\nExpected: <code>{expected}</code> USDT\nFound: <code>{found}</code> USDT\nOnly an exact match is accepted.",
    "claim_error": "❌ Could not verify this TXID right now. Please try again shortly.",
    "deposit_invalid_amount": "❌ Invalid amount. Please enter a valid number (e.g. <code>10</code> or <code>5.5</code>):",
    "deposit_min_amount": "❌ Minimum deposit is $1. Please enter a higher amount:",
    "deposit_baridimob_info": (
        "🇩🇿 <b>BaridiMob Deposit</b>\n\n"
        "RIB: <code>{rib}</code>\n\n"
        "📌 Rate: 1000 DA = $4 | 630 DA = $2.5\n\n"
        "After payment, send screenshot to /support and admin will credit your balance."
    ),
    "balance_msg": (
        "💰 <b>Your Balance:</b> ${balance}\n"
        "💵 <b>Activate Price:</b> ${price}\n"
        "✅ <b>Successes:</b> {succ}\n"
        "❌ <b>Failed:</b> {fail}"
    ),
    "profile_msg": (
        "👤 <b>Your Profile</b>\n\n"
        "🆔 <b>ID:</b> <code>{uid}</code>\n"
        "👤 <b>Name:</b> {name}\n"
        "💰 <b>Balance:</b> ${balance}\n"
        "💵 <b>Activate Price:</b> ${price}\n"
        "✅ <b>Successes:</b> {succ}\n"
        "❌ <b>Failed:</b> {fail}"
    ),
    "profile_ext_msg": (
        "👤 <b>Your Store Profile</b>\n\n"
        "🆔 <b>ID:</b> <code>{uid}</code>\n"
        "👤 <b>Name:</b> {name}\n"
        "💰 <b>Balance:</b> ${balance}\n"
        "🛍️ <b>Purchases:</b> {orders}"
    ),
    "shop_title": "🛒 <b>Digital Store</b>\nSelect a Category:",
    "shop_empty": "📭 The shop is currently empty.",
    "shop_cat": "📂 <b>Category: {cat}</b>\nSelect a product:",
    "shop_prod_view": (
        "📦 <b>{name}</b>\n\n"
        "📝 {desc}\n\n"
        "💵 Price: ${price}\n"
        "🚚 Delivery: {delivery}\n\n"
        "👇 Click Buy to purchase."
    ),
    "shop_ask_qty": "🔢 <b>How many do you want to buy?</b>\n(Send a number)",
    "prod_buy_confirm": (
        "📝 <b>Confirm Purchase</b>\n"
        "📦 Product: {name}\n"
        "🔢 Quantity: {qty}\n"
        "💵 Total Cost: ${total}\n\n"
        "Type 'yes' to confirm or 'cancel'."
    ),
    "prod_bought": (
        "✅ <b>Purchase Successful!</b>\n"
        "📦 <b>Product:</b> {name}\n"
        "🔢 <b>Qty:</b> {qty}\n"
        "💵 <b>Cost:</b> ${total}"
    ),
    "prod_codes_delivered": "🔐 <b>Your codes:</b>\n\n<code>{codes}</code>",
    "prod_file_delivered": "📎 <b>Your file was delivered automatically.</b>",
    "prod_no_stock": "❌ This product is out of stock or the quantity is unavailable.",
    "prod_no_bal": "❌ You do not have enough shop balance ($) to buy <b>{name}</b>.",
    "buy_cancelled": "🚫 Purchase cancelled.",
    "btn_buy": "🛒 Buy",
    "btn_activate": "⚡ Activate",
    "activate_choose_service": (
        "⚡ <b>Choose Activation Service</b>\n\n"
        "Select the service you want:"
    ),
    "btn_google_one": "⚡ Google One Activation (24/7)",
    "btn_ws_seat": "🤖 ChatGPT Workspace Business Seat",
    "ws_seat_choose_method": (
        "🤖 <b>ChatGPT Workspace Business Seat</b>\n\n"
        "💰 Seat Price: <b>${price}</b>\n"
        "💳 Your Balance: <b>${balance}</b>\n\n"
        "How would you like to join?"
    ),
    "btn_ws_by_key": "🔑 I have an Invite Key",
    "btn_ws_by_balance": "💳 Pay from my Balance (${price})",
    "ws_seat_no_bal": "❌ Insufficient balance. You need <b>${need}</b> more to purchase a Workspace seat.",
    "ws_seat_no_ws": "❌ No Workspaces are available right now. Please try again later.",
    "ws_seat_confirm_bal": (
        "✅ <b>Confirm Purchase</b>\n\n"
        "🤖 ChatGPT Workspace Business Seat\n"
        "💰 Amount: <b>${price}</b>\n"
        "💳 Remaining Balance: <b>${remaining}</b>\n\n"
        "Confirm?"
    ),
    "ws_seat_bought": (
        "✅ <b>Seat Reserved!</b>\n\n"
        "📧 Email: <code>{email}</code>\n"
        "🏢 Workspace: <b>{ws_name}</b>\n\n"
        "⏳ You will be added shortly. We'll notify you."
    ),
    "ws_seat_email_prompt": "📧 Send your <b>email address</b> to be added to the Workspace:",
    "ws_enter_key_prompt": "🔑 Send your <b>Invite Key</b>:",
    "btn_shop": "🛒 Shop",
    "btn_deposit": "💰 Deposit",
    "btn_profile": "👤 Profile",
    "btn_history": "📜 History",
    "btn_daily": "📅 Check-in",
    "btn_check": "💰 Balance",
    "btn_lang": "🌐 Language",
    "btn_help": "❓ Help",
    "btn_support": "📞 Support",
    "btn_invite": "🤝 Invite",
    "btn_back": "🔙 Back",
    "btn_home": "🏠 Home",
    "btn_confirm": "✅ Confirm",
    "btn_cancel": "❌ Cancel",
    "btn_adm_stats": "📊 Stats",
    "btn_adm_bal": "💰 My Wallet",
    "btn_adm_users": "👥 Users",
    "btn_adm_reseller": "💼 Reseller Tools",
    "btn_adm_owner": "👑 Owner Tools",
    "btn_adm_shop": "🛒 Shop Manager",
    "btn_adm_sys": "⚙️ System",
    "btn_adm_data": "📦 Data Backup",
    "btn_adm_external": "🌐 External Shops",
    "support_welcome": "📞 <b>Support Center</b>\n\nPlease describe your issue in one message.",
    "support_sent": "✅ <b>Message Sent!</b> Please wait for a reply.",
    "support_reply": "📩 <b>Admin Reply:</b>\n{msg}",
    "history_title": "📜 <b>Last 5 Activations:</b>\n\n{log}",
    "history_empty": "📭 No history found.",
    "invite_msg_user": (
        "🤝 <b>Invitation Link:</b>\n\n"
        "Share this link with others:\n"
        "<code>https://t.me/{bot}?start={uid}</code>"
    ),
    "balance_added_msg": "💰 <b>Balance Updated!</b>\nAdmin added +${amount} to your account.",
    "balance_removed_msg": "💰 <b>Balance Updated!</b>\nAdmin removed ${amount} from your account.",
    "shop_added_msg": "🛒 <b>Shop Wallet Updated!</b>\nAdmin added +${amount} to your shop wallet.",
    "shop_removed_msg": "🛒 <b>Shop Wallet Updated!</b>\nAdmin removed ${amount} from your shop wallet.",
    "reseller_notify": (
        "✅ <b>Activation successful for your client!</b>\n\n"
        "👤 User: <code>{uid}</code>\n"
        "📧 <code>{email}</code>\n"
        "🆔 Job ID: <code>{job_id}</code>\n"
        "🧾 TX: <code>#{tx}</code>\n"
        "💵 Amount: ${amount}\n"
        "💰 Your profit: +${profit}"
    ),
    "owner_notify": (
        "🔔 <b>Activation Notification</b>\n\n"
        "🧾 Transaction: <code>#{tx}</code>\n"
        "👤 User: <code>{uid}</code>\n"
        "📧 Email: <code>{email}</code>\n"
        "💵 Amount: ${amount}\n"
        "🔗 Link: {url}"
    ),
}

LANGS = {"en": EN_TEXTS}

HELP_SETS = {
    "main_user": {
        "en": [
            ("/start", "Open the main menu."),
            ("/activate", "Activate Google One with your account."),
            ("/shop", "Browse products and buy with your shop wallet."),
            ("/profile", "View your balance and stats."),
            ("/deposit", "Create a deposit request."),
            ("/claim TXID", "Confirm a paid deposit by transaction hash."),
            ("/invite", "Get your referral link."),
            ("/daily", "Claim your daily bonus."),
            ("/history", "View your activation history."),
            ("/language", "Change bot language."),
            ("/help", "Show this command guide."),
            ("/support", "Send a support ticket."),
        ],
        "ar": [
            ("/start", ""),
            ("/activate", "Activate Google One with your account."),
            ("/shop", ""),
            ("/profile", ""),
            ("/deposit", ""),
            ("/invite", ""),
            ("/daily", ""),
            ("/history", ""),
            ("/language", ""),
            ("/help", ""),
            ("/support", ""),
        ],
    },
    "external_user": {
        "en": [
            ("/start", "Open the store home."),
            ("/shop", "Browse products and buy."),
            ("/profile", "View your wallet and purchase count."),
            ("/language", "Change bot language."),
            ("/help", "Show this command guide."),
            ("/support", "Send a support ticket."),
        ],
        "ar": [
            ("/start", ""),
            ("/shop", ""),
            ("/profile", ""),
            ("/language", ""),
            ("/help", ""),
            ("/support", ""),
        ],
    },
    "admin_owner": {
        "en": [
            ("/start", "Open owner control panel."),
            ("/help", "Show owner commands."),
            ("/add", "Add $ balance to a user."),
            ("/remove", "Remove $ balance from a user."),
            ("/addshop", "Add main-shop wallet $ to a user."),
            ("/removeshop", "Remove main-shop wallet $ from a user."),
            ("/check", "Check user details."),
            ("/addreseller", "Promote a user to reseller."),
            ("/delreseller", "Remove reseller role."),
            ("/setprice", "Set global activation price."),
            ("/setwsprice", "Set ChatGPT Workspace monthly seat price."),
            ("/addrc", "Add reseller wallet."),
            ("/removerc", "Remove reseller wallet."),
            ("/setprofit", "Set reseller profit per activation."),
            ("/rusers", "View reseller clients."),
            ("/uinvites", "View invites of a user + channel subscription stats."),
            ("/rlink", "Link user to reseller."),
            ("/runlink", "Unlink user."),
            ("/addprod", "Add main shop product."),
            ("/delprod", "Delete main shop product."),
            ("/listprod", "List main shop products."),
            ("/addcode", "Add one instant-delivery code to a product."),
            ("/addcodes", "Add multiple instant-delivery codes to a product."),
            ("/addextshop", "Create external shop step by step."),
            ("/delextshop", "Delete an external shop."),
            ("/listextshops", "List external shops."),
            ("/broadcast", "Broadcast to main bot users."),
            ("/broadcast_inactive", "Broadcast to inactive users."),
            ("/maintenance", "Toggle activation maintenance."),
            ("/ban", "Ban a user."),
            ("/unban", "Unban a user."),
            ("/reply", "Reply to a support ticket."),
            ("/resellers", "View resellers report."),
            ("/myinvite", "Your invite link."),
            ("/language", "Change language."),
        ],
        "ar": [
            ("/start", ""),
            ("/help", ""),
            ("/add", ""),
            ("/remove", ""),
            ("/addshop", ""),
            ("/removeshop", ""),
            ("/check", ""),
            ("/addreseller", ""),
            ("/delreseller", ""),
            ("/setprice", ""),
            ("/setwsprice", "Set ChatGPT Workspace seat price (monthly)."),
            ("/addrc", ""),
            ("/removerc", ""),
            ("/setprofit", ""),
            ("/rusers", ""),
            ("/uinvites", ""),
            ("/rlink", ""),
            ("/runlink", ""),
            ("/addprod", ""),
            ("/delprod", ""),
            ("/listprod", ""),
            ("/addcode", ""),
            ("/addcodes", ""),
            ("/addextshop", ""),
            ("/delextshop", ""),
            ("/listextshops", ""),
            ("/broadcast", ""),
            ("/broadcast_inactive", ""),
            ("/maintenance", ""),
            ("/ban", ""),
            ("/unban", ""),
            ("/reply", ""),
            ("/resellers", ""),
            ("/myinvite", ""),
            ("/language", ""),
        ],
    },
    "admin_reseller": {
        "en": [
            ("/start", "Open reseller panel."),
            ("/help", "Show commands."),
            ("/myinvite", "My invite link."),
            ("/add", "Add balance to a user."),
            ("/remove", "Remove balance from a user."),
            ("/check", "Check user details."),
            ("/resellers", "My report."),
            ("/language", "Change language."),
        ],
        "ar": [
            ("/start", ""),
            ("/help", ""),
            ("/myinvite", ""),
            ("/add", ""),
            ("/remove", ""),
            ("/check", ""),
            ("/resellers", ""),
            ("/language", ""),
        ],
    },
    "ext_admin": {
        "en": [
            ("/start", "Store control."),
            ("/help", "Help."),
            ("/addshop", "Add wallet $"),
            ("/removeshop", "Remove wallet $"),
            ("/check", "Check user"),
            ("/addprod", "Add product"),
            ("/delprod", "Delete product"),
            ("/listprod", "List products"),
            ("/addcode", "Add one code"),
            ("/addcodes", "Add bulk codes"),
            ("/reply", "Reply ticket"),
            ("/broadcast", "Broadcast"),
            ("/settitle", "Set title"),
        ],
        "ar": [
            ("/start", ""),
            ("/help", ""),
            ("/addshop", ""),
            ("/removeshop", ""),
            ("/check", ""),
            ("/addprod", ""),
            ("/delprod", ""),
            ("/listprod", ""),
            ("/addcode", ""),
            ("/addcodes", ""),
            ("/reply", ""),
            ("/broadcast", ""),
            ("/settitle", ""),
        ],
    },
}


# =========================
# DATABASE
# =========================
def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db_connect()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            balance REAL DEFAULT 0.0,
            shop_balance REAL DEFAULT 0.0,
            last_daily TEXT,
            referrer_id INTEGER,
            lang TEXT DEFAULT 'en',
            is_banned INTEGER DEFAULT 0,
            owner_id INTEGER DEFAULT 0,
            last_activity DATETIME,
            success_count INTEGER DEFAULT 0,
            fail_count INTEGER DEFAULT 0,
            first_name TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            email TEXT,
            status TEXT,
            url TEXT,
            reason TEXT,
            tx_id TEXT,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS deposits (
            txid TEXT PRIMARY KEY,
            user_id INTEGER,
            amount REAL,
            network TEXT,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS pending_deposits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            network TEXT,
            expected_amount REAL,
            base_amount REAL,
            wallet_address TEXT,
            status TEXT DEFAULT 'pending',
            tx_hash TEXT DEFAULT '',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            expires_at DATETIME
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS resellers (
            user_id INTEGER PRIMARY KEY,
            balance REAL DEFAULT 0.0,
            total_sold INTEGER DEFAULT 0,
            profit_per_activation REAL DEFAULT 0.5,
            amount_paid REAL DEFAULT 0.0
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shop_id INTEGER DEFAULT 0,
            name TEXT,
            price REAL DEFAULT 0.0,
            stock INTEGER DEFAULT 0,
            description TEXT DEFAULT '',
            category TEXT DEFAULT 'General',
            file_id TEXT DEFAULT NULL,
            image_id TEXT DEFAULT NULL,
            delivery_type TEXT DEFAULT 'manual',
            auto_delivery INTEGER DEFAULT 0
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS product_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shop_id INTEGER DEFAULT 0,
            product_id INTEGER NOT NULL,
            code_text TEXT NOT NULL,
            is_sold INTEGER DEFAULT 0,
            sold_to INTEGER DEFAULT NULL,
            sold_at DATETIME DEFAULT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(shop_id, product_id, code_text, is_sold)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shop_id INTEGER DEFAULT 0,
            user_id INTEGER,
            product_id INTEGER,
            input_data TEXT,
            price REAL,
            qty INTEGER DEFAULT 1,
            delivery_data TEXT DEFAULT '',
            ts DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shop_id INTEGER DEFAULT 0,
            user_id INTEGER,
            status TEXT DEFAULT 'open',
            bot_token TEXT DEFAULT '',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS external_shops (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shop_token TEXT UNIQUE,
            admin_token TEXT UNIQUE,
            owner_id INTEGER NOT NULL,
            title TEXT DEFAULT 'External Shop',
            shop_username TEXT DEFAULT '',
            admin_username TEXT DEFAULT '',
            is_active INTEGER DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS shop_wallets (
            shop_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            balance REAL DEFAULT 0.0,
            PRIMARY KEY (shop_id, user_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS shop_users (
            shop_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT DEFAULT '',
            first_name TEXT DEFAULT '',
            lang TEXT DEFAULT 'en',
            last_activity DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (shop_id, user_id)
        )
    """)

    # ── ChatGPT Workspace tables ──────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS chatgpt_workspaces (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            url TEXT DEFAULT '',
            session_file TEXT DEFAULT '',
            personal_session_file TEXT DEFAULT '',
            organization_id TEXT DEFAULT '',
            account_id TEXT DEFAULT '',
            max_invites INTEGER DEFAULT 5,
            status TEXT DEFAULT 'active',
            expires_at TEXT DEFAULT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Migration: add account_id column if not exists
    try:
        c.execute("ALTER TABLE chatgpt_workspaces ADD COLUMN account_id TEXT DEFAULT ''")
    except Exception:
        pass
    c.execute("""
        CREATE TABLE IF NOT EXISTS chatgpt_invite_keys (
            id TEXT PRIMARY KEY,
            code TEXT UNIQUE NOT NULL,
            workspace_id TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            expires_at TEXT NOT NULL,
            created_by INTEGER NOT NULL,
            used_by_email TEXT DEFAULT NULL,
            used_at TEXT DEFAULT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS chatgpt_requests (
            id TEXT PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            email TEXT NOT NULL,
            normalized_email TEXT NOT NULL,
            invite_code TEXT NOT NULL,
            telegram_user_id INTEGER NOT NULL,
            telegram_username TEXT DEFAULT NULL,
            status TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            last_error TEXT DEFAULT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            authorized_at TEXT DEFAULT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS chatgpt_protected_members (
            id TEXT PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            email TEXT NOT NULL,
            normalized_email TEXT NOT NULL,
            role TEXT DEFAULT 'owner',
            reason TEXT DEFAULT 'manual',
            active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS chatgpt_workspace_members (
            id TEXT PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            email TEXT NOT NULL,
            normalized_email TEXT NOT NULL,
            member_id TEXT DEFAULT NULL,
            role TEXT DEFAULT 'member',
            cached_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(workspace_id, normalized_email)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS chatgpt_subscriptions (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            email TEXT NOT NULL,
            normalized_email TEXT NOT NULL,
            workspace_id TEXT NOT NULL,
            request_id TEXT DEFAULT NULL,
            subscription_hours INTEGER NOT NULL DEFAULT 720,
            activated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            expires_at TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS active_jobs_db (
            job_id TEXT PRIMARY KEY,
            uid INTEGER NOT NULL,
            email TEXT NOT NULL,
            cost REAL NOT NULL,
            reseller_id INTEGER NOT NULL DEFAULT 0,
            tx_id TEXT NOT NULL,
            submitted_at REAL NOT NULL,
            status_msg_id INTEGER DEFAULT 0,
            estimated_wait REAL DEFAULT 0
        )
    """)

    migrations = [
        "ALTER TABLE users ADD COLUMN balance REAL DEFAULT 0.0",
        "ALTER TABLE users ADD COLUMN shop_balance REAL DEFAULT 0.0",
        "ALTER TABLE users ADD COLUMN first_name TEXT",
        "ALTER TABLE products ADD COLUMN shop_id INTEGER DEFAULT 0",
        "ALTER TABLE products ADD COLUMN description TEXT DEFAULT ''",
        "ALTER TABLE products ADD COLUMN category TEXT DEFAULT 'General'",
        "ALTER TABLE products ADD COLUMN image_id TEXT DEFAULT NULL",
        "ALTER TABLE products ADD COLUMN delivery_type TEXT DEFAULT 'manual'",
        "ALTER TABLE products ADD COLUMN auto_delivery INTEGER DEFAULT 0",
        "ALTER TABLE purchases ADD COLUMN shop_id INTEGER DEFAULT 0",
        "ALTER TABLE purchases ADD COLUMN qty INTEGER DEFAULT 1",
        "ALTER TABLE purchases ADD COLUMN delivery_data TEXT DEFAULT ''",
        "ALTER TABLE tickets ADD COLUMN shop_id INTEGER DEFAULT 0",
        "ALTER TABLE tickets ADD COLUMN bot_token TEXT DEFAULT ''",
        "ALTER TABLE resellers ADD COLUMN profit_per_activation REAL DEFAULT 0.5",
        "ALTER TABLE resellers ADD COLUMN amount_paid REAL DEFAULT 0.0",
        "ALTER TABLE chatgpt_requests ADD COLUMN paid_amount REAL DEFAULT 0.0",
        "ALTER TABLE chatgpt_workspaces ADD COLUMN account_id TEXT DEFAULT ''",
        "ALTER TABLE active_jobs_db ADD COLUMN status_msg_id INTEGER DEFAULT 0",
        "ALTER TABLE active_jobs_db ADD COLUMN estimated_wait REAL DEFAULT 0",
        "ALTER TABLE history ADD COLUMN email TEXT DEFAULT ''",
        "ALTER TABLE history ADD COLUMN tx_id TEXT DEFAULT ''",
        "ALTER TABLE history ADD COLUMN reason TEXT DEFAULT ''",
        "ALTER TABLE pending_deposits ADD COLUMN expected_amount_str TEXT DEFAULT ''",
        "ALTER TABLE chatgpt_invite_keys ADD COLUMN subscription_hours INTEGER DEFAULT 720",
        "ALTER TABLE chatgpt_workspaces ADD COLUMN chatgpt_totp_secret TEXT DEFAULT ''",
    ]
    for q in migrations:
        try:
            c.execute(q)
        except Exception:
            pass

    # Backfill exact expected amount strings for legacy pending deposits
    try:
        rows = c.execute("SELECT id, network, expected_amount FROM pending_deposits WHERE COALESCE(expected_amount_str, '')='' ").fetchall()
        for row in rows:
            c.execute(
                "UPDATE pending_deposits SET expected_amount_str=? WHERE id=?",
                (format_amount_for_network(row[2] or 0, row[1] or 'TRC20', trim=False), row[0])
            )
    except Exception:
        pass

    # Migrate p_credits -> balance for existing databases (ONE-TIME only)
    try:
        already_migrated = c.execute("SELECT value FROM config WHERE key='p_credits_migrated'").fetchone()
        if not already_migrated:
            cols = [r[1] for r in c.execute("PRAGMA table_info(users)").fetchall()]
            if "p_credits" in cols and "balance" in cols:
                c.execute("UPDATE users SET balance=COALESCE(p_credits,0.0) WHERE balance=0.0 AND p_credits IS NOT NULL AND p_credits > 0")
            c.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('p_credits_migrated', '1')")
    except Exception:
        pass

    # Retroactively link users invited by resellers to owner_id (ONE-TIME)
    try:
        already_linked = c.execute("SELECT value FROM config WHERE key='reseller_owner_linked'").fetchone()
        if not already_linked:
            c.execute("""
                UPDATE users
                SET owner_id = referrer_id
                WHERE referrer_id IS NOT NULL
                  AND referrer_id != 0
                  AND (owner_id IS NULL OR owner_id = 0)
                  AND referrer_id IN (SELECT user_id FROM resellers)
            """)
            c.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('reseller_owner_linked', '1')")
    except Exception:
        pass

    # ONE-TIME: link ALL users inserted after position 271 to reseller 6914433826
    # v2: applies to everyone after row 271 regardless of existing owner_id
    LEGACY_RESELLER_ID = 6914433826
    try:
        already_done = c.execute("SELECT value FROM config WHERE key='legacy_reseller_link_6914433826_v2'").fetchone()
        if not already_done:
            # Ensure reseller exists in users and resellers tables
            c.execute("INSERT OR IGNORE INTO users (user_id, lang) VALUES (?, 'en')", (LEGACY_RESELLER_ID,))
            c.execute("INSERT OR IGNORE INTO resellers (user_id) VALUES (?)", (LEGACY_RESELLER_ID,))
            # Force-link ALL users inserted after the first 271 rows (including those with existing owner_id)
            c.execute("""
                UPDATE users
                SET owner_id = ?
                WHERE user_id != ?
                  AND user_id IN (
                      SELECT user_id FROM users ORDER BY rowid LIMIT -1 OFFSET 271
                  )
            """, (LEGACY_RESELLER_ID, LEGACY_RESELLER_ID))
            c.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('legacy_reseller_link_6914433826_v2', '1')")
    except Exception:
        pass

    c.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('activate_price', ?)", (str(DEFAULT_ACTIVATE_PRICE),))
    c.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('reseller_profit', ?)", (str(DEFAULT_RESELLER_PROFIT),))
    c.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('ws_seat_price', ?)", (str(DEFAULT_WS_SEAT_PRICE),))
    c.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('ws_monthly_price', ?)", (str(DEFAULT_WS_MONTHLY_PRICE),))
    c.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('maintenance', '0')")

    conn.commit()
    conn.close()


# =========================
# CONTEXT HELPERS
# =========================
def current_shop_id(context: ContextTypes.DEFAULT_TYPE) -> int:
    return int(context.bot_data.get("shop_id", 0))

def current_bot_mode(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.bot_data.get("bot_mode", "main_user")

def current_external_owner_id(context: ContextTypes.DEFAULT_TYPE) -> int:
    return int(context.bot_data.get("external_owner_id", 0))

def current_external_title(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.bot_data.get("external_title", "External Shop")

def current_external_store_token(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.bot_data.get("external_store_token", "")

def current_external_admin_token(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.bot_data.get("external_admin_token", "")


# =========================
# CONFIG DB
# =========================
def get_config_float(key: str, default: float) -> float:
    conn = db_connect()
    row = conn.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
    conn.close()
    return float(row["value"]) if row and row["value"] is not None else float(default)

def set_config(key: str, value):
    conn = db_connect()
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()

def get_activate_price() -> float:
    return get_config_float("activate_price", DEFAULT_ACTIVATE_PRICE)

def set_activate_price(price: float):
    set_config("activate_price", price)

def get_ws_seat_price() -> float:
    return get_config_float("ws_seat_price", DEFAULT_WS_SEAT_PRICE)

def set_ws_seat_price(price: float):
    set_config("ws_seat_price", price)

def get_ws_monthly_price() -> float:
    return get_config_float("ws_monthly_price", DEFAULT_WS_MONTHLY_PRICE)

def set_ws_monthly_price(price: float):
    set_config("ws_monthly_price", price)

def ws_calc_price(duration_hours: int) -> float:
    """Calculate price for a given duration based on monthly rate."""
    monthly = get_ws_monthly_price()
    return round(monthly * duration_hours / 720.0, 2)

def get_reseller_profit() -> float:
    return get_config_float("reseller_profit", DEFAULT_RESELLER_PROFIT)

def set_reseller_profit(profit: float):
    set_config("reseller_profit", profit)

def get_maintenance_data():
    conn = db_connect()
    status = conn.execute("SELECT value FROM config WHERE key='maintenance'").fetchone()
    conn.close()
    return bool(status and status["value"] == "1")

def set_maintenance_mode(state: bool):
    conn = db_connect()
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('maintenance', ?)", ("1" if state else "0",))
    conn.commit()
    conn.close()


# =========================
# USER DB
# =========================
def get_user_lang(user_id: int) -> str:
    try:
        conn = db_connect()
        row = conn.execute("SELECT COALESCE(lang, 'en') AS lang FROM users WHERE user_id=?", (user_id,)).fetchone()
        conn.close()
        if row and (row["lang"] or "").strip():
            return (row["lang"] or "en").strip().lower()
    except Exception:
        pass
    return "en"

def t(user_id: int, key: str, **kwargs):
    lang = get_user_lang(user_id)
    d = LANGS.get(lang, LANGS["en"])
    template = d.get(key, LANGS["en"].get(key, key))
    fmt = {}
    for k, v in kwargs.items():
        if isinstance(v, float):
            fmt[k] = "{:.2f}".format(v)
        else:
            fmt[k] = v
    try:
        return template.format(**fmt)
    except Exception:
        return template

def help_text_for(user_id: int, key: str) -> str:
    lang = get_user_lang(user_id)
    items = HELP_SETS.get(key, {}).get("en", [])
    titles = {
        "main_user": "📚 <b>User Commands</b>",
        "external_user": "📚 <b>Store Commands</b>",
        "admin_owner": "👑 <b>Owner Commands</b>",
        "admin_reseller": "💼 <b>Reseller Commands</b>",
        "ext_admin": "🌐 <b>Store Control Commands</b>",
    }
    title = titles.get(key, "📚 <b>Commands</b>")
    lines = [title, ""]
    for cmd_name, desc in items:
        lines.append(f"<code>{cmd_name}</code> - {desc}")
    return "\n".join(lines)

def get_total_users() -> int:
    conn = db_connect()
    row = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()
    conn.close()
    return int(row["c"] if row else 0)

def get_all_users():
    conn = db_connect()
    rows = conn.execute("SELECT user_id FROM users").fetchall()
    conn.close()
    return [int(r["user_id"]) for r in rows]

def get_all_users_detailed():
    conn = db_connect()
    rows = conn.execute("""
        SELECT u.user_id, u.username, u.balance,
               COALESCE(u.success_count, 0) AS activations,
               (SELECT COUNT(*) FROM users inv WHERE inv.referrer_id = u.user_id) AS invites
        FROM users u
        ORDER BY u.rowid ASC
    """).fetchall()
    conn.close()
    return [
        {
            "user_id": int(r["user_id"]),
            "username": r["username"] or "No Username",
            "balance": float(r["balance"] or 0),
            "activations": int(r["activations"] or 0),
            "invites": int(r["invites"] or 0),
        }
        for r in rows
    ]

def update_user_info(user_id: int, username: str, first_name: str = "") -> bool:
    conn = db_connect()
    exists = conn.execute("SELECT 1 FROM users WHERE user_id=?", (user_id,)).fetchone()
    if not exists:
        conn.execute(
            "INSERT INTO users (user_id, username, first_name, last_activity) VALUES (?, ?, ?, datetime('now'))",
            (user_id, username or "", first_name or "")
        )
        new_user = True
    else:
        conn.execute(
            "UPDATE users SET username=?, first_name=?, last_activity=datetime('now') WHERE user_id=?",
            (username or "", first_name or "", user_id)
        )
        new_user = False
    conn.commit()
    conn.close()
    return new_user

def update_shop_user_info(shop_id: int, user_id: int, username: str, first_name: str = "", lang: str = "en"):
    if shop_id <= 0:
        return
    conn = db_connect()
    conn.execute("""
        INSERT INTO shop_users (shop_id, user_id, username, first_name, lang, last_activity)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(shop_id, user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            lang=excluded.lang,
            last_activity=datetime('now')
    """, (shop_id, user_id, username or "", first_name or "", lang or "en"))
    conn.commit()
    conn.close()

def get_user_data(user_id: int) -> dict:
    conn = db_connect()
    conn.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
    conn.commit()
    # Check if legacy p_credits column exists
    cols = [r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
    if "p_credits" in cols:
        balance_expr = "COALESCE(balance, COALESCE(p_credits, 0.0)) AS balance"
    else:
        balance_expr = "COALESCE(balance, 0.0) AS balance"
    row = conn.execute(f"""
        SELECT
            {balance_expr},
            COALESCE(shop_balance, 0.0) AS shop_balance,
            COALESCE(lang, 'en') AS lang,
            COALESCE(is_banned, 0) AS is_banned,
            COALESCE(owner_id, 0) AS owner_id,
            last_daily,
            COALESCE(success_count, 0) AS success_count,
            COALESCE(fail_count, 0) AS fail_count,
            COALESCE(first_name, '') AS first_name,
            COALESCE(username, '') AS username,
            COALESCE(referrer_id, 0) AS referrer_id
        FROM users
        WHERE user_id=?
    """, (user_id,)).fetchone()
    conn.close()
    return dict(row) if row else {
        "balance": 0.0, "shop_balance": 0.0, "lang": "en",
        "is_banned": 0, "owner_id": 0, "last_daily": None,
        "success_count": 0, "fail_count": 0, "first_name": "", "username": "", "referrer_id": 0
    }

def is_user_banned(user_id: int) -> bool:
    return int(get_user_data(user_id).get("is_banned", 0)) == 1

def increment_stats(user_id: int, success: bool = True):
    conn = db_connect()
    if success:
        conn.execute("UPDATE users SET success_count=COALESCE(success_count,0)+1 WHERE user_id=?", (user_id,))
    else:
        conn.execute("UPDATE users SET fail_count=COALESCE(fail_count,0)+1 WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()

def set_last_checkin(user_id: int, date_str: str):
    conn = db_connect()
    conn.execute("UPDATE users SET last_daily=? WHERE user_id=?", (date_str, user_id))
    conn.commit()
    conn.close()

def get_id_by_username(username: str):
    conn = db_connect()
    uname = username.replace("@", "").strip()
    row = conn.execute("SELECT user_id FROM users WHERE username LIKE ?", (uname,)).fetchone()
    conn.close()
    return int(row["user_id"]) if row else None

def ban_user(user_id: int, status: int = 1):
    conn = db_connect()
    conn.execute("UPDATE users SET is_banned=? WHERE user_id=?", (int(status), user_id))
    conn.commit()
    conn.close()

def set_lang(user_id: int, lang_code: str):
    conn = db_connect()
    conn.execute("UPDATE users SET lang=? WHERE user_id=?", (lang_code, user_id))
    conn.commit()
    conn.close()

def add_balance(user_id: int, amount: float = 0.0):
    conn = db_connect()
    conn.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
    if amount != 0:
        conn.execute("UPDATE users SET balance=MAX(0, COALESCE(balance,0)+?) WHERE user_id=?", (float(amount), user_id))
    conn.commit()
    conn.close()

def get_user_balance(user_id: int) -> float:
    return float(get_user_data(user_id).get("balance", 0.0))

def get_shop_balance(user_id: int, shop_id: int = 0) -> float:
    # All shops (main and external) use the unified balance
    return get_user_balance(user_id)

def add_shop_balance(user_id: int, amount: float = 0.0, shop_id: int = 0):
    # All shops (main and external) use the unified balance
    add_balance(user_id, amount)
    if shop_id != 0:
        # Still register the user in shop_users for tracking purposes
        conn = db_connect()
        conn.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        conn.execute("INSERT OR IGNORE INTO shop_users (shop_id, user_id) VALUES (?, ?)", (shop_id, user_id))
        conn.commit()
        conn.close()

def bind_referrer(user_id: int, referrer_id: int) -> bool:
    if not referrer_id or referrer_id == user_id:
        return False
    conn = db_connect()
    row = conn.execute("SELECT COALESCE(referrer_id,0) AS rid FROM users WHERE user_id=?", (user_id,)).fetchone()
    if row and int(row["rid"] or 0) != 0:
        conn.close()
        return False
    conn.execute("UPDATE users SET referrer_id=? WHERE user_id=?", (referrer_id, user_id))
    conn.commit()
    conn.close()
    return True

def get_stats():
    conn = db_connect()
    users = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
    deps = conn.execute("SELECT COALESCE(SUM(amount),0) AS s FROM deposits").fetchone()["s"]
    resellers = conn.execute("SELECT COUNT(*) AS c FROM resellers").fetchone()["c"]
    conn.close()
    return int(users), float(deps), int(resellers)

def get_user_history(user_id: int):
    conn = db_connect()
    rows = conn.execute("SELECT email, status, url, reason, tx_id, ts FROM history WHERE user_id=? ORDER BY id DESC LIMIT 5", (user_id,)).fetchall()
    conn.close()
    return rows


# =========================
# RESELLER DB
# =========================
def is_reseller(user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    conn = db_connect()
    row = conn.execute("SELECT 1 FROM resellers WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return row is not None

def get_reseller_balance(user_id: int) -> float:
    conn = db_connect()
    row = conn.execute("SELECT COALESCE(balance,0) AS b FROM resellers WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return float(row["b"]) if row else 0.0

def add_reseller_balance(user_id: int, amount: float):
    conn = db_connect()
    conn.execute("INSERT OR IGNORE INTO resellers (user_id) VALUES (?)", (user_id,))
    conn.execute("UPDATE resellers SET balance=COALESCE(balance,0)+? WHERE user_id=?", (float(amount), user_id))
    conn.commit()
    conn.close()

def get_reseller_stats(reseller_id: int) -> int:
    conn = db_connect()
    row = conn.execute("SELECT COUNT(*) AS c FROM users WHERE owner_id=?", (reseller_id,)).fetchone()
    conn.close()
    return int(row["c"]) if row else 0

def get_reseller_clients(reseller_id: int):
    conn = db_connect()
    rows = conn.execute("SELECT user_id, username FROM users WHERE owner_id=?", (reseller_id,)).fetchall()
    conn.close()
    return [(int(r["user_id"]), r["username"]) for r in rows]

def get_reseller_clients_detailed(reseller_id: int):
    conn = db_connect()
    rows = conn.execute("""
        SELECT u.user_id, u.username, u.balance,
               COALESCE(u.success_count, 0) AS activations,
               (SELECT COUNT(*) FROM users inv WHERE inv.referrer_id = u.user_id) AS invites
        FROM users u
        WHERE u.owner_id = ?
        ORDER BY u.user_id ASC
    """, (reseller_id,)).fetchall()
    conn.close()
    return [
        {
            "user_id": int(r["user_id"]),
            "username": r["username"] or "No Username",
            "balance": float(r["balance"] or 0),
            "activations": int(r["activations"] or 0),
            "invites": int(r["invites"] or 0),
        }
        for r in rows
    ]

def get_user_invitees(user_id: int):
    conn = db_connect()
    rows = conn.execute("""
        SELECT user_id, username, COALESCE(success_count,0) AS activations
        FROM users
        WHERE referrer_id = ?
        ORDER BY rowid ASC
    """, (user_id,)).fetchall()
    conn.close()
    return [
        {
            "user_id": int(r["user_id"]),
            "username": r["username"] or "No Username",
            "activations": int(r["activations"] or 0),
        }
        for r in rows
    ]

def delete_reseller(user_id: int):
    conn = db_connect()
    conn.execute("DELETE FROM resellers WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()

def set_user_owner(user_id: int, owner_id: int) -> bool:
    if user_id == owner_id:
        return False
    conn = db_connect()
    conn.execute("UPDATE users SET owner_id=? WHERE user_id=?", (owner_id, user_id))
    conn.commit()
    conn.close()
    return True

def reseller_give_balance(reseller_id: int, customer_id: int, amount: float):
    conn = db_connect()
    bal = conn.execute("SELECT COALESCE(balance,0) AS b FROM resellers WHERE user_id=?", (reseller_id,)).fetchone()
    if not bal or float(bal["b"]) < amount:
        conn.close()
        return False, "⚠️ Insufficient Reseller Balance."
    conn.execute("UPDATE resellers SET balance=balance-?, total_sold=COALESCE(total_sold,0)+? WHERE user_id=?", (float(amount), float(amount), reseller_id))
    conn.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (customer_id,))
    conn.execute("UPDATE users SET balance=COALESCE(balance,0)+?, owner_id=CASE WHEN COALESCE(owner_id,0)=0 THEN ? ELSE owner_id END WHERE user_id=?", (float(amount), reseller_id, customer_id))
    conn.commit()
    conn.close()
    return True, "Success"

def reseller_remove_balance(reseller_id: int, customer_id: int, amount: float):
    conn = db_connect()
    row = conn.execute("SELECT COALESCE(owner_id,0) AS owner_id, COALESCE(balance,0) AS b FROM users WHERE user_id=?", (customer_id,)).fetchone()
    if not row:
        conn.close()
        return False, "User not found."
    if int(row["owner_id"] or 0) != reseller_id and reseller_id != OWNER_ID:
        conn.close()
        return False, "⛔ Not your user!"
    if float(row["b"] or 0) < amount:
        conn.close()
        return False, "Insufficient balance."
    conn.execute("UPDATE users SET balance=MAX(0,balance-?) WHERE user_id=?", (float(amount), customer_id))
    conn.execute("UPDATE resellers SET balance=COALESCE(balance,0)+? WHERE user_id=?", (float(amount), reseller_id))
    conn.commit()
    conn.close()
    return True, "Success"

def get_activate_price_for_user(user_id: int) -> float:
    return get_activate_price()


# =========================
# SHOP / PRODUCT LOGIC
# =========================
def sync_product_stock_from_codes(shop_id: int, product_id: int):
    conn = db_connect()
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM product_codes WHERE shop_id=? AND product_id=? AND is_sold=0",
        (shop_id, product_id)
    ).fetchone()
    available = int(row["c"]) if row else 0
    conn.execute(
        "UPDATE products SET stock=?, delivery_type='codes', auto_delivery=1 WHERE id=? AND shop_id=?",
        (available, product_id, shop_id)
    )
    conn.commit()
    conn.close()

def add_product_db(shop_id: int, name: str, price: float, stock: int, category: str = "General", desc: str = "", file_id: str = None, image_id: str = None):
    delivery_type = "file" if file_id else "manual"
    auto_delivery = 1 if file_id else 0
    conn = db_connect()
    conn.execute("""
        INSERT INTO products (shop_id, name, price, stock, category, description, file_id, image_id, delivery_type, auto_delivery)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (shop_id, name, float(price), int(stock), category or "General", desc or "", file_id, image_id, delivery_type, auto_delivery))
    conn.commit()
    conn.close()

def del_product(shop_id: int, product_id: int):
    conn = db_connect()
    conn.execute("DELETE FROM product_codes WHERE shop_id=? AND product_id=?", (shop_id, product_id))
    conn.execute("DELETE FROM products WHERE id=? AND shop_id=?", (product_id, shop_id))
    conn.commit()
    conn.close()

def get_product(shop_id: int, product_id: int):
    conn = db_connect()
    row = conn.execute("""
        SELECT id, shop_id, name, price, stock, file_id, description, image_id, category, delivery_type, auto_delivery
        FROM products WHERE id=? AND shop_id=?
    """, (product_id, shop_id)).fetchone()
    conn.close()
    return dict(row) if row else None

def get_all_products(shop_id: int):
    conn = db_connect()
    rows = conn.execute("""
        SELECT id, name, price, stock, category, delivery_type, auto_delivery
        FROM products WHERE shop_id=? ORDER BY category, name
    """, (shop_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_categories(shop_id: int):
    conn = db_connect()
    rows = conn.execute("SELECT DISTINCT category FROM products WHERE shop_id=? ORDER BY category", (shop_id,)).fetchall()
    conn.close()
    return [r["category"] for r in rows]

def get_products_by_cat(shop_id: int, cat: str):
    conn = db_connect()
    rows = conn.execute("""
        SELECT id, name, price, stock, delivery_type, auto_delivery
        FROM products WHERE shop_id=? AND category=? ORDER BY name
    """, (shop_id, cat)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def reduce_stock(shop_id: int, product_id: int, qty: int = 1):
    conn = db_connect()
    conn.execute("UPDATE products SET stock=MAX(0, stock-?) WHERE id=? AND shop_id=?", (int(qty), product_id, shop_id))
    conn.commit()
    conn.close()

def record_purchase(shop_id: int, user_id: int, product_id: int, price: float, qty: int, input_data: str, delivery_data: str = ""):
    conn = db_connect()
    conn.execute("""
        INSERT INTO purchases (shop_id, user_id, product_id, input_data, price, qty, delivery_data)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (shop_id, user_id, product_id, input_data, float(price), int(qty), delivery_data or ""))
    conn.commit()
    conn.close()

def get_purchase_count(user_id: int, shop_id: int = 0) -> int:
    conn = db_connect()
    row = conn.execute("SELECT COUNT(*) AS c FROM purchases WHERE user_id=? AND shop_id=?", (user_id, shop_id)).fetchone()
    conn.close()
    return int(row["c"]) if row else 0

def get_shop_user_count(shop_id: int) -> int:
    if shop_id == 0:
        return get_total_users()
    conn = db_connect()
    row = conn.execute("SELECT COUNT(*) AS c FROM shop_users WHERE shop_id=?", (shop_id,)).fetchone()
    conn.close()
    return int(row["c"]) if row else 0

def get_shop_product_count(shop_id: int) -> int:
    conn = db_connect()
    row = conn.execute("SELECT COUNT(*) AS c FROM products WHERE shop_id=?", (shop_id,)).fetchone()
    conn.close()
    return int(row["c"]) if row else 0

def add_product_code(shop_id: int, product_id: int, code_text: str):
    conn = db_connect()
    conn.execute("""
        INSERT OR IGNORE INTO product_codes (shop_id, product_id, code_text, is_sold)
        VALUES (?, ?, ?, 0)
    """, (shop_id, product_id, code_text.strip()))
    conn.commit()
    conn.close()
    sync_product_stock_from_codes(shop_id, product_id)

def add_product_codes_bulk(shop_id: int, product_id: int, codes):
    conn = db_connect()
    inserted = 0
    for code in codes:
        code = code.strip()
        if not code:
            continue
        try:
            conn.execute("""
                INSERT OR IGNORE INTO product_codes (shop_id, product_id, code_text, is_sold)
                VALUES (?, ?, ?, 0)
            """, (shop_id, product_id, code))
            inserted += 1
        except Exception:
            pass
    conn.commit()
    conn.close()
    sync_product_stock_from_codes(shop_id, product_id)
    return inserted

def get_available_code_count(shop_id: int, product_id: int) -> int:
    conn = db_connect()
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM product_codes WHERE shop_id=? AND product_id=? AND is_sold=0",
        (shop_id, product_id)
    ).fetchone()
    conn.close()
    return int(row["c"]) if row else 0

def claim_product_codes(shop_id: int, product_id: int, user_id: int, qty: int):
    conn = db_connect()
    rows = conn.execute("""
        SELECT id, code_text FROM product_codes
        WHERE shop_id=? AND product_id=? AND is_sold=0
        ORDER BY id ASC LIMIT ?
    """, (shop_id, product_id, int(qty))).fetchall()
    if len(rows) < int(qty):
        conn.close()
        return []
    ids = [int(r["id"]) for r in rows]
    placeholders = ",".join(["?"] * len(ids))
    conn.execute(
        f"UPDATE product_codes SET is_sold=1, sold_to=?, sold_at=datetime('now') WHERE id IN ({placeholders})",
        [user_id] + ids
    )
    conn.commit()
    conn.close()
    sync_product_stock_from_codes(shop_id, product_id)
    return [r["code_text"] for r in rows]

def delivery_type_label(user_id: int, product: dict) -> str:
    mode = (product.get("delivery_type") or "manual").lower()
    lang = get_user_lang(user_id)
    if lang == "ar":
        return {"codes": "", "file": ""}.get(mode, "")
    return {"codes": "Auto Code", "file": "Auto File"}.get(mode, "Manual")


# =========================
# EXTERNAL SHOPS DB
# =========================
def add_external_shop_db(shop_token: str, admin_token: str, owner_id: int, title: str = "External Shop"):
    conn = db_connect()
    conn.execute("""
        INSERT INTO external_shops (shop_token, admin_token, owner_id, title, is_active)
        VALUES (?, ?, ?, ?, 1)
    """, (shop_token.strip(), admin_token.strip(), int(owner_id), title.strip() or "External Shop"))
    conn.commit()
    conn.close()

def get_external_shop_by_shop_token(shop_token: str):
    conn = db_connect()
    row = conn.execute("SELECT * FROM external_shops WHERE shop_token=?", (shop_token.strip(),)).fetchone()
    conn.close()
    return dict(row) if row else None

def get_external_shop_by_admin_token(admin_token: str):
    conn = db_connect()
    row = conn.execute("SELECT * FROM external_shops WHERE admin_token=?", (admin_token.strip(),)).fetchone()
    conn.close()
    return dict(row) if row else None

def get_external_shop_by_id(shop_id: int):
    conn = db_connect()
    row = conn.execute("SELECT * FROM external_shops WHERE id=?", (shop_id,)).fetchone()
    conn.close()
    return dict(row) if row else None

def get_external_shops():
    conn = db_connect()
    rows = conn.execute("SELECT * FROM external_shops ORDER BY id ASC").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def get_active_external_shops():
    conn = db_connect()
    rows = conn.execute("SELECT * FROM external_shops WHERE is_active=1 ORDER BY id ASC").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def update_external_shop_usernames(shop_id: int, shop_username: str = "", admin_username: str = ""):
    conn = db_connect()
    conn.execute("UPDATE external_shops SET shop_username=?, admin_username=? WHERE id=?", (shop_username or "", admin_username or "", shop_id))
    conn.commit()
    conn.close()

def update_external_shop_title(shop_id: int, title: str):
    conn = db_connect()
    conn.execute("UPDATE external_shops SET title=? WHERE id=?", (title.strip(), shop_id))
    conn.commit()
    conn.close()

def remove_external_shop_db(shop_id: int):
    row = get_external_shop_by_id(shop_id)
    if not row:
        return None
    conn = db_connect()
    conn.execute("DELETE FROM external_shops WHERE id=?", (shop_id,))
    conn.execute("DELETE FROM products WHERE shop_id=?", (shop_id,))
    conn.execute("DELETE FROM product_codes WHERE shop_id=?", (shop_id,))
    conn.execute("DELETE FROM purchases WHERE shop_id=?", (shop_id,))
    conn.execute("DELETE FROM shop_wallets WHERE shop_id=?", (shop_id,))
    conn.execute("DELETE FROM shop_users WHERE shop_id=?", (shop_id,))
    conn.execute("DELETE FROM tickets WHERE shop_id=?", (shop_id,))
    conn.commit()
    conn.close()
    return row

def get_shop_users(shop_id: int):
    if shop_id == 0:
        return get_all_users()
    conn = db_connect()
    rows = conn.execute("SELECT user_id FROM shop_users WHERE shop_id=?", (shop_id,)).fetchall()
    conn.close()
    return [int(r["user_id"]) for r in rows]


# =========================
# IQLESS API
# =========================
async def iqless_pick_best_device() -> tuple:
    """
    Returns (serial, status_label) of the best available device.
    Priority:
      1. connected=True, busy=True  (active/alive) — best
      2. connected=True, busy=False (idle/ready)   — fallback
      3. None if no connected device found
    """
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{IQLESS_BASE_URL}/api/health")
        h = resp.json()
        devices = h.get("pools", {}).get("unified", {}).get("devices", [])
        if not devices:
            return None, "no_devices"
        busy_devices   = [d for d in devices if d.get("connected") and d.get("busy")]
        ready_devices  = [d for d in devices if d.get("connected") and not d.get("busy")]
        if busy_devices:
            return busy_devices[0]["serial"], "busy"
        if ready_devices:
            return ready_devices[0]["serial"], "ready"
        return None, "all_unavailable"
    except Exception:
        return None, "health_error"

async def iqless_submit_job(email: str, password: str, totp_secret: str, device: str = None) -> dict:
    headers = {"X-API-Key": IQLESS_API_KEY, "Content-Type": "application/json"}
    payload = {"email": email, "password": password, "totp_secret": totp_secret}
    if device:
        payload["device"] = device
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(f"{IQLESS_BASE_URL}/api/jobs", headers=headers, json=payload)
        try:
            body = resp.json()
        except Exception:
            body = {"detail": {"code": "PARSE_ERROR", "message": resp.text}}
        return resp.status_code, body
    except httpx.TimeoutException:
        return 504, {"detail": {"code": "TIMEOUT", "message": "Request timed out"}}
    except Exception as e:
        return 503, {"detail": {"code": "NETWORK_ERROR", "message": str(e)}}

async def iqless_poll_job(job_id: str) -> dict:
    headers = {"X-API-Key": IQLESS_API_KEY}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"{IQLESS_BASE_URL}/api/jobs/{job_id}", headers=headers)
        try:
            data = resp.json()
        except Exception:
            data = {}
        if resp.status_code != 200:
            detail = data.get("detail", {})
            if isinstance(detail, str):
                detail = {"code": "api_error", "message": detail}
            return {"status": "error", "error": detail.get("code", "HTTP_" + str(resp.status_code)), "detail": detail}
        return data
    except httpx.TimeoutException:
        return {"status": "error", "error": "TIMEOUT", "detail": {"message": "Request timed out"}}
    except Exception as e:
        return {"status": "error", "error": "NETWORK_ERROR", "detail": {"message": str(e)}}

async def iqless_get_balance() -> dict:
    headers = {"X-API-Key": IQLESS_API_KEY}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{IQLESS_BASE_URL}/api/balance", headers=headers)
        return resp.json()
    except Exception as e:
        return {"error": str(e)}

async def iqless_get_queue() -> dict:
    headers = {"X-API-Key": IQLESS_API_KEY}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{IQLESS_BASE_URL}/api/queue", headers=headers)
        return resp.json()
    except Exception as e:
        return {"error": str(e)}

async def iqless_cancel_job(job_id: str) -> tuple:
    headers = {"X-API-Key": IQLESS_API_KEY, "Content-Type": "application/json"}
    attempts = [
        ("POST", f"{IQLESS_BASE_URL}/api/jobs/{job_id}/cancel"),
        ("DELETE", f"{IQLESS_BASE_URL}/api/queue/{job_id}"),
        ("POST", f"{IQLESS_BASE_URL}/api/queue/remove"),
    ]
    for method, url in attempts:
        async with httpx.AsyncClient(timeout=15) as client:
            kwargs = {"headers": headers}
            if method == "POST" and "remove" in url:
                kwargs["json"] = {"job_id": job_id}
            resp = await client.request(method, url, **kwargs)
        if resp.status_code not in (404, 405):
            try:
                body = resp.json()
            except Exception:
                body = {"message": resp.text}
            if not isinstance(body, dict):
                body = {"message": str(body)}
            return resp.status_code, body
    return 404, {"detail": {"code": "NO_CANCEL_ENDPOINT", "message": "This API does not support cancellations yet"}}


# =========================
# COMMAND MENUS / BUTTONS
# =========================
def main_user_commands():
    cmds = [
        BotCommand("start", "Start"),
        BotCommand("activate", "Activate Google One"),
        BotCommand("shop", "Shop"),
        BotCommand("profile", "Profile"),
        BotCommand("deposit", "Deposit"),
        BotCommand("claim", "Claim deposit"),
        BotCommand("invite", "Invite"),
        BotCommand("daily", "Daily"),
        BotCommand("history", "History"),
        BotCommand("language", "Language"),
        BotCommand("help", "Help"),
        BotCommand("support", "Support"),
    ]
    return cmds

def external_user_commands():
    return [
        BotCommand("start", "Store"),
        BotCommand("shop", "Shop"),
        BotCommand("profile", "Profile"),
        BotCommand("language", "Language"),
        BotCommand("help", "Help"),
        BotCommand("support", "Support"),
    ]

def owner_admin_commands():
    return [
        BotCommand("start", "Admin panel"),
        BotCommand("help", "Help"),
        BotCommand("myinvite", "My invite"),
        BotCommand("add", "Add balance"),
        BotCommand("remove", "Remove balance"),
        BotCommand("addshop", "Add shop $"),
        BotCommand("removeshop", "Remove shop $"),
        BotCommand("check", "Check user"),
        BotCommand("addreseller", "Add reseller"),
        BotCommand("delreseller", "Delete reseller"),
        BotCommand("backup", "Create manual backup"),
        BotCommand("setprice", "Set activate price"),
        BotCommand("setprofit", "Set reseller profit"),
        BotCommand("addrc", "Add reseller wallet"),
        BotCommand("removerc", "Remove reseller wallet"),
        BotCommand("rusers", "Reseller users"),
        BotCommand("uinvites", "User invites + channel subscription stats"),
        BotCommand("rlink", "Link user"),
        BotCommand("runlink", "Unlink user"),
        BotCommand("addprod", "Add product"),
        BotCommand("delprod", "Delete product"),
        BotCommand("listprod", "List products"),
        BotCommand("addcode", "Add one code"),
        BotCommand("addcodes", "Add bulk codes"),
        BotCommand("addextshop", "Add ext shop"),
        BotCommand("delextshop", "Delete ext shop"),
        BotCommand("listextshops", "List ext shops"),
        BotCommand("broadcast", "Broadcast"),
        BotCommand("broadcast_inactive", "Broadcast inactive"),
        BotCommand("maintenance", "Maintenance"),
        BotCommand("ban", "Ban user"),
        BotCommand("unban", "Unban user"),
        BotCommand("reply", "Reply ticket"),
        BotCommand("resellers", "Resellers report"),
        BotCommand("checktx", "Check TX status"),
        BotCommand("language", "Language"),
    ]

def reseller_admin_commands():
    return [
        BotCommand("start", "Reseller panel"),
        BotCommand("help", "Help"),
        BotCommand("myinvite", "My invite"),
        BotCommand("add", "Add balance"),
        BotCommand("remove", "Remove balance"),
        BotCommand("check", "Check user"),
        BotCommand("resellers", "My report"),
        BotCommand("uinvites", "View client invites + channel stats"),
        BotCommand("language", "Language"),
    ]

def basic_admin_commands():
    return [
        BotCommand("start", "Start"),
        BotCommand("help", "Help"),
        BotCommand("language", "Language"),
    ]

def ext_admin_commands():
    return [
        BotCommand("start", "Store control"),
        BotCommand("help", "Help"),
        BotCommand("addshop", "Add wallet $"),
        BotCommand("removeshop", "Remove wallet $"),
        BotCommand("check", "Check user"),
        BotCommand("addprod", "Add product"),
        BotCommand("delprod", "Delete product"),
        BotCommand("listprod", "List products"),
        BotCommand("addcode", "Add one code"),
        BotCommand("addcodes", "Add bulk codes"),
        BotCommand("reply", "Reply ticket"),
        BotCommand("broadcast", "Broadcast"),
        BotCommand("settitle", "Set title"),
        BotCommand("language", "Language"),
    ]

def build_main_user_keyboard(uid: int, show_activate: bool = True):
    rows = []
    if show_activate:
        rows.append([InlineKeyboardButton(t(uid, "btn_activate"), callback_data="user_activate"), InlineKeyboardButton(t(uid, "btn_shop"), callback_data="user_shop")])
    else:
        rows.append([InlineKeyboardButton(t(uid, "btn_shop"), callback_data="user_shop")])
    rows.extend([
        [InlineKeyboardButton(t(uid, "btn_profile"), callback_data="user_profile"), InlineKeyboardButton(t(uid, "btn_deposit"), callback_data="user_deposit")],
        [InlineKeyboardButton(t(uid, "btn_check"), callback_data="user_balance"), InlineKeyboardButton(t(uid, "btn_history"), callback_data="user_history")],
        [InlineKeyboardButton(t(uid, "btn_invite"), callback_data="user_invite"), InlineKeyboardButton(t(uid, "btn_lang"), callback_data="user_lang")],
        [InlineKeyboardButton(t(uid, "btn_help"), callback_data="user_help"), InlineKeyboardButton(t(uid, "btn_support"), callback_data="user_support")],
    ])
    return InlineKeyboardMarkup(rows)

def build_external_user_keyboard(uid: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(t(uid, "btn_shop"), callback_data="user_shop"), InlineKeyboardButton(t(uid, "btn_profile"), callback_data="user_profile")],
        [InlineKeyboardButton(t(uid, "btn_lang"), callback_data="user_lang"), InlineKeyboardButton(t(uid, "btn_help"), callback_data="user_help")],
        [InlineKeyboardButton(t(uid, "btn_support"), callback_data="user_support")],
    ])

def build_main_admin_keyboard(uid: int):
    rows = [
        [InlineKeyboardButton(t(uid, "btn_adm_stats"), callback_data="adm_stats"), InlineKeyboardButton(t(uid, "btn_adm_bal"), callback_data="adm_balance")],
        [InlineKeyboardButton(t(uid, "btn_adm_users"), callback_data="adm_users"), InlineKeyboardButton(t(uid, "btn_adm_reseller"), callback_data="adm_cat_reseller")],
    ]
    if uid == OWNER_ID:
        rows.append([InlineKeyboardButton(t(uid, "btn_adm_owner"), callback_data="adm_cat_owner"), InlineKeyboardButton(t(uid, "btn_adm_shop"), callback_data="adm_cat_shop")])
        rows.append([InlineKeyboardButton(t(uid, "btn_adm_external"), callback_data="adm_cat_external"), InlineKeyboardButton(t(uid, "btn_adm_sys"), callback_data="adm_cat_system")])
        rows.append([InlineKeyboardButton("🤖 ChatGPT Workspace", callback_data="adm_cat_workspace"), InlineKeyboardButton("⚡ API Control", callback_data="adm_cat_api")])
        rows.append([InlineKeyboardButton(t(uid, "btn_adm_data"), callback_data="adm_data")])
        rows.append([InlineKeyboardButton("", callback_data="adm_backup")])
    rows.append([InlineKeyboardButton(t(uid, "btn_help"), callback_data="adm_help"), InlineKeyboardButton(t(uid, "btn_lang"), callback_data="user_lang")])
    return InlineKeyboardMarkup(rows)

def build_ext_admin_keyboard(uid: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Stats", callback_data="ext_stats"), InlineKeyboardButton("💰 Wallet", callback_data="ext_wallet")],
        [InlineKeyboardButton("📦 Add Product", callback_data="ext_act_addprod"), InlineKeyboardButton("📋 Products", callback_data="ext_act_listprod")],
        [InlineKeyboardButton("🔐 Add Code", callback_data="ext_act_addcode"), InlineKeyboardButton("📥 Bulk Codes", callback_data="ext_act_addcodes")],
        [InlineKeyboardButton("➕ Add User $", callback_data="ext_act_addshop"), InlineKeyboardButton("➖ Remove User $", callback_data="ext_act_removeshop")],
        [InlineKeyboardButton("🔍 Check User", callback_data="ext_act_check"), InlineKeyboardButton("📢 Broadcast", callback_data="ext_act_broadcast")],
        [InlineKeyboardButton("💬 Reply Ticket", callback_data="ext_act_reply"), InlineKeyboardButton("🏷️ Title", callback_data="ext_act_settitle")],
        [InlineKeyboardButton(t(uid, "btn_help"), callback_data="ext_help"), InlineKeyboardButton(t(uid, "btn_lang"), callback_data="user_lang")],
    ])

def build_help_keyboard(uid: int, mode: str):
    if mode == "main_admin":
        return build_main_admin_keyboard(uid)
    if mode == "ext_admin":
        return build_ext_admin_keyboard(uid)
    if mode == "external_user":
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(t(uid, "btn_shop"), callback_data="user_shop"), InlineKeyboardButton(t(uid, "btn_profile"), callback_data="user_profile")],
            [InlineKeyboardButton(t(uid, "btn_support"), callback_data="user_support"), InlineKeyboardButton(t(uid, "btn_home"), callback_data="user_home")],
        ])
    rows = []
    if not MAINTENANCE_MODE:
        rows.append([InlineKeyboardButton(t(uid, "btn_activate"), callback_data="user_activate"), InlineKeyboardButton(t(uid, "btn_shop"), callback_data="user_shop")])
    else:
        rows.append([InlineKeyboardButton(t(uid, "btn_shop"), callback_data="user_shop")])
    rows.extend([
        [InlineKeyboardButton(t(uid, "btn_profile"), callback_data="user_profile"), InlineKeyboardButton(t(uid, "btn_deposit"), callback_data="user_deposit")],
        [InlineKeyboardButton(t(uid, "btn_support"), callback_data="user_support"), InlineKeyboardButton(t(uid, "btn_home"), callback_data="user_home")],
    ])
    return InlineKeyboardMarkup(rows)

async def sync_commands_for_chat(bot: Bot, chat_id: int, mode: str, is_owner: bool = False, is_reseller_user: bool = False):
    try:
        scope = BotCommandScopeChat(chat_id=chat_id)
        if mode == "main_user":
            cmds = main_user_commands()
            if MAINTENANCE_MODE:
                cmds = [cmd for cmd in cmds if cmd.command != "activate"]
            await bot.set_my_commands(cmds, scope=scope)
        elif mode == "external_user":
            await bot.set_my_commands(external_user_commands(), scope=scope)
        elif mode == "main_admin":
            if is_owner:
                await bot.set_my_commands(owner_admin_commands(), scope=scope)
            elif is_reseller_user:
                await bot.set_my_commands(reseller_admin_commands(), scope=scope)
            else:
                await bot.set_my_commands(basic_admin_commands(), scope=scope)
        elif mode == "ext_admin":
            await bot.set_my_commands(ext_admin_commands(), scope=scope)
    except Exception as e:
        logger.warning(f"Could not sync commands for chat {chat_id}: {e}")


# =========================
# BROADCAST / LOG / SUPPORT
# =========================
async def send_log_via_second_bot(text: str, document=None, filename=None):
    if not global_log_bot or not ADMIN_LOG_ID:
        return
    try:
        if document:
            await global_log_bot.send_document(chat_id=ADMIN_LOG_ID, document=document, filename=filename, caption=text[:1000])
        else:
            await global_log_bot.send_message(chat_id=ADMIN_LOG_ID, text=text, parse_mode="HTML")
    except Exception:
        pass

async def send_to_external_admin(admin_token: str, owner_id: int, text: str):
    if not admin_token or not owner_id:
        return
    try:
        temp_bot = Bot(admin_token)
        await temp_bot.send_message(owner_id, text=text, parse_mode="HTML")
    except Exception:
        pass

async def broadcast_system_msg(context, key_name: str):
    conn = db_connect()
    users_data = conn.execute("SELECT user_id, lang FROM users").fetchall()
    conn.close()
    bot = context.bot if context else Bot(MAIN_BOT_TOKEN)
    count = 0
    for row in users_data:
        uid = int(row["user_id"])
        lang = row["lang"] or "en"
        try:
            await bot.send_message(uid, LANGS.get(lang, LANGS["en"]).get(key_name, key_name), parse_mode="HTML")
            count += 1
            await asyncio.sleep(0.04)
        except Exception:
            pass
    return count

async def check_channel_join(user_id: int, bot: Bot, mode: str = "main_user") -> bool:
    if mode != "main_user":
        return True
    if user_id == OWNER_ID:
        return True
    try:
        member = await bot.get_chat_member(chat_id=REQUIRED_CHANNEL, user_id=user_id)
        return member.status not in ["left", "kicked"]
    except Exception:
        return True

async def send_join_alert(update: Update, user_id: int):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(t(user_id, "btn_join_ch"), url=f"https://t.me/{REQUIRED_CHANNEL.replace('@', '')}")],
        [InlineKeyboardButton(t(user_id, "btn_i_joined"), callback_data="check_join")],
    ])
    if update.callback_query:
        await update.callback_query.message.reply_text(t(user_id, "must_join"), parse_mode="HTML", reply_markup=kb)
    else:
        await update.message.reply_text(t(user_id, "must_join"), parse_mode="HTML", reply_markup=kb)


# =========================
# ACTIVATION (IQLESS API)
# =========================
def generate_tx_id() -> str:
    return secrets.token_hex(4).upper()

ERROR_LABELS = {
    "WRONG_PASSWORD":      "",
    "WRONG_TOTP":          "Wrong TOTP code",
    "INVALID_TOTP":        "Invalid TOTP code",
    "ACCOUNT_LOCKED":      "",
    "ACCOUNT_DISABLED":    "Account disabled by Google",
    "INVALID_EMAIL":       "",
    "NO_GOOGLE_ONE":       "Google One offer not found",
    "DEVICE_ERROR":        "",
    "TIMEOUT":             "",
    "CAPTCHA":             "Google captcha required",
    "UNKNOWN_ERROR":       "",
    "NETWORK_ERROR":       "",
    "2FA_REQUIRED":        "",
    "WRONG_CREDENTIALS":   "",
    "SESSION_EXPIRED":     "",
    "URL_CAPTURE_FAILED":  "",
    "NOT_ELIGIBLE":        "",
    "PLAN_NOT_FOUND":      "",
    "ALREADY_SUBSCRIBED":  "",
    "PAYMENT_FAILED":      "",
}

async def handle_activation_result(bot, uid: int, job_id: str, email: str, cost: float, reseller_id: int, tx_id: str, url: str = "", error: str = "", success: bool = False, msg_id: int = 0):
    # Idempotency: skip if this tx_id was already recorded in history (duplicate protection)
    if tx_id:
        _chk = db_connect()
        _existing = _chk.execute("SELECT 1 FROM history WHERE tx_id=?", (tx_id,)).fetchone()
        _chk.close()
        if _existing:
            logger.warning(f"JOB_RESULT duplicate skipped uid={uid} tx_id={tx_id} job_id={job_id}")
            return

    if success:
        logger.info(f"JOB_SUCCESS uid={uid} email={email} job_id={job_id} tx_id={tx_id} url={url}")
        conn = db_connect()
        conn.execute("""
            INSERT INTO history (user_id, vid, email, status, msg, reason, tx_id)
            VALUES (?, ?, ?, 'SUCCESS', ?, '', ?)
        """, (uid, job_id, email, url, tx_id))
        conn.commit()
        conn.close()
        increment_stats(uid, success=True)

        if msg_id:
            try:
                await bot.edit_message_text(
                    chat_id=uid, message_id=msg_id,
                    text=f"✅ <b>Activation successful!</b>\n\n📧 <code>{email}</code>\n🆔 Job ID: <code>{job_id}</code>\n🧾 TX: <code>{tx_id}</code>",
                    parse_mode="HTML"
                )
            except Exception:
                pass

        await bot.send_message(
            uid,
            t(uid, "activate_success", email=email, url=url, tx=tx_id, job_id=job_id),
            parse_mode="HTML",
            disable_web_page_preview=True
        )

        log_text = t(OWNER_ID, "owner_notify", tx=tx_id, uid=uid, email=email, amount=cost, url=url)
        await send_log_via_second_bot(log_text)

        if reseller_id and reseller_id != OWNER_ID:
            profit = get_reseller_profit()
            add_reseller_balance(reseller_id, profit)
            conn2 = db_connect()
            conn2.execute("UPDATE resellers SET total_sold=COALESCE(total_sold,0)+1 WHERE user_id=?", (reseller_id,))
            conn2.commit()
            conn2.close()
            try:
                await bot.send_message(
                    reseller_id,
                    t(reseller_id, "reseller_notify", tx=tx_id, uid=uid, email=email, job_id=job_id, amount=cost, profit=profit),
                    parse_mode="HTML"
                )
            except Exception:
                pass
    else:
        logger.info(f"JOB_FAILED uid={uid} email={email} job_id={job_id} tx_id={tx_id} error={error}")
        add_balance(uid, cost)
        conn = db_connect()
        conn.execute("""
            INSERT INTO history (user_id, vid, email, status, msg, reason, tx_id)
            VALUES (?, ?, ?, 'FAILED', '', ?, ?)
        """, (uid, job_id, email, error[:200], tx_id))
        conn.commit()
        conn.close()
        increment_stats(uid, success=False)

        error_label = ERROR_LABELS.get(error, "")
        fail_text = (
            f"❌ <b>Activation failed</b>\n\n"
            f"📧 <code>{email}</code>\n"
            f"🆔 Job ID: <code>{job_id}</code>\n"
            f"🧾 TX: <code>{tx_id}</code>\n"
            f"🔴 {error_label or error}"
            + f"\n\n💰 Your balance has been refunded."
        )

        if msg_id:
            try:
                await bot.edit_message_text(
                    chat_id=uid, message_id=msg_id,
                    text=fail_text, parse_mode="HTML"
                )
            except Exception:
                await bot.send_message(uid, fail_text, parse_mode="HTML")
        else:
            await bot.send_message(uid, fail_text, parse_mode="HTML")

        # ── Owner log: failure ───────────────────────────────────────────────
        await send_log_via_second_bot(
            f"❌ <b>Activation failed</b>\n\n"
            f"👤 User: <code>{uid}</code>\n"
            f"📧 <code>{email}</code>\n"
            f"🆔 Job ID: <code>{job_id}</code>\n"
            f"🧾 TX: <code>{tx_id}</code>\n"
            f"🔴 {error_label or error}\n"
            f"💰 Balance refunded ${cost:.2f}"
        )
        # ── Reseller: failure notification ──────────────────────────────────
        if reseller_id and reseller_id != OWNER_ID:
            try:
                await bot.send_message(
                    reseller_id,
                    f"❌ <b>Client activation failed</b>\n\n"
                    f"👤 User: <code>{uid}</code>\n"
                    f"📧 <code>{email}</code>\n"
                    f"🆔 Job ID: <code>{job_id}</code>\n"
                    f"🧾 TX: <code>{tx_id}</code>\n"
                    f"🔴 {error_label or error}\n"
                    f"",
                    parse_mode="HTML"
                )
            except Exception:
                pass

def _queue_msg(uid: int, email: str, job_id: str, pos: int, wait: float, tx_id: str = "") -> str:
    mins = int(wait // 60)
    secs = int(wait % 60)
    wait_str = f"{mins}m {secs}s" if mins else f"{secs}s"
    lang = get_user_lang(uid)
    tx_line = f"🧾 <code>{tx_id}</code>\n" if tx_id else ""
    if lang == "ar":
        return (
            f"⏳ <b>Your request is in queue...</b>\n\n"
            f"📧 <code>{email}</code>\n"
            f"🆔 Job ID: <code>{job_id}</code>\n"
            f"{tx_line}"
            f"📊 Your position in queue: <b>{pos}</b>\n"
            f"⏱ Estimated time: ~<b>{wait_str}</b>"
        )
    mins_en = int(wait // 60)
    wait_str_en = f"{mins_en}m {secs}s" if mins_en else f"{secs}s"
    return (
        f"⏳ <b>Your job is queued...</b>\n\n"
        f"📧 <code>{email}</code>\n"
        f"🆔 Job ID: <code>{job_id}</code>\n"
        f"{tx_line}"
        f"📊 Queue position: <b>{pos}</b>\n"
        f"⏱ Est. wait: ~<b>{wait_str_en}</b>"
    )


async def activation_poller(bot):
    # ── Recover jobs that survived a restart ─────────────────────────────────
    recovered = db_load_jobs()
    for j in recovered:
        jid = j["job_id"]
        if jid not in active_jobs:
            active_jobs[jid] = {
                "uid": j["uid"],
                "email": j["email"],
                "cost": j["cost"],
                "reseller_id": j["reseller_id"],
                "tx_id": j["tx_id"],
                "submitted_at": j["submitted_at"],
                "status_msg_id": j.get("status_msg_id", 0),
                "estimated_wait": j.get("estimated_wait", 0.0),
                "last_pos": -1,
                "last_stage": -1,
            }
            logger.info(f"Recovered job {jid} for uid={j['uid']}")
            reconnect_text = (
                    f"⏳ <b>Reconnecting!</b>\n"
                    f""
                    f"📧 <code>{j['email']}</code>\n"
                    f"🆔 Request: <code>{jid}</code>"
                )
            existing_msg_id = j.get("status_msg_id", 0)
            if existing_msg_id:
                try:
                    await bot.edit_message_text(
                        chat_id=j["uid"],
                        message_id=existing_msg_id,
                        text=reconnect_text,
                        parse_mode="HTML"
                    )
                except Exception:
                    try:
                        sent = await bot.send_message(
                            j["uid"],
                            reconnect_text,
                            parse_mode="HTML"
                        )
                        active_jobs[jid]["status_msg_id"] = sent.message_id
                        db_update_job_msg(jid, sent.message_id)
                    except Exception:
                        pass
            else:
                try:
                    sent = await bot.send_message(
                        j["uid"],
                        reconnect_text,
                        parse_mode="HTML"
                    )
                    active_jobs[jid]["status_msg_id"] = sent.message_id
                    db_update_job_msg(jid, sent.message_id)
                except Exception:
                    pass

    # ── Main polling loop ─────────────────────────────────────────────────────
    while True:
        await asyncio.sleep(3)
        if not active_jobs:
            continue
        for job_id in list(active_jobs.keys()):
            job_data = active_jobs.get(job_id)
            if not job_data:
                continue
            try:
                data = await iqless_poll_job(job_id)
                status = data.get("status", "")
                stage = data.get("stage", 0)
                stage_label = data.get("stage_label", "")
                total_stages = data.get("total_stages", 8)
                pos = data.get("queue_position", 0)
                wait = float(data.get("estimated_wait_seconds", 0) or 0)
                uid = job_data["uid"]
                email = job_data["email"]
                msg_id = job_data.get("status_msg_id", 0)

                # ── Dynamic timeout: estimated_wait * 3 (min 5 min) ──────────
                est = job_data.get("estimated_wait", 0.0) or wait
                job_timeout = max(est * 3, 300)
                elapsed = time.time() - job_data.get("submitted_at", time.time())

                if status == "success":
                    active_jobs.pop(job_id, None)
                    if db_remove_job(job_id):
                        await handle_activation_result(
                            bot, uid=uid, job_id=job_id, email=email,
                            cost=job_data["cost"], reseller_id=job_data["reseller_id"],
                            tx_id=job_data["tx_id"], url=data.get("url", ""), success=True,
                            msg_id=msg_id
                        )

                elif status == "failed":
                    active_jobs.pop(job_id, None)
                    if db_remove_job(job_id):
                        await handle_activation_result(
                            bot, uid=uid, job_id=job_id, email=email,
                            cost=job_data["cost"], reseller_id=job_data["reseller_id"],
                            tx_id=job_data["tx_id"], error=data.get("error", "UNKNOWN_ERROR"), success=False,
                            msg_id=msg_id
                        )

                elif status == "queued":
                    # Update queue message when position or wait changes
                    last_pos = job_data.get("last_pos", -1)
                    if pos != last_pos:
                        job_data["last_pos"] = pos
                        job_data["estimated_wait"] = wait
                        new_text = _queue_msg(uid, email, job_id, pos, wait, tx_id=job_data.get("tx_id", ""))
                        if msg_id:
                            try:
                                await bot.edit_message_text(
                                    chat_id=uid, message_id=msg_id,
                                    text=new_text, parse_mode="HTML"
                                )
                            except Exception:
                                pass
                        # ── Owner log: queue position changed ────────────────
                        await send_log_via_second_bot(
                            f"🔄 <b>Queue Update</b>\n\n"
                            f"👤 <code>{uid}</code> | 📧 <code>{email}</code>\n"
                            f"🆔 <code>{job_id}</code>\n"
                            f"🧾 <code>{job_data.get('tx_id', '')}</code>\n"
                            f"📊 Queue position: <b>{pos}</b> | ⏱ ~{int(wait)}s"
                        )
                    # Timeout: submitted_at + estimated_wait * 3 (min 5 min)
                    if elapsed > job_timeout and job_timeout > 0:
                        active_jobs.pop(job_id, None)
                        if db_remove_job(job_id):
                            await handle_activation_result(
                                bot, uid=uid, job_id=job_id, email=email,
                                cost=job_data["cost"], reseller_id=job_data["reseller_id"],
                                tx_id=job_data["tx_id"], error="TIMEOUT", success=False,
                                msg_id=msg_id
                            )

                elif status == "running":
                    # Update message with stage progress
                    last_stage = job_data.get("last_stage", -1)
                    if stage != last_stage:
                        job_data["last_stage"] = stage
                        tx_id_display = job_data.get("tx_id", "")
                        progress_text = (
                            f"⚙️ <b>Activating...</b>\n\n"
                            f"📧 <code>{email}</code>\n"
                            f"🧾 <code>{tx_id_display}</code>\n"
                            f"🔄 Stage {stage}/{total_stages}: <b>{stage_label}</b>"
                        )
                        if msg_id:
                            try:
                                await bot.edit_message_text(
                                    chat_id=uid, message_id=msg_id,
                                    text=progress_text, parse_mode="HTML"
                                )
                            except Exception:
                                pass
                        # ── Owner log: stage changed ──────────────────────────
                        await send_log_via_second_bot(
                            f"⚙️ <b>New Stage</b>\n\n"
                            f"👤 <code>{uid}</code> | 📧 <code>{email}</code>\n"
                            f"🆔 <code>{job_id}</code>\n"
                            f"🧾 <code>{tx_id_display}</code>\n"
                            f"🔄 {stage}/{total_stages}: <b>{stage_label}</b>"
                        )
                    # Running timeout: 6 minutes max
                    if elapsed > 360:
                        active_jobs.pop(job_id, None)
                        if db_remove_job(job_id):
                            await handle_activation_result(
                                bot, uid=uid, job_id=job_id, email=email,
                                cost=job_data["cost"], reseller_id=job_data["reseller_id"],
                                tx_id=job_data["tx_id"], error="TIMEOUT", success=False,
                                msg_id=msg_id
                            )

                else:
                    # Unknown status — timeout after 5 min
                    if elapsed > 300:
                        active_jobs.pop(job_id, None)
                        if db_remove_job(job_id):
                            await handle_activation_result(
                                bot, uid=uid, job_id=job_id, email=email,
                                cost=job_data["cost"], reseller_id=job_data["reseller_id"],
                                tx_id=job_data["tx_id"], error="TIMEOUT", success=False,
                                msg_id=msg_id
                            )

            except Exception as e:
                logger.error(f"Poller error for job {job_id}: {e}")


# =========================
# ADD PRODUCT CONVERSATION
# =========================
async def start_add_prod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mode = current_bot_mode(context)
    uid = update.effective_user.id
    if mode == "main_admin" and uid != OWNER_ID:
        return ConversationHandler.END
    if mode == "ext_admin" and uid != current_external_owner_id(context):
        return ConversationHandler.END
    msg = update.callback_query.message if update.callback_query else update.message
    await msg.reply_text("📦 Enter Product Name:")
    return ADD_NAME

async def start_add_prod_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        await update.callback_query.answer()
    return await start_add_prod(update, context)

async def add_prod_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["p_name"] = update.message.text.strip()
    await update.message.reply_text("💰 Enter Price ($):")
    return ADD_PRICE

async def add_prod_price(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data["p_price"] = float(update.message.text.strip())
        await update.message.reply_text("📊 Enter Stock Quantity:")
        return ADD_STOCK
    except Exception:
        await update.message.reply_text("❌ Invalid price. Try again.")
        return ADD_PRICE

async def add_prod_stock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data["p_stock"] = int(update.message.text.strip())
        await update.message.reply_text("📂 Enter Category:")
        return ADD_CAT
    except Exception:
        await update.message.reply_text("❌ Invalid stock. Try again.")
        return ADD_STOCK

async def add_prod_cat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["p_cat"] = update.message.text.strip()
    await update.message.reply_text("📝 Enter Description:")
    return ADD_DESC

async def add_prod_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["p_desc"] = update.message.text.strip()
    await update.message.reply_text("🖼️ Send Product Image (photo) or type 'skip':")
    return ADD_IMG

async def add_prod_img(update: Update, context: ContextTypes.DEFAULT_TYPE):
    photo_id = update.message.photo[-1].file_id if update.message.photo else None
    context.user_data["p_img"] = photo_id
    await update.message.reply_text("📎 Send Product File (document) or type 'skip':")
    return ADD_FILE

async def add_prod_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    file_id = update.message.document.file_id if update.message.document else None
    d = context.user_data
    add_product_db(
        shop_id=current_shop_id(context),
        name=d.get("p_name", "Product"),
        price=float(d.get("p_price", 0)),
        stock=int(d.get("p_stock", 0)),
        category=d.get("p_cat", "General"),
        desc=d.get("p_desc", ""),
        file_id=file_id,
        image_id=d.get("p_img"),
    )
    await update.message.reply_text("✅ Product Added Successfully!")
    for k in ["p_name", "p_price", "p_stock", "p_cat", "p_desc", "p_img"]:
        context.user_data.pop(k, None)
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.callback_query.message if update.callback_query else update.message
    if context.user_data.get("act_step") or context.user_data.get("pending_activation"):
        uid = update.effective_user.id
        await msg.reply_text("⛔ Cannot cancel during activation.\n\nPlease complete the activation process.", parse_mode="HTML")
        return ConversationHandler.END
    context.user_data.pop("admin_action", None)
    context.user_data.pop("ext_admin_action", None)
    context.user_data.pop("waiting_for_credentials", None)
    context.user_data.pop("state", None)
    context.user_data.pop("addextshop_step", None)
    await msg.reply_text("🚫 Cancelled.")
    return ConversationHandler.END


# =========================
# SUPPORT
# =========================
async def cmd_support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if is_user_banned(uid):
        if update.callback_query:
            await update.callback_query.answer(t(uid, "banned"), show_alert=True)
        else:
            await update.message.reply_text(t(uid, "banned"), parse_mode="HTML")
        return ConversationHandler.END
    msg = update.callback_query.message if update.callback_query else update.message
    if update.callback_query:
        await update.callback_query.answer()
    await msg.reply_text(t(uid, "support_welcome"), parse_mode="HTML")
    return SUPPORT_CHAT

async def handle_support_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = update.message.text
    username = update.effective_user.username or "Unknown"
    first_name = update.effective_user.first_name or ""
    mode = current_bot_mode(context)
    shop_id = current_shop_id(context)
    update_user_info(uid, username, first_name)
    update_shop_user_info(shop_id, uid, username, first_name, get_user_lang(uid))

    conn = db_connect()
    conn.execute(
        "INSERT INTO tickets (shop_id, user_id, status, bot_token) VALUES (?, ?, 'open', ?)",
        (shop_id, uid, context.bot.token)
    )
    conn.commit()
    conn.close()

    if mode == "external_user":
        admin_token = current_external_admin_token(context)
        owner_id = current_external_owner_id(context)
        log_text = (
            f"📞 <b>New Store Support Ticket</b>\n"
            f"Shop: <b>{current_external_title(context)}</b>\n"
            f"User: @{username} (<code>{uid}</code>)\n"
            f"Msg: {text}\n\n"
            f"👇 <b>Reply:</b>\n<code>/reply {uid} Message</code>"
        )
        await send_to_external_admin(admin_token, owner_id, log_text)
    else:
        log_text = (
            f"📞 <b>New Support Ticket</b>\n"
            f"User: @{username} (<code>{uid}</code>)\n"
            f"Msg: {text}\n\n"
            f"👇 <b>Reply:</b>\n<code>/reply {uid} Message</code>"
        )
        await send_log_via_second_bot(log_text)

    await update.message.reply_text(t(uid, "support_sent"), parse_mode="HTML")
    return ConversationHandler.END


# =========================
# USER COMMANDS
# =========================
async def cmd_language(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    msg = update.callback_query.message if update.callback_query else update.message
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")]
    ])
    await msg.reply_text(t(uid, "lang_select"), parse_mode="HTML", reply_markup=kb)

async def callback_lang(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    mode = current_bot_mode(context)
    if query.data == "check_join":
        if await check_channel_join(uid, context.bot, mode):
            try:
                await query.message.delete()
            except Exception:
                pass
            await start(update, context)
            try:
                await query.answer(t(uid, "join_success"), show_alert=True)
            except Exception:
                pass
        else:
            try:
                await query.answer(t(uid, "still_not_joined"), show_alert=True)
            except Exception:
                pass
        return
    if query.data.startswith("lang_"):
        set_lang(uid, query.data.split("_")[1])
        await start(update, context)
        try:
            await query.answer("✅")
        except Exception:
            pass

# =========================
# AUTO DEPOSIT HELPERS
# =========================

def expire_pending_deposits(now: datetime.datetime | None = None) -> int:
    now_dt = now or datetime.datetime.utcnow()
    conn = db_connect()
    cur = conn.execute(
        "UPDATE pending_deposits SET status='expired' WHERE status='pending' AND expires_at < ?",
        (now_dt.isoformat(),)
    )
    conn.commit()
    conn.close()
    return int(cur.rowcount or 0)


def generate_unique_deposit_amount(base_amount, network: str) -> str:
    """Create a unique, exact amount string per network without float drift."""
    net = normalize_network_name(network)
    base_dec = normalize_amount_decimal(base_amount, net)
    if base_dec is None:
        raise ValueError("Invalid deposit amount")
    digits = network_amount_decimals(net)
    max_noise = max(9, (10 ** max(digits - 2, 1)) - 1)
    for _ in range(200):
        noise = Decimal(random.randint(1, max_noise)).scaleb(-digits)
        unique = (base_dec + noise).quantize(network_quantizer(net), rounding=ROUND_DOWN)
        unique_str = format_amount_for_network(unique, net, trim=False)
        conn = db_connect()
        existing = conn.execute(
            "SELECT id FROM pending_deposits WHERE expected_amount_str=? AND network=? AND status='pending'",
            (unique_str, net)
        ).fetchone()
        conn.close()
        if not existing:
            return unique_str
    fallback = (base_dec + Decimal(random.randint(1, max_noise)).scaleb(-digits)).quantize(network_quantizer(net), rounding=ROUND_DOWN)
    return format_amount_for_network(fallback, net, trim=False)


def get_user_pending_deposit(user_id: int):
    expire_pending_deposits()
    conn = db_connect()
    row = conn.execute(
        "SELECT * FROM pending_deposits WHERE user_id=? AND status='pending' ORDER BY id DESC LIMIT 1",
        (user_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def is_deposit_txid_already_used(txid: str) -> bool:
    conn = db_connect()
    row = conn.execute(
        "SELECT 1 FROM deposits WHERE lower(txid)=lower(?) LIMIT 1",
        (txid,)
    ).fetchone()
    if not row:
        row = conn.execute(
            "SELECT 1 FROM pending_deposits WHERE status='confirmed' AND lower(tx_hash)=lower(?) LIMIT 1",
            (txid,)
        ).fetchone()
    conn.close()
    return bool(row)


async def _check_trc20_deposits(wallet: str, pending: list) -> list:
    """Query TronScan for recent USDT TRC20 transfers to wallet and match with pending."""
    USDT_TRC20 = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    url = (
        f"https://apilist.tronscan.org/api/token_trc20/transfers"
        f"?toAddress={wallet}&contract_address={USDT_TRC20}&limit=30&start=0"
    )
    confirmed = []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url)
            data = resp.json()
        transfers = data.get("token_transfers", [])
        for tx in transfers:
            if not tx.get("confirmed", False):
                continue
            tx_hash = tx.get("transaction_id", "")
            if not tx_hash:
                continue
            quant_raw = tx.get("quant", "0")
            try:
                quant = int(quant_raw)
            except Exception:
                continue
            amount = quant / 1_000_000.0
            for p in pending:
                if abs(amount - p["expected_amount"]) < 0.000002:
                    confirmed.append((p, tx_hash, amount))
                    break
    except Exception as e:
        logging.getLogger(__name__).warning(f"[TRC20 CHECK] {e}")
    return confirmed


async def _check_bep20_deposits(wallet: str, pending: list) -> list:
    """Query BscScan for recent USDT BEP20 transfers to wallet and match with pending."""
    USDT_BEP20 = "0x55d398326f99059fF775485246999027B3197955"
    url = (
        f"https://api.bscscan.com/api?module=account&action=tokentx"
        f"&contractaddress={USDT_BEP20}&address={wallet}"
        f"&page=1&offset=30&sort=desc&apikey={BSCSCAN_API_KEY}"
    )
    confirmed = []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url)
            data = resp.json()
        if data.get("status") != "1":
            return []
        for tx in data.get("result", []):
            if tx.get("to", "").lower() != wallet.lower():
                continue
            tx_hash = tx.get("hash", "")
            if not tx_hash:
                continue
            try:
                decimals = int(tx.get("tokenDecimal", 18))
                value = int(tx.get("value", 0))
                amount = value / (10 ** decimals)
            except Exception:
                continue
            for p in pending:
                if abs(amount - p["expected_amount"]) < 0.000002:
                    confirmed.append((p, tx_hash, amount))
                    break
    except Exception as e:
        logging.getLogger(__name__).warning(f"[BEP20 CHECK] {e}")
    return confirmed


async def _confirm_deposit(context: ContextTypes.DEFAULT_TYPE, deposit: dict, tx_hash: str, amount: float | Decimal):
    """Mark a pending deposit as confirmed exactly once, credit user balance, and notify."""
    tx_hash_norm = canonicalize_txid(tx_hash, deposit.get("network", "")) or (tx_hash or "").strip().lower()
    amount_dec = parse_amount_decimal(amount) or Decimal("0")
    amount_str = decimal_to_display(amount_dec)

    conn = db_connect()
    already = conn.execute(
        "SELECT 1 FROM deposits WHERE lower(txid)=lower(?) LIMIT 1",
        (tx_hash_norm,)
    ).fetchone()
    if already:
        conn.close()
        return False

    row = conn.execute(
        "SELECT status FROM pending_deposits WHERE id=?",
        (deposit["id"],)
    ).fetchone()
    if not row or row["status"] != "pending":
        conn.close()
        return False

    cur = conn.execute(
        "UPDATE pending_deposits SET status='confirmed', tx_hash=? WHERE id=? AND status='pending'",
        (tx_hash_norm, deposit["id"])
    )
    if (cur.rowcount or 0) < 1:
        conn.rollback()
        conn.close()
        return False

    conn.execute(
        "INSERT OR IGNORE INTO deposits (txid, user_id, amount, network) VALUES (?, ?, ?, ?)",
        (tx_hash_norm, deposit["user_id"], float(amount_dec), normalize_network_name(deposit.get("network", "TRC20")))
    )
    conn.commit()
    conn.close()

    add_balance(deposit["user_id"], float(amount_dec))

    try:
        await context.bot.send_message(
            deposit["user_id"],
            t(deposit["user_id"], "deposit_confirmed", amount=amount_str, txhash=tx_hash_norm),
            parse_mode="HTML"
        )
    except Exception:
        pass

    try:
        await context.bot.send_message(
            OWNER_ID,
            f"✅ <b>Deposit Confirmed</b>\n"
            f"👤 User ID: <code>{deposit['user_id']}</code>\n"
            f"💰 Amount: <b>${amount_str} USDT</b>\n"
            f"🔹 Network: {normalize_network_name(deposit.get('network', 'TRC20'))}\n"
            f"🔗 TX: <code>{tx_hash_norm}</code>",
            parse_mode="HTML"
        )
    except Exception:
        pass

    return True


async def _bsc_rpc_call(method: str, params: list):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(BSC_RPC_URL, json=payload)
        data = resp.json()
    if data.get("error"):
        raise RuntimeError(str(data["error"]))
    return data.get("result")


async def _fetch_trc20_transfer_by_txid(wallet: str, txid: str) -> dict:
    txid_norm = canonicalize_txid(txid, "TRC20")
    url = (
        f"https://apilist.tronscan.org/api/token_trc20/transfers"
        f"?toAddress={wallet}&contract_address={USDT_TRC20_CONTRACT}&limit=100&start=0"
    )
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(url)
            data = resp.json()
        for tx in data.get("token_transfers", []):
            current_txid = canonicalize_txid(tx.get("transaction_id", ""), "TRC20")
            if current_txid != txid_norm:
                continue
            quant_raw = tx.get("quant", "0")
            quant = Decimal(int(str(quant_raw))) / (Decimal(10) ** network_chain_decimals("TRC20"))
            return {
                "found": True,
                "confirmed": bool(tx.get("confirmed", False)),
                "amount_decimal": quant,
                "amount_text": decimal_to_display(quant),
                "txid": current_txid,
            }
    except Exception as e:
        logger.warning(f"[TRC20 CLAIM] {e}")
        return {"error": str(e)}
    return {"found": False}


async def _fetch_bep20_transfer_by_txid(wallet: str, txid: str) -> dict:
    txid_norm = canonicalize_txid(txid, "BEP20")
    try:
        receipt = await _bsc_rpc_call("eth_getTransactionReceipt", [txid_norm])
        if not receipt:
            tx = await _bsc_rpc_call("eth_getTransactionByHash", [txid_norm])
            if tx:
                return {"found": True, "confirmed": False, "txid": txid_norm}
            return {"found": False}

        confirmed = str(receipt.get("status", "0x0")).lower() in ("0x1", "1")
        wallet_topic = "0x" + wallet.lower().replace("0x", "").rjust(64, "0")
        usdt_logs = []
        for log in receipt.get("logs", []):
            if (log.get("address") or "").lower() != USDT_BEP20_CONTRACT.lower():
                continue
            topics = [str(t).lower() for t in log.get("topics", [])]
            if len(topics) < 3 or topics[0] != TRANSFER_EVENT_TOPIC:
                continue
            usdt_logs.append(log)
            if topics[2] != wallet_topic:
                continue
            data_hex = (log.get("data") or "0x0").replace("0x", "")
            amount_int = int(data_hex or "0", 16)
            amount_dec = Decimal(amount_int) / (Decimal(10) ** network_chain_decimals("BEP20"))
            return {
                "found": True,
                "confirmed": confirmed,
                "amount_decimal": amount_dec,
                "amount_text": decimal_to_display(amount_dec),
                "txid": txid_norm,
            }
        if usdt_logs:
            return {"found": True, "confirmed": confirmed, "wrong_wallet": True, "txid": txid_norm}
    except Exception as e:
        logger.warning(f"[BEP20 CLAIM] {e}")
        return {"error": str(e)}
    return {"found": False}


def _claim_failure_text(uid: int, network: str, result: dict, expected_amount: str) -> str:
    reason = result.get("reason")
    if reason == "already_used":
        return t(uid, "claim_already_used")
    if reason == "not_confirmed":
        return t(uid, "claim_not_confirmed")
    if reason == "amount_mismatch":
        return t(uid, "claim_amount_mismatch", expected=expected_amount, found=result.get("found_amount", "0"))
    if reason == "expired":
        return t(uid, "claim_expired")
    if reason == "not_found":
        return t(uid, "claim_not_found", network=network)
    return t(uid, "claim_error")


async def verify_pending_deposit_tx(deposit: dict, txid: str) -> dict:
    network = normalize_network_name(deposit.get("network", "TRC20"))
    txid_norm = canonicalize_txid(txid, network)
    if not txid_norm:
        return {"ok": False, "reason": "invalid_txid"}
    if is_deposit_txid_already_used(txid_norm):
        return {"ok": False, "reason": "already_used"}

    if network == "BINANCE":
        import asyncio as _aio
        from services.binance_pay_api import verify_binance_pay_order, verify_binance_spot_deposit

        tx_info = await _aio.to_thread(verify_binance_pay_order, txid_norm)
        if not tx_info:
            spot_amount = await _aio.to_thread(verify_binance_spot_deposit, txid_norm, "BINANCE")
            if spot_amount:
                tx_info = {"amount": spot_amount}

        if not tx_info:
            return {"ok": False, "reason": "not_found"}

        expected_amount = pending_expected_amount_decimal(deposit)
        found_dec = parse_amount_decimal(tx_info.get("amount"))
        if found_dec is None or found_dec != expected_amount:
            return {
                "ok": False,
                "reason": "amount_mismatch",
                "found_amount": decimal_to_display(found_dec) if found_dec is not None else str(tx_info.get("amount"))
            }

        return {
            "ok": True,
            "txid": txid_norm,
            "amount_decimal": found_dec,
            "amount_text": decimal_to_display(found_dec),
            "network": network,
        }

    if network == "TRC20":
        tx_data = await _fetch_trc20_transfer_by_txid(deposit.get("wallet_address", MY_TRC20_ADDRESS), txid_norm)
    else:
        tx_data = await _fetch_bep20_transfer_by_txid(deposit.get("wallet_address", MY_BEP20_ADDRESS), txid_norm)

    if tx_data.get("error"):
        return {"ok": False, "reason": "error", "error": tx_data.get("error")}
    if not tx_data.get("found"):
        return {"ok": False, "reason": "not_found"}
    if not tx_data.get("confirmed", False):
        return {"ok": False, "reason": "not_confirmed"}
    if tx_data.get("wrong_wallet"):
        return {"ok": False, "reason": "not_found"}

    actual_amount = tx_data.get("amount_decimal")
    expected_amount = pending_expected_amount_decimal(deposit)
    if actual_amount is None or actual_amount != expected_amount:
        return {
            "ok": False,
            "reason": "amount_mismatch",
            "found_amount": tx_data.get("amount_text") or decimal_to_display(actual_amount),
        }

    return {
        "ok": True,
        "txid": tx_data.get("txid", txid_norm),
        "amount_decimal": actual_amount,
        "amount_text": tx_data.get("amount_text") or decimal_to_display(actual_amount),
        "network": network,
    }


async def process_deposit_claim(update: Update, context: ContextTypes.DEFAULT_TYPE, raw_txid: str):
    uid = update.effective_user.id
    message = update.message or (update.callback_query.message if update.callback_query else None)
    if not message:
        return

    pending = get_user_pending_deposit(uid)
    if not pending:
        return await message.reply_text(t(uid, "deposit_no_pending"), parse_mode="HTML")

    expires_at = parse_db_datetime(pending.get("expires_at"))
    if expires_at and expires_at < datetime.datetime.utcnow():
        expire_pending_deposits()
        return await message.reply_text(t(uid, "claim_expired"), parse_mode="HTML")

    txid_norm = canonicalize_txid(raw_txid, pending.get("network", "TRC20"))
    if not txid_norm:
        return await message.reply_text(t(uid, "claim_invalid_txid"), parse_mode="HTML")

    status_msg = await message.reply_text(t(uid, "claim_checking"), parse_mode="HTML")
    result = await verify_pending_deposit_tx(pending, txid_norm)
    if not result.get("ok"):
        failure_text = _claim_failure_text(uid, normalize_network_name(pending.get("network", "TRC20")), result, pending_expected_amount_str(pending))
        try:
            await status_msg.edit_text(failure_text, parse_mode="HTML")
        except Exception:
            await message.reply_text(failure_text, parse_mode="HTML")
        return

    confirmed = await _confirm_deposit(context, pending, result["txid"], result["amount_decimal"])
    if confirmed:
        try:
            await status_msg.delete()
        except Exception:
            pass
    else:
        try:
            await status_msg.edit_text(t(uid, "claim_already_used"), parse_mode="HTML")
        except Exception:
            await message.reply_text(t(uid, "claim_already_used"), parse_mode="HTML")


async def cmd_claim(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if current_bot_mode(context) != "main_user":
        return await update.message.reply_text(t(uid, "activate_not_available"), parse_mode="HTML")
    if not context.args:
        return await update.message.reply_text(t(uid, "claim_usage"), parse_mode="HTML")
    txid = context.args[0].strip()
    return await process_deposit_claim(update, context, txid)


async def check_blockchain_deposits(context: ContextTypes.DEFAULT_TYPE):
    """Lightweight cleanup job: only expire stale pending deposits."""
    expire_pending_deposits()


async def handle_deposit_amount_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the user typing an amount after selecting TRC20/BEP20."""
    uid = update.effective_user.id
    text = (update.message.text or "").strip()
    network = context.user_data.get("deposit_network", "TRC20")

    base_amount = parse_amount_decimal(text)
    if base_amount is None:
        return await update.message.reply_text(t(uid, "deposit_invalid_amount"), parse_mode="HTML")

    if base_amount < Decimal("1"):
        return await update.message.reply_text(t(uid, "deposit_min_amount"), parse_mode="HTML")

    context.user_data.pop("deposit_step", None)
    context.user_data.pop("deposit_network", None)

    existing = get_user_pending_deposit(uid)
    if existing:
        expires_str = existing.get("expires_at", "")[:16].replace("T", " ")
        return await update.message.reply_text(
            t(uid, "deposit_already_pending",
              amount=pending_expected_amount_str(existing),
              network=existing["network"],
              wallet=existing["wallet_address"],
              expires=expires_str),
            parse_mode="HTML"
        )

    unique_amount_str = generate_unique_deposit_amount(base_amount, network)
    wallet = MY_TRC20_ADDRESS if network == "TRC20" else MY_BEP20_ADDRESS
    expires_at = (datetime.datetime.utcnow() + datetime.timedelta(minutes=30)).isoformat()

    conn = db_connect()
    conn.execute(
        "INSERT INTO pending_deposits (user_id, network, expected_amount, expected_amount_str, base_amount, wallet_address, expires_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (uid, network, float(parse_amount_decimal(unique_amount_str) or 0), unique_amount_str, float(base_amount), wallet, expires_at)
    )
    conn.commit()
    conn.close()

    amount_display = unique_amount_str
    key = "deposit_pending_trc20" if network == "TRC20" else "deposit_pending_bep20"
    await update.message.reply_text(
        t(uid, key, amount=amount_display, wallet=wallet),
        parse_mode="HTML"
    )


async def cmd_deposit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    msg = update.callback_query.message if update.callback_query else update.message
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💎 USDT TRC20", callback_data="dep_trc20"),
            InlineKeyboardButton("🟡 USDT BEP20", callback_data="dep_bep20"),
        ],
        [InlineKeyboardButton("🇩🇿 BaridiMob", callback_data="dep_baridimob")],
    ])
    await msg.reply_text(t(uid, "deposit_choose_network"), parse_mode="HTML", reply_markup=kb)

async def cmd_activate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    mode = current_bot_mode(context)
    if mode != "main_user":
        return await update.message.reply_text(t(uid, "activate_not_available"), parse_mode="HTML")
    if is_user_banned(uid):
        return await update.message.reply_text(t(uid, "banned"), parse_mode="HTML")
    if not await check_channel_join(uid, context.bot, mode):
        return await send_join_alert(update, uid)
    if MAINTENANCE_MODE and uid != OWNER_ID:
        return await update.message.reply_text(t(uid, "maint_msg"), parse_mode="HTML")
    clear_all_user_flow_states(context)
    context.user_data["act_step"] = "email"
    context.user_data.pop("waiting_for_credentials", None)
    is_ar = False
    cancel_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🚫 " + ("Cancel"), callback_data="act_cancel_flow"),
    ]])
    await update.message.reply_text(t(uid, "send_activate_prompt"), parse_mode="HTML", reply_markup=cancel_kb)

async def cmd_daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    msg = update.callback_query.message if update.callback_query else update.message
    if update.callback_query:
        try:
            await update.callback_query.answer()
        except Exception:
            pass
    is_ar = False
    home_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🏠 " + ("Home"), callback_data="start_home"),
    ]])
    await msg.reply_text(
        "⚠️ This feature is not available.",
        parse_mode="HTML", reply_markup=home_kb
    )

async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    mode = current_bot_mode(context)
    msg = update.callback_query.message if update.callback_query else update.message
    if update.callback_query:
        try:
            await update.callback_query.answer()
        except Exception:
            pass
    if mode != "main_user":
        return await msg.reply_text(t(uid, "activate_not_available"), parse_mode="HTML")
    if is_user_banned(uid):
        return await msg.reply_text(t(uid, "banned"), parse_mode="HTML")
    is_ar = False
    nav_kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💰 " + ("Deposit"), callback_data="user_deposit"),
            InlineKeyboardButton("🏠 " + ("Home"), callback_data="start_home"),
        ]
    ])
    try:
        hist = get_user_history(uid)
        if not hist:
            await msg.reply_text(t(uid, "history_empty"), parse_mode="HTML", reply_markup=nav_kb)
        else:
            log_text = ""
            for row in hist:
                status = row[1] if isinstance(row, (list, tuple)) else row['status']
                ts = row[5] if isinstance(row, (list, tuple)) else row['ts']
                email = row[0] if isinstance(row, (list, tuple)) else row['email']
                url = row[2] if isinstance(row, (list, tuple)) else row['url']
                reason = row[3] if isinstance(row, (list, tuple)) else row['reason']
                tx_id = row[4] if isinstance(row, (list, tuple)) else row['tx_id']
                log_text += f"🔹 <b>{status}</b> | {ts}\n📧 {email}\n"
                if url:
                    log_text += f"🔗 {url}\n"
                if reason:
                    log_text += f"❌ {reason}\n"
                if tx_id:
                    log_text += f"🧾 #{tx_id}\n"
                log_text += "\n"
            await msg.reply_text(t(uid, "history_title", log=log_text), parse_mode="HTML", reply_markup=nav_kb)
    except Exception:
        await msg.reply_text("⚠️ Could not load history. Please try again.", parse_mode="HTML", reply_markup=nav_kb)

async def cmd_invite(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    msg = update.callback_query.message if update.callback_query else update.message
    mode = current_bot_mode(context)
    if update.callback_query:
        try:
            await update.callback_query.answer()
        except Exception:
            pass
    if mode == "main_user":
        bot_username = context.bot.username or MY_BOT_USERNAME
    else:
        bot_username = MY_BOT_USERNAME
    is_ar = False
    invite_link = f"https://t.me/{bot_username}?start={uid}"
    nav_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 " + ("Copy Link"), url=invite_link)],
        [InlineKeyboardButton("🏠 " + ("Home"), callback_data="start_home")],
    ])
    await msg.reply_text(t(uid, "invite_msg_user", bot=bot_username, uid=uid), parse_mode="HTML", reply_markup=nav_kb)

async def cmd_myinvite(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    msg = update.callback_query.message if update.callback_query else update.message
    mode = current_bot_mode(context)
    if mode == "main_user":
        bot_username = context.bot.username or MY_BOT_USERNAME
    else:
        bot_username = MY_BOT_USERNAME
    invite_link = f"https://t.me/{bot_username}?start={uid}"
    nav_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Copy Link", url=invite_link)],
        [InlineKeyboardButton("🏠 Home", callback_data="start_home")],
    ])
    await msg.reply_text(
        f"🔗 <b>Your Link:</b>\n<code>{invite_link}</code>",
        parse_mode="HTML", reply_markup=nav_kb
    )

async def cmd_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    mode = current_bot_mode(context)
    msg = update.callback_query.message if update.callback_query else update.message
    if is_user_banned(uid):
        if update.callback_query:
            return await update.callback_query.answer(t(uid, "banned"), show_alert=True)
        return await msg.reply_text(t(uid, "banned"), parse_mode="HTML")
    if mode == "main_user" and not await check_channel_join(uid, context.bot, mode):
        return await send_join_alert(update, uid)

    res = get_user_data(uid)
    display_name = update.effective_user.first_name or update.effective_user.username or "Unknown"
    is_ar = False

    if mode == "external_user":
        shop_id = current_shop_id(context)
        text = t(uid, "profile_ext_msg", uid=uid, name=display_name, balance=get_user_balance(uid), orders=get_purchase_count(uid, shop_id))
        nav_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(t(uid, "btn_shop"), callback_data="user_shop")],
            [InlineKeyboardButton("🏠 " + ("Home"), callback_data="start_home")],
        ])
    else:
        text = t(
            uid, "profile_msg",
            uid=uid, name=display_name,
            balance=float(res["balance"]),
            price=float(get_activate_price_for_user(uid)),
            succ=int(res["success_count"]),
            fail=int(res["fail_count"])
        )
        nav_kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("💰 " + ("Deposit"), callback_data="user_deposit"),
                InlineKeyboardButton("📋 " + ("History"), callback_data="user_history"),
            ],
            [InlineKeyboardButton("🏠 " + ("Home"), callback_data="start_home")],
        ])
    await msg.reply_text(text, parse_mode="HTML", reply_markup=nav_kb)

async def cmd_shop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    mode = current_bot_mode(context)
    msg = update.callback_query.message if update.callback_query else update.message
    if is_user_banned(uid):
        if update.callback_query:
            return await update.callback_query.answer(t(uid, "banned"), show_alert=True)
        return await msg.reply_text(t(uid, "banned"), parse_mode="HTML")
    if mode == "main_user" and not await check_channel_join(uid, context.bot, mode):
        return await send_join_alert(update, uid)

    shop_id = current_shop_id(context)
    cats = get_categories(shop_id)
    if not cats:
        return await msg.reply_text(t(uid, "shop_empty"), parse_mode="HTML")

    kb = [[InlineKeyboardButton(f"📂 {c}", callback_data=f"shop_cat_{c}")] for c in cats]
    if update.callback_query:
        try:
            await msg.edit_text(t(uid, "shop_title"), parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))
            return
        except Exception:
            pass
    await msg.reply_text(t(uid, "shop_title"), parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    mode = current_bot_mode(context)
    if mode == "main_admin":
        key = "admin_owner" if uid == OWNER_ID else "admin_reseller"
    elif mode == "ext_admin":
        key = "ext_admin"
    elif mode == "external_user":
        key = "external_user"
    else:
        key = "main_user"
    text = help_text_for(uid, key)
    kb = build_help_keyboard(uid, mode)
    msg = update.callback_query.message if update.callback_query else update.message
    await msg.reply_text(text, parse_mode="HTML", reply_markup=kb)


# =========================
# START / MAIN MENU
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mode = current_bot_mode(context)
    if mode == "main_admin":
        return await main_admin_start(update, context)
    if mode == "ext_admin":
        return await external_admin_start(update, context)

    user = update.effective_user
    uid = user.id
    username = user.username or "Unknown"
    first_name = user.first_name or ""
    msg = update.callback_query.message if update.callback_query else update.message

    new_user = update_user_info(uid, username, first_name)
    if mode == "external_user":
        update_shop_user_info(current_shop_id(context), uid, username, first_name, get_user_lang(uid))

    if mode == "main_user" and new_user and context.args:
        try:
            ref_id = int(context.args[0])
            if bind_referrer(uid, ref_id):
                if is_reseller(ref_id):
                    # Link new user to reseller's client list (enables profit per activation)
                    try:
                        _c = db_connect()
                        _c.execute(
                            "UPDATE users SET owner_id=? WHERE user_id=? AND (owner_id IS NULL OR owner_id=0)",
                            (ref_id, uid)
                        )
                        _c.commit()
                        _c.close()
                    except Exception:
                        pass
        except Exception:
            pass

    if mode == "main_user" and new_user:
        try:
            await context.bot.send_message(OWNER_ID, f"🔔 <b>New User:</b>\n@{username} (ID: {uid})", parse_mode="HTML")
        except Exception:
            pass

    if is_user_banned(uid):
        return await msg.reply_text(t(uid, "banned"), parse_mode="HTML")

    if mode == "main_user" and not await check_channel_join(uid, context.bot, mode):
        return await send_join_alert(update, uid)


    if mode == "external_user":
        shop_id = current_shop_id(context)
        text = t(
            uid, "welcome_external",
            uid=uid,
            users_count=get_shop_user_count(shop_id),
            balance=float(get_user_balance(uid)),
            products=get_shop_product_count(shop_id),
            shop_title=current_external_title(context),
        )
        kb = build_external_user_keyboard(uid)
    else:
        res = get_user_data(uid)
        service_line = ""
        if not MAINTENANCE_MODE:
            service_line = "⚡ <b>Service:</b> Google One Activation (24/7)\n"
        text = t(
            uid, "welcome",
            uid=uid,
            users_count=get_total_users(),
            balance=float(res["balance"]),
            price=float(get_activate_price_for_user(uid)),
            service_line=service_line,
            status=t(uid, "status_maint") if MAINTENANCE_MODE else t(uid, "status_active"),
        )
        kb = build_main_user_keyboard(uid, show_activate=not MAINTENANCE_MODE)

    if update.callback_query:
        try:
            await msg.edit_text(text, parse_mode="HTML", reply_markup=kb)
        except Exception:
            await msg.reply_text(text, parse_mode="HTML", reply_markup=kb)
    else:
        await msg.reply_text(text, parse_mode="HTML", reply_markup=kb)


# =========================
# SHOP CALLBACK
# =========================
async def callback_shop_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    mode = current_bot_mode(context)
    shop_id = current_shop_id(context)

    if is_user_banned(uid):
        return await query.answer(t(uid, "banned"), show_alert=True)
    if mode == "main_user" and not await check_channel_join(uid, context.bot, mode):
        return await send_join_alert(update, uid)

    data = query.data
    if data.startswith("shop_cat_"):
        cat = data.split("_", 2)[2]
        products = get_products_by_cat(shop_id, cat)
        kb = []
        for prod in products:
            kb.append([InlineKeyboardButton(f"✅ {prod['name']} (${float(prod['price']):.2f})", callback_data=f"view_prod_{prod['id']}")])
        kb.append([InlineKeyboardButton(t(uid, "btn_back"), callback_data="user_shop")])
        await query.message.edit_text(t(uid, "shop_cat", cat=cat), parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))
        return

    if data.startswith("view_prod_"):
        pid = int(data.split("_")[2])
        prod = get_product(shop_id, pid)
        if not prod:
            return await query.answer("Unavailable", show_alert=True)
        is_ar = False
        kb = [
            [InlineKeyboardButton(t(uid, "btn_buy"), callback_data=f"buy_ask_{pid}")],
            [InlineKeyboardButton(t(uid, "btn_back"), callback_data="user_shop")]
        ]
        text = t(uid, "shop_prod_view", name=prod["name"], desc=prod["description"] or "-", price=float(prod["price"]), stock=int(prod["stock"]), delivery=delivery_type_label(uid, prod))
        if prod.get("image_id"):
            try:
                await query.message.delete()
                await context.bot.send_photo(uid, photo=prod["image_id"], caption=text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))
            except Exception:
                await query.message.reply_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))
        else:
            try:
                await query.message.edit_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))
            except Exception:
                await query.message.reply_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))
        return

    if data.startswith("buy_ask_"):
        pid = int(data.split("_")[2])
        prod = get_product(shop_id, pid)
        if not prod:
            return await query.answer("Unavailable", show_alert=True)
        stock_val = int(prod["stock"])
        context.user_data["buying_pid"] = pid
        context.user_data["buying_shop_id"] = shop_id
        context.user_data["buying_max"] = stock_val if stock_val > 0 else 9999
        context.user_data["buying_manual_delivery"] = stock_val < 1
        context.user_data["buying_price"] = float(prod["price"])
        context.user_data["buying_name"] = prod["name"]
        context.user_data["state"] = "WAIT_QTY"
        await query.message.reply_text(t(uid, "shop_ask_qty"), parse_mode="HTML")
        await query.answer()


# =========================
# MAIN USER CALLBACK MENU
# =========================
async def callback_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    mode = current_bot_mode(context)

    if is_user_banned(uid):
        return await query.answer(t(uid, "banned"), show_alert=True)
    if mode == "main_user" and not await check_channel_join(uid, context.bot, mode):
        await send_join_alert(update, uid)
        try:
            await query.answer()
        except Exception:
            pass
        return

    data = query.data

    if data == "user_home":
        await start(update, context)
        try:
            await query.answer()
        except Exception:
            pass
        return

    if data == "user_activate":
        if mode != "main_user":
            return await query.answer(t(uid, "activate_not_available"), show_alert=True)
        if MAINTENANCE_MODE and uid != OWNER_ID:
            return await query.answer(t(uid, "maint_msg"), show_alert=True)
        direct_prods = get_all_products(0)
        visible_prods = [p for p in direct_prods if not p.get("hidden")]
        rows = [
            [InlineKeyboardButton(t(uid, "btn_google_one"), callback_data="user_google_one")],
            [InlineKeyboardButton(t(uid, "btn_ws_seat"), callback_data="user_ws_seat")],
        ]
        if visible_prods:
            is_ar = False
            rows.append([InlineKeyboardButton(
                "🛍️ More Products",
                callback_data="user_shop_direct"
            )])
        kb = InlineKeyboardMarkup(rows)
        await query.message.reply_text(t(uid, "activate_choose_service"), reply_markup=kb, parse_mode="HTML")
        await query.answer()
        return

    if data == "user_google_one":
        if mode != "main_user":
            return await query.answer(t(uid, "activate_not_available"), show_alert=True)
        if MAINTENANCE_MODE and uid != OWNER_ID:
            return await query.answer(t(uid, "maint_msg"), show_alert=True)
        # If already mid-flow, don't restart — ask to cancel first
        if context.user_data.get("act_step"):
            step = context.user_data["act_step"]
            step_labels = {"email": "", "password": "", "totp": "Enter TOTP"}
            label = step_labels.get(step, step)
            cancel_kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("", callback_data="act_cancel_flow"),
            ]])
            await query.answer("", show_alert=True)
            await query.message.reply_text(
                f"⚠️ <b>You already have an activation in progress</b>\n\n"
                f"📍 Stage Current: <b>{label}</b>\n\n"
                f"",
                parse_mode="HTML",
                reply_markup=cancel_kb
            )
            return
        # Clear any lingering WS/deposit states before starting activation
        clear_all_user_flow_states(context)
        context.user_data["act_step"] = "email"
        context.user_data.pop("waiting_for_credentials", None)
        await query.message.reply_text(t(uid, "send_activate_prompt"), parse_mode="HTML")
        await query.answer()
        return

    if data == "user_ws_seat":
        if mode != "main_user":
            return await query.answer(t(uid, "activate_not_available"), show_alert=True)
        if MAINTENANCE_MODE and uid != OWNER_ID:
            return await query.answer(t(uid, "maint_msg"), show_alert=True)
        monthly = get_ws_monthly_price()
        user_bal = get_user_balance(uid)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(t(uid, "btn_ws_by_key"), callback_data="user_ws_key")],
            [InlineKeyboardButton(
                t(uid, "btn_ws_by_balance", price=f"{monthly:.2f}"),
                callback_data="user_ws_bal"
            )],
        ])
        await query.message.reply_text(
            t(uid, "ws_seat_choose_method", price=f"{monthly:.2f}", balance=f"{user_bal:.2f}"),
            reply_markup=kb, parse_mode="HTML"
        )
        await query.answer()
        return

    if data == "user_ws_key":
        if mode != "main_user":
            return await query.answer(t(uid, "activate_not_available"), show_alert=True)
        clear_all_user_flow_states(context)
        context.user_data["waiting_ws_invite_key"] = True
        await query.message.reply_text(t(uid, "ws_enter_key_prompt"), parse_mode="HTML")
        await query.answer()
        return

    if data == "user_ws_bal":
        if mode != "main_user":
            return await query.answer(t(uid, "activate_not_available"), show_alert=True)
        if not _get_available_workspace():
            return await query.answer(t(uid, "ws_seat_no_ws"), show_alert=True)
        lang = get_user_lang(uid)
        user_bal = get_user_balance(uid)
        monthly = get_ws_monthly_price()
        is_ar = lang == "ar"
        rows = []
        for hours, lbl_ar, lbl_en in WS_DURATION_OPTIONS:
            price = ws_calc_price(hours)
            label = lbl_ar if is_ar else lbl_en
            btn_text = f"{label} — ${price:.2f}"
            rows.append([InlineKeyboardButton(btn_text, callback_data=f"user_ws_dur_{hours}")])
        rows.append([InlineKeyboardButton("❌ " + ("Cancel"), callback_data="user_ws_cancel")])
        header = (
            f"🤖 <b>{'Choose Subscription Duration'}</b>\n\n"
            f"💳 {'Your Balance'}: <b>${user_bal:.2f}</b>\n"
            f"💰 {'Monthly Price'}: <b>${monthly:.2f}</b>\n\n"
            f"{'Duration starts from when you submit your email.'}"
        )
        await query.message.reply_text(header, reply_markup=InlineKeyboardMarkup(rows), parse_mode="HTML")
        await query.answer()
        return

    if data.startswith("user_ws_dur_"):
        if mode != "main_user":
            return await query.answer(t(uid, "activate_not_available"), show_alert=True)
        try:
            dur_hours = int(data[len("user_ws_dur_"):])
        except Exception:
            return await query.answer("❌ Error", show_alert=True)
        price = ws_calc_price(dur_hours)
        user_bal = get_user_balance(uid)
        if user_bal < price:
            need = price - user_bal
            return await query.answer(t(uid, "ws_seat_no_bal", need=f"{need:.2f}"), show_alert=True)
        ws = _get_available_workspace()
        if not ws:
            return await query.answer(t(uid, "ws_seat_no_ws"), show_alert=True)
        lang = get_user_lang(uid)
        is_ar = lang == "ar"
        dur_label = next((lbl_ar if is_ar else lbl_en for h, lbl_ar, lbl_en in WS_DURATION_OPTIONS if h == dur_hours), str(dur_hours) + "h")
        remaining = user_bal - price
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(
                    "✅ " + ("Confirm"),
                    callback_data=f"user_ws_cfm2_{ws['id']}_{dur_hours}"
                ),
                InlineKeyboardButton(
                    "❌ " + ("Cancel"),
                    callback_data="user_ws_cancel"
                ),
            ]
        ])
        confirm_text = (
            f"✅ <b>{'Confirm Subscription'}</b>\n\n"
            f"🕒 {'Duration'}: <b>{dur_label}</b>\n"
            f"💰 {'Price'}: <b>${price:.2f}</b>\n"
            f"💳 {'Remaining Balance'}: <b>${remaining:.2f}</b>\n"
            f"🏢 Workspace: <b>{ws['name']}</b>"
        )
        await query.message.reply_text(confirm_text, reply_markup=kb, parse_mode="HTML")
        await query.answer()
        return

    if data.startswith("user_ws_cfm2_"):
        if mode != "main_user":
            return await query.answer(t(uid, "activate_not_available"), show_alert=True)
        try:
            parts = data[len("user_ws_cfm2_"):].rsplit("_", 1)
            ws_id, dur_hours = parts[0], int(parts[1])
        except Exception:
            return await query.answer("❌ Error", show_alert=True)
        price = ws_calc_price(dur_hours)
        user_bal = get_user_balance(uid)
        if user_bal < price:
            need = price - user_bal
            return await query.answer(t(uid, "ws_seat_no_bal", need=f"{need:.2f}"), show_alert=True)
        ws = _get_workspace_by_id(ws_id)
        if not ws or ws["status"] != "active":
            return await query.answer(t(uid, "ws_seat_no_ws"), show_alert=True)
        if not _workspace_has_capacity(ws_id):
            return await query.answer(t(uid, "ws_seat_no_ws"), show_alert=True)
        add_balance(uid, -price)
        clear_all_user_flow_states(context)
        context.user_data["ws_seat_pending_ws_id"] = ws_id
        context.user_data["ws_seat_pending_paid"] = True
        context.user_data["ws_seat_sub_hours"] = dur_hours
        # Suggest saved email if available
        last_email = context.user_data.get("ws_last_email", "")
        if last_email:
            is_ar = False
            kb_hint = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    f"{'Use'} {last_email}",
                    callback_data=f"user_ws_use_last_email"
                )
            ]])
            await query.message.reply_text(t(uid, "ws_seat_email_prompt"), parse_mode="HTML", reply_markup=kb_hint)
        else:
            await query.message.reply_text(t(uid, "ws_seat_email_prompt"), parse_mode="HTML")
        await query.answer()
        return

    if data == "user_ws_cancel":
        try:
            await query.message.delete()
        except Exception:
            pass
        await query.answer()
        return

    if data == "user_ws_use_last_email":
        # User chose to reuse their saved email for workspace subscription
        last_email = context.user_data.get("ws_last_email", "")
        if not last_email or not context.user_data.get("ws_seat_pending_paid"):
            await query.answer("❌ Session expired, please re-enter your email.", show_alert=True)
            return
        # Reuse the saved email — process subscription directly
        ws_id = context.user_data.pop("ws_seat_pending_ws_id", None)
        context.user_data.pop("ws_seat_pending_paid", None)
        sub_hours = context.user_data.pop("ws_seat_sub_hours", 720)
        username = update.effective_user.username
        if not ws_id:
            await query.answer("❌ Session expired.", show_alert=True)
            return
        ws = _get_workspace_by_id(ws_id)
        ws_name = ws["name"] if ws else str(ws_id)
        price = ws_calc_price(sub_hours)
        req = ws_create_request(str(ws_id), last_email, "", uid, username, paid_amount=price)
        sub = ws_create_subscription(uid, last_email, str(ws_id), sub_hours, request_id=req["id"])
        dur_label = next((lbl_en for h, _, lbl_en in WS_DURATION_OPTIONS if h == sub_hours), f"{sub_hours}h")
        expires_str = sub["expires_at"][:16]
        is_ar = False
        done_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("💳 " + ("My Subscriptions"), callback_data="user_my_subs"),
            InlineKeyboardButton("🏠 " + ("Home"), callback_data="start_home"),
        ]])
        await query.message.reply_text(
            (f"✅ <b>Subscribed!</b>\n\n📧 Account: <code>{last_email}</code>\n"
             f"🏢 Workspace: <b>{ws_name}</b>\n"
             f"🕒 Duration: <b>{next(lbl_en for h, _, lbl_en in WS_DURATION_OPTIONS if h == sub_hours)}</b>\n"
             f"📅 Expires: <b>{expires_str}</b>\n\n⏳ You will be added shortly."
             if is_ar else
             f"✅ <b>Subscribed!</b>\n\n📧 Email: <code>{last_email}</code>\n"
             f"🏢 Workspace: <b>{ws_name}</b>\n🕒 Duration: <b>{dur_label}</b>\n"
             f"📅 Expires: <b>{expires_str}</b>\n\n⏳ You will be added shortly."),
            parse_mode="HTML", reply_markup=done_kb
        )
        await query.answer()
        return

    # ── Shop Direct (flat product list inside Activate menu) ─────────────
    if data == "user_shop_direct":
        if mode != "main_user":
            return await query.answer(t(uid, "activate_not_available"), show_alert=True)
        is_ar = False
        prods = [p for p in get_all_products(0) if not p.get("hidden")]
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton(
            "🔙 " + ("Back"), callback_data="user_activate"
        )]])
        if not prods:
            return await query.message.reply_text(
                "📭 " + ("No products available."),
                reply_markup=back_kb
            )
        rows = []
        for p in prods:
            rows.append([InlineKeyboardButton(
                f"✅ {p['name']} (${float(p['price']):.2f})",
                callback_data=f"user_prod_{p['id']}"
            )])
        rows.append([InlineKeyboardButton(
            "🔙 " + ("Back"), callback_data="user_activate"
        )])
        title = "🛍️ <b>Products</b>"
        await query.message.reply_text(title, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows))
        return await query.answer()

    if data.startswith("user_prod_"):
        if mode != "main_user":
            return await query.answer(t(uid, "activate_not_available"), show_alert=True)
        try:
            pid = int(data.split("_")[2])
        except Exception:
            return await query.answer("❌", show_alert=True)
        prod = get_product(0, pid)
        if not prod:
            return await query.answer("❌ Unavailable", show_alert=True)
        is_ar = False
        kb = [
            [InlineKeyboardButton(t(uid, "btn_buy"), callback_data=f"buy_ask_{pid}")],
            [InlineKeyboardButton("🔙 " + ("Back"), callback_data="user_shop_direct")]
        ]
        text = t(uid, "shop_prod_view",
                 name=prod["name"],
                 desc=prod.get("description") or "-",
                 price=float(prod["price"]),
                 stock=int(prod["stock"]),
                 delivery=delivery_type_label(uid, prod))
        kb_markup = InlineKeyboardMarkup(kb)
        if prod.get("image_id"):
            try:
                await query.message.delete()
                await context.bot.send_photo(uid, photo=prod["image_id"], caption=text, parse_mode="HTML", reply_markup=kb_markup)
            except Exception:
                await query.message.reply_text(text, parse_mode="HTML", reply_markup=kb_markup)
        else:
            try:
                await query.message.edit_text(text, parse_mode="HTML", reply_markup=kb_markup)
            except Exception:
                await query.message.reply_text(text, parse_mode="HTML", reply_markup=kb_markup)
        return await query.answer()

    if data == "user_balance":
        res = get_user_data(uid)
        await query.message.reply_text(
            t(uid, "balance_msg",
              balance=float(res["balance"]),
              price=float(get_activate_price_for_user(uid)),
              succ=int(res["success_count"]),
              fail=int(res["fail_count"])),
            parse_mode="HTML"
        )
        return await query.answer()

    if data == "user_profile":
        await cmd_profile(update, context)
        return await query.answer()
    if data == "user_daily":
        await cmd_daily(update, context)
        return await query.answer()
    if data == "user_shop":
        await cmd_shop(update, context)
        return await query.answer()
    if data == "user_deposit":
        if mode != "main_user":
            return await query.answer(t(uid, "activate_not_available"), show_alert=True)
        await cmd_deposit(update, context)
        return await query.answer()

    if data in ("dep_trc20", "dep_bep20"):
        if mode != "main_user":
            return await query.answer(t(uid, "activate_not_available"), show_alert=True)
        network = "TRC20" if data == "dep_trc20" else "BEP20"
        existing = get_user_pending_deposit(uid)
        if existing:
            expires_str = existing.get("expires_at", "")[:16].replace("T", " ")
            await query.message.reply_text(
                t(uid, "deposit_already_pending",
                  amount=pending_expected_amount_str(existing),
                  network=existing["network"],
                  wallet=existing["wallet_address"],
                  expires=expires_str),
                parse_mode="HTML"
            )
            return await query.answer()
        clear_all_user_flow_states(context)
        context.user_data["deposit_step"] = "amount"
        context.user_data["deposit_network"] = network
        await query.message.reply_text(t(uid, "deposit_ask_amount"), parse_mode="HTML")
        return await query.answer()

    if data == "dep_baridimob":
        await query.message.reply_text(
            t(uid, "deposit_baridimob_info", rib=MY_BARIDIMOB_RIB or "N/A"),
            parse_mode="HTML"
        )
        return await query.answer()

    if data == "user_invite":
        if mode != "main_user":
            return await query.answer(t(uid, "activate_not_available"), show_alert=True)
        await cmd_invite(update, context)
        return await query.answer()
    if data == "user_lang":
        await cmd_language(update, context)
        return await query.answer()
    if data == "user_help":
        await cmd_help(update, context)
        return await query.answer()
    if data == "user_history":
        if mode != "main_user":
            return await query.answer(t(uid, "activate_not_available"), show_alert=True)
        await cmd_history(update, context)
        try:
            await query.answer()
        except Exception:
            pass
        return
    if data == "user_support":
        await cmd_support(update, context)
        return

    # ── My Subscriptions ────────────────────────────────────────────────
    if data == "user_my_subs":
        is_ar = False
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            subs = conn.execute(
                "SELECT * FROM chatgpt_subscriptions WHERE user_id=? ORDER BY created_at DESC LIMIT 10",
                (uid,)
            ).fetchall()
        if not subs:
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("🏠 Home", callback_data="start_home")
            ]])
            return await query.message.reply_text(
                "📭 No subscriptions found.",
                reply_markup=kb
            )
        status_icon = {"active": "🟢", "expired": "🔴", "migrated": "🔄", "revoked": "🚫"}
        lines = []
        for s in subs:
            icon = status_icon.get(s["status"], "⚪")
            sub_h = int(s["subscription_hours"] or 720)
            dur_label = next(
                (lbl_ar if is_ar else lbl_en for h, lbl_ar, lbl_en in WS_DURATION_OPTIONS if h == sub_h),
                f"{sub_h}h"
            )
            exp = str(s["expires_at"])[:16]
            ws_obj = ws_get_workspace(s["workspace_id"])
            ws_name = ws_obj["name"] if ws_obj else "?"
            lines.append(
                f"{icon} <code>{s['email']}</code>\n"
                f"   🏢 {ws_name} | 🕒 {dur_label}\n"
                f"   📅 {'Expires'}: <b>{exp}</b>"
            )
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 " + ("Refresh"), callback_data="user_my_subs"),
            InlineKeyboardButton("🏠 " + ("Home"), callback_data="start_home"),
        ]])
        title = "💳 <b>My Subscriptions</b>"
        await query.message.reply_text(
            f"{title}\n\n" + "\n\n".join(lines),
            parse_mode="HTML", reply_markup=kb
        )
        return await query.answer()

    if data == "start_home":
        clear_all_user_flow_states(context)
        await cmd_start(update, context)
        return await query.answer()

    if data.startswith("confirm_activate_"):
        await handle_confirm_activate(update, context)
        return

    if data in ("cancel_activate", "act_cancel_flow"):
        context.user_data.pop("pending_activation", None)
        context.user_data.pop("act_step", None)
        context.user_data.pop("act_email", None)
        context.user_data.pop("act_password", None)
        await query.message.reply_text(
            "🚫 <b>Activation cancelled.</b>\n\nYou can restart when needed.",
            parse_mode="HTML"
        )
        await query.answer()
        return


# =========================
# FINALIZE PURCHASE
# =========================
async def finalize_purchase(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int, shop_id: int, product: dict, qty: int, cost: float):
    add_shop_balance(user_id, -cost, shop_id=shop_id)
    delivery_data = ""
    mode = (product.get("delivery_type") or "manual").lower()

    if mode == "codes":
        codes = claim_product_codes(shop_id, int(product["id"]), user_id, qty)
        if len(codes) < qty:
            add_shop_balance(user_id, cost, shop_id=shop_id)
            return False, "Not enough codes available.", ""
        delivery_data = "\n".join(codes)
        sync_product_stock_from_codes(shop_id, int(product["id"]))
    else:
        reduce_stock(shop_id, int(product["id"]), qty)

    record_purchase(shop_id, user_id, int(product["id"]), float(cost), int(qty), "Instant", delivery_data)
    await update.effective_message.reply_text(t(user_id, "prod_bought", name=product["name"], qty=qty, total=float(cost)), parse_mode="HTML")

    if mode == "codes" and delivery_data:
        await update.effective_message.reply_text(t(user_id, "prod_codes_delivered", codes=delivery_data), parse_mode="HTML")
    elif mode == "file" and product.get("file_id"):
        try:
            await context.bot.send_document(chat_id=user_id, document=product["file_id"], caption=product["name"])
            await update.effective_message.reply_text(t(user_id, "prod_file_delivered"), parse_mode="HTML")
        except Exception:
            pass

    if current_bot_mode(context) == "external_user":
        await send_to_external_admin(
            current_external_admin_token(context),
            current_external_owner_id(context),
            f"🛒 <b>Sale</b>\nShop: <b>{current_external_title(context)}</b>\nUser: <code>{user_id}</code>\nItem: {product['name']}\nQty: {qty}\nTotal: ${float(cost):.2f}"
        )
    else:
        await send_log_via_second_bot(f"🛒 <b>Sale</b>\nUser: {user_id}\nItem: {product['name']}\nQty: {qty}\nTotal: ${float(cost):.2f}")
    return True, "", delivery_data


# =========================
# ACTIVATION CONFIRMATION
# =========================
async def handle_confirm_activate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    pending = context.user_data.pop("pending_activation", None)
    if not pending:
        await query.answer("Session expired. Please try again.", show_alert=True)
        return

    await query.answer()
    email = pending["email"]
    password = pending["password"]
    totp_secret = pending["totp_secret"]
    cost = pending["cost"]

    bal = get_user_balance(uid)
    if bal < cost:
        await query.message.reply_text(t(uid, "activate_no_bal", price=cost, balance=bal), parse_mode="HTML")
        return

    add_balance(uid, -cost)

    # Pick best available device before submitting
    device, dev_status = await iqless_pick_best_device()
    if dev_status == "all_unavailable":
        add_balance(uid, cost)
        await query.message.reply_text(
            t(uid, "activate_no_devices") + "\n\n<i>All devices are currently offline. Try again later.</i>",
            parse_mode="HTML"
        )
        return

    status_code, resp = await iqless_submit_job(email, password, totp_secret, device=device)

    if status_code == 409:
        add_balance(uid, cost)
        code = resp.get("detail", {}).get("code", "")
        if code == "already_queued":
            await query.message.reply_text(t(uid, "activate_already_queued"), parse_mode="HTML")
        elif code == "already_processed":
            await query.message.reply_text(t(uid, "activate_already_done"), parse_mode="HTML")
        else:
            await query.message.reply_text(t(uid, "activate_api_error", error=str(resp)), parse_mode="HTML")
        return

    if status_code == 402:
        add_balance(uid, cost)
        await query.message.reply_text(
            "❌ Service temporarily unavailable (API balance issue). Please contact support.\n\n"
            "💰 Your balance has been refunded.",
            parse_mode="HTML"
        )
        return

    if status_code in (503, 504):
        add_balance(uid, cost)
        code = resp.get("detail", {}).get("code", "") if isinstance(resp.get("detail"), dict) else ""
        if code in ("NETWORK_ERROR", "TIMEOUT"):
            await query.message.reply_text(
                "⚠️ Could not connect to iqless server. Check connection and try again.\n💰 Your balance has been refunded.",
                parse_mode="HTML"
            )
        else:
            await query.message.reply_text(t(uid, "activate_service_paused"), parse_mode="HTML")
        return

    if status_code == 400:
        add_balance(uid, cost)
        detail = resp.get("detail", {})
        code = detail.get("code", "") if isinstance(detail, dict) else str(detail)
        if code == "no_devices":
            await query.message.reply_text(t(uid, "activate_no_devices"), parse_mode="HTML")
        else:
            await query.message.reply_text(t(uid, "activate_api_error", error=code), parse_mode="HTML")
        return

    if status_code == 422:
        add_balance(uid, cost)
        detail = resp.get("detail", [])
        if isinstance(detail, list) and detail:
            first = detail[0]
            field = first.get("loc", ["?"])[-1] if isinstance(first.get("loc"), list) else "?"
            msg = first.get("msg", str(detail))
            err_text = f"field '{field}': {msg}"
        else:
            err_text = str(detail or resp)
        logger.warning(f"422 from iqless for uid={uid}: {resp}")
        await query.message.reply_text(t(uid, "activate_api_error", error=err_text), parse_mode="HTML")
        return

    if status_code not in (200, 201):
        add_balance(uid, cost)
        await query.message.reply_text(t(uid, "activate_api_error", error=f"HTTP {status_code}"), parse_mode="HTML")
        return

    job_id = resp.get("job_id", "")
    pos = resp.get("queue_position", 0)
    wait = resp.get("estimated_wait_seconds", 0)
    tx_id = generate_tx_id()

    res_data = get_user_data(uid)
    reseller_id = int(res_data.get("owner_id") or 0)

    submitted_at = time.time()
    sent_msg = await query.message.reply_text(
        t(uid, "activate_queued", email=email, job_id=job_id, tx_id=tx_id, pos=pos, wait=wait),
        parse_mode="HTML"
    )
    status_msg_id = sent_msg.message_id

    active_jobs[job_id] = {
        "uid": uid,
        "email": email,
        "cost": cost,
        "reseller_id": reseller_id,
        "tx_id": tx_id,
        "submitted_at": submitted_at,
        "status_msg_id": status_msg_id,
        "estimated_wait": float(wait),
        "last_pos": pos,
    }
    db_save_job(job_id, uid, email, cost, reseller_id, tx_id, submitted_at,
                status_msg_id=status_msg_id, estimated_wait=float(wait))

    # ── Owner log: Job Submitted ─────────────────────────────────────────────
    await send_log_via_second_bot(
        f"📤 <b>New Activation Request</b>\n\n"
        f"👤 User: <code>{uid}</code>\n"
        f"📧 Email: <code>{email}</code>\n"
        f"🆔 Job ID: <code>{job_id}</code>\n"
        f"🧾 TX: <code>{tx_id}</code>\n"
        f"📊 Queue position: <b>{pos}</b>\n"
        f"💵 Amount: ${cost:.2f}"
    )
    # ── Reseller: Job Submitted ──────────────────────────────────────────────
    if reseller_id and reseller_id != OWNER_ID:
        try:
            await context.bot.send_message(
                reseller_id,
                f"📤 <b>New request from your client</b>\n\n"
                f"👤 User: <code>{uid}</code>\n"
                f"📧 <code>{email}</code>\n"
                f"🆔 Job ID: <code>{job_id}</code>\n"
                f"🧾 TX: <code>{tx_id}</code>\n"
                f"💵 Amount: ${cost:.2f}",
                parse_mode="HTML"
            )
        except Exception:
            pass


# =========================
# TEXT HANDLER
# =========================
def clear_all_user_flow_states(context):
    """Clear all pending flow states to prevent cross-flow contamination.
    Does NOT clear permanent preferences like ws_last_email."""
    for key in [
        # Activation flow
        "act_step", "pending_activation", "act_email", "act_password",
        # WS invite key flow
        "waiting_ws_invite_key",
        # WS paid seat flow
        "ws_seat_pending_paid", "ws_seat_pending_ws_id", "ws_seat_sub_hours",
        # WS key+email flow
        "ws_user_flow", "ws_invite_code", "ws_workspace_id", "ws_key_sub_hours",
        # WS admin session upload
        "ws_flow", "ws_target_id", "ws_session_type",
        # Shop buy flow
        "state", "buying_pid", "buying_shop_id", "buy_final_qty",
        "buy_final_cost", "buying_manual_delivery",
        # Deposit flow
        "deposit_step", "deposit_network",
    ]:
        context.user_data.pop(key, None)


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = (update.message.text or "").strip()
    mode = current_bot_mode(context)

    if is_user_banned(uid):
        return await update.message.reply_text(t(uid, "banned"), parse_mode="HTML")

    # ── ChatGPT Workspace flows (defined later in file but accessible) ────
    if mode == "main_admin":
        if await handle_ws_admin_text(update, context):
            return
    if mode == "main_user":
        if await handle_ws_user_text(update, context):
            return

    # ── Auto deposit amount input ─────────────────────────────────────────
    if context.user_data.get("deposit_step") == "amount":
        await handle_deposit_amount_input(update, context)
        return

    if context.user_data.get("act_step"):
        if mode != "main_user":
            context.user_data.pop("act_step", None)
            return await update.message.reply_text(t(uid, "activate_not_available"), parse_mode="HTML")

        step = context.user_data["act_step"]

        _cancel_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("", callback_data="act_cancel_flow")
        ]])

        if step == "email":
            email = text.strip()
            if "@" not in email or "." not in email.split("@")[-1]:
                return await update.message.reply_text(t(uid, "act_bad_email"), parse_mode="HTML")
            context.user_data["act_email"] = email
            context.user_data["act_step"] = "password"
            return await update.message.reply_text(
                t(uid, "act_ask_password"), parse_mode="HTML", reply_markup=_cancel_kb
            )

        if step == "password":
            pwd = text.strip()
            if len(pwd) < 1:
                return await update.message.reply_text("❌ Password cannot be empty. Please re-enter:", parse_mode="HTML")
            if len(pwd) > 256:
                return await update.message.reply_text("❌ Password too long (max 256 characters). Please re-enter:", parse_mode="HTML")
            context.user_data["act_password"] = pwd
            context.user_data["act_step"] = "totp"
            return await update.message.reply_text(
                t(uid, "act_ask_totp"), parse_mode="HTML", reply_markup=_cancel_kb
            )

        if step == "totp":
            # Clean TOTP: remove spaces, dashes, newlines and convert to uppercase (common copy-paste formats)
            totp_secret = text.strip().replace(" ", "").replace("-", "").replace("\n", "").upper()
            if len(totp_secret) < 1:
                return await update.message.reply_text(
                    "❌ TOTP Secret cannot be empty. Please re-enter:",
                    parse_mode="HTML"
                )
            if len(totp_secret) > 64:
                return await update.message.reply_text(
                    "❌ TOTP Secret too long (max 64 characters). Please re-enter:",
                    parse_mode="HTML"
                )
            if not re.match(r'^[A-Z2-7]+=*$', totp_secret):
                return await update.message.reply_text(
                    "❌ Invalid TOTP Secret format. It must be a Base32 key (letters A-Z and digits 2-7 only).\n\nPlease re-enter:",
                    parse_mode="HTML"
                )
            email = context.user_data.get("act_email", "")
            password = context.user_data.get("act_password", "")
            cost = get_activate_price_for_user(uid)
            bal = get_user_balance(uid)

            context.user_data.pop("act_step", None)
            context.user_data.pop("act_email", None)
            context.user_data.pop("act_password", None)

            context.user_data["pending_activation"] = {
                "email": email,
                "password": password,
                "totp_secret": totp_secret,
                "cost": cost,
            }

            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton(t(uid, "btn_confirm"), callback_data="confirm_activate_1")]
            ])
            await update.message.reply_text(
                t(uid, "activate_cost", price=cost, balance=bal),
                parse_mode="HTML",
                reply_markup=kb
            )
            return

    if mode == "main_user" and is_txid_like(text):
        await process_deposit_claim(update, context, text)
        return

    if context.user_data.get("state") == "WAIT_QTY":
        try:
            qty = int(text)
            if qty < 1:
                raise ValueError
        except Exception:
            return await update.message.reply_text("❌ Please send a valid number.")

        shop_id = int(context.user_data.get("buying_shop_id", current_shop_id(context)))
        pid = int(context.user_data["buying_pid"])
        product = get_product(shop_id, pid)
        if not product:
            context.user_data.pop("state", None)
            return await update.message.reply_text("❌ Product not found.")

        max_stock = int(product["stock"])
        is_manual = context.user_data.get("buying_manual_delivery", False)
        if not is_manual and qty > max_stock:
            return await update.message.reply_text(f"❌ Invalid Quantity. Max: {max_stock}. Try again.")

        total_cost = float(product["price"]) * qty
        if get_shop_balance(uid, shop_id) < total_cost:
            context.user_data.pop("state", None)
            return await update.message.reply_text(t(uid, "prod_no_bal", name=product["name"]), parse_mode="HTML")

        context.user_data["buy_final_qty"] = qty
        context.user_data["buy_final_cost"] = total_cost
        context.user_data["state"] = "WAIT_CONFIRM"
        return await update.message.reply_text(t(uid, "prod_buy_confirm", name=product["name"], qty=qty, total=float(total_cost)), parse_mode="HTML")

    if context.user_data.get("state") == "WAIT_CONFIRM":
        if text.lower() != "yes":
            context.user_data.pop("state", None)
            return await update.message.reply_text(t(uid, "buy_cancelled"), parse_mode="HTML")

        shop_id = int(context.user_data.get("buying_shop_id", current_shop_id(context)))
        pid = int(context.user_data["buying_pid"])
        qty = int(context.user_data["buy_final_qty"])
        cost = float(context.user_data["buy_final_cost"])
        product = get_product(shop_id, pid)
        if not product:
            context.user_data.pop("state", None)
            return await update.message.reply_text("❌ Product not found.")

        if get_shop_balance(uid, shop_id) < cost:
            context.user_data.pop("state", None)
            return await update.message.reply_text(t(uid, "prod_no_bal", name=product["name"]), parse_mode="HTML")

        is_manual_delivery = context.user_data.get("buying_manual_delivery", False)

        def _clear_buy_state():
            for k in ("state", "buying_pid", "buying_shop_id", "buy_final_qty", "buy_final_cost", "buying_manual_delivery", "buying_max", "buying_price", "buying_name"):
                context.user_data.pop(k, None)

        if is_manual_delivery:
            # Deduct balance and record purchase, but no code delivery
            add_shop_balance(uid, -cost, shop_id=shop_id)
            record_purchase(shop_id, uid, int(product["id"]), float(cost), int(qty), "Manual", "")
            is_ar = False
            contact_kb = InlineKeyboardMarkup([[InlineKeyboardButton(
                "📞 Contact to Complete Order",
                url=f"https://t.me/{SUPPORT_USER.lstrip('@')}"
            )]])
            await update.message.reply_text(
                t(uid, "prod_bought", name=product["name"], qty=qty, total=float(cost)),
                parse_mode="HTML"
            )
            await update.message.reply_text(
                (f"📦 <b>Please contact manually</b>\n\n"
                 f"Deducted <b>${float(cost):.2f}</b> from your balance.\n"
                 f""
                 f"Please contact {SUPPORT_USER} to receive your order.")
                if is_ar else
                (f"📦 <b>Manual Delivery Required</b>\n\n"
                 f"<b>${float(cost):.2f}</b> has been deducted from your balance.\n"
                 f"Codes for this product are temporarily out of stock.\n"
                 f"Please contact {SUPPORT_USER} to receive your order."),
                parse_mode="HTML", reply_markup=contact_kb
            )
            if current_bot_mode(context) == "external_user":
                await send_to_external_admin(
                    current_external_admin_token(context),
                    current_external_owner_id(context),
                    f"🛒 <b>Sale (Manual)</b>\nShop: <b>{current_external_title(context)}</b>\nUser: <code>{uid}</code>\nItem: {product['name']}\nQty: {qty}\nTotal: ${float(cost):.2f}"
                )
            else:
                await send_log_via_second_bot(f"🛒 <b>Sale (Manual — No Stock)</b>\nUser: {uid}\nItem: {product['name']}\nQty: {qty}\nTotal: ${float(cost):.2f}")
            _clear_buy_state()
            return

        ok, err, _ = await finalize_purchase(update, context, uid, shop_id, product, qty, cost)
        _clear_buy_state()
        if not ok:
            return await update.message.reply_text(f"❌ {err}")
        return

    if context.user_data.get("addextshop_step"):
        return await handle_addextshop_wizard(update, context)

    if "admin_action" in context.user_data:
        action = context.user_data.pop("admin_action")
        cmd_str = f"/{action} {text}"
        return await main_admin_cmds_handler(update, context, direct_cmd=cmd_str)

    if "ext_admin_action" in context.user_data:
        action = context.user_data.pop("ext_admin_action")
        cmd_str = f"/{action} {text}"
        return await ext_admin_cmds_handler(update, context, direct_cmd=cmd_str)


# =========================
# BOT TOKEN / WIZARD UTILS
# =========================
def is_probably_bot_token(token: str) -> bool:
    return bool(re.fullmatch(r"\d+:[A-Za-z0-9_-]{20,}", (token or "").strip()))

async def inspect_bot_token(token: str):
    token = (token or "").strip()
    if not is_probably_bot_token(token):
        return False, None, "Invalid token format"
    bot = Bot(token)
    try:
        await bot.initialize()
        me = await bot.get_me()
        return True, me, None
    except Exception as e:
        return False, None, str(e)
    finally:
        try:
            await bot.shutdown()
        except Exception:
            pass

def parse_addextshop_compact_input(text: str):
    parts = (text or "").strip().split()
    if len(parts) < 3:
        return None
    shop_token, admin_token, owner_id_text = parts[0], parts[1], parts[2]
    title = " ".join(parts[3:]).strip() or "External Shop"
    if not (is_probably_bot_token(shop_token) and is_probably_bot_token(admin_token) and owner_id_text.isdigit()):
        return None
    return shop_token, admin_token, int(owner_id_text), title

def clear_addextshop_wizard(context: ContextTypes.DEFAULT_TYPE):
    for key in ["addextshop_step", "addextshop_shop_token", "addextshop_admin_token", "addextshop_owner_id"]:
        context.user_data.pop(key, None)

async def provision_external_shop(shop_token: str, admin_token: str, owner_id: int, title: str):
    try:
        add_external_shop_db(shop_token, admin_token, owner_id, title)
        row = get_external_shop_by_shop_token(shop_token)
        if not row:
            return False, "❌ Failed to save shop."
        ok, result = await start_external_shop_runtime(int(row["id"]))
        if ok:
            return True, f"✅ <b>External Shop Created!</b>\nTitle: <b>{title}</b>\n{result}"
        else:
            return False, f"❌ Shop saved but failed to start: {result}"
    except Exception as e:
        return False, f"❌ Error: {html.escape(str(e))}"

async def start_addextshop_wizard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.callback_query.message if update.callback_query else update.message
    txt = (update.message.text or "") if update.message else ""
    parts = txt.split(maxsplit=1)
    if len(parts) > 1:
        compact = parts[1].strip()
        parsed = parse_addextshop_compact_input(compact)
        if parsed:
            shop_token, admin_token, owner_id, title = parsed
            ok, result_text = await provision_external_shop(shop_token, admin_token, owner_id, title)
            return await msg.reply_text(result_text, parse_mode="HTML")

    context.user_data["addextshop_step"] = "shop_token"
    await msg.reply_text(
        "🌐 <b>External Shop Wizard — Step 1/3</b>\n\nSend <b>SHOP BOT TOKEN</b>.\n<i>Type /cancel to stop.</i>",
        parse_mode="HTML"
    )

async def handle_addextshop_wizard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_text = (msg.text or "").strip()
    lowered = user_text.lower()
    step = context.user_data.get("addextshop_step")

    if lowered in {"cancel", "/cancel"}:
        clear_addextshop_wizard(context)
        return await msg.reply_text("🚫 Cancelled.")

    if step == "shop_token":
        if not is_probably_bot_token(user_text):
            return await msg.reply_text("❌ Invalid token. Please send a valid SHOP BOT TOKEN.")
        if get_external_shop_by_shop_token(user_text) or get_external_shop_by_admin_token(user_text):
            return await msg.reply_text("⚠️ This token is already in use.")
        ok, me, err = await inspect_bot_token(user_text)
        if not ok:
            return await msg.reply_text(f"❌ Could not validate token.\n<code>{html.escape(str(err))}</code>", parse_mode="HTML")
        context.user_data["addextshop_shop_token"] = user_text
        context.user_data["addextshop_step"] = "admin_token"
        return await msg.reply_text(f"✅ Shop bot: @{me.username or me.id}\n\nStep 2/3\nSend <b>ADMIN BOT TOKEN</b>.", parse_mode="HTML")

    if step == "admin_token":
        if not is_probably_bot_token(user_text):
            return await msg.reply_text("❌ Invalid token. Please send a valid ADMIN BOT TOKEN.")
        if user_text == context.user_data.get("addextshop_shop_token"):
            return await msg.reply_text("⚠️ Admin bot token must be different from the shop bot token.")
        if get_external_shop_by_shop_token(user_text) or get_external_shop_by_admin_token(user_text):
            return await msg.reply_text("⚠️ This token is already in use.")
        ok, me, err = await inspect_bot_token(user_text)
        if not ok:
            return await msg.reply_text(f"❌ Could not validate token.\n<code>{html.escape(str(err))}</code>", parse_mode="HTML")
        context.user_data["addextshop_admin_token"] = user_text
        context.user_data["addextshop_step"] = "owner_id"
        return await msg.reply_text(f"✅ Admin bot: @{me.username or me.id}\n\nStep 3/3\nSend <b>OWNER ID</b>.", parse_mode="HTML")

    if step == "owner_id":
        if not user_text.isdigit():
            return await msg.reply_text("❌ Owner ID must be a number.")
        owner_id = int(user_text)
        shop_token = context.user_data.get("addextshop_shop_token", "")
        admin_token = context.user_data.get("addextshop_admin_token", "")
        clear_addextshop_wizard(context)
        ok, result_text = await provision_external_shop(shop_token, admin_token, owner_id, "External Shop")
        return await msg.reply_text(result_text, parse_mode="HTML")

    clear_addextshop_wizard(context)
    return await msg.reply_text("❌ Wizard state lost. Start again with /addextshop")


# =========================
# MAIN ADMIN
# =========================
async def main_admin_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    msg = update.callback_query.message if update.callback_query else update.message
    if uid != OWNER_ID and not is_reseller(uid):
        return await msg.reply_text("⛔ Access Denied")
    await sync_commands_for_chat(context.bot, uid, "main_admin", is_owner=(uid == OWNER_ID), is_reseller_user=is_reseller(uid))
    text = t(uid, "welcome_admin")
    kb = build_main_admin_keyboard(uid)
    if update.callback_query:
        try:
            await msg.edit_text(text, parse_mode="HTML", reply_markup=kb)
        except Exception:
            await msg.reply_text(text, parse_mode="HTML", reply_markup=kb)
    else:
        await msg.reply_text(text, parse_mode="HTML", reply_markup=kb)

async def callback_main_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    if uid != OWNER_ID and not is_reseller(uid):
        return

    data = query.data

    if data == "adm_home":
        return await main_admin_start(update, context)
    if data == "adm_help":
        return await cmd_help(update, context)

    if data == "adm_backup" and uid == OWNER_ID:
        await query.answer("")
        msg = await query.message.reply_text("💾 Creating and sending backup...")
        result = await do_backup(bot=context.bot)
        await msg.edit_text(f"💾 <b>Backup</b>\n\n{result}", parse_mode="HTML")
        return

    if data == "adm_stats":
        kb = [[InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")]]
        if uid == OWNER_ID:
            u, d, r = get_stats()
            txt = f"📊 <b>Stats</b>\n👥 Users: {u}\n💰 Deposits: ${d:.2f}\n💼 Resellers: {r}"
        else:
            txt = f"📊 <b>My Stats</b>\n👥 My Users: {get_reseller_stats(uid)}"
        return await query.message.edit_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

    if data == "adm_balance":
        kb = [[InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")]]
        res_bal = get_reseller_balance(uid) if is_reseller(uid) else 0
        user_bal = get_user_balance(uid)
        txt = f"💰 <b>My Balance:</b> ${user_bal:.2f}\n💼 <b>Reseller Wallet:</b> ${res_bal:.2f}"
        return await query.message.edit_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

    if data == "adm_users":
        kb = [[InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")]]
        if uid == OWNER_ID:
            total = get_total_users()
            all_users = get_all_users_detailed()
            header = f"Total Users: {total}\n"
            header += f"{'ID':<15} {'Username':<25} {'Balance':>8} {'Acts':>5} {'Invites':>8}\n"
            header += "-" * 65 + "\n"
            content = header + "\n".join([
                f"{u['user_id']:<15} {('@'+u['username']):<25} ${u['balance']:>7.2f} {u['activations']:>5} {u['invites']:>8}"
                for u in all_users
            ])
            bio = io.BytesIO(content.encode("utf-8"))
            bio.name = "users.txt"
            try:
                await query.message.reply_document(document=bio, caption=f"👥 <b>Total Users: {total}</b>", parse_mode="HTML")
            except Exception:
                await query.message.reply_text(f"👥 <b>Total Users:</b> {total}", parse_mode="HTML")
        else:
            clients = get_reseller_clients_detailed(uid)
            if not clients:
                txt = "👥 <b>My Clients</b>\n\nNo clients."
                await query.message.edit_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))
            elif len(clients) <= 30:
                rows_txt = []
                for c in clients:
                    rows_txt.append(
                        f"🆔 <code>{c['user_id']}</code> | @{c['username']}\n"
                        f"   💰 ${c['balance']:.2f} | ✅ {c['activations']} acts | 👥 {c['invites']} invites"
                    )
                txt = f"👥 <b>My Clients ({len(clients)} total)</b>\n\n" + "\n\n".join(rows_txt)
                await query.message.edit_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))
            else:
                header = f"{'ID':<15} {'Username':<25} {'Balance':>8} {'Acts':>5} {'Invites':>8}\n"
                header += "-" * 65 + "\n"
                lines = header + "\n".join([
                    f"{c['user_id']:<15} {('@'+c['username']):<25} ${c['balance']:>7.2f} {c['activations']:>5} {c['invites']:>8}"
                    for c in clients
                ])
                bio = io.BytesIO(lines.encode("utf-8"))
                bio.name = f"clients_{uid}.txt"
                await query.message.reply_document(document=bio, caption=f"👥 <b>My Clients — {len(clients)} total</b>", parse_mode="HTML")
        return

    if data == "adm_data" and uid == OWNER_ID:
        try:
            bio = io.BytesIO()
            today = str(datetime.date.today())
            # Essential files needed to run on another host
            essential_files = [
                "bot.py",
                "bot.db",
                "pyproject.toml",
                "uv.lock",
                "main.py",
            ]
            essential_dirs = [
                "storage",   # sessions + any other runtime data
            ]
            with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as zf:
                # Add individual files
                for fname in essential_files:
                    if os.path.isfile(fname):
                        try:
                            zf.write(fname, fname)
                        except Exception:
                            pass
                # Add directories recursively
                for dname in essential_dirs:
                    if os.path.isdir(dname):
                        for root, dirs, files in os.walk(dname):
                            dirs[:] = [d for d in dirs if d not in ["__pycache__"]]
                            for file_name in files:
                                if file_name.endswith((".db-journal", ".db-wal", ".db-shm")):
                                    continue
                                path = os.path.join(root, file_name)
                                try:
                                    zf.write(path, path)
                                except Exception:
                                    pass
            bio.seek(0)
            size_kb = bio.getbuffer().nbytes // 1024
            caption = (
                f"📦 <b>Bot Backup — {today}</b>\n\n"
                f"📁 Contains:\n"
                f"  • bot.py\n"
                f"  • bot.db (database)\n"
                f"  • storage/sessions/ (Sessions)\n"
                f"  • pyproject.toml + uv.lock\n\n"
                f"📏 Size: {size_kb} KB"
            )
            return await query.message.reply_document(
                document=bio,
                filename=f"bot_backup_{today}.zip",
                caption=caption,
                parse_mode="HTML"
            )
        except Exception as e:
            return await query.message.reply_text(f"❌ Error: {e}")

    if data == "adm_cat_reseller":
        kb = [
            [InlineKeyboardButton("➕ Add Balance", callback_data="act_add"), InlineKeyboardButton("➖ Remove Balance", callback_data="act_remove")],
            [InlineKeyboardButton("🔍 Check User", callback_data="act_check"), InlineKeyboardButton("🔗 My Invite", callback_data="act_myinvite")],
            [InlineKeyboardButton("📊 Report", callback_data="act_resellers")],
            [InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")]
        ]
        return await query.message.edit_text("💼 <b>Reseller Tools</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

    if data == "adm_cat_owner" and uid == OWNER_ID:
        kb = [
            [InlineKeyboardButton("➕ Add Reseller", callback_data="act_addreseller"), InlineKeyboardButton("🗑️ Del Reseller", callback_data="act_delreseller")],
            [InlineKeyboardButton("💲 Set Activate Price", callback_data="act_setprice"), InlineKeyboardButton("🤖 Set WS Seat Price/Month", callback_data="act_setwsprice")],
            [InlineKeyboardButton("💲 Set Reseller Profit", callback_data="act_setprofit")],
            [InlineKeyboardButton("💳 Add R. Balance", callback_data="act_addrc"), InlineKeyboardButton("➖ Rem R. Balance", callback_data="act_removerc")],
            [InlineKeyboardButton("👥 View R. Users", callback_data="act_rusers"), InlineKeyboardButton("🔗 Link User", callback_data="act_rlink")],
            [InlineKeyboardButton("⛓️ Unlink User", callback_data="act_runlink")],
            [InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")]
        ]
        return await query.message.edit_text("👑 <b>Owner Management</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

    if data == "adm_cat_shop" and uid == OWNER_ID:
        kb = [
            [InlineKeyboardButton("📦 Add Product", callback_data="act_addprod"), InlineKeyboardButton("🗑️ Delete Product", callback_data="act_delprod")],
            [InlineKeyboardButton("📋 List Products", callback_data="act_listprod"), InlineKeyboardButton("🔐 Add Code", callback_data="act_addcode")],
            [InlineKeyboardButton("📥 Bulk Codes", callback_data="act_addcodes"), InlineKeyboardButton("➕ Add Shop $", callback_data="act_addshop")],
            [InlineKeyboardButton("➖ Remove Shop $", callback_data="act_removeshop")],
            [InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")]
        ]
        return await query.message.edit_text("🛒 <b>Main Shop Manager</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

    if data == "adm_cat_external" and uid == OWNER_ID:
        kb = [
            [InlineKeyboardButton("➕ Add External Shop", callback_data="act_addextshop"), InlineKeyboardButton("📋 List External Shops", callback_data="act_listextshops")],
            [InlineKeyboardButton("🗑️ Delete External Shop", callback_data="act_delextshop")],
            [InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")]
        ]
        return await query.message.edit_text("🌐 <b>External Shops</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

    if data == "adm_cat_system" and uid == OWNER_ID:
        kb = [
            [InlineKeyboardButton("🚧 Toggle Maint.", callback_data="act_maintenance"), InlineKeyboardButton("📢 Broadcast", callback_data="act_broadcast")],
            [InlineKeyboardButton("📢 Broadcast Inactive", callback_data="act_broadcastinactive"), InlineKeyboardButton("💬 Reply Ticket", callback_data="act_reply")],
            [InlineKeyboardButton("🚫 Ban User", callback_data="act_ban"), InlineKeyboardButton("✅ Unban User", callback_data="act_unban")],
            [InlineKeyboardButton("💰 API Balance", callback_data="act_apibalance"), InlineKeyboardButton("⚡ Active Jobs", callback_data="act_activejobs")],
            [InlineKeyboardButton("🔍 Check TX ID", callback_data="act_checktx")],
            [InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")]
        ]
        return await query.message.edit_text("⚙️ <b>System Tools</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

    if data == "adm_cat_workspace" and uid == OWNER_ID:
        txt, kb = _ws_home_panel()
        return await query.message.edit_text(txt, parse_mode="HTML", reply_markup=kb)

    if data == "adm_cat_api" and uid == OWNER_ID:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Health Check", callback_data="api_health"), InlineKeyboardButton("💰 Balance", callback_data="api_balance")],
            [InlineKeyboardButton("📋 Queue Status", callback_data="api_queue"), InlineKeyboardButton("📜 History", callback_data="api_history")],
            [InlineKeyboardButton("➕ Submit Job", callback_data="api_submit"), InlineKeyboardButton("🔎 Track Job", callback_data="api_trackjob")],
            [InlineKeyboardButton("🚫 Cancel Job", callback_data="api_canceljob"), InlineKeyboardButton("⚡ Active Jobs", callback_data="act_activejobs")],
            [InlineKeyboardButton("🔍 Check TX", callback_data="act_checktx")],
            [InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")],
        ])
        return await query.message.edit_text("⚡ <b>API Control Panel</b>\nManage Google One API", parse_mode="HTML", reply_markup=kb)

    if data == "api_health" and uid == OWNER_ID:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{IQLESS_BASE_URL}/api/health")
            h = resp.json()
            devices = h.get("pools", {}).get("unified", {}).get("devices", [])
            dev_lines = ""
            for d in devices:
                status = "🟡 Busy" if d.get("busy") else ("🟢 Ready" if d.get("ready") else "🔴 Not Ready")
                dev_lines += f"\n  📱 <code>{d['serial']}</code> — {status}"
            txt = (
                f"🔍 <b>Health Check</b>\n\n"
                f"🌐 Status: <b>{'✅ OK' if h.get('status') == 'ok' else '❌ Down'}</b>\n"
                f"📱 Devices: {h.get('devices_connected', 0)}/{h.get('device_count', 0)} connected\n"
                f"🔌 Hotplug: {'On' if h.get('hotplug') else 'Off'}\n"
                f"\n<b>Devices:</b>{dev_lines}"
            )
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm_cat_api")]])
            return await query.message.reply_text(txt, parse_mode="HTML", reply_markup=kb)
        except Exception as e:
            return await query.message.reply_text(f"❌ Error: {e}")

    if data == "api_balance" and uid == OWNER_ID:
        try:
            bal = await iqless_get_balance()
            txt = (
                f"💰 <b>API Balance</b>\n\n"
                f"🔑 Key: <code>{bal.get('key', 'N/A')}</code>\n"
                f"📛 Name: {bal.get('name', 'N/A')}\n"
                f"💳 Balance: <b>{bal.get('balance', 0)} credits</b>\n"
                f"📊 Total Used: {bal.get('total_used', 0)}\n"
                f"💵 Cost/Job: {bal.get('cost_per_job', 1)} credit"
            )
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm_cat_api")]])
            return await query.message.reply_text(txt, parse_mode="HTML", reply_markup=kb)
        except Exception as e:
            return await query.message.reply_text(f"❌ Error: {e}")

    if data == "api_queue" and uid == OWNER_ID:
        try:
            q = await iqless_get_queue()
            current = q.get("current_job_ids", [])
            busy_slots = [jid for jid in current if jid]
            txt = (
                f"📋 <b>Queue Status</b>\n\n"
                f"⏳ Pending: <b>{q.get('pending_count', 0)}</b> jobs\n"
                f"📱 Devices: {q.get('devices_connected', 0)} connected | {q.get('devices_ready', 0)} ready\n"
                f"⚙️ Preparing: {q.get('devices_preparing', 0)}\n"
                f"⏱️ Est. per job: ~{q.get('est_seconds_per_job', 0)}s\n"
            )
            if busy_slots:
                txt += f"\n<b>Running Jobs ({len(busy_slots)}):</b>\n"
                for jid in busy_slots:
                    txt += f"  • <code>{jid}</code>\n"
            if q.get("pending_job_ids"):
                txt += f"\n<b>Pending ({len(q['pending_job_ids'])}):</b>\n"
                for jid in q["pending_job_ids"][:5]:
                    txt += f"  • <code>{jid}</code>\n"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm_cat_api")]])
            return await query.message.reply_text(txt, parse_mode="HTML", reply_markup=kb)
        except Exception as e:
            return await query.message.reply_text(f"❌ Error: {e}")

    if data == "api_history" and uid == OWNER_ID:
        try:
            headers = {"X-API-Key": IQLESS_API_KEY}
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{IQLESS_BASE_URL}/api/history?limit=10", headers=headers)
            h = resp.json()
            records = h.get("records", [])
            total = h.get("total", 0)
            txt = f"📜 <b>Success History</b>\nTotal: {total} records\n\n"
            for r in records:
                txt += f"📧 <code>{r['email']}</code>\n🔗 <a href='{r['url']}'>Link</a> | {r.get('created_at', '')}\n\n"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm_cat_api")]])
            return await query.message.reply_text(txt, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
        except Exception as e:
            return await query.message.reply_text(f"❌ Error: {e}")

    if data == "api_submit" and uid == OWNER_ID:
        context.user_data["admin_action"] = "api_submit_email"
        cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data="api_submit_cancel")]])
        return await query.message.reply_text(
            "➕ <b>Submit Activation Job</b>\n\nStep 1/3 — Send Gmail address:",
            parse_mode="HTML", reply_markup=cancel_kb
        )

    if data == "api_submit_cancel" and uid == OWNER_ID:
        context.user_data.pop("admin_action", None)
        context.user_data.pop("api_submit_email", None)
        context.user_data.pop("api_submit_pass", None)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Health Check", callback_data="api_health"), InlineKeyboardButton("💰 Balance", callback_data="api_balance")],
            [InlineKeyboardButton("📋 Queue Status", callback_data="api_queue"), InlineKeyboardButton("📜 History", callback_data="api_history")],
            [InlineKeyboardButton("➕ Submit Job", callback_data="api_submit"), InlineKeyboardButton("🔎 Track Job", callback_data="api_trackjob")],
            [InlineKeyboardButton("⚡ Active Jobs", callback_data="act_activejobs"), InlineKeyboardButton("🔍 Check TX", callback_data="act_checktx")],
            [InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")],
        ])
        return await query.message.edit_text("❌ <b>Operation cancelled</b>\n\n⚡ <b>API Control Panel</b>", parse_mode="HTML", reply_markup=kb)

    if data == "api_trackjob" and uid == OWNER_ID:
        context.user_data["admin_action"] = "api_trackjob"
        cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data="api_submit_cancel")]])
        return await query.message.reply_text(
            "🔎 <b>Track Job</b>\n\nSend the Job ID to check its status:",
            parse_mode="HTML", reply_markup=cancel_kb
        )

    if data == "api_canceljob" and uid == OWNER_ID:
        context.user_data["admin_action"] = "api_canceljob"
        cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data="api_submit_cancel")]])
        return await query.message.reply_text(
            "🚫 <b>Cancel Job</b>\n\nSend the Job ID of the request to cancel:",
            parse_mode="HTML", reply_markup=cancel_kb
        )

    if data.startswith("api_cancel_confirm:") and uid == OWNER_ID:
        job_id = data.split(":", 1)[1]
        await query.answer("")
        try:
            status_code, resp = await iqless_cancel_job(job_id)
            if status_code in (200, 204):
                txt = (
                    f"✅ <b>Request cancelled successfully</b>\n\n"
                    f"🆔 Job ID: <code>{job_id}</code>\n"
                    f"📌 {resp.get('message', 'Cancelled successfully')}"
                )
            else:
                detail = resp.get("detail", {}) if isinstance(resp, dict) else {}
                msg = detail.get("message", resp.get("message", str(resp))) if isinstance(resp, dict) else str(resp)
                code = detail.get("code", str(status_code)) if isinstance(detail, dict) else str(status_code)
                txt = (
                    f"❌ <b>Cancellation failed</b>\n\n"
                    f"Code: <code>{code}</code>\n"
                    f"Message: {msg}"
                )
        except Exception as e:
            txt = f"❌ Unexpected error: {e}"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 API Control", callback_data="adm_cat_api")]])
        return await query.message.edit_text(txt, parse_mode="HTML", reply_markup=kb)

    if data.startswith("api_cancel_abort:") and uid == OWNER_ID:
        job_id = data.split(":", 1)[1]
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 API Control", callback_data="adm_cat_api")]])
        return await query.message.edit_text(f"↩️ <b>Reverted</b>\n\nRequest was not cancelled <code>{job_id}</code>.", parse_mode="HTML", reply_markup=kb)

    if data == "act_apibalance" and uid == OWNER_ID:
        try:
            bal_data = await iqless_get_balance()
            txt = (
                f"🔑 <b>API Balance</b>\n\n"
                f"Key: <code>{bal_data.get('key', 'N/A')}</code>\n"
                f"Balance: <b>{bal_data.get('balance', 0)}</b> credits\n"
                f"Total Used: {bal_data.get('total_used', 0)}\n"
                f"Cost/Job: {bal_data.get('cost_per_job', 1)}"
            )
            return await query.message.reply_text(txt, parse_mode="HTML")
        except Exception as e:
            return await query.message.reply_text(f"❌ Error: {e}")

    if data == "act_activejobs" and uid == OWNER_ID:
        if not active_jobs:
            return await query.message.reply_text("✅ <b>No active activation jobs currently.</b>", parse_mode="HTML")
        lines = [f"⚡ <b>Active Jobs ({len(active_jobs)})</b>\n"]
        now = time.time()
        for jid, jdata in active_jobs.items():
            elapsed = int(now - jdata.get("submitted_at", now))
            mins, secs = divmod(elapsed, 60)
            est_wait = jdata.get("estimated_wait", 0)
            pos = jdata.get("last_pos", -1)
            stage = jdata.get("last_stage", -1)
            status_str = ""
            if pos >= 0:
                status_str = f"📋 Queue: {pos}"
            elif stage >= 0:
                status_str = f"⚙️ Stage: {stage}"
            lines.append(
                f"🆔 <code>{jid}</code>\n"
                f"👤 UID: <code>{jdata.get('uid')}</code>\n"
                f"📧 {jdata.get('email', 'N/A')}\n"
                f"💰 ${jdata.get('cost', 0):.2f} | {status_str}\n"
                f"⏱️ Since: {mins}m {secs}s | Expected: ~{int(est_wait)}s\n"
                f"🔖 TX: <code>{jdata.get('tx_id', 'N/A')}</code>\n"
            )
        return await query.message.reply_text("\n".join(lines), parse_mode="HTML")

    if data.startswith("act_"):
        action = data.split("_", 1)[1]
        if action == "myinvite":
            return await cmd_myinvite(update, context)
        if action == "resellers":
            return await main_admin_cmds_handler(update, context, direct_cmd="/resellers")
        if action == "listprod":
            return await main_admin_cmds_handler(update, context, direct_cmd="/listprod")
        if action == "listextshops":
            return await main_admin_cmds_handler(update, context, direct_cmd="/listextshops")
        if action == "maintenance":
            return await main_admin_cmds_handler(update, context, direct_cmd="/maintenance")
        if action == "addprod":
            return
        if action == "addextshop":
            return await start_addextshop_wizard(update, context)

        context.user_data["admin_action"] = action
        mapping = {
            "add": "📝 <b>Send User ID and Amount ($):</b>\nExample: <code>123456789 5</code>",
            "remove": "📝 <b>Send User ID and Amount ($):</b>\nExample: <code>123456789 5</code>",
            "addshop": "📝 <b>Send User ID and Amount ($):</b>\nExample: <code>123456789 10</code>",
            "removeshop": "📝 <b>Send User ID and Amount ($):</b>\nExample: <code>123456789 10</code>",
            "addrc": "📝 <b>Send Reseller ID and Amount ($):</b>\nExample: <code>123456789 10</code>",
            "removerc": "📝 <b>Send Reseller ID and Amount ($):</b>\nExample: <code>123456789 10</code>",
            "check": "📝 <b>Send User ID:</b>",
            "addreseller": "📝 <b>Send User ID:</b>",
            "delreseller": "📝 <b>Send User ID:</b>",
            "ban": "📝 <b>Send User ID:</b>",
            "unban": "📝 <b>Send User ID:</b>",
            "delprod": "📝 <b>Send Product ID:</b>",
            "addcode": "📝 <b>Send Product ID and one code:</b>\nExample: <code>5 ABCD-1234</code>",
            "addcodes": "📝 <b>Send Product ID then codes on new lines:</b>\nExample:\n<code>5\nCODE-1\nCODE-2\nCODE-3</code>",
            "setprice": "💰 <b>Send New Activation Price ($):</b>",
            "setwsprice": "🤖 <b>Send monthly ChatGPT Workspace seat price ($):</b>\n(Prices for all durations will be calculated automatically)",
            "setprofit": "💰 <b>Send Reseller Profit Per Activation ($):</b>",
            "broadcast": "📢 <b>Send Message:</b>",
            "broadcastinactive": "📢 <b>Send Message for Inactive Users:</b>",
            "rusers": "📝 <b>Send Reseller ID:</b>",
            "rlink": "📝 <b>Send User ID and Reseller ID:</b>\nExample: <code>12345 67890</code>",
            "runlink": "📝 <b>Send User ID:</b>",
            "reply": "📝 <b>Send User ID and Message:</b>\nExample: <code>12345 Hello</code>",
            "addextshop": "📝 <b>Send SHOP_TOKEN ADMIN_TOKEN OWNER_ID [TITLE]</b>",
            "delextshop": "📝 <b>Send External Shop ID:</b>",
            "checktx": "🔍 <b>Enter the transaction number (TX ID) to search:</b>",
        }
        prompt = mapping.get(action, "📝 <b>Enter Input:</b>")
        return await query.message.reply_text(prompt, parse_mode="HTML")

    if data == "maint_notify_yes":
        return await _apply_maintenance_notify(update, context, notify=True)
    if data == "maint_notify_no":
        return await _apply_maintenance_notify(update, context, notify=False)

async def _apply_maintenance_notify(update: Update, context: ContextTypes.DEFAULT_TYPE, notify: bool):
    """Send broadcast notification after maintenance toggle. Does NOT toggle again."""
    msg = f"✅ Maintenance set to <b>{'ON' if MAINTENANCE_MODE else 'OFF'}</b>."
    if notify:
        key = "maint_start_broadcast" if MAINTENANCE_MODE else "maint_end_broadcast"
        count = await broadcast_system_msg(context, key)
        msg += f"\n📢 Sent to {count} users."
    await update.callback_query.message.edit_text(msg, parse_mode="HTML")


async def main_admin_cmds_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, direct_cmd: str = None):
    uid = update.effective_user.id
    if uid != OWNER_ID and not is_reseller(uid):
        msg = update.callback_query.message if update.callback_query else update.message
        return await msg.reply_text("⛔ Access Denied.")

    txt = direct_cmd if direct_cmd else (update.message.text or "")
    parts = txt.split(maxsplit=2)
    cmd = parts[0] if parts else ""
    context.args = txt.split()[1:] if txt else []

    async def reply(text: str, reply_markup=None):
        msg = update.callback_query.message if update.callback_query else update.message
        await msg.reply_text(text, parse_mode="HTML", reply_markup=reply_markup)

    if cmd == "/help":
        return await cmd_help(update, context)
    if cmd == "/myinvite":
        return await cmd_myinvite(update, context)

    target_id = None
    if len(context.args) >= 1:
        if context.args[0].isdigit():
            target_id = int(context.args[0])
        elif context.args[0].startswith("@"):
            target_id = get_id_by_username(context.args[0])

    if cmd == "/reply":
        try:
            target_id = int(context.args[0])
            msg_text = " ".join(context.args[1:])
            conn = db_connect()
            ticket = conn.execute("SELECT shop_id, bot_token FROM tickets WHERE user_id=? ORDER BY id DESC LIMIT 1", (target_id,)).fetchone()
            conn.execute("UPDATE tickets SET status='closed' WHERE user_id=? AND status='open'", (target_id,))
            conn.commit()
            conn.close()
            bot_token = ticket["bot_token"] if ticket and ticket["bot_token"] else MAIN_BOT_TOKEN
            await Bot(bot_token).send_message(target_id, t(target_id, "support_reply", msg=msg_text), parse_mode="HTML")
            return await reply(f"✅ Reply sent to {target_id}")
        except Exception:
            return await reply("❌ Usage: /reply ID Message")

    if cmd in ["/broadcast_inactive", "/broadcastinactive"]:
        conn = db_connect()
        users = conn.execute("SELECT user_id FROM users WHERE last_activity < datetime('now', '-7 days')").fetchall()
        conn.close()
        msg_text = " ".join(context.args)
        await reply(f"📣 Sending to {len(users)} inactive users...")
        for row in users:
            try:
                await Bot(MAIN_BOT_TOKEN).send_message(int(row["user_id"]), msg_text, parse_mode="HTML")
            except Exception:
                pass
        return await reply("✅ Done.")

    if cmd == "/add" and target_id:
        try:
            amt = float(context.args[1])
            if uid == OWNER_ID:
                add_balance(target_id, amt)
                try:
                    await Bot(MAIN_BOT_TOKEN).send_message(target_id, t(target_id, "balance_added_msg", amount=amt), parse_mode="HTML")
                except Exception:
                    pass
                return await reply("✅ Added.")
            ok, res = reseller_give_balance(uid, target_id, amt)
            if ok:
                try:
                    await Bot(MAIN_BOT_TOKEN).send_message(target_id, t(target_id, "balance_added_msg", amount=amt), parse_mode="HTML")
                except Exception:
                    pass
            return await reply(res)
        except Exception:
            return await reply("❌ Usage: /add ID Amount")

    if cmd == "/remove" and target_id:
        try:
            amt = float(context.args[1])
            if uid == OWNER_ID:
                add_balance(target_id, -amt)
                try:
                    await Bot(MAIN_BOT_TOKEN).send_message(target_id, t(target_id, "balance_removed_msg", amount=amt), parse_mode="HTML")
                except Exception:
                    pass
                return await reply("✅ Removed.")
            ok, res = reseller_remove_balance(uid, target_id, amt)
            return await reply(res)
        except Exception:
            return await reply("❌ Usage: /remove ID Amount")

    if cmd == "/addshop" and uid == OWNER_ID and target_id:
        try:
            amt = float(context.args[1])
            add_shop_balance(target_id, amt, shop_id=0)
            try:
                await Bot(MAIN_BOT_TOKEN).send_message(target_id, t(target_id, "shop_added_msg", amount=amt), parse_mode="HTML")
            except Exception:
                pass
            return await reply(f"✅ Added ${amt:.2f} to main shop wallet.")
        except Exception:
            return await reply("❌ Usage: /addshop ID Amount")

    if cmd == "/removeshop" and uid == OWNER_ID and target_id:
        try:
            amt = float(context.args[1])
            add_shop_balance(target_id, -amt, shop_id=0)
            try:
                await Bot(MAIN_BOT_TOKEN).send_message(target_id, t(target_id, "shop_removed_msg", amount=amt), parse_mode="HTML")
            except Exception:
                pass
            return await reply(f"✅ Removed ${amt:.2f} from main shop wallet.")
        except Exception:
            return await reply("❌ Usage: /removeshop ID Amount")

    if cmd == "/addcode" and uid == OWNER_ID:
        try:
            pid = int(context.args[0])
            code_text = txt.split(maxsplit=2)[2]
            prod = get_product(0, pid)
            if not prod:
                return await reply("❌ Product not found.")
            add_product_code(0, pid, code_text)
            return await reply(f"✅ Code added to product {pid}. New stock: {get_product(0, pid)['stock']}")
        except Exception:
            return await reply("❌ Usage: /addcode PRODUCT_ID CODE")

    if cmd == "/addcodes" and uid == OWNER_ID:
        try:
            raw = txt.split(maxsplit=2)
            pid = int(raw[1])
            data = raw[2] if len(raw) > 2 else ""
            codes = [x.strip() for x in data.replace("||", "\n").splitlines() if x.strip()]
            if not codes:
                return await reply("❌ No codes provided.")
            if not get_product(0, pid):
                return await reply("❌ Product not found.")
            inserted = add_product_codes_bulk(0, pid, codes)
            skipped = len(codes) - inserted
            msg_txt = f"✅ Added {inserted} codes to product {pid}. New stock: {get_product(0, pid)['stock']}"
            if skipped > 0:
                msg_txt += f"\n⚠️ {skipped} duplicate(s) skipped."
            return await reply(msg_txt)
        except Exception:
            return await reply("❌ Usage: /addcodes PRODUCT_ID then codes on new lines")

    if cmd == "/addreseller" and uid == OWNER_ID and target_id:
        conn = db_connect()
        conn.execute("INSERT OR IGNORE INTO resellers (user_id) VALUES (?)", (target_id,))
        conn.commit()
        conn.close()
        return await reply(f"✅ User {target_id} is now a Reseller.")

    if cmd == "/delreseller" and uid == OWNER_ID and target_id:
        delete_reseller(target_id)
        return await reply(f"🗑️ User {target_id} removed from resellers.")

    if cmd == "/ban" and uid == OWNER_ID and target_id:
        ban_user(target_id, 1)
        return await reply(f"🚫 Banned {target_id}")

    if cmd == "/unban" and uid == OWNER_ID and target_id:
        ban_user(target_id, 0)
        return await reply(f"✅ Unbanned {target_id}")

    if cmd == "/check" and target_id:
        res = get_user_data(target_id)
        conn = db_connect()
        r_data = conn.execute("SELECT balance, total_sold, profit_per_activation FROM resellers WHERE user_id=?", (target_id,)).fetchone()
        conn.close()
        msg = (
            f"👤 <b>User Info</b>\n"
            f"ID: <code>{target_id}</code>\n"
            f"Username: @{res.get('username', 'N/A')}\n"
            f"💰 Balance: ${float(res.get('balance', 0)):.2f}\n"
            f"🛒 Shop Wallet: ${float(get_shop_balance(target_id, 0)):.2f}"
        )
        if int(res.get("owner_id") or 0):
            msg += f"\n🔗 Reseller: {int(res['owner_id'])}"
        if r_data:
            msg += (
                f"\n\n💼 <b>Reseller Stats</b>\n"
                f"Wallet: ${float(r_data['balance'] or 0):.2f}\n"
                f"Sold: {int(r_data['total_sold'] or 0)}\n"
                f"Profit/Activation: ${float(r_data['profit_per_activation'] or 0):.2f}"
            )
        return await reply(msg)

    if cmd == "/backup" and uid == OWNER_ID:
        prog = await update.message.reply_text("💾 Creating backup...")
        result = await do_backup(bot=context.bot)
        await prog.edit_text(f"💾 <b>Backup</b>\n\n{result}", parse_mode="HTML")
        return

    if cmd == "/setprice" and uid == OWNER_ID:
        try:
            price = float(context.args[0])
            set_activate_price(price)
            return await reply(f"✅ Activation Price set to <b>${price:.2f}</b>")
        except Exception:
            return await reply("❌ Usage: /setprice PRICE")

    if cmd in ("/setwsprice", "/setwsmonthlyprice") and uid == OWNER_ID:
        try:
            price = float(context.args[0])
            set_ws_monthly_price(price)
            set_ws_seat_price(price)
            lines = [f"✅ <b>ChatGPT Workspace seat price set to ${price:.2f}/month</b>\n\n"
                     f"📋 <b>Duration prices:</b>"]
            for hours, lbl_ar, lbl_en in WS_DURATION_OPTIONS:
                p = ws_calc_price(hours)
                lines.append(f"  • {lbl_ar} ({lbl_en}): <b>${p:.2f}</b>")
            return await reply("\n".join(lines))
        except Exception:
            return await reply("❌ Usage: /setwsprice PRICE\nExample: /setwsprice 10")

    if cmd == "/setprofit" and uid == OWNER_ID:
        try:
            profit = float(context.args[0])
            set_reseller_profit(profit)
            return await reply(f"✅ Reseller Profit set to <b>${profit:.2f}</b> per activation.")
        except Exception:
            return await reply("❌ Usage: /setprofit AMOUNT")

    if cmd == "/addrc" and uid == OWNER_ID and target_id:
        try:
            amt = float(context.args[1])
            add_reseller_balance(target_id, amt)
            return await reply(f"✅ Added ${amt:.2f} to reseller wallet.")
        except Exception:
            return await reply("❌ Usage: /addrc RID AMOUNT")

    if cmd == "/removerc" and uid == OWNER_ID and target_id:
        try:
            amt = float(context.args[1])
            add_reseller_balance(target_id, -amt)
            return await reply(f"✅ Removed ${amt:.2f} from reseller wallet.")
        except Exception:
            return await reply("❌ Usage: /removerc RID AMOUNT")

    if cmd == "/resellers":
        conn = db_connect()
        rows = conn.execute("""
            SELECT r.user_id, r.balance, r.total_sold, r.profit_per_activation, u.username, u.first_name
            FROM resellers r LEFT JOIN users u ON r.user_id=u.user_id
        """).fetchall()
        conn.close()
        msg = "💼 <b>Resellers Report</b>\n\n"
        for r in rows:
            profit = get_reseller_profit()
            msg += (
                f"👤 <b>{r['first_name'] or 'No Name'}</b> | @{r['username'] or 'No Username'}\n"
                f"🆔 <code>{int(r['user_id'])}</code>\n"
                f"💰 Wallet: ${float(r['balance'] or 0):.2f}\n"
                f"📉 Sold: {int(r['total_sold'] or 0)}\n"
                f"💵 Profit/Act: ${float(r['profit_per_activation'] or profit):.2f}\n\n"
            )
        return await reply(msg)

    if cmd == "/rusers" and uid == OWNER_ID:
        try:
            rid = int(context.args[0])
            clients = get_reseller_clients_detailed(rid)
            if not clients:
                return await reply(f"📭 Reseller {rid} has no users.")
            if len(clients) <= 25:
                msg = f"👥 <b>Users of Reseller {rid}</b> ({len(clients)} total)\n\n"
                for c in clients:
                    msg += (
                        f"🆔 <code>{c['user_id']}</code> | @{c['username']}\n"
                        f"   💰 ${c['balance']:.2f} | ✅ {c['activations']} acts | 👥 {c['invites']} invites\n\n"
                    )
                return await reply(msg)
            else:
                header = f"Reseller: {rid} | Total: {len(clients)} users\n"
                header += f"{'ID':<15} {'Username':<25} {'Balance':>8} {'Acts':>5} {'Invites':>8}\n"
                header += "-" * 65 + "\n"
                lines = header + "\n".join([
                    f"{c['user_id']:<15} {('@'+c['username']):<25} ${c['balance']:>7.2f} {c['activations']:>5} {c['invites']:>8}"
                    for c in clients
                ])
                bio = io.BytesIO(lines.encode("utf-8"))
                bio.name = f"rusers_{rid}.txt"
                await context.bot.send_document(chat_id=uid, document=bio, caption=f"👥 <b>Reseller {rid} — {len(clients)} users</b>", parse_mode="HTML")
        except Exception:
            return await reply("❌ Usage: /rusers RID")

    if cmd == "/rlink" and uid == OWNER_ID:
        try:
            u_id = int(context.args[0])
            r_id = int(context.args[1])
            return await reply(f"✅ Linked User {u_id} to Reseller {r_id}" if set_user_owner(u_id, r_id) else "✅ Done.")
        except Exception:
            return await reply("❌ Usage: /rlink USER_ID RESELLER_ID")

    if cmd == "/runlink" and uid == OWNER_ID:
        try:
            u_id = int(context.args[0])
            conn = db_connect()
            conn.execute("UPDATE users SET owner_id=0 WHERE user_id=?", (u_id,))
            conn.commit()
            conn.close()
            return await reply(f"✅ Unlinked User {u_id}.")
        except Exception:
            return await reply("❌ Usage: /runlink USER_ID")

    if cmd == "/uinvites":
        # Owner can check any user; reseller can only check their own clients
        try:
            target_id = int(context.args[0])
        except Exception:
            return await reply("❌ Usage: /uinvites USER_ID")
        # Access check for resellers
        is_owner = (uid == OWNER_ID)
        is_reseller = is_owner
        if not is_owner:
            conn = db_connect()
            res_row = conn.execute("SELECT 1 FROM resellers WHERE user_id=?", (uid,)).fetchone()
            conn.close()
            if res_row:
                clients = get_reseller_clients(uid)
                client_ids = [c[0] for c in clients]
                if target_id in client_ids:
                    is_reseller = True
        if not is_owner and not is_reseller:
            return await reply("❌ Access denied.")
        invitees = get_user_invitees(target_id)
        if not invitees:
            return await reply(f"📭 User <code>{target_id}</code> has no invites.", parse_mode="HTML")
        # Check channel subscription for each invitee
        sub_status = {}
        for inv in invitees:
            try:
                member = await context.bot.get_chat_member(chat_id=REQUIRED_CHANNEL, user_id=inv["user_id"])
                sub_status[inv["user_id"]] = member.status not in ["left", "kicked"]
            except Exception:
                sub_status[inv["user_id"]] = None
        total = len(invitees)
        subscribed = sum(1 for v in sub_status.values() if v is True)
        not_subbed = sum(1 for v in sub_status.values() if v is False)
        unknown = sum(1 for v in sub_status.values() if v is None)
        rate = (subscribed / total * 100) if total > 0 else 0
        if total <= 50:
            lines = []
            for inv in invitees:
                s = sub_status.get(inv["user_id"])
                icon = "✅" if s is True else ("❓" if s is None else "❌")
                lines.append(f"{icon} <code>{inv['user_id']}</code> | @{inv['username']} | ✨ {inv['activations']} acts")
            msg = (
                f"👥 <b>Invites by <code>{target_id}</code></b>\n\n"
                + "\n".join(lines)
                + f"\n\n📊 <b>Channel Subscription Stats:</b>\n"
                f"✅ Subscribed: {subscribed} ({rate:.0f}%)\n"
                f"❌ Not subscribed: {not_subbed}\n"
                f"❓ Unknown: {unknown}\n"
                f"👥 Total invited: {total}"
            )
            return await reply(msg)
        else:
            file_lines = [
                f"Invites by: {target_id}",
                f"Total: {total} | Subscribed: {subscribed} ({rate:.0f}%) | Not: {not_subbed} | Unknown: {unknown}",
                "-" * 70,
                f"{'ID':<15} {'Username':<25} {'Acts':>5} {'Channel':>10}",
                "-" * 70,
            ]
            for inv in invitees:
                s = sub_status.get(inv["user_id"])
                ch = "Subscribed" if s is True else ("Unknown" if s is None else "Not Sub")
                file_lines.append(f"{inv['user_id']:<15} {'@'+inv['username']:<25} {inv['activations']:>5} {ch:>10}")
            bio = io.BytesIO("\n".join(file_lines).encode("utf-8"))
            bio.name = f"invites_{target_id}.txt"
            caption = (
                f"👥 <b>Invites by <code>{target_id}</code></b> — {total} total\n"
                f"✅ {subscribed} subscribed ({rate:.0f}%) | ❌ {not_subbed} not subscribed"
            )
            await context.bot.send_document(chat_id=uid, document=bio, caption=caption, parse_mode="HTML")

    if cmd == "/listprod":
        prods = get_all_products(0)
        if not prods:
            return await reply("📭 No products found.")
        msg = "🛒 <b>Main Shop Products</b>\n"
        for p in prods:
            msg += f"ID: {p['id']} | {p['name']} | ${float(p['price']):.2f} | Stock: {int(p['stock'])} | {p['delivery_type']} | Cat: {p['category']}\n"
        return await reply(msg)

    if cmd == "/delprod" and uid == OWNER_ID:
        try:
            pid = int(context.args[0])
            del_product(0, pid)
            return await reply(f"✅ Product {pid} deleted.")
        except Exception:
            return await reply("❌ Usage: /delprod PRODUCT_ID")

    if cmd == "/maintenance" and uid == OWNER_ID:
        global MAINTENANCE_MODE
        MAINTENANCE_MODE = not MAINTENANCE_MODE
        set_maintenance_mode(MAINTENANCE_MODE)  # Save to DB immediately
        state_label = "ON 🔴" if MAINTENANCE_MODE else "OFF 🟢"
        kb = [
            [InlineKeyboardButton("✅ Yes, Broadcast", callback_data="maint_notify_yes")],
            [InlineKeyboardButton("🔕 No, Silent", callback_data="maint_notify_no")],
        ]
        return await reply(
            f"🚧 <b>Maintenance: {state_label}</b>\n\nNotify all users?",
            reply_markup=InlineKeyboardMarkup(kb)
        )

    if cmd == "/broadcast" and uid == OWNER_ID:
        msg_text = " ".join(context.args)
        photo_to_send = None
        if update.message and update.message.reply_to_message:
            msg_text = update.message.reply_to_message.text or update.message.reply_to_message.caption or msg_text
            if update.message.reply_to_message.photo:
                photo_to_send = update.message.reply_to_message.photo[-1].file_id
        if not msg_text and not photo_to_send:
            return await reply("⚠️ Usage: /broadcast MESSAGE")
        users = get_all_users()
        await reply("🚀 <b>Broadcasting...</b>")
        count = 0
        temp_bot = Bot(MAIN_BOT_TOKEN)
        for user_id in users:
            try:
                if photo_to_send:
                    await temp_bot.send_photo(user_id, photo=photo_to_send, caption=msg_text, parse_mode="HTML")
                else:
                    await temp_bot.send_message(user_id, msg_text, parse_mode="HTML")
                count += 1
                await asyncio.sleep(0.04)
            except Exception:
                pass
        return await reply(f"✅ <b>Broadcast Complete.</b>\nSent to: {count} users.")

    if cmd == "/addextshop" and uid == OWNER_ID:
        return await start_addextshop_wizard(update, context)

    if cmd == "/delextshop" and uid == OWNER_ID:
        try:
            shop_id = int(context.args[0])
            row = get_external_shop_by_id(shop_id)
            if not row:
                return await reply("❌ External shop not found.")
            await stop_external_shop_runtime(shop_id)
            remove_external_shop_db(shop_id)
            return await reply(f"✅ External shop <b>{row['title']}</b> deleted.")
        except Exception:
            return await reply("❌ Usage: /delextshop SHOP_ID")

    if cmd == "/listextshops" and uid == OWNER_ID:
        rows = get_external_shops()
        if not rows:
            return await reply("📭 No external shops found.")
        msg = "🌐 <b>External Shops</b>\n\n"
        for r in rows:
            msg += (
                f"ID: <code>{r['id']}</code>\n"
                f"Title: <b>{r['title']}</b>\n"
                f"Owner: <code>{r['owner_id']}</code>\n"
                f"Store Bot: @{r['shop_username'] or 'Unknown'}\n"
                f"Admin Bot: @{r['admin_username'] or 'Unknown'}\n"
                f"Status: {'🟢 Active' if int(r['is_active']) == 1 else '🔴 Stopped'}\n\n"
            )
        return await reply(msg)

    if cmd == "/api_submit_email" and uid == OWNER_ID:
        email = " ".join(context.args).strip()
        cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data="api_submit_cancel")]])
        if not email or "@" not in email:
            context.user_data["admin_action"] = "api_submit_email"
            return await reply("", reply_markup=cancel_kb)
        context.user_data["api_submit_email"] = email
        context.user_data["admin_action"] = "api_submit_pass"
        return await reply(f"✅ Email: <code>{email}</code>\n\nStep 2/3 — Send password:", reply_markup=cancel_kb)

    if cmd == "/api_submit_pass" and uid == OWNER_ID:
        password = " ".join(context.args).strip()
        cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data="api_submit_cancel")]])
        if not password:
            context.user_data["admin_action"] = "api_submit_pass"
            return await reply("", reply_markup=cancel_kb)
        context.user_data["api_submit_pass"] = password
        context.user_data["admin_action"] = "api_submit_totp"
        return await reply("✅ Password saved.\n\nStep 3/3 — Send 2FA key (TOTP Secret):\n<i>Base32 key, not the 6-digit code digits</i>", reply_markup=cancel_kb)

    if cmd == "/api_submit_totp" and uid == OWNER_ID:
        totp = " ".join(context.args).strip()
        if not totp:
            return await reply("❌ TOTP key is empty. Please resend:")
        email = context.user_data.pop("api_submit_email", None)
        password = context.user_data.pop("api_submit_pass", None)
        if not email or not password:
            return await reply("❌ Session expired. Start again via ➕ Submit Job.")
        await reply("⏳ <b>Checking available devices...</b>")
        try:
            device, dev_status = await iqless_pick_best_device()
            dev_label = {"ready": "", "busy": "🟡 busy (alive)", "all_unavailable": "", "health_error": ""}.get(dev_status, dev_status)
            if dev_status == "all_unavailable":
                return await reply(f"❌ <b>All devices offline</b>\n\nCannot submit request now. Try later.")
            status_code, resp = await iqless_submit_job(email, password, totp, device=device)
            if status_code == 200:
                txt = (
                    f"✅ <b>Request submitted successfully!</b>\n\n"
                    f"🆔 Job ID: <code>{resp.get('job_id')}</code>\n"
                    f"📱 Device: <code>{device}</code> ({dev_label})\n"
                    f"📊 Status: {resp.get('status')}\n"
                    f"📋 Queue Position: {resp.get('queue_position')}\n"
                    f"⏱️ Est. Wait: ~{resp.get('estimated_wait_seconds')}s\n\n"
                    f"<i>Use 🔎 Track Job to track the request</i>"
                )
            else:
                detail = resp.get("detail", {})
                txt = (
                    f"❌ <b>Submission failed</b>\n\n"
                    f"Code: <code>{detail.get('code', 'unknown')}</code>\n"
                    f"Message: {detail.get('message', str(resp))}"
                )
            return await reply(txt)
        except Exception as e:
            return await reply(f"❌ Connection error: {e}")

    if cmd == "/api_canceljob" and uid == OWNER_ID:
        job_id = " ".join(context.args).strip()
        if not job_id:
            return await reply("❌ Enter a valid Job ID.")
        try:
            job_data = await iqless_poll_job(job_id)
            status = job_data.get("status", "unknown")
            if status == "error":
                detail = job_data.get("detail", {})
                err_msg = detail.get("message", "") if isinstance(detail, dict) else str(detail)
                err_code = job_data.get("error", "UNKNOWN")
                if err_code.startswith("HTTP_4"):
                    friendly = "Request not found — may have completed or ID is wrong."
                elif err_code.startswith("HTTP_5"):
                    friendly = "Error in iqless server. Try again later."
                else:
                    friendly = err_msg or ""
                return await reply(
                    f"❓ <b>Request unavailable</b>\n\n"
                    f"🆔 ID: <code>{job_id}</code>\n"
                    f"📝 {friendly}"
                )
            status_emoji = {"queued": "⏳", "running": "⚙️", "success": "✅", "failed": "❌"}.get(status, "❓")
            if status in ("success", "failed"):
                return await reply(
                    f"⚠️ Cannot cancel this request\n\n"
                    f"🆔 Job ID: <code>{job_id}</code>\n"
                    f"📊 Status: {status_emoji} <b>{status}</b>\n\n"
                    f"<i>Only queued or running requests can be cancelled.</i>"
                )
            txt = (
                f"⚠️ <b>Confirm request cancellation</b>\n\n"
                f"🆔 Job ID: <code>{job_id}</code>\n"
                f"📊 Status: {status_emoji} <b>{status}</b>\n"
                f"🔄 Stage: {job_data.get('stage', 0)}/{job_data.get('total_stages', 8)}\n\n"
                f""
            )
            kb = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("", callback_data=f"api_cancel_confirm:{job_id}"),
                    InlineKeyboardButton("", callback_data=f"api_cancel_abort:{job_id}"),
                ]
            ])
            return await reply(txt, reply_markup=kb)
        except Exception as e:
            return await reply(f"❌ Connection error: {e}")

    if cmd == "/api_trackjob" and uid == OWNER_ID:
        job_id = " ".join(context.args).strip()
        if not job_id:
            return await reply("❌ Enter a valid Job ID.")
        try:
            data = await iqless_poll_job(job_id)
            status = data.get("status", "unknown")
            if status == "error":
                detail = data.get("detail", {})
                err_msg = detail.get("message", "") if isinstance(detail, dict) else str(detail)
                err_code = data.get("error", "UNKNOWN")
                if err_code.startswith("HTTP_4"):
                    friendly = "Request not found — may have completed or ID is wrong."
                elif err_code.startswith("HTTP_5"):
                    friendly = "Error in iqless server. Try again later."
                else:
                    friendly = err_msg or ""
                return await reply(
                    f"❓ <b>Request unavailable</b>\n\n"
                    f"🆔 ID: <code>{job_id}</code>\n"
                    f"📝 {friendly}"
                )
            stage = data.get("stage", 0)
            total = data.get("total_stages", 8)
            stage_label = data.get("stage_label", "")
            elapsed = float(data.get("elapsed_seconds", 0) or 0)
            pos = data.get("queue_position", -1)
            status_emoji = {"queued": "⏳", "running": "⚙️", "success": "✅", "failed": "❌"}.get(status, "❓")
            txt = (
                f"{status_emoji} <b>Job Status</b>\n\n"
                f"🆔 ID: <code>{job_id}</code>\n"
                f"📊 Status: <b>{status}</b>\n"
                f"🔄 Stage: {stage}/{total} — {stage_label}\n"
                f"⏱️ Elapsed: {elapsed:.1f}s\n"
            )
            if pos is not None and pos >= 0:
                txt += f"📋 Queue Position: #{pos}\n"
            if status == "success" and data.get("url"):
                txt += f"\n🔗 <b>Google One Link:</b>\n{data['url']}"
            elif status == "failed" and data.get("error"):
                txt += f"\n⚠️ Error: <code>{data['error']}</code>"
            return await reply(txt)
        except Exception as e:
            return await reply(f"❌ Connection error: {e}")

    if cmd == "/checktx" and uid == OWNER_ID:
        tx_input = " ".join(context.args).strip()
        if not tx_input:
            return await reply("❌ Enter the transaction number (TX ID).")
        found_active = None
        for jid, jdata in active_jobs.items():
            if jdata.get("tx_id", "") == tx_input or jid == tx_input:
                found_active = (jid, jdata)
                break
        if found_active:
            jid, jdata = found_active
            now = time.time()
            elapsed = int(now - jdata.get("submitted_at", now))
            mins, secs = divmod(elapsed, 60)
            pos = jdata.get("last_pos", -1)
            stage = jdata.get("last_stage", -1)
            status_str = f"📋 In Queue: {pos}" if pos >= 0 else (f"⚙️ Stage: {stage}" if stage >= 0 else "")
            txt = (
                f"⚡ <b>Active transaction</b>\n\n"
                f"🆔 Job: <code>{jid}</code>\n"
                f"🔖 TX: <code>{jdata.get('tx_id', 'N/A')}</code>\n"
                f"👤 UID: <code>{jdata.get('uid')}</code>\n"
                f"📧 {jdata.get('email', 'N/A')}\n"
                f"💰 ${jdata.get('cost', 0):.2f}\n"
                f"📊 Status: {status_str}\n"
                f"⏱️ Since: {mins}m {secs}s"
            )
            return await reply(txt)
        conn = db_connect()
        row = conn.execute(
            "SELECT user_id, email, status, url, reason, tx_id, ts FROM history WHERE tx_id=? ORDER BY id DESC LIMIT 1",
            (tx_input,)
        ).fetchone()
        conn.close()
        if row:
            status_emoji = "✅" if row["status"] == "success" else "❌"
            txt = (
                f"🔍 <b>Search result</b>\n\n"
                f"🔖 TX: <code>{row['tx_id']}</code>\n"
                f"👤 UID: <code>{row['user_id']}</code>\n"
                f"📧 {row['email']}\n"
                f"{status_emoji} Status: <b>{row['status']}</b>\n"
                f"🕐 Timestamp: {row['ts']}\n"
            )
            if row["url"]:
                txt += f"🔗 URL: {row['url']}\n"
            if row["reason"]:
                txt += f"⚠️ Reason: {row['reason']}\n"
            return await reply(txt)
        return await reply(f"⚠️ No transaction found with this ID:\n<code>{tx_input}</code>")

    return await reply("⚠️ Unknown or unauthorized command.")


# =========================
# EXTERNAL ADMIN
# =========================
async def external_admin_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    owner_id = current_external_owner_id(context)
    msg = update.callback_query.message if update.callback_query else update.message
    if uid != owner_id:
        return await msg.reply_text("⛔ Access Denied")
    await sync_commands_for_chat(context.bot, uid, "ext_admin", is_owner=True)
    text = t(uid, "welcome_ext_admin", shop_title=current_external_title(context))
    kb = build_ext_admin_keyboard(uid)
    if update.callback_query:
        try:
            await msg.edit_text(text, parse_mode="HTML", reply_markup=kb)
        except Exception:
            await msg.reply_text(text, parse_mode="HTML", reply_markup=kb)
    else:
        await msg.reply_text(text, parse_mode="HTML", reply_markup=kb)

async def callback_ext_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    if uid != current_external_owner_id(context):
        return
    shop_id = current_shop_id(context)
    data = query.data

    if data == "ext_help":
        return await cmd_help(update, context)

    if data == "ext_stats":
        users = get_shop_user_count(shop_id)
        products = get_shop_product_count(shop_id)
        conn = db_connect()
        row = conn.execute("SELECT COUNT(*) AS c, COALESCE(SUM(price),0) AS s FROM purchases WHERE shop_id=?", (shop_id,)).fetchone()
        conn.close()
        orders = int(row["c"]) if row else 0
        sales = float(row["s"]) if row else 0.0
        return await query.message.edit_text(
            f"📊 <b>{current_external_title(context)} Stats</b>\n\n👥 Users: {users}\n📦 Products: {products}\n🛒 Orders: {orders}\n💵 Sales: ${sales:.2f}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(t(uid, "btn_back"), callback_data="user_home")]])
        )

    if data == "ext_wallet":
        return await query.message.edit_text(
            f"💰 <b>Store Wallet Control</b>\nUse <code>/addshop</code> and <code>/removeshop</code> from this bot.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(t(uid, "btn_back"), callback_data="user_home")]])
        )

    if data == "user_home":
        return await external_admin_start(update, context)

    if data.startswith("ext_act_"):
        action = data.split("_", 2)[2]
        if action == "listprod":
            return await ext_admin_cmds_handler(update, context, direct_cmd="/listprod")
        context.user_data["ext_admin_action"] = action
        prompts = {
            "addshop": "📝 <b>Send User ID and Amount:</b>\nExample: <code>123456789 10</code>",
            "removeshop": "📝 <b>Send User ID and Amount:</b>\nExample: <code>123456789 10</code>",
            "check": "📝 <b>Send User ID:</b>",
            "addcode": "📝 <b>Send Product ID and one code:</b>\nExample: <code>5 ABCD-1234</code>",
            "addcodes": "📝 <b>Send Product ID then codes on new lines:</b>\nExample:\n<code>5\nCODE-1\nCODE-2</code>",
            "broadcast": "📢 <b>Send Message:</b>",
            "reply": "📝 <b>Send User ID and Message:</b>\nExample: <code>12345 Hello</code>",
            "settitle": "🏷️ <b>Send New Store Title:</b>",
        }
        return await query.message.reply_text(prompts.get(action, "📝 <b>Enter Input:</b>"), parse_mode="HTML")

async def ext_admin_cmds_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, direct_cmd: str = None):
    uid = update.effective_user.id
    owner_id = current_external_owner_id(context)
    shop_id = current_shop_id(context)
    if uid != owner_id:
        msg = update.callback_query.message if update.callback_query else update.message
        return await msg.reply_text("⛔ Access Denied.")

    txt = direct_cmd if direct_cmd else (update.message.text or "")
    cmd = txt.split()[0] if txt else ""
    context.args = txt.split()[1:] if txt else []

    async def reply(text: str, reply_markup=None):
        msg = update.callback_query.message if update.callback_query else update.message
        await msg.reply_text(text, parse_mode="HTML", reply_markup=reply_markup)

    if cmd == "/help":
        return await cmd_help(update, context)

    target_id = None
    if len(context.args) >= 1 and context.args[0].isdigit():
        target_id = int(context.args[0])

    if cmd == "/settitle":
        try:
            title = txt.split(maxsplit=1)[1]
            update_external_shop_title(shop_id, title)
            context.bot_data["external_title"] = title
            return await reply(f"✅ Title updated to <b>{title}</b>")
        except Exception:
            return await reply("❌ Usage: /settitle New Title")

    if cmd == "/addshop" and target_id:
        try:
            amt = float(context.args[1])
            add_shop_balance(target_id, amt, shop_id=shop_id)
            try:
                await Bot(current_external_store_token(context)).send_message(target_id, t(target_id, "shop_added_msg", amount=amt), parse_mode="HTML")
            except Exception:
                pass
            return await reply(f"✅ Added ${amt:.2f} to user {target_id} in this store.")
        except Exception:
            return await reply("❌ Usage: /addshop ID Amount")

    if cmd == "/removeshop" and target_id:
        try:
            amt = float(context.args[1])
            add_shop_balance(target_id, -amt, shop_id=shop_id)
            try:
                await Bot(current_external_store_token(context)).send_message(target_id, t(target_id, "shop_removed_msg", amount=amt), parse_mode="HTML")
            except Exception:
                pass
            return await reply(f"✅ Removed ${amt:.2f} from user {target_id} in this store.")
        except Exception:
            return await reply("❌ Usage: /removeshop ID Amount")

    if cmd == "/check" and target_id:
        user_name = get_user_data(target_id).get("username", "")
        balance = get_shop_balance(target_id, shop_id)
        orders = get_purchase_count(target_id, shop_id)
        return await reply(f"👤 <b>Store User Info</b>\nID: <code>{target_id}</code>\nUsername: @{user_name or 'No Username'}\n🛒 Wallet: ${balance:.2f}\n🛍️ Orders: {orders}")

    if cmd == "/addcode":
        try:
            pid = int(context.args[0])
            code_text = txt.split(maxsplit=2)[2]
            if not get_product(shop_id, pid):
                return await reply("❌ Product not found.")
            add_product_code(shop_id, pid, code_text)
            return await reply(f"✅ Code added to product {pid}. New stock: {get_product(shop_id, pid)['stock']}")
        except Exception:
            return await reply("❌ Usage: /addcode PRODUCT_ID CODE")

    if cmd == "/addcodes":
        try:
            raw = txt.split(maxsplit=2)
            pid = int(raw[1])
            data = raw[2] if len(raw) > 2 else ""
            codes = [x.strip() for x in data.replace("||", "\n").splitlines() if x.strip()]
            if not codes:
                return await reply("❌ No codes provided.")
            if not get_product(shop_id, pid):
                return await reply("❌ Product not found.")
            inserted = add_product_codes_bulk(shop_id, pid, codes)
            skipped = len(codes) - inserted
            msg_txt = f"✅ Added {inserted} codes to product {pid}. New stock: {get_product(shop_id, pid)['stock']}"
            if skipped > 0:
                msg_txt += f"\n⚠️ {skipped} duplicate(s) skipped."
            return await reply(msg_txt)
        except Exception:
            return await reply("❌ Usage: /addcodes PRODUCT_ID then codes on new lines")

    if cmd == "/delprod":
        try:
            pid = int(context.args[0])
            del_product(shop_id, pid)
            return await reply(f"✅ Product {pid} deleted from this store.")
        except Exception:
            return await reply("❌ Usage: /delprod PRODUCT_ID")

    if cmd == "/listprod":
        prods = get_all_products(shop_id)
        if not prods:
            return await reply("📭 No products in this store.")
        msg = f"🛒 <b>{current_external_title(context)} Products</b>\n"
        for p in prods:
            msg += f"ID: {p['id']} | {p['name']} | ${float(p['price']):.2f} | Stock: {int(p['stock'])} | {p['delivery_type']} | Cat: {p['category']}\n"
        return await reply(msg)

    if cmd == "/reply":
        try:
            target_id = int(context.args[0])
            msg_text = " ".join(context.args[1:])
            conn = db_connect()
            conn.execute("UPDATE tickets SET status='closed' WHERE user_id=? AND shop_id=? AND status='open'", (target_id, shop_id))
            conn.commit()
            conn.close()
            await Bot(current_external_store_token(context)).send_message(target_id, t(target_id, "support_reply", msg=msg_text), parse_mode="HTML")
            return await reply(f"✅ Reply sent to {target_id}")
        except Exception:
            return await reply("❌ Usage: /reply ID Message")

    if cmd == "/broadcast":
        msg_text = " ".join(context.args)
        if not msg_text:
            return await reply("❌ Usage: /broadcast MESSAGE")
        users = get_shop_users(shop_id)
        await reply(f"🚀 Broadcasting to {len(users)} users...")
        count = 0
        store_bot = Bot(current_external_store_token(context))
        for user_id in users:
            try:
                await store_bot.send_message(user_id, msg_text, parse_mode="HTML")
                count += 1
                await asyncio.sleep(0.04)
            except Exception:
                pass
        return await reply(f"✅ Done. Sent to {count} users.")

    return await reply("⚠️ Unknown command.")


# =========================
# APP BUILDERS / RUNTIME
# =========================
def build_support_conversation():
    return ConversationHandler(
        entry_points=[
            CallbackQueryHandler(cmd_support, pattern="^user_support$"),
            CommandHandler("support", cmd_support),
        ],
        states={SUPPORT_CHAT: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_support_message)]},
        fallbacks=[CommandHandler("cancel", cancel)]
    )

def build_add_product_conversation(callback_pattern: str):
    return ConversationHandler(
        entry_points=[
            CommandHandler("addprod", start_add_prod),
            CallbackQueryHandler(start_add_prod_callback, pattern=callback_pattern),
        ],
        states={
            ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_prod_name)],
            ADD_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_prod_price)],
            ADD_STOCK: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_prod_stock)],
            ADD_CAT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_prod_cat)],
            ADD_DESC: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_prod_desc)],
            ADD_IMG: [MessageHandler((filters.PHOTO | filters.TEXT) & ~filters.COMMAND, add_prod_img)],
            ADD_FILE: [MessageHandler(filters.ALL & ~filters.COMMAND, add_prod_file)],
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )

def build_main_user_app():
    persistence = PicklePersistence(filepath=PERSISTENCE_PATH)
    app = Application.builder().token(MAIN_BOT_TOKEN).connect_timeout(30).read_timeout(30).write_timeout(30).pool_timeout(30).persistence(persistence).build()
    app.bot_data["bot_mode"] = "main_user"
    app.bot_data["shop_id"] = 0
    app.job_queue.run_repeating(check_ws_expirations, interval=1800, first=120)
    app.job_queue.run_repeating(check_blockchain_deposits, interval=300, first=120)
    _beirut = ZoneInfo("Asia/Beirut")
    app.job_queue.run_daily(scheduled_backup, time=datetime.time(0, 0, 0, tzinfo=_beirut))
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("activate", cmd_activate))
    app.add_handler(CommandHandler("shop", cmd_shop))
    app.add_handler(CommandHandler("profile", cmd_profile))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("invite", cmd_invite))
    app.add_handler(CommandHandler("language", cmd_language))
    app.add_handler(CommandHandler("daily", cmd_daily))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("deposit", cmd_deposit))
    app.add_handler(CommandHandler("claim", cmd_claim))
    app.add_handler(CommandHandler("workspace", cmd_workspace))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(build_support_conversation())
    app.add_handler(CallbackQueryHandler(callback_lang, pattern="^lang_|^check_join$"))
    app.add_handler(CallbackQueryHandler(callback_shop_handler, pattern="^(shop_|view_prod_|buy_ask_)"))
    app.add_handler(CallbackQueryHandler(callback_main_menu, pattern="^user_|^dep_|^confirm_activate_|^cancel_activate$|^act_cancel_flow$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    return app

def build_main_admin_app():
    app = Application.builder().token(LOG_BOT_TOKEN).connect_timeout(30).read_timeout(30).write_timeout(30).pool_timeout(30).build()
    app.bot_data["bot_mode"] = "main_admin"
    app.bot_data["shop_id"] = 0
    app.add_handler(build_add_product_conversation("^act_addprod$"))
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("myinvite", cmd_myinvite))
    app.add_handler(CommandHandler("language", cmd_language))
    app.add_handler(CommandHandler("ws", cmd_ws))
    app.add_handler(CommandHandler("wsprotect", cmd_wsprotect))
    app.add_handler(CommandHandler("wskeys", cmd_wskeys))
    app.add_handler(CommandHandler("wsrefresh", cmd_wsrefresh))
    app.add_handler(CommandHandler([
        "add", "remove", "addshop", "removeshop", "addreseller", "delreseller",
        "backup", "setprice", "setwsprice", "setwsmonthlyprice", "setprofit", "addrc", "removerc",
        "rusers", "rlink", "runlink", "check",
        "reply", "broadcast", "broadcast_inactive", "maintenance", "ban", "unban",
        "listprod", "delprod", "addcode", "addcodes",
        "addextshop", "delextshop", "listextshops", "resellers"
    ], main_admin_cmds_handler))
    app.add_handler(CallbackQueryHandler(callback_ws_admin, pattern="^ws_"))
    app.add_handler(CallbackQueryHandler(callback_main_admin_menu, pattern="^adm_|^act_|^api_|^maint_notify_"))
    app.add_handler(CallbackQueryHandler(callback_lang, pattern="^lang_"))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_ws_admin_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    return app

def build_external_user_app(shop_row: dict):
    app = Application.builder().token(shop_row["shop_token"]).connect_timeout(30).read_timeout(30).write_timeout(30).pool_timeout(30).build()
    app.bot_data["bot_mode"] = "external_user"
    app.bot_data["shop_id"] = int(shop_row["id"])
    app.bot_data["external_owner_id"] = int(shop_row["owner_id"])
    app.bot_data["external_title"] = shop_row["title"]
    app.bot_data["external_store_token"] = shop_row["shop_token"]
    app.bot_data["external_admin_token"] = shop_row["admin_token"]
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("shop", cmd_shop))
    app.add_handler(CommandHandler("profile", cmd_profile))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("language", cmd_language))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(build_support_conversation())
    app.add_handler(CallbackQueryHandler(callback_lang, pattern="^lang_|^check_join$"))
    app.add_handler(CallbackQueryHandler(callback_shop_handler, pattern="^(shop_|view_prod_|buy_ask_)"))
    app.add_handler(CallbackQueryHandler(callback_main_menu, pattern="^user_"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    return app

def build_external_admin_app(shop_row: dict):
    app = Application.builder().token(shop_row["admin_token"]).connect_timeout(30).read_timeout(30).write_timeout(30).pool_timeout(30).build()
    app.bot_data["bot_mode"] = "ext_admin"
    app.bot_data["shop_id"] = int(shop_row["id"])
    app.bot_data["external_owner_id"] = int(shop_row["owner_id"])
    app.bot_data["external_title"] = shop_row["title"]
    app.bot_data["external_store_token"] = shop_row["shop_token"]
    app.bot_data["external_admin_token"] = shop_row["admin_token"]
    app.add_handler(build_add_product_conversation("^ext_act_addprod$"))
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("language", cmd_language))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler([
        "addshop", "removeshop", "check", "delprod", "listprod", "addcode", "addcodes",
        "reply", "broadcast", "settitle"
    ], ext_admin_cmds_handler))
    app.add_handler(CallbackQueryHandler(callback_ext_admin_menu, pattern="^ext_|^user_home$"))
    app.add_handler(CallbackQueryHandler(callback_lang, pattern="^lang_"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    return app

async def start_external_shop_runtime(shop_id: int):
    row = get_external_shop_by_id(shop_id)
    if not row:
        return False, "Shop not found."
    if shop_id in EXTERNAL_USER_APPS or shop_id in EXTERNAL_ADMIN_APPS:
        return True, "Already active."
    try:
        user_app = build_external_user_app(row)
        admin_app = build_external_admin_app(row)
        await user_app.initialize()
        await admin_app.initialize()
        user_me = await user_app.bot.get_me()
        admin_me = await admin_app.bot.get_me()
        update_external_shop_usernames(shop_id, user_me.username or "", admin_me.username or "")
        await user_app.bot.set_my_commands(external_user_commands())
        await admin_app.bot.set_my_commands(ext_admin_commands())
        await user_app.start()
        await admin_app.start()
        await user_app.updater.start_polling()
        await admin_app.updater.start_polling()
        EXTERNAL_USER_APPS[shop_id] = user_app
        EXTERNAL_ADMIN_APPS[shop_id] = admin_app
        return True, (
            f"🛍️ Store Bot: @{user_me.username or user_me.id}\n"
            f"🛠️ Control Bot: @{admin_me.username or admin_me.id}"
        )
    except Exception as e:
        for d in [EXTERNAL_USER_APPS, EXTERNAL_ADMIN_APPS]:
            app = d.pop(shop_id, None)
            if app:
                try:
                    await app.stop()
                    await app.shutdown()
                except Exception:
                    pass
        return False, str(e)

async def stop_external_shop_runtime(shop_id: int):
    user_app = EXTERNAL_USER_APPS.pop(shop_id, None)
    admin_app = EXTERNAL_ADMIN_APPS.pop(shop_id, None)
    for app in [user_app, admin_app]:
        if not app:
            continue
        try:
            if app.updater and app.updater.running:
                await app.updater.stop()
        except Exception:
            pass
        try:
            await app.stop()
        except Exception:
            pass
        try:
            await app.shutdown()
        except Exception:
            pass


# =========================
# SCHEDULED TASKS
# =========================
async def daily_stats_job():
    conn = db_connect()
    today_date = str(datetime.date.today())
    new_users = conn.execute("SELECT COUNT(*) AS c FROM users WHERE date(last_activity)=date('now')").fetchone()["c"]
    total_users = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
    deposits_today = conn.execute("SELECT COALESCE(SUM(amount),0) AS s FROM deposits WHERE date(ts)=date('now')").fetchone()["s"]
    success = conn.execute("SELECT COUNT(*) AS c FROM history WHERE status='SUCCESS' AND date(ts)=date('now')").fetchone()["c"]
    fail = conn.execute("SELECT COUNT(*) AS c FROM history WHERE status='FAILED' AND date(ts)=date('now')").fetchone()["c"]
    conn.close()

    report = (
        f"📊 <b>Daily Report ({today_date})</b>\n\n"
        f"👥 New Active: {int(new_users)}\n"
        f"🌍 Total Users: {int(total_users)}\n"
        f"💰 Deposits: ${float(deposits_today):.2f}\n"
        f"✅ Activations: {int(success)} | ❌ Failed: {int(fail)}"
    )
    try:
        with open(DB_PATH, "rb") as f:
            await send_log_via_second_bot(report, document=f, filename=f"backup_{today_date}.db")
    except Exception as e:
        await send_log_via_second_bot(f"{report}\n\n⚠️ DB Backup Failed: {e}")

async def scheduled_tasks():
    while True:
        now = datetime.datetime.now()
        tomorrow = now + datetime.timedelta(days=1)
        next_run = datetime.datetime.combine(tomorrow, datetime.time.min)
        await asyncio.sleep(max(1, (next_run - now).total_seconds()))
        try:
            await daily_stats_job()
        except Exception:
            pass
        await asyncio.sleep(60)


# =========================
# CHATGPT WORKSPACE FEATURE
# =========================
import json as _json
import uuid as _uuid

WORKSPACE_SESSIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "storage", "sessions")
os.makedirs(WORKSPACE_SESSIONS_DIR, exist_ok=True)

# ── DB helpers ────────────────────────────────────────────────────────────

def ws_normalize_email(email: str) -> str:
    return email.strip().lower()

def ws_generate_code(length: int = 8) -> str:
    import random, string
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))

def ws_create_workspace(name: str, url: str = "", max_invites: int = 5) -> dict:
    conn = db_connect()
    ws_id = f"ws-{secrets.token_hex(4)}"
    conn.execute(
        "INSERT INTO chatgpt_workspaces (id, name, url, max_invites) VALUES (?,?,?,?)",
        (ws_id, name, url, max_invites)
    )
    conn.commit()
    row = conn.execute("SELECT * FROM chatgpt_workspaces WHERE id=?", (ws_id,)).fetchone()
    conn.close()
    return dict(row)

def ws_list_workspaces() -> list:
    conn = db_connect()
    rows = conn.execute("SELECT * FROM chatgpt_workspaces ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def ws_get_workspace(ws_id: str) -> dict | None:
    conn = db_connect()
    row = conn.execute("SELECT * FROM chatgpt_workspaces WHERE id=?", (ws_id,)).fetchone()
    conn.close()
    return dict(row) if row else None

def ws_get_workspace_by_name(name: str) -> dict | None:
    conn = db_connect()
    row = conn.execute("SELECT * FROM chatgpt_workspaces WHERE name=?", (name,)).fetchone()
    conn.close()
    return dict(row) if row else None

def ws_update_workspace(ws_id: str, **kwargs):
    allowed = {"name", "url", "session_file", "personal_session_file", "organization_id",
                "account_id", "max_invites", "status", "expires_at", "chatgpt_totp_secret"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return
    set_clause = ", ".join(f"{k}=?" for k in fields)
    conn = db_connect()
    conn.execute(f"UPDATE chatgpt_workspaces SET {set_clause} WHERE id=?", (*fields.values(), ws_id))
    conn.commit()
    conn.close()

def ws_delete_workspace(ws_id: str):
    conn = db_connect()
    conn.execute("DELETE FROM chatgpt_workspaces WHERE id=?", (ws_id,))
    conn.commit()
    conn.close()

def _get_available_workspace() -> dict | None:
    """Return the first active workspace with available capacity."""
    conn = db_connect()
    rows = conn.execute(
        "SELECT * FROM chatgpt_workspaces WHERE status='active' ORDER BY created_at"
    ).fetchall()
    conn.close()
    for row in rows:
        ws = dict(row)
        if _workspace_has_capacity(ws["id"], _ws_obj=ws):
            return ws
    return None

def _get_workspace_by_id(ws_id) -> dict | None:
    conn = db_connect()
    row = conn.execute("SELECT * FROM chatgpt_workspaces WHERE id=?", (str(ws_id),)).fetchone()
    conn.close()
    return dict(row) if row else None

def _workspace_has_capacity(ws_id, _ws_obj=None) -> bool:
    ws = _ws_obj or _get_workspace_by_id(ws_id)
    if not ws:
        return False
    max_inv = int(ws.get("max_invites") or 0)
    if max_inv <= 0:
        return True  # unlimited
    conn = db_connect()
    count = conn.execute(
        "SELECT COUNT(*) FROM chatgpt_requests WHERE workspace_id=? AND status NOT IN ('failed','rejected')",
        (str(ws_id),)
    ).fetchone()[0]
    conn.close()
    return count < max_inv

def ws_create_key(workspace_id: str, created_by: int, expiry_hours: int = 72,
                  subscription_hours: int = 720) -> dict:
    conn = db_connect()
    key_id = str(_uuid.uuid4())
    code = ws_generate_code(8)
    while conn.execute("SELECT 1 FROM chatgpt_invite_keys WHERE code=?", (code,)).fetchone():
        code = ws_generate_code(8)
    expires_at = (datetime.datetime.utcnow() + datetime.timedelta(hours=expiry_hours)).isoformat()
    conn.execute(
        "INSERT INTO chatgpt_invite_keys (id, code, workspace_id, expires_at, created_by, subscription_hours) VALUES (?,?,?,?,?,?)",
        (key_id, code, workspace_id, expires_at, created_by, subscription_hours)
    )
    conn.commit()
    row = conn.execute("SELECT * FROM chatgpt_invite_keys WHERE id=?", (key_id,)).fetchone()
    conn.close()
    return dict(row)

def ws_validate_key(code: str) -> tuple[bool, str, dict | None]:
    """Returns (ok, reason, key_row).

    Floating keys (workspace_id == '' or 'any') are assigned to the
    first available workspace at validation time and the DB is updated.
    """
    conn = db_connect()
    row = conn.execute("SELECT * FROM chatgpt_invite_keys WHERE code=?", (code.upper(),)).fetchone()
    conn.close()
    if not row:
        return False, "invalid", None
    row = dict(row)
    if row["status"] != "active":
        return False, row["status"], None
    if datetime.datetime.utcnow().isoformat() > row["expires_at"]:
        return False, "expired", None

    # ── Floating key: assign to first available workspace ──────────────
    ws_id = row.get("workspace_id", "")
    if not ws_id or ws_id == "any":
        ws = _get_available_workspace()
        if not ws:
            return False, "workspace_full", None
        # Lock it into this workspace so subsequent reads are consistent
        conn = db_connect()
        conn.execute("UPDATE chatgpt_invite_keys SET workspace_id=? WHERE id=?", (ws["id"], row["id"]))
        conn.commit()
        conn.close()
        row["workspace_id"] = ws["id"]
    else:
        ws = ws_get_workspace(ws_id)

    if not ws:
        return False, "invalid", None
    if ws["status"] in ("disabled", "flushed"):
        return False, "workspace_disabled", None

    # Check capacity
    conn = db_connect()
    used = conn.execute(
        "SELECT COUNT(*) as c FROM chatgpt_requests WHERE workspace_id=? AND status NOT IN ('failed','rejected')",
        (row["workspace_id"],)
    ).fetchone()["c"]
    conn.close()
    if used >= ws["max_invites"]:
        return False, "workspace_full", None
    return True, "ok", row

def ws_mark_key_used(code: str, email: str):
    conn = db_connect()
    conn.execute(
        "UPDATE chatgpt_invite_keys SET status='used', used_by_email=?, used_at=? WHERE code=?",
        (email, datetime.datetime.utcnow().isoformat(), code.upper())
    )
    conn.commit()
    conn.close()

def ws_create_request(workspace_id: str, email: str, invite_code: str,
                       telegram_user_id: int, telegram_username: str = None,
                       paid_amount: float = 0.0) -> dict:
    conn = db_connect()
    normalized = ws_normalize_email(email)
    # Check duplicate
    existing = conn.execute(
        "SELECT * FROM chatgpt_requests WHERE workspace_id=? AND normalized_email=? AND status NOT IN ('failed','rejected')",
        (workspace_id, normalized)
    ).fetchone()
    if existing:
        conn.close()
        return dict(existing)
    req_id = str(_uuid.uuid4())
    conn.execute(
        "INSERT INTO chatgpt_requests (id, workspace_id, email, normalized_email, invite_code, telegram_user_id, telegram_username, paid_amount) VALUES (?,?,?,?,?,?,?,?)",
        (req_id, workspace_id, email, normalized, invite_code.upper(), telegram_user_id, telegram_username, paid_amount)
    )
    conn.commit()
    row = conn.execute("SELECT * FROM chatgpt_requests WHERE id=?", (req_id,)).fetchone()
    conn.close()
    return dict(row)

def ws_get_pending_requests(workspace_id: str = None) -> list:
    conn = db_connect()
    if workspace_id:
        rows = conn.execute(
            "SELECT * FROM chatgpt_requests WHERE workspace_id=? AND status='pending' ORDER BY created_at",
            (workspace_id,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM chatgpt_requests WHERE status='pending' ORDER BY created_at"
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def ws_get_all_requests(workspace_id: str = None, limit: int = 20) -> list:
    conn = db_connect()
    if workspace_id:
        rows = conn.execute(
            "SELECT * FROM chatgpt_requests WHERE workspace_id=? ORDER BY created_at DESC LIMIT ?",
            (workspace_id, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM chatgpt_requests ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def ws_update_request(req_id: str, **kwargs):
    allowed = {"status", "attempts", "last_error", "authorized_at"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return
    set_clause = ", ".join(f"{k}=?" for k in fields)
    conn = db_connect()
    conn.execute(f"UPDATE chatgpt_requests SET {set_clause} WHERE id=?", (*fields.values(), req_id))
    conn.commit()
    conn.close()

def ws_add_protected_member(workspace_id: str, email: str, role: str = "owner", reason: str = "manual"):
    conn = db_connect()
    normalized = ws_normalize_email(email)
    existing = conn.execute(
        "SELECT id FROM chatgpt_protected_members WHERE workspace_id=? AND normalized_email=?",
        (workspace_id, normalized)
    ).fetchone()
    if existing:
        conn.execute("UPDATE chatgpt_protected_members SET active=1, role=?, reason=? WHERE id=?",
                     (role, reason, existing["id"]))
    else:
        conn.execute(
            "INSERT INTO chatgpt_protected_members (id, workspace_id, email, normalized_email, role, reason) VALUES (?,?,?,?,?,?)",
            (str(_uuid.uuid4()), workspace_id, email, normalized, role, reason)
        )
    conn.commit()
    conn.close()

def ws_list_protected_members(workspace_id: str = None) -> list:
    conn = db_connect()
    if workspace_id:
        rows = conn.execute(
            "SELECT * FROM chatgpt_protected_members WHERE active=1 AND workspace_id=?",
            (workspace_id,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM chatgpt_protected_members WHERE active=1").fetchall()
    conn.close()
    return [dict(r) for r in rows]

def ws_remove_protected_member(workspace_id: str, email: str):
    conn = db_connect()
    conn.execute(
        "UPDATE chatgpt_protected_members SET active=0 WHERE workspace_id=? AND normalized_email=?",
        (workspace_id, ws_normalize_email(email))
    )
    conn.commit()
    conn.close()

def ws_replace_cached_members(workspace_id: str, members: list):
    conn = db_connect()
    conn.execute("DELETE FROM chatgpt_workspace_members WHERE workspace_id=?", (workspace_id,))
    for m in members:
        conn.execute(
            "INSERT OR IGNORE INTO chatgpt_workspace_members (id, workspace_id, email, normalized_email, member_id, role) VALUES (?,?,?,?,?,?)",
            (str(_uuid.uuid4()), workspace_id, m.get("email", ""), ws_normalize_email(m.get("email", "")),
             m.get("id"), m.get("role", "member"))
        )
    conn.commit()
    conn.close()

def ws_get_cached_members(workspace_id: str) -> list:
    conn = db_connect()
    rows = conn.execute("SELECT * FROM chatgpt_workspace_members WHERE workspace_id=?", (workspace_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def ws_load_session(session_file: str) -> dict | None:
    """Load a session JSON file and return parsed content."""
    if not session_file or not os.path.exists(session_file):
        return None
    try:
        with open(session_file, "r", encoding="utf-8") as f:
            return _json.load(f)
    except Exception:
        return None

def ws_save_session_file(workspace_id: str, content: str, session_type: str = "ws") -> str:
    """Save a session file and return its path."""
    os.makedirs(WORKSPACE_SESSIONS_DIR, exist_ok=True)
    path = os.path.join(WORKSPACE_SESSIONS_DIR, f"{workspace_id}-{session_type}.json")
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path

def ws_get_usage() -> list:
    """Return workspace usage summary."""
    workspaces = ws_list_workspaces()
    conn = db_connect()
    result = []
    for ws in workspaces:
        used = conn.execute(
            "SELECT COUNT(*) as c FROM chatgpt_requests WHERE workspace_id=? AND status NOT IN ('failed','rejected','pending')",
            (ws["id"],)
        ).fetchone()["c"]
        result.append({**ws, "used": used, "available": max(0, ws["max_invites"] - used)})
    conn.close()
    return result

# ── Subscription duration options ─────────────────────────────────────────
# (hours, label_ar, label_en)
WS_DURATION_OPTIONS = [
    (24,   "",  "1 Day"),
    (168,  "",    "7 Days"),
    (720,  "",       "1 Month"),
    (2160, "",    "3 Months"),
    (4320, "",    "6 Months"),
    (8760, "",       "1 Year"),
]

# ── Subscription DB functions ─────────────────────────────────────────────

def ws_create_subscription(user_id: int, email: str, workspace_id: str,
                            subscription_hours: int, request_id: str = None) -> dict:
    conn = db_connect()
    sub_id = str(_uuid.uuid4())
    normalized = ws_normalize_email(email)
    activated_at = datetime.datetime.utcnow()
    expires_at = (activated_at + datetime.timedelta(hours=subscription_hours)).isoformat()
    conn.execute(
        "INSERT INTO chatgpt_subscriptions "
        "(id, user_id, email, normalized_email, workspace_id, request_id, subscription_hours, activated_at, expires_at) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (sub_id, user_id, email, normalized, workspace_id, request_id,
         subscription_hours, activated_at.isoformat(), expires_at)
    )
    conn.commit()
    row = conn.execute("SELECT * FROM chatgpt_subscriptions WHERE id=?", (sub_id,)).fetchone()
    conn.close()
    return dict(row)

def ws_update_subscription(sub_id: str, **kwargs):
    conn = db_connect()
    fields = {k: v for k, v in kwargs.items()}
    set_clause = ", ".join(f"{k}=?" for k in fields)
    conn.execute(f"UPDATE chatgpt_subscriptions SET {set_clause} WHERE id=?", (*fields.values(), sub_id))
    conn.commit()
    conn.close()

def ws_get_active_subscriptions(ws_id: str = None) -> list:
    conn = db_connect()
    if ws_id:
        rows = conn.execute(
            "SELECT * FROM chatgpt_subscriptions WHERE workspace_id=? AND status='active' ORDER BY expires_at",
            (ws_id,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM chatgpt_subscriptions WHERE status='active' ORDER BY expires_at"
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def ws_get_expired_subscriptions() -> list:
    now = datetime.datetime.utcnow().isoformat()
    conn = db_connect()
    rows = conn.execute(
        "SELECT * FROM chatgpt_subscriptions WHERE status='active' AND expires_at <= ?",
        (now,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def ws_get_user_subscription(user_id: int) -> dict | None:
    """Get the most recent active subscription for a user."""
    conn = db_connect()
    row = conn.execute(
        "SELECT * FROM chatgpt_subscriptions WHERE user_id=? AND status='active' ORDER BY expires_at DESC LIMIT 1",
        (user_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None

def ws_get_user_subscriptions(user_id: int) -> list:
    conn = db_connect()
    rows = conn.execute(
        "SELECT * FROM chatgpt_subscriptions WHERE user_id=? ORDER BY created_at DESC LIMIT 10",
        (user_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

# ── Backup system ─────────────────────────────────────────────────────────

BACKUP_IMPORTANT_ENV = [
    "TELEGRAM_BOT_TOKEN", "LOG_BOT_TOKEN", "IQLESS_API_KEY",
    "SESSION_SECRET", "BOT_OWNER_ID", "ADMIN_LOG_ID",
    "REQUIRED_CHANNEL", "SUPPORT_USER", "MY_BOT_USERNAME",
    "DEFAULT_ACTIVATE_PRICE", "DEFAULT_RESELLER_PROFIT",
    "DEFAULT_WS_SEAT_PRICE", "DEFAULT_WS_MONTHLY_PRICE",
    "MIN_DEPOSIT", "MY_TRC20_ADDRESS", "MY_BEP20_ADDRESS", "BSCSCAN_API_KEY",
    "CHECKIN_REWARD", "REFERRAL_REWARD", "DB_PATH",
]

BACKUP_FILES = ["bot.py", "main.py", "pyproject.toml"]

def create_backup_zip() -> bytes:
    """Create a zip archive of all important bot files + .env."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # Add source files
        for fname in BACKUP_FILES:
            if os.path.isfile(fname):
                zf.write(fname)
        # Add database
        db_path = DB_PATH if os.path.isfile(DB_PATH) else "bot.db"
        if os.path.isfile(db_path):
            zf.write(db_path)
        # Add .env (generated from current env)
        env_lines = []
        for key in BACKUP_IMPORTANT_ENV:
            val = os.environ.get(key, "")
            if val:
                env_lines.append(f"{key}={val}")
        zf.writestr(".env", "\n".join(env_lines) + "\n")
    return buf.getvalue()

async def do_backup(bot=None, silent=False) -> str:
    """Create and send a backup. Returns status message."""
    try:
        now = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M")
        zip_bytes = create_backup_zip()
        filename = f"bot_backup_{now}.zip"
        bio = io.BytesIO(zip_bytes)
        bio.name = filename
        size_kb = len(zip_bytes) / 1024
        caption = (
            f"💾 <b>Bot Backup</b>\n"
            f"📅 {now} UTC\n"
            f"📦 {size_kb:.1f} KB\n"
            f"📁 {', '.join(BACKUP_FILES + [DB_PATH or 'bot.db', '.env'])}"
        )
        # Send via log bot
        await send_log_via_second_bot(caption, document=bio, filename=filename)
        # Also send directly to owner via main bot if provided
        if bot and OWNER_ID:
            bio2 = io.BytesIO(zip_bytes)
            bio2.name = filename
            try:
                await bot.send_document(chat_id=OWNER_ID, document=bio2, filename=filename, caption=caption, parse_mode="HTML")
            except Exception:
                pass
        return f"✅ Backup sent ({size_kb:.1f} KB)"
    except Exception as e:
        return f"❌ Backup failed: {e}"

async def scheduled_backup(context):
    """Daily scheduled backup job."""
    result = await do_backup(bot=context.bot)
    logging.getLogger(__name__).info(f"[BACKUP] Scheduled: {result}")

# ── Subscription scheduler + workspace migration ──────────────────────────

async def check_ws_expirations(context):
    """Scheduled job every 30 min: expire subscriptions and remove users from workspace."""
    expired = ws_get_expired_subscriptions()
    for sub in expired:
        sub_id = sub["id"]
        user_id = sub["user_id"]
        email = sub["email"]
        ws_id = sub["workspace_id"]
        ws_update_subscription(sub_id, status="expired")
        ws = ws_get_workspace(ws_id)
        ws_name = ws["name"] if ws else ws_id
        if ws and _ws_get_session_file(ws):
            try:
                # Use cached member list to find the member_id, then remove via correct endpoint
                cached = ws_get_cached_members(ws_id)
                normalized = ws_normalize_email(email)
                member = next((m for m in cached if m["normalized_email"] == normalized), None)
                if member and member.get("member_id"):
                    rem = await ws_api_remove_member(ws, member["member_id"])
                    if not rem["ok"]:
                        logger.warning(f"WS_EXPIRE_REMOVE_FAIL sub={sub_id} email={email}: {rem.get('error')}")
            except Exception as e:
                logger.warning(f"WS_EXPIRE_REMOVE_FAIL sub={sub_id} email={email}: {e}")
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    f"⏰ <b>Your ChatGPT Workspace subscription has expired</b>\n\n"
                    f"📧 Account: <code>{email}</code>\n"
                    f"🏢 Workspace: <b>{ws_name}</b>\n\n"
                    f"To renew, go to Main Menu → ChatGPT Workspace."
                ),
                parse_mode="HTML"
            )
        except Exception:
            pass
        logger.info(f"WS_SUB_EXPIRED sub={sub_id} user={user_id} email={email} ws={ws_id}")

async def ws_migrate_workspace_subscribers(old_ws_id: str, bot=None):
    """Called when workspace is disabled/flushed. Migrates active subscribers to another workspace."""
    subs = ws_get_active_subscriptions(old_ws_id)
    if not subs:
        return
    old_ws = ws_get_workspace(old_ws_id)
    old_name = old_ws["name"] if old_ws else old_ws_id
    for sub in subs:
        sub_id = sub["id"]
        user_id = sub["user_id"]
        email = sub["email"]
        normalized = ws_normalize_email(email)
        # Find new workspace with capacity (skip old one)
        new_ws = None
        for candidate in ws_list_workspaces():
            if candidate["id"] == old_ws_id:
                continue
            if candidate["status"] != "active":
                continue
            if _workspace_has_capacity(candidate["id"]):
                new_ws = candidate
                break
        if new_ws:
            ws_update_subscription(sub_id, workspace_id=new_ws["id"])
            conn = db_connect()
            req_id = str(_uuid.uuid4())
            conn.execute(
                "INSERT OR IGNORE INTO chatgpt_requests "
                "(id, workspace_id, email, normalized_email, invite_code, telegram_user_id, paid_amount) "
                "VALUES (?,?,?,?,?,?,?)",
                (req_id, new_ws["id"], email, normalized, "MIGRATED", user_id, 0.0)
            )
            conn.commit()
            conn.close()
            logger.info(f"WS_SUB_MIGRATED sub={sub_id} email={email} old={old_ws_id} new={new_ws['id']}")
            if bot:
                try:
                    await bot.send_message(
                        chat_id=user_id,
                        text=(
                            f"🔄 <b>Your subscription was automatically transferred</b>\n\n"
                            f"📧 Account: <code>{email}</code>\n"
                            f"🏢 From: {old_name} ← To: <b>{new_ws['name']}</b>\n\n"
                            f""
                        ),
                        parse_mode="HTML"
                    )
                except Exception:
                    pass
        else:
            ws_update_subscription(sub_id, status="migrating")
            logger.warning(f"WS_SUB_NO_WS_AVAILABLE sub={sub_id} email={email}")
            if bot:
                try:
                    await bot.send_message(
                        chat_id=user_id,
                        text=(
                            f"⚠️ <b>Could not transfer your subscription temporarily</b>\n\n"
                            f"📧 Account: <code>{email}</code>\n\n"
                            f"No Workspace available now. You will be transferred when one is available.\n"
                            f""
                        ),
                        parse_mode="HTML"
                    )
                except Exception:
                    pass

# ── ChatGPT API helpers ───────────────────────────────────────────────────

CHATGPT_BASE = "https://chatgpt.com"

def ws_decode_token_exp(token: str):
    """Decode the exp (expiry) claim from a JWT without signature verification.
    Returns a UTC datetime or None if the token is malformed / has no exp."""
    try:
        payload_b64 = token.split(".")[1]
        # JWT base64url: pad to multiple of 4
        payload_b64 += "=" * (4 - len(payload_b64) % 4)
        payload = _json.loads(base64.b64decode(payload_b64.replace("-", "+").replace("_", "/")))
        exp = payload.get("exp")
        if exp:
            return datetime.datetime.utcfromtimestamp(exp)
    except Exception:
        pass
    return None


def ws_get_session_expiry(session_file: str):
    """Return the accessToken expiry as a UTC datetime, reading from:
    1. JWT exp claim in accessToken (most accurate — actual API token lifetime)
    2. Falls back to session-level 'expires' ISO string (next-auth session)
    Returns None if expiry cannot be determined."""
    session = ws_load_session(session_file)
    if not session:
        return None
    token = session.get("accessToken", "")
    if token:
        exp = ws_decode_token_exp(token)
        if exp:
            return exp
    # Fallback: session-level expires field
    expires_str = session.get("expires")
    if expires_str:
        try:
            # Strip the trailing Z and fractional seconds if present
            clean = expires_str.rstrip("Z").split(".")[0]
            return datetime.datetime.strptime(clean, "%Y-%m-%dT%H:%M:%S")
        except Exception:
            pass
    return None


def ws_format_expiry_delta(exp: datetime.datetime) -> str:
    """Return a human-readable string like '9d 14h' or '1h 32m' or 'EXPIRED'."""
    now = datetime.datetime.utcnow()
    delta = exp - now
    total_secs = int(delta.total_seconds())
    if total_secs <= 0:
        return ""
    days = total_secs // 86400
    hours = (total_secs % 86400) // 3600
    mins = (total_secs % 3600) // 60
    if days > 0:
        return f"{days}d {hours}h"
    elif hours > 0:
        return f"{hours}h {mins}m"
    else:
        return f"{mins}m"


async def ws_try_refresh_token(session_file: str) -> bool:
    """Try to refresh the ChatGPT accessToken by calling /api/auth/session.
    Looks for the session cookie in multiple places the export tool may save it:
      1. 'sessionToken' field  (ChatGPT Session Exporter — most common)
      2. 'cookies' dict        ({name: value})
      3. 'cookies' list        ([{name, value, ...}])
    Saves the new accessToken + sessionToken back to the file if successful.
    Returns True if refresh succeeded."""
    session = ws_load_session(session_file)
    if not session:
        return False

    # ── Extract the __Secure-next-auth.session-token cookie value ─────────
    next_auth_token = None

    # Priority 1: 'sessionToken' top-level field (ChatGPT Session Exporter)
    if session.get("sessionToken"):
        next_auth_token = session["sessionToken"]

    # Priority 2: 'cookies' as dict
    if not next_auth_token:
        raw_cookies = session.get("cookies")
        if isinstance(raw_cookies, dict):
            next_auth_token = (
                raw_cookies.get("__Secure-next-auth.session-token")
                or raw_cookies.get("next-auth.session-token")
            )
        elif isinstance(raw_cookies, list):
            for c in raw_cookies:
                if isinstance(c, dict) and c.get("name") in (
                    "__Secure-next-auth.session-token", "next-auth.session-token"
                ):
                    next_auth_token = c.get("value")
                    break

    if not next_auth_token:
        return False  # No cookie available — cannot refresh

    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(
                "https://chatgpt.com/api/auth/session",
                headers={
                    "Cookie": f"__Secure-next-auth.session-token={next_auth_token}",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Referer": "https://chatgpt.com/",
                    "Accept": "application/json",
                }
            )
            if resp.status_code == 200:
                data = resp.json()
                new_token = data.get("accessToken")
                if new_token:
                    session["accessToken"] = new_token
                    if "expires" in data:
                        session["expires"] = data["expires"]
                    # Also update sessionToken if the server issued a new one
                    if data.get("sessionToken"):
                        session["sessionToken"] = data["sessionToken"]
                    with open(session_file, "w", encoding="utf-8") as f:
                        _json.dump(session, f, ensure_ascii=False)
                    logger.info(f"[WS_REFRESH] Token auto-refreshed: {session_file}")
                    return True
                else:
                    logger.warning(f"[WS_REFRESH] Got 200 but no accessToken in response for {session_file}")
    except Exception as e:
        logger.warning(f"[WS_REFRESH] Refresh failed for {session_file}: {e}")
    return False


async def ws_api_call(session_file: str, method: str, endpoint: str, json_body: dict = None) -> dict:
    """Make an authenticated call to ChatGPT's internal API using the session's accessToken.
    Auto-refreshes the token on expiry (if cookies are stored) and retries once."""
    session = ws_load_session(session_file)
    if not session:
        return {"ok": False, "error": "No session file"}
    access_token = session.get("accessToken")
    if not access_token:
        return {"ok": False, "error": "No accessToken in session"}

    # ── Pre-check: is the token already expired? ──
    exp = ws_get_session_expiry(session_file)
    if exp and datetime.datetime.utcnow() >= exp:
        logger.info(f"[WS_API] Token expired (exp={exp}), attempting refresh before call…")
        refreshed = await ws_try_refresh_token(session_file)
        if refreshed:
            session = ws_load_session(session_file)
            access_token = session.get("accessToken")
        else:
            return {"ok": False, "error": "session_expired", "expired": True, "status": 401}

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Origin": "https://chatgpt.com",
        "Referer": "https://chatgpt.com/",
    }
    url = f"{CHATGPT_BASE}{endpoint}"

    async def _do_request(client, hdrs):
        m = method.upper()
        if m == "GET":
            return await client.get(url, headers=hdrs)
        elif m == "POST":
            return await client.post(url, headers=hdrs, json=json_body or {})
        elif m == "DELETE":
            return await client.delete(url, headers=hdrs)
        elif m == "PATCH":
            return await client.patch(url, headers=hdrs, json=json_body or {})
        return None

    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await _do_request(client, headers)
            if resp is None:
                return {"ok": False, "error": f"Unknown method: {method}"}

            # ── 401 → try one token refresh then retry ──
            if resp.status_code == 401:
                logger.info(f"[WS_API] 401 on {endpoint}, attempting token refresh…")
                refreshed = await ws_try_refresh_token(session_file)
                if refreshed:
                    new_sess = ws_load_session(session_file)
                    headers["Authorization"] = f"Bearer {new_sess.get('accessToken', '')}"
                    resp = await _do_request(client, headers)
                    if resp is None:
                        return {"ok": False, "error": f"Unknown method: {method}"}
                else:
                    return {"ok": False, "error": "session_expired", "expired": True, "status": 401}

            if resp.status_code in (200, 201, 204):
                try:
                    data = resp.json()
                except Exception:
                    data = {}
                return {"ok": True, "data": data, "status": resp.status_code}
            else:
                # 401 again after refresh
                if resp.status_code == 401:
                    return {"ok": False, "error": "session_expired", "expired": True, "status": 401}
                try:
                    err_json = resp.json()
                    if isinstance(err_json, dict):
                        # Extract a simple plain-text message if available
                        msg = (err_json.get("message") or err_json.get("detail") or
                               err_json.get("error") or "")
                        if isinstance(msg, str) and msg and '<' not in msg and len(msg) < 300:
                            error_msg = msg
                        else:
                            error_msg = f"HTTP {resp.status_code}"
                    else:
                        error_msg = f"HTTP {resp.status_code}"
                except Exception:
                    error_msg = f"HTTP {resp.status_code}"
                return {"ok": False, "error": error_msg, "status": resp.status_code}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# ── In-memory pending sessions for direct login (TOTP multi-step) ────────────
_direct_pending: dict = {}   # pending_id → {"auth_cookies", "chatgpt_cookies", "mfa_request_id", "expires_at"}

def _direct_new_id() -> str:
    return secrets.token_hex(12)

async def _chatgpt_login_direct(email: str, password: str, otp: str = "", pending_id: str = "") -> dict:
      """
      All auth steps run on Replit via HTTP API calls:
        /api/hybrid/start?email=  -> CSRF + auth URL + follow + email submit
        /api/hybrid/password      -> password verify
        /api/hybrid/totp          -> TOTP verify (if needed)
        /api/hybrid/finish        -> complete session via chatgpt.com callback
      No curl_cffi needed - Replit handles all OpenAI auth steps to bypass both IP blocks.
      """
      import httpx as _httpx

      TTL = time.time() + 900

      async def _rpc_get(path, **params):
          async with _httpx.AsyncClient(timeout=60) as hx:
              r = await hx.get(f"{WS_LOGIN_API_URL}{path}", params=params)
              try: return r.json()
              except Exception: return {"ok": False, "error": f"\u0627\u0633\u062a\u062c\u0627\u0628\u0629 \u063a\u064a\u0631 \u0635\u0627\u0644\u062d\u0629: {r.text[:150]}"}

      async def _rpc_post(path, body):
          async with _httpx.AsyncClient(timeout=60) as hx:
              r = await hx.post(f"{WS_LOGIN_API_URL}{path}", json=body,
                                headers={"Content-Type": "application/json"})
              try: return r.json()
              except Exception: return {"ok": False, "error": f"\u0627\u0633\u062a\u062c\u0627\u0628\u0629 \u063a\u064a\u0631 \u0635\u0627\u0644\u062d\u0629: {r.text[:150]}"}

      # --- Resume pending TOTP session ---
      if pending_id:
          pending = _direct_pending.get(pending_id)
          if not pending:
              return {"ok": False, "needs_otp": False,
                      "error": "\u0627\u0644\u062c\u0644\u0633\u0629 \u0627\u0644\u0645\u0639\u0644\u0651\u0642\u0629 \u0645\u0646\u062a\u0647\u064a\u0629\u060c \u0627\u0628\u062f\u0623 \u062a\u0633\u062c\u064a\u0644 \u0627\u0644\u062f\u062e\u0648\u0644 \u0645\u0646 \u062c\u062f\u064a\u062f"}
          if time.time() > pending["expires_at"]:
              _direct_pending.pop(pending_id, None)
              return {"ok": False, "needs_otp": False,
                      "error": "\u0627\u0646\u062a\u0647\u062a \u0645\u0647\u0644\u0629 \u0627\u0644\u062c\u0644\u0633\u0629 \u0627\u0644\u0645\u0639\u0644\u0651\u0642\u0629\u060c \u0627\u0628\u062f\u0623 \u0645\u0646 \u062c\u062f\u064a\u062f"}
          _direct_pending.pop(pending_id, None)

          session_id     = pending["session_id"]
          mfa_request_id = pending["mfa_request_id"]

          if not otp:
              return {"ok": False, "needs_otp": False, "error": "\u0644\u0645 \u064a\u064f\u0631\u0633\u064e\u0644 \u0631\u0645\u0632 TOTP"}

          totp_r = await _rpc_post("/api/hybrid/totp",
                                   {"session_id": session_id, "code": otp.strip(),
                                    "mfa_request_id": mfa_request_id})
          if not totp_r.get("ok"):
              return {"ok": False, "needs_otp": False, "error": totp_r.get("error", "\u062e\u0637\u0623 TOTP")}
          callback_url = totp_r.get("callback_url", "")
          if not callback_url:
              return {"ok": False, "needs_otp": False,
                      "error": "\u0644\u0645 \u064a\u064f\u0633\u062a\u0644\u0645 callback_url \u0628\u0639\u062f TOTP"}
          return await _rpc_post("/api/hybrid/finish",
                                 {"session_id": session_id, "callback_url": callback_url})

      # --- Fresh login ---
      try:
          # Step 1: Email step (CSRF + auth URL + follow + email submit) all on Replit
          start = await _rpc_get("/api/hybrid/start", email=email)
          if not start.get("ok"):
              return {"ok": False, "needs_otp": False,
                      "error": f"\u0641\u0634\u0644 \u062e\u0637\u0648\u0629 \u0627\u0644\u0625\u064a\u0645\u064a\u0644: {start.get('error', '')}"}

          session_id = start["session_id"]
          page_type  = start.get("page_type", "")

          # Step 2: Password
          if page_type == "login_password":
              pw_r = await _rpc_post("/api/hybrid/password",
                                     {"session_id": session_id, "password": password})
              if not pw_r.get("ok"):
                  return {"ok": False, "needs_otp": False,
                          "error": pw_r.get("error", "\u0641\u0634\u0644 \u0643\u0644\u0645\u0629 \u0627\u0644\u0633\u0631")}
              if pw_r.get("needs_totp"):
                  mfa_request_id = pw_r.get("mfa_request_id", "")
                  new_session_id = pw_r.get("session_id", session_id)
                  if otp:
                      totp_r = await _rpc_post("/api/hybrid/totp",
                                              {"session_id": new_session_id, "code": otp.strip(),
                                               "mfa_request_id": mfa_request_id})
                      if not totp_r.get("ok"):
                          return {"ok": False, "needs_otp": False,
                                  "error": totp_r.get("error", "\u0641\u0634\u0644 \u0627\u0644\u062a\u062d\u0642\u0642 \u0645\u0646 TOTP")}
                      callback_url = totp_r.get("callback_url", "")
                  else:
                      pid = _direct_new_id()
                      _direct_pending[pid] = {
                          "session_id": new_session_id,
                          "mfa_request_id": mfa_request_id,
                          "expires_at": TTL,
                      }
                      return {"ok": False, "needs_otp": True, "otp_type": "totp",
                              "pending_id": pid, "error": "\u064a\u062a\u0637\u0644\u0628 \u0631\u0645\u0632 TOTP"}
              else:
                  callback_url = pw_r.get("callback_url", "")

          elif page_type in ("email_verification", "email_otp"):
              return {"ok": False, "needs_otp": True, "otp_type": "email", "pending_id": "",
                      "error": "\u26d4 \u0647\u0630\u0627 \u0627\u0644\u062d\u0633\u0627\u0628 \u0628\u062f\u0648\u0646 \u0643\u0644\u0645\u0629 \u0633\u0631 \u2014 \u0623\u0636\u0641 \u0643\u0644\u0645\u0629 \u0633\u0631 + TOTP \u0645\u0646 \u0625\u0639\u062f\u0627\u062f\u0627\u062a ChatGPT \u062b\u0645 \u0623\u0639\u062f \u0627\u0644\u0645\u062d\u0627\u0648\u0644\u0629"}
          else:
              return {"ok": False, "needs_otp": False,
                      "error": f"\u0646\u0648\u0639 \u0635\u0641\u062d\u0629 \u063a\u064a\u0631 \u0645\u062a\u0648\u0642\u0639: {page_type} \u2014 {str(start)[:200]}"}

          if not callback_url:
              return {"ok": False, "needs_otp": False,
                      "error": "\u0644\u0645 \u064a\u064f\u0633\u062a\u0644\u0645 callback_url"}

          # Step 3: Complete session via chatgpt.com on Replit
          return await _rpc_post("/api/hybrid/finish",
                                 {"session_id": session_id, "callback_url": callback_url})

      except Exception as e:
          return {"ok": False, "needs_otp": False,
                  "error": f"\u062e\u0637\u0623 \u0623\u062b\u0646\u0627\u0621 \u062a\u0633\u062c\u064a\u0644 \u0627\u0644\u062f\u062e\u0648\u0644: {e}"}

async def ws_chatgpt_login(email: str, password: str, otp: str = "", pending_id: str = "") -> dict:
    """
    Login to ChatGPT.
    Strategy:
    1. Try direct login via curl_cffi (impersonates Chrome, bypasses Cloudflare)
    2. Fall back to Replit API proxy if curl_cffi is not installed
    """
    # ── Try direct (curl_cffi) ────────────────────────────────────────────
    try:
        from curl_cffi.requests import AsyncSession as _check  # noqa: F401
        has_curl_cffi = True
    except ImportError:
        has_curl_cffi = False

    if has_curl_cffi:
        return await _chatgpt_login_direct(email, password, otp=otp, pending_id=pending_id)

    # ── Fallback: Replit API proxy ────────────────────────────────────────
    try:
        import httpx as _httpx

        payload: dict = {"email": email, "password": password, "ws_id": "login"}
        if otp:
            payload["otp"] = otp.strip()
        if pending_id:
            payload["pending_id"] = pending_id

        async with _httpx.AsyncClient(timeout=90) as client:
            r = await client.post(
                f"{WS_LOGIN_API_URL}/api/login",
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            try:
                result = r.json()
            except Exception:
                return {"ok": False, "needs_otp": False,
                        "error": f"Invalid response from API server (HTTP {r.status_code}): {r.text[:150]}"}
            return result

    except Exception as e:
        return {"ok": False, "needs_otp": False, "error": f"Connection error to API server: {e}"}


def _ws_strip_html(text: str, status_code: int = 0, max_len: int = 200) -> str:
    """Aggressively strip HTML/SVG/CSS from a string and return clean plain text."""
    if not text:
        return f"HTTP {status_code}" if status_code else "Unknown error"
    t = str(text)
    # Remove entire blocks: style, script, svg, noscript
    for tag in ("style", "script", "svg", "noscript"):
        t = re.sub(rf'(?si)<{tag}[^>]*>.*?</{tag}>', '', t)
    # Remove all remaining HTML/XML tags
    t = re.sub(r'<[^>]+>', '', t)
    # Collapse whitespace
    t = re.sub(r'\s+', ' ', t).strip()
    # Filter out lines that look like CSS/SVG data (no normal words)
    words = [w for w in t.split() if re.search(r'[a-zA-Z]{2,}', w) and not re.match(r'^[MmCcLlZzAaQqTtSsHhVv][0-9\.\-,\s]+$', w)]
    clean = ' '.join(words)
    if not clean:
        return f"HTTP {status_code}" if status_code else "Unknown error"
    return clean[:max_len]

def _ws_get_account_id(ws: dict) -> str:
    """Get the account UUID for API calls.
    Prefers account_id (UUID), falls back to organization_id.
    The account_id is extracted from account.id in the session JSON.
    """
    return ws.get("account_id") or ws.get("organization_id") or ""

def _ws_get_session_file(ws: dict) -> str | None:
    """Get the primary session file for API calls (WS session preferred)."""
    return ws.get("session_file") or ws.get("personal_session_file") or None

def _ws_error_str(err, max_len: int = 200) -> str:
    """Convert a ws_api_call error (str, dict, or anything) to a clean displayable string."""
    if err is None:
        return "Unknown error"
    if isinstance(err, dict):
        # Prefer 'message' > 'detail' > 'error' > first value > repr
        text = (err.get("message") or err.get("detail") or
                err.get("error") or next(iter(err.values()), None) or str(err))
    else:
        text = str(err)
    return _ws_strip_html(text, max_len=max_len)

async def ws_api_list_members(ws: dict) -> dict:
    """List workspace members via ChatGPT API.
    Endpoint: GET /backend-api/accounts/{account_id}/users
    Returns items with fields: id, email, role, name, account_user_id
    """
    account_id = _ws_get_account_id(ws)
    sf = _ws_get_session_file(ws)
    if not account_id or not sf:
        return {"ok": False, "error": "Missing account_id or session_file. Upload a WS Session first."}
    result = await ws_api_call(sf, "GET", f"/backend-api/accounts/{account_id}/users")
    if result["ok"]:
        data = result["data"]
        members = data.get("items", []) if isinstance(data, dict) else data
        return {"ok": True, "members": members}
    return result

async def ws_api_invite(ws: dict, email: str, role: str = "member") -> dict:
    """Invite an email to the ChatGPT workspace via API.
    Endpoint: POST /backend-api/accounts/{account_id}/invites
    Body: {"email_addresses": ["email@example.com"]}
    Returns: {"account_invites": [...], "errored_emails": [...]}
    """
    account_id = _ws_get_account_id(ws)
    sf = _ws_get_session_file(ws)
    if not account_id or not sf:
        return {"ok": False, "error": "Missing account_id or session_file. Upload a WS Session first."}
    result = await ws_api_call(sf, "POST", f"/backend-api/accounts/{account_id}/invites",
                               {"email_addresses": [email]})
    if result["ok"]:
        data = result["data"]
        errored = data.get("errored_emails", []) if isinstance(data, dict) else []
        if errored:
            return {"ok": False, "error": f"Email rejected by ChatGPT: {errored}"}
        return {"ok": True, "data": data}
    return result

async def ws_api_remove_member(ws: dict, member_id: str) -> dict:
    """Remove a member from the ChatGPT workspace via API.
    Endpoint: DELETE /backend-api/accounts/{account_id}/users/{user_id}
    member_id must be the 'id' field (e.g. 'user-xxx'), NOT account_user_id.
    """
    account_id = _ws_get_account_id(ws)
    sf = _ws_get_session_file(ws)
    if not account_id or not sf:
        return {"ok": False, "error": "Missing account_id or session_file."}
    result = await ws_api_call(sf, "DELETE", f"/backend-api/accounts/{account_id}/users/{member_id}")
    return result

# ── Workspace background worker ───────────────────────────────────────────

_ws_worker_busy = False
_ws_last_token_refresh = 0.0
_ws_last_expiry_check = 0.0
_ws_expiry_warned: set = set()  # set of (session_file, threshold) already warned

async def ws_invite_worker(bot):
    """Process pending workspace invite requests."""
    global _ws_worker_busy
    if _ws_worker_busy:
        return
    _ws_worker_busy = True
    try:
        pending = ws_get_pending_requests()
        for req in pending:
            ws = ws_get_workspace(req["workspace_id"])
            uid = req["telegram_user_id"]
            uname = req.get("telegram_username")
            uname_display = f"@{uname}" if uname else f"#{uid}"
            email = req["email"]
            paid = float(req.get("paid_amount") or 0.0)

            if not ws:
                ws_update_request(req["id"], status="failed", last_error="Workspace not found")
                continue
            if ws["status"] in ("disabled", "flushed"):
                ws_update_request(req["id"], status="failed", last_error=f"Workspace is {ws['status']}")
                continue

            attempts = req.get("attempts", 0) + 1
            ws_update_request(req["id"], status="processing", attempts=attempts)

            result = await ws_api_invite(ws, email)
            if result["ok"]:
                ws_update_request(req["id"], status="authorized",
                                  authorized_at=datetime.datetime.utcnow().isoformat())
                ws_mark_key_used(req.get("invite_code", ""), email)

                # Auto-protect: add authorized member as protected so audit doesn't kick them
                ws_add_protected_member(req["workspace_id"], email, role="member", reason="paid_seat")

                # Notify user
                try:
                    await bot.send_message(
                        uid,
                        f"✅ <b>You have been added to the Workspace!</b>\n\n"
                        f"📧 Email: <code>{email}</code>\n"
                        f"🏢 Workspace: <b>{ws['name']}</b>\n\n"
                        f"Open ChatGPT and check your invitation 📩",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass

                # Notify admin via log bot
                try:
                    await send_log_via_second_bot(
                        f"✅ <b>New member invited</b>\n\n"
                        f"📧 <code>{email}</code>\n"
                        f"🏢 {ws['name']}\n"
                        f"👤 {uname_display}"
                        + (f"\n💰 ${paid:.2f}" if paid > 0 else "")
                    )
                except Exception:
                    pass

            else:
                err_str = _ws_error_str(result.get("error", "Unknown error"))
                is_expired = result.get("expired") or result.get("error") == "session_expired"

                # ── Fast-fail: expired session — don't waste retries ──────
                if is_expired:
                    ws_update_request(req["id"], status="pending",
                                      last_error="session_expired — please upload a new Session")
                    try:
                        await send_log_via_second_bot(
                            f"⚠️ <b>Session expired — {ws['name']}</b>\n\n"
                            f"Upload a new Session with command:\n"
                            f"<code>/wssession {ws['id']}</code>\n\n"
                            f"📧 Pending request: <code>{email}</code>\n"
                            f"👤 {uname_display}"
                        )
                    except Exception:
                        pass
                    continue  # Try next request; this one waits for new session

                if attempts >= 3:
                    ws_update_request(req["id"], status="failed", last_error=err_str)

                    # ── Auto-refund if user paid with balance ──────────────
                    if paid > 0:
                        add_balance(uid, paid)
                        refund_note = f"\n💸 <b>Refunded ${paid:.2f} to your balance automatically.</b>"
                    else:
                        refund_note = ""

                    # Notify user
                    try:
                        await bot.send_message(
                            uid,
                            f"❌ <b>Failed to add you to the Workspace</b>\n\n"
                            f"📧 <code>{email}</code>\n"
                            f"⚠️ {err_str}\n\n"
                            f""
                            + refund_note,
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass

                    # Notify admin via log bot
                    try:
                        await send_log_via_second_bot(
                            f"❌ <b>Invite failed — {ws['name']}</b>\n\n"
                            f"📧 <code>{email}</code>\n"
                            f"👤 {uname_display}\n"
                            f"⚠️ {err_str}"
                            + (f"\n💸 Refund ${paid:.2f}" if paid > 0 else "")
                        )
                    except Exception:
                        pass
                else:
                    ws_update_request(req["id"], status="pending", last_error=err_str)
    except Exception as e:
        logger.error(f"[ws_worker] Error: {e}")
    finally:
        _ws_worker_busy = False

async def ws_worker_loop(bot):
    """Background worker: invites every 15s, auto-audit every 5 min,
    token refresh every 20 hours, expiry warning every 4 hours."""
    global _ws_last_token_refresh, _ws_last_expiry_check, _ws_expiry_warned
    audit_counter = 0
    AUDIT_EVERY = 20  # every 20 × 15s = 5 minutes
    while True:
        await asyncio.sleep(15)
        # ── Process pending invites ────────────────────────────────────
        try:
            await ws_invite_worker(bot)
        except Exception as e:
            logger.error(f"[ws_worker_loop] invite: {e}")

        # ── Auto-audit: kick unauthorized members ─────────────────────
        audit_counter += 1
        if audit_counter >= AUDIT_EVERY:
            audit_counter = 0
            try:
                for ws in ws_list_workspaces():
                    if ws["status"] not in ("active",):
                        continue
                    result = await ws_audit_kick_unauthorized(ws)
                    kicked = result.get("kicked", [])
                    if kicked:
                        logger.info(f"[ws_audit] {ws['name']}: kicked {kicked}")
                        try:
                            await send_log_via_second_bot(
                                f"🚫 <b>Auto-Audit — {ws['name']}</b>\n\n"
                                f"Removed {len(kicked)} unauthorized member(s) automatically:\n"
                                + "\n".join(f"• <code>{e}</code>" for e in kicked)
                            )
                        except Exception:
                            pass
            except Exception as e:
                logger.error(f"[ws_worker_loop] audit: {e}")

        # ── Proactive token refresh every 20 hours ────────────────────
        now = time.time()
        if now - _ws_last_token_refresh >= 20 * 3600:
            _ws_last_token_refresh = now
            try:
                refreshed_any = False
                for ws in ws_list_workspaces():
                    for sf_key in ("session_file", "personal_session_file"):
                        sf = ws.get(sf_key)
                        if not sf:
                            continue
                        ok = await ws_try_refresh_token(sf)
                        if ok:
                            logger.info(f"[WS_REFRESH] Proactive refresh OK: {ws['name']} {sf_key}")
                            refreshed_any = True
                if refreshed_any:
                    try:
                        await send_log_via_second_bot(
                            "🔄 <b>Workspace Session renewed automatically</b>\n"
                            "✅ Access Token ready for the next 24 hours."
                        )
                    except Exception:
                        pass
            except Exception as e:
                logger.error(f"[ws_worker_loop] token_refresh: {e}")

        # ── Expiry warning: check every 4 hours ───────────────────────
        now = time.time()
        if now - _ws_last_expiry_check >= 4 * 3600:
            _ws_last_expiry_check = now
            try:
                now_utc = datetime.datetime.utcnow()
                for ws in ws_list_workspaces():
                    for sf_key, label in [("session_file", "WS"), ("personal_session_file", "Personal")]:
                        sf = ws.get(sf_key)
                        if not sf:
                            continue
                        exp = ws_get_session_expiry(sf)
                        if exp is None:
                            continue
                        delta_secs = (exp - now_utc).total_seconds()
                        # Warn at 48h threshold (once), then again at 2h threshold (once)
                        for threshold_h, warn_key in [(48, "48h"), (2, "2h")]:
                            warn_id = (sf, warn_key)
                            if delta_secs <= threshold_h * 3600 and warn_id not in _ws_expiry_warned:
                                _ws_expiry_warned.add(warn_id)
                                remaining = ws_format_expiry_delta(exp)
                                urgency = "🚨" if threshold_h == 2 else "⚠️"
                                try:
                                    session = ws_load_session(sf)
                                    owner_email = (session or {}).get("user", {}).get("email", "—")
                                    await send_log_via_second_bot(
                                        f"{urgency} <b>Session expiring soon — {ws['name']}</b>\n\n"
                                        f"📋 Type: {label} Session\n"
                                        f"📧 Account: {owner_email}\n"
                                        f"⏳ Time remaining: <b>{remaining}</b>\n\n"
                                        f"🔄 Upload a new Session now via:\n"
                                        f"/ws → ⚙️ Settings → Upload Session"
                                    )
                                except Exception:
                                    pass
                # Clear old warnings for sessions that were renewed (expiry extended)
                _ws_expiry_warned = {
                    (sf, k) for sf, k in _ws_expiry_warned
                    if (ws_get_session_expiry(sf) or datetime.datetime.utcnow()) > datetime.datetime.utcnow()
                }
            except Exception as e:
                logger.error(f"[ws_worker_loop] expiry_check: {e}")

async def ws_audit_kick_unauthorized(ws: dict) -> dict:
    """Fetch live member list and remove anyone not protected or authorized.

    Safe list:
    - role == 'account-owner' (workspace owner, always safe)
    - email in chatgpt_protected_members (active)
    - normalized email has an authorized request in chatgpt_requests

    Returns {"kicked": [...], "kept": [...], "errors": [...]}
    """
    ws_id = ws["id"]
    kicked, kept, errors = [], [], []

    # Build safe email sets
    protected = {m["normalized_email"] for m in ws_list_protected_members(ws_id)}
    conn = db_connect()
    auth_rows = conn.execute(
        "SELECT DISTINCT normalized_email FROM chatgpt_requests WHERE workspace_id=? AND status='authorized'",
        (ws_id,)
    ).fetchall()
    conn.close()
    authorized = {r["normalized_email"] for r in auth_rows}

    # Fetch live members
    result = await ws_api_list_members(ws)
    if not result["ok"]:
        return {"kicked": [], "kept": [], "errors": [f"Failed to list members: {_ws_error_str(result.get('error'))}"]}

    for member in result.get("members", []):
        email = member.get("email", "")
        role = member.get("role", "")
        member_id = member.get("id", "")
        norm = ws_normalize_email(email)

        # Always keep account-owner
        if role == "account-owner":
            kept.append(email)
            continue

        # Keep if protected or authorized
        if norm in protected or norm in authorized:
            kept.append(email)
            continue

        # Remove unauthorized member
        rem = await ws_api_remove_member(ws, member_id)
        if rem["ok"]:
            kicked.append(email)
        else:
            errors.append(f"{email}: {_ws_error_str(rem.get('error'))}")

    return {"kicked": kicked, "kept": kept, "errors": errors}


# ── Admin workspace command handlers ─────────────────────────────────────

def _ws_home_panel():
    """Returns (text, InlineKeyboardMarkup) for the WS main panel."""
    workspaces = ws_list_workspaces()
    usage = ws_get_usage()
    lines = []
    for ws in workspaces:
        u = next((x for x in usage if x["id"] == ws["id"]), None)
        icon = {"active": "🟢", "disabled": "🔴", "flushed": "🗑️"}.get(ws["status"], "⚪")
        used = u["used"] if u else "?"
        max_i = ws["max_invites"] or "∞"
        lines.append(f"{icon} <b>{ws['name']}</b> — {used}/{max_i}")
    ws_text = "\n".join(lines) if lines else "No Workspaces yet.\n"
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        active_count = conn.execute(
            "SELECT COUNT(*) FROM chatgpt_subscriptions WHERE status='active'"
        ).fetchone()[0]
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Create new Workspace", callback_data="ws_create")],
        [InlineKeyboardButton("⚙️ Manage Workspace", callback_data="ws_settings"),
         InlineKeyboardButton("", callback_data="ws_status")],
        [InlineKeyboardButton("", callback_data="ws_genkey"),
         InlineKeyboardButton("", callback_data="ws_requests")],
        [InlineKeyboardButton(f"💳 Subscriptions ({active_count} active)", callback_data="ws_subs")],
        [InlineKeyboardButton("", callback_data="ws_protected"),
         InlineKeyboardButton("", callback_data="ws_addprotect_sel")],
        [InlineKeyboardButton("", callback_data="ws_kick_unauth")],
        [InlineKeyboardButton("", callback_data="adm_home")],
    ])
    txt = f"🤖 <b>Manage ChatGPT Workspaces</b>\n\n{ws_text}"
    return txt, kb

async def cmd_ws(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /ws — workspace management menu."""
    uid = update.effective_user.id
    if uid != OWNER_ID:
        return await update.message.reply_text("❌ Owner only.")
    txt, kb = _ws_home_panel()
    await update.message.reply_text(txt, parse_mode="HTML", reply_markup=kb)

def _ws_cfg_markup(ws_id: str) -> InlineKeyboardMarkup:
    """Inline keyboard for a single workspace config panel."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔐 Login (email+password)", callback_data=f"ws_dologin_{ws_id}")],
        [InlineKeyboardButton("📤 Upload WS Session", callback_data=f"ws_upload_{ws_id}"),
         InlineKeyboardButton("👤 Upload Personal Session", callback_data=f"ws_upload_personal_{ws_id}")],
        [InlineKeyboardButton("🔗 Edit URL", callback_data=f"ws_seturl_{ws_id}"),
         InlineKeyboardButton("", callback_data=f"ws_setmax_{ws_id}")],
        [InlineKeyboardButton("", callback_data=f"ws_genkey_{ws_id}")],
        [InlineKeyboardButton("", callback_data=f"ws_addprotect_{ws_id}"),
         InlineKeyboardButton("", callback_data=f"ws_showprotect_{ws_id}")],
        [InlineKeyboardButton("", callback_data=f"ws_audit_{ws_id}")],
        [InlineKeyboardButton("🗑️ Flush", callback_data=f"ws_flush_{ws_id}"),
         InlineKeyboardButton("", callback_data=f"ws_disable_{ws_id}"),
         InlineKeyboardButton("", callback_data=f"ws_enable_{ws_id}")],
        [InlineKeyboardButton("", callback_data="ws_settings")],
    ])

async def callback_ws_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all ws_ callback queries from the admin."""
    query = update.callback_query
    uid = query.from_user.id
    if uid != OWNER_ID:
        return await query.answer("Owner only.", show_alert=True)
    await query.answer()
    data = query.data

    # ── Home ──────────────────────────────────────────────────────────────
    if data == "ws_home":
        txt, kb = _ws_home_panel()
        try:
            return await query.message.edit_text(txt, parse_mode="HTML", reply_markup=kb)
        except Exception:
            return await query.message.reply_text(txt, parse_mode="HTML", reply_markup=kb)

    # ── Refresh all sessions (from /wsrefresh button) ─────────────────────
    if data == "ws_refresh_all":
        await query.message.edit_text("🔄 Renewing all Sessions...", parse_mode="HTML")
        workspaces = ws_list_workspaces()
        results = []
        for ws in workspaces:
            for sf_key, label in [("session_file", "WS 🏢"), ("personal_session_file", "Personal 👤")]:
                sf = ws.get(sf_key)
                if not sf:
                    continue
                session = ws_load_session(sf)
                if not session:
                    results.append(f"❌ <b>{ws['name']}</b> [{label}] — File not found")
                    continue
                has_refresh = bool(
                    session.get("sessionToken")
                    or (isinstance(session.get("cookies"), dict) and (
                        session["cookies"].get("__Secure-next-auth.session-token")
                        or session["cookies"].get("next-auth.session-token")
                    ))
                    or (isinstance(session.get("cookies"), list) and any(
                        c.get("name") in ("__Secure-next-auth.session-token", "next-auth.session-token")
                        for c in session.get("cookies", []) if isinstance(c, dict)
                    ))
                )
                if not has_refresh:
                    results.append(f"⚠️ <b>{ws['name']}</b> [{label}] — No sessionToken found")
                    continue
                ok = await ws_try_refresh_token(sf)
                if ok:
                    exp = ws_get_session_expiry(sf)
                    results.append(f"✅ <b>{ws['name']}</b> [{label}] — Renewed | Valid for {ws_format_expiry_delta(exp)}")
                else:
                    results.append(f"🔴 <b>{ws['name']}</b> [{label}] — Renewal failed — sessionToken expired")
        text = "📋 <b>Session Renewal Results</b>\n\n" + "\n".join(results) if results else "ℹ️ No Sessions found."
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Workspace Panel", callback_data="ws_home")]])
        try:
            return await query.message.edit_text(text, parse_mode="HTML", reply_markup=back_kb)
        except Exception:
            return await query.message.reply_text(text, parse_mode="HTML", reply_markup=back_kb)

    # ── Create ────────────────────────────────────────────────────────────
    if data == "ws_create":
        context.user_data["ws_flow"] = "create_name"
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data="ws_home")]])
        return await query.message.reply_text(
            "📝 Send the new <b>Workspace name</b>:",
            parse_mode="HTML", reply_markup=back_kb
        )

    # ── Generate floating invite key — show duration picker first ──────────
    if data == "ws_genkey":
        workspaces = ws_list_workspaces()
        if not workspaces:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data="ws_home")]])
            return await query.message.reply_text("❌ No Workspaces. Create one first.", reply_markup=kb)
        rows = []
        for hours, lbl_ar, lbl_en in WS_DURATION_OPTIONS:
            rows.append([InlineKeyboardButton(f"{lbl_ar} ({lbl_en})", callback_data=f"ws_keydur___{hours}")])
        rows.append([InlineKeyboardButton("", callback_data="ws_home")])
        return await query.message.reply_text(
            "🔑 <b>Choose subscription duration for the key:</b>\n\n"
            "",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows)
        )

    # ── Generate Key tied to a specific workspace — show duration picker ───
    if data.startswith("ws_genkey_") and not data.startswith("ws_keydur_"):
        ws_id = data[len("ws_genkey_"):]
        ws = ws_get_workspace(ws_id)
        if not ws:
            return await query.message.reply_text("❌ Workspace not found.")
        rows = []
        for hours, lbl_ar, lbl_en in WS_DURATION_OPTIONS:
            rows.append([InlineKeyboardButton(f"{lbl_ar} ({lbl_en})", callback_data=f"ws_keydur_{ws_id}_{hours}")])
        rows.append([InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")])
        return await query.message.reply_text(
            f"🔑 <b>Choose subscription duration for the key</b>\n🏢 {ws['name']}:",
            parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows)
        )

    # ── Generate key after duration selected ──────────────────────────────
    if data.startswith("ws_keydur_"):
        rest = data[len("ws_keydur_"):]
        # rest = "{ws_id}_{hours}" or "__{hours}" (floating)
        try:
            parts = rest.rsplit("_", 1)
            sub_hours = int(parts[1])
            ws_id_raw = parts[0]
            is_floating = ws_id_raw in ("", "_")
        except Exception:
            return await query.answer("❌ Error", show_alert=True)
        if is_floating:
            key = ws_create_key("", uid, subscription_hours=sub_hours)
            expiry = key["expires_at"][:16]
            dur_label = next((f"{la} ({le})" for h, la, le in WS_DURATION_OPTIONS if h == sub_hours), f"{sub_hours}h")
            back_kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("", callback_data="ws_genkey"),
                 InlineKeyboardButton("", callback_data="ws_home")]
            ])
            return await query.message.reply_text(
                f"✅ <b>New Invitation Key</b>\n\n"
                f"🔑 Code: <code>{key['code']}</code>\n"
                f"🏢 Workspace: <b>First available when used</b>\n"
                f"🕒 Subscription duration: <b>{dur_label}</b>\n"
                f"⏰ Key valid until: {expiry}",
                parse_mode="HTML", reply_markup=back_kb
            )
        else:
            ws = ws_get_workspace(ws_id_raw)
            if not ws:
                return await query.message.reply_text("❌ Workspace not found.")
            key = ws_create_key(ws_id_raw, uid, subscription_hours=sub_hours)
            expiry = key["expires_at"][:16]
            dur_label = next((f"{la} ({le})" for h, la, le in WS_DURATION_OPTIONS if h == sub_hours), f"{sub_hours}h")
            back_kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("", callback_data=f"ws_genkey_{ws_id_raw}"),
                 InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id_raw}")]
            ])
            return await query.message.reply_text(
                f"✅ <b>New Invitation Key</b>\n\n"
                f"🔑 Code: <code>{key['code']}</code>\n"
                f"🏢 Workspace: <b>{ws['name']}</b>\n"
                f"🕒 Subscription duration: <b>{dur_label}</b>\n"
                f"⏰ Key valid until: {expiry}",
                parse_mode="HTML", reply_markup=back_kb
            )

    # ── Requests ──────────────────────────────────────────────────────────
    if data == "ws_requests":
        reqs = ws_get_all_requests(limit=20)
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data="ws_home")]])
        if not reqs:
            return await query.message.reply_text("", reply_markup=back_kb)
        s_icon = {"pending": "⏳", "processing": "🔄", "authorized": "✅", "failed": "❌", "rejected": "🚫", "invited": "📨"}
        lines = []
        for r in reqs:
            wsr = ws_get_workspace(r["workspace_id"])
            ws_name = wsr["name"] if wsr else "?"
            icon = s_icon.get(r["status"], "❓")
            uname = f"@{r['telegram_username']}" if r.get("telegram_username") else str(r["telegram_user_id"])
            lines.append(f"{icon} <code>{r['email']}</code>\n   🏢 {ws_name} | {uname}")
        return await query.message.reply_text(
            f"📋 <b>Last 20 requests</b>\n\n" + "\n\n".join(lines),
            parse_mode="HTML", reply_markup=back_kb
        )

    # ── Status ────────────────────────────────────────────────────────────
    if data == "ws_status":
        usage = ws_get_usage()
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data="ws_home")]])
        if not usage:
            return await query.message.reply_text("📭 No Workspaces.", reply_markup=back_kb)
        lines = []
        for ws in usage:
            icon = {"active": "🟢", "disabled": "🔴", "flushed": "🗑️"}.get(ws["status"], "⚪")
            lines.append(
                f"{icon} <b>{ws['name']}</b>\n"
                f"   👥 {ws['used']}/{ws['max_invites']} | Available: {ws['available']}\n"
                f"   🆔 <code>{ws['id']}</code>"
            )
        return await query.message.reply_text(
            f"📊 <b>Workspaces Status</b>\n\n" + "\n\n".join(lines),
            parse_mode="HTML", reply_markup=back_kb
        )

    # ── Subscriptions list ────────────────────────────────────────────────
    if data == "ws_subs" or data.startswith("ws_subs_ws_"):
        ws_filter = None
        if data.startswith("ws_subs_ws_"):
            ws_filter = data[len("ws_subs_ws_"):]
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            if ws_filter:
                rows = conn.execute(
                    "SELECT * FROM chatgpt_subscriptions WHERE workspace_id=? ORDER BY expires_at ASC LIMIT 20",
                    (ws_filter,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM chatgpt_subscriptions ORDER BY status='active' DESC, expires_at ASC LIMIT 20"
                ).fetchall()
        workspaces = ws_list_workspaces()
        # Build header with workspace filter buttons
        ws_rows = [[InlineKeyboardButton(f"🏢 {ws['name']}", callback_data=f"ws_subs_ws_{ws['id']}")]
                   for ws in workspaces]
        if not rows:
            kb = InlineKeyboardMarkup(ws_rows + [[InlineKeyboardButton("", callback_data="ws_home")]])
            return await query.message.reply_text(
                "",
                reply_markup=kb
            )
        status_icon = {"active": "🟢", "expired": "🔴", "migrated": "🔄", "revoked": "🚫"}
        lines = []
        btn_rows = []
        for r in rows:
            ws_obj = ws_get_workspace(r["workspace_id"])
            ws_name = ws_obj["name"] if ws_obj else "?"
            icon = status_icon.get(r["status"], "⚪")
            sub_h = int(r["subscription_hours"] or 720)
            dur_label = next((le for h, la, le in WS_DURATION_OPTIONS if h == sub_h), f"{sub_h}h")
            exp_str = str(r["expires_at"])[:16]
            lines.append(
                f"{icon} <code>{r['email']}</code>\n"
                f"   🏢 {ws_name} | 🕒 {dur_label}\n"
                f"   📅 Expires: <b>{exp_str}</b>"
            )
            if r["status"] == "active":
                btn_rows.append([InlineKeyboardButton(
                    f"⛔ Terminate {r['email'][:22]}", callback_data=f"ws_sub_expire_{r['id']}"
                )])
        kb = InlineKeyboardMarkup(
            ws_rows +
            [[InlineKeyboardButton("", callback_data="ws_subs")]] +
            btn_rows +
            [[InlineKeyboardButton("", callback_data="ws_home")]]
        )
        title = f"💳 <b>Subscriptions</b>" + (f" — {ws_filter}" if ws_filter else "")
        return await query.message.reply_text(
            f"{title}\n\n" + "\n\n".join(lines),
            parse_mode="HTML", reply_markup=kb
        )

    # ── Expire individual subscription ───────────────────────────────────
    if data.startswith("ws_sub_expire_"):
        sub_id = data[len("ws_sub_expire_"):]
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            sub = conn.execute("SELECT * FROM chatgpt_subscriptions WHERE id=?", (sub_id,)).fetchone()
        if not sub or sub["status"] != "active":
            return await query.answer("", show_alert=True)
        ws = ws_get_workspace(sub["workspace_id"])
        if ws:
            await ws_api_remove_member(ws, sub["email"])
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "UPDATE chatgpt_subscriptions SET status='revoked' WHERE id=?", (sub_id,)
            )
        # Notify user
        try:
            await context.bot.send_message(
                chat_id=sub["user_id"],
                text=f"⛔ <b>Your subscription has been cancelled</b>\n\n"
                     f"📧 {sub['email']}\n"
                     f"🏢 {ws['name'] if ws else '?'}\n\n"
                     f"",
                parse_mode="HTML"
            )
        except Exception:
            pass
        await query.answer("", show_alert=True)
        txt, kb = _ws_home_panel()
        return await query.message.edit_text(txt, parse_mode="HTML", reply_markup=kb)

    # ── Audit & Kick unauthorized members ────────────────────────────────
    if data == "ws_kick_unauth":
        workspaces = ws_list_workspaces()
        if not workspaces:
            return await query.answer("❌ No Workspaces.", show_alert=True)
        # Build workspace selection keyboard
        rows = [[InlineKeyboardButton(f"🔍 {ws['name']}", callback_data=f"ws_kick_ws_{ws['id']}")]
                for ws in workspaces]
        rows.append([InlineKeyboardButton("", callback_data="ws_home")])
        return await query.message.reply_text(
            "🚫 <b>Remove Unauthorized Members</b>\n\nChoose Workspace to audit:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(rows)
        )

    if data.startswith("ws_kick_ws_"):
        ws_id = data[len("ws_kick_ws_"):]
        ws = ws_get_workspace(ws_id)
        if not ws:
            return await query.answer("❌ Workspace not found.", show_alert=True)
        await query.message.reply_text(
            f"⏳ Auditing members of <b>{ws['name']}</b> and removing unauthorized members...",
            parse_mode="HTML"
        )
        result = await ws_audit_kick_unauthorized(ws)
        kicked = result["kicked"]
        kept = result["kept"]
        errors = result["errors"]
        lines = []
        if kicked:
            lines.append(f"🚫 <b>Removed ({len(kicked)}):</b>")
            for e in kicked:
                lines.append(f"  • <code>{e}</code>")
        if kept:
            lines.append(f"\n✅ <b>Kept ({len(kept)}):</b>")
            for e in kept:
                lines.append(f"  • <code>{e}</code>")
        if errors:
            lines.append(f"\n⚠️ <b>Errors ({len(errors)}):</b>")
            for e in errors:
                lines.append(f"  • {html.escape(str(e)[:200])}")
        if not kicked and not errors:
            lines.append("")
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data="ws_home")]])
        # Also notify via log bot
        if kicked:
            try:
                await send_log_via_second_bot(
                    f"🚫 <b>Audit — {ws['name']}</b>\n\n"
                    f"Removed unauthorized:\n"
                    + "\n".join(f"• <code>{e}</code>" for e in kicked)
                )
            except Exception:
                pass
        return await query.message.reply_text(
            f"🚫 <b>Audit results for {ws['name']}</b>\n\n" + "\n".join(lines),
            parse_mode="HTML",
            reply_markup=back_kb
        )

    if data == "ws_protected":
        members = ws_list_protected_members()
        kb_rows = [
            [InlineKeyboardButton("", callback_data="ws_addprotect_sel"),
             InlineKeyboardButton("", callback_data="ws_home")]
        ]
        back_kb = InlineKeyboardMarkup(kb_rows)
        if not members:
            return await query.message.reply_text("", reply_markup=back_kb)
        lines = [f"🛡 <code>{m['email']}</code>\n   🏢 <code>{m['workspace_id']}</code> | {m['role']}" for m in members]
        return await query.message.reply_text(
            f"🛡 <b>Protected Members ({len(members)})</b>\n\n" + "\n\n".join(lines),
            parse_mode="HTML", reply_markup=back_kb
        )

    # ── Protected per-WS ──────────────────────────────────────────────────
    if data.startswith("ws_showprotect_"):
        ws_id = data[len("ws_showprotect_"):]
        ws = ws_get_workspace(ws_id)
        members = ws_list_protected_members(ws_id)
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]])
        ws_name = ws["name"] if ws else ws_id
        if not members:
            return await query.message.reply_text(f"🛡 No protected members in <b>{ws_name}</b>.", parse_mode="HTML", reply_markup=back_kb)
        lines = [f"🛡 <code>{m['email']}</code> | {m['role']}" for m in members]
        return await query.message.reply_text(
            f"🛡 <b>Protected in {ws_name}</b>\n\n" + "\n".join(lines),
            parse_mode="HTML", reply_markup=back_kb
        )

    # ── Add Protected (select WS) ─────────────────────────────────────────
    if data == "ws_addprotect_sel":
        workspaces = ws_list_workspaces()
        if not workspaces:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data="ws_home")]])
            return await query.message.reply_text("❌ No Workspaces.", reply_markup=kb)
        rows = [[InlineKeyboardButton(f"🏢 {ws['name']}", callback_data=f"ws_addprotect_{ws['id']}")] for ws in workspaces]
        rows.append([InlineKeyboardButton("", callback_data="ws_home")])
        return await query.message.reply_text("🛡 Choose Workspace to add a protected member:", reply_markup=InlineKeyboardMarkup(rows))

    # ── Add Protected (enter email) ───────────────────────────────────────
    if data.startswith("ws_addprotect_") and not data.startswith("ws_addprotect_sel"):
        ws_id = data[len("ws_addprotect_"):]
        ws = ws_get_workspace(ws_id)
        if not ws:
            return await query.message.reply_text("❌ Workspace not found.")
        context.user_data["ws_flow"] = "add_protect"
        context.user_data["ws_target_id"] = ws_id
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]])
        return await query.message.reply_text(
            f"🛡 Send the <b>email</b> of the member to protect in <b>{ws['name']}</b>:",
            parse_mode="HTML", reply_markup=back_kb
        )

    # ── Settings (list of workspaces to pick) ────────────────────────────
    if data == "ws_settings":
        workspaces = ws_list_workspaces()
        if not workspaces:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("➕ Create Workspace", callback_data="ws_create"), InlineKeyboardButton("", callback_data="ws_home")]])
            return await query.message.reply_text("❌ No Workspaces.", reply_markup=kb)
        rows = [[InlineKeyboardButton(
            f"{'🟢' if ws['status']=='active' else '🔴' if ws['status']=='disabled' else '🗑️'} {ws['name']}",
            callback_data=f"ws_cfg_{ws['id']}"
        )] for ws in workspaces]
        rows.append([InlineKeyboardButton("", callback_data="ws_home")])
        return await query.message.reply_text("⚙️ Choose Workspace to manage:", reply_markup=InlineKeyboardMarkup(rows))

    # ── Single WS Config panel ────────────────────────────────────────────
    if data.startswith("ws_cfg_"):
        ws_id = data[len("ws_cfg_"):]
        # Cancel any active admin flow when returning to config panel
        for _fk in ("ws_flow", "ws_login_email", "ws_login_password", "ws_login_mfa_state",
                    "ws_login_otp_type", "ws_login_pending_id", "ws_login_totp_secret",
                    "ws_target_id", "ws_new_name", "ws_session_type"):
            context.user_data.pop(_fk, None)
        ws = ws_get_workspace(ws_id)
        if not ws:
            return await query.message.reply_text("❌ Workspace not found.")
        usage = ws_get_usage()
        u = next((x for x in usage if x["id"] == ws_id), None)
        used = u["used"] if u else "?"
        avail = u["available"] if u else "?"
        icon = {"active": "🟢", "disabled": "🔴", "flushed": "🗑️"}.get(ws["status"], "⚪")
        ws_sf = ws.get("session_file")
        personal_sf = ws.get("personal_session_file")
        ws_sf_status = "" if ws_sf and os.path.exists(ws_sf) else ""
        personal_sf_status = "" if personal_sf and os.path.exists(personal_sf) else ""
        acc_id = ws.get("account_id") or "❌ not set (Upload WS Session first)"
        info = (
            f"⚙️ <b>{ws['name']}</b>  {icon}\n\n"
            f"🆔 WS ID: <code>{ws['id']}</code>\n"
            f"🏛 Account ID: <code>{acc_id}</code>\n"
            f"🔗 URL: {ws.get('url') or ''}\n"
            f"📤 WS Session: {ws_sf_status}\n"
            f"👤 Personal Session: {personal_sf_status}\n"
            f"👥 Members: {used} / {ws['max_invites']} | Available: {avail}\n"
            f"📊 Status: {ws['status']}"
        )
        return await query.message.reply_text(info, parse_mode="HTML", reply_markup=_ws_cfg_markup(ws_id))

    # ── Upload WS Session ─────────────────────────────────────────────────
    if data.startswith("ws_upload_") and not data.startswith("ws_upload_personal_"):
        ws_id = data[len("ws_upload_"):]
        context.user_data["ws_flow"] = "upload_session"
        context.user_data["ws_session_type"] = "ws"
        context.user_data["ws_target_id"] = ws_id
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]])
        return await query.message.reply_text(
            "📤 <b>Upload WS Session</b>\n\n"
            "Upload the session file after switching to the Workspace account.\n"
            "Must contain <code>accessToken</code> and <code>account.organizationId</code> and <code>account.structure = workspace</code>.",
            parse_mode="HTML", reply_markup=back_kb
        )

    # ── Upload Personal Session ───────────────────────────────────────────
    if data.startswith("ws_upload_personal_"):
        ws_id = data[len("ws_upload_personal_"):]
        context.user_data["ws_flow"] = "upload_session"
        context.user_data["ws_session_type"] = "personal"
        context.user_data["ws_target_id"] = ws_id
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]])
        return await query.message.reply_text(
            "👤 <b>Upload Personal Session</b>\n\n"
            "Upload the personal account session file (before joining the Workspace).\n"
            "Contains <code>accessToken</code> and <code>account.structure = personal</code>.",
            parse_mode="HTML", reply_markup=back_kb
        )

    # ── Login with email/password ─────────────────────────────────────────
    if data.startswith("ws_dologin_"):
        ws_id = data[len("ws_dologin_"):]
        ws = ws_get_workspace(ws_id)
        if not ws:
            return await query.message.reply_text("❌ Workspace not found.")
        context.user_data["ws_flow"] = "login_email"
        context.user_data["ws_target_id"] = ws_id
        context.user_data.pop("ws_login_email", None)
        context.user_data.pop("ws_login_mfa_state", None)
        context.user_data.pop("ws_login_otp_type", None)
        context.user_data.pop("ws_login_pending_id", None)
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]])
        return await query.message.reply_text(
            f"🔐 <b>Login to ChatGPT</b>\n\n"
            f"Workspace: <b>{html.escape(ws['name'])}</b>\n\n"
            f"Send the <b>email</b> linked to the account:",
            parse_mode="HTML", reply_markup=back_kb
        )

    # ── Set URL ───────────────────────────────────────────────────────────
    if data.startswith("ws_seturl_"):
        ws_id = data[len("ws_seturl_"):]
        context.user_data["ws_flow"] = "set_url"
        context.user_data["ws_target_id"] = ws_id
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]])
        return await query.message.reply_text(
            "🔗 Send Workspace URL:\n<code>https://chatgpt.com/admin/members?ws=xxx</code>",
            parse_mode="HTML", reply_markup=back_kb
        )

    # ── Set Max Members ───────────────────────────────────────────────────
    if data.startswith("ws_setmax_"):
        ws_id = data[len("ws_setmax_"):]
        context.user_data["ws_flow"] = "set_max"
        context.user_data["ws_target_id"] = ws_id
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]])
        return await query.message.reply_text("", reply_markup=back_kb)

    # ── Audit Members (fetch + auto-kick unauthorized) ─────────────────────
    if data.startswith("ws_audit_"):
        ws_id = data[len("ws_audit_"):]
        ws = ws_get_workspace(ws_id)
        if not ws:
            return await query.message.reply_text("❌ Workspace not found.")
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]])
        await query.message.reply_text(
            f"🔍 Auditing <b>{ws['name']}</b> and removing unauthorized members...",
            parse_mode="HTML"
        )
        result = await ws_audit_kick_unauthorized(ws)
        kicked = result["kicked"]
        kept = result["kept"]
        errors = result["errors"]

        # Build report
        lines = []
        if kept:
            lines.append(f"✅ <b>Authorized ({len(kept)}):</b>")
            for e in kept:
                lines.append(f"  • <code>{e}</code>")
        if kicked:
            lines.append(f"\n🚫 <b>Removed ({len(kicked)}):</b>")
            for e in kicked:
                lines.append(f"  • <code>{e}</code>")
        if errors:
            lines.append(f"\n⚠️ <b>Errors ({len(errors)}):</b>")
            for e in errors:
                lines.append(f"  • {html.escape(str(e)[:200])}")
        if not kicked and not errors:
            lines.append("")

        # Notify admin via log bot if anyone was kicked
        if kicked:
            try:
                await send_log_via_second_bot(
                    f"🚫 <b>Manual audit — {ws['name']}</b>\n\n"
                    f"Removed unauthorized:\n"
                    + "\n".join(f"• <code>{e}</code>" for e in kicked)
                )
            except Exception:
                pass

        return await query.message.reply_text(
            f"🔍 <b>Audit results for {ws['name']}</b>\n\n" + "\n".join(lines),
            parse_mode="HTML", reply_markup=back_kb
        )

    # ── Flush (confirm prompt) ────────────────────────────────────────────
    if data.startswith("ws_flush_") and not data.startswith("ws_flush_confirm_"):
        ws_id = data[len("ws_flush_"):]
        ws = ws_get_workspace(ws_id)
        if not ws:
            return await query.message.reply_text("❌ Workspace not found.")
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("", callback_data=f"ws_flush_confirm_{ws_id}"),
             InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]
        ])
        return await query.message.reply_text(
            f"⚠️ <b>Confirm Flush {ws['name']}</b>\n\n"
            f"",
            parse_mode="HTML", reply_markup=kb
        )

    # ── Flush (execute) ───────────────────────────────────────────────────
    if data.startswith("ws_flush_confirm_"):
        ws_id = data[len("ws_flush_confirm_"):]
        ws = ws_get_workspace(ws_id)
        if not ws:
            return await query.message.reply_text("❌ Workspace not found.")
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]])
        await query.message.reply_text(f"🗑️ Flushing <b>{ws['name']}</b>...", parse_mode="HTML")
        result = await ws_api_list_members(ws)
        if not result["ok"]:
            return await query.message.reply_text(
                f"❌ Failed to fetch members: {_ws_error_str(result.get('error'))}",
                reply_markup=back_kb
            )
        members = result["members"]
        protected_emails = {m["normalized_email"] for m in ws_list_protected_members(ws_id)}
        removed = failed = 0
        for member in members:
            if ws_normalize_email(member.get("email", "")) in protected_emails:
                continue
            member_id = member.get("id")
            if member_id:
                r = await ws_api_remove_member(ws, member_id)
                if r["ok"]:
                    removed += 1
                else:
                    failed += 1
        ws_update_workspace(ws_id, status="flushed")
        await query.message.reply_text(
            f"✅ <b>Flush complete</b>\n\n"
            f"🗑️ Removed:\n"
            f"❌ Failed to remove:\n"
            f"",
            parse_mode="HTML", reply_markup=back_kb
        )
        await ws_migrate_workspace_subscribers(ws_id, bot=context.bot)
        return

    # ── Disable / Enable ──────────────────────────────────────────────────
    if data.startswith("ws_disable_"):
        ws_id = data[len("ws_disable_"):]
        ws_update_workspace(ws_id, status="disabled")
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]])
        await query.message.reply_text("🔒 Workspace disabled. Transferring subscribers...", reply_markup=back_kb)
        await ws_migrate_workspace_subscribers(ws_id, bot=context.bot)
        return

    if data.startswith("ws_enable_"):
        ws_id = data[len("ws_enable_"):]
        ws_update_workspace(ws_id, status="active")
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]])
        return await query.message.reply_text("✅ Workspace enabled.", reply_markup=back_kb)

async def handle_ws_admin_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle session file upload for workspace configuration.
    Supports both personal and workspace sessions.
    Auto-detects type from account.structure field.
    """
    uid = update.effective_user.id
    if uid != OWNER_ID:
        return
    if context.user_data.get("ws_flow") != "upload_session":
        return
    ws_id = context.user_data.get("ws_target_id")
    if not ws_id:
        return
    intended_type = context.user_data.get("ws_session_type", "ws")  # "ws" or "personal"

    doc = update.message.document
    if not doc or not doc.file_name.endswith(".json"):
        return await update.message.reply_text("❌ Please send a valid JSON file.")
    try:
        file = await context.bot.get_file(doc.file_id)
        content_bytes = await file.download_as_bytearray()
        content_str = content_bytes.decode("utf-8")
        parsed = _json.loads(content_str)
    except Exception as e:
        return await update.message.reply_text(f"")

    access_token = parsed.get("accessToken")
    account = parsed.get("account", {})
    org_id = account.get("organizationId")
    owner_email = parsed.get("user", {}).get("email")
    plan_type = account.get("planType", "unknown")
    structure = account.get("structure", "unknown")  # "personal" or "workspace"

    if not access_token:
        return await update.message.reply_text(
            "❌ File does not contain <code>accessToken</code>.\n"
            "Make sure you are sending the correct session file from ChatGPT.",
            parse_mode="HTML"
        )

    # Auto-detect actual session type
    actual_type = "ws" if structure == "workspace" else "personal"

    # Warn if type mismatch
    warning = ""
    if intended_type == "ws" and actual_type == "personal":
        warning = ("\n\n⚠️ <b>Warning:</b> This file appears to be a Personal Session (individual account), not a Workspace Session.\n"
                   "It is recommended to upload the WS Session (after switching to Workspace) via <b>Upload WS Session</b> button.\n"
                   "Saved as Personal Session.")
        actual_type = "personal"
    elif intended_type == "personal" and actual_type == "ws":
        warning = ("\n\n⚠️ <b>Warning:</b> This file appears to be a WS Session (contains organizationId).\n"
                   "Saved as WS Session automatically.")
        actual_type = "ws"

    # Save to correct file
    session_path = ws_save_session_file(ws_id, content_str, actual_type)

    # Build DB updates
    account_uuid = account.get("id") or ""  # The UUID used for API calls

    db_updates = {}
    if actual_type == "ws":
        db_updates["session_file"] = session_path
        if org_id:
            db_updates["organization_id"] = org_id
        if account_uuid:
            db_updates["account_id"] = account_uuid  # Store UUID for API calls
    else:
        db_updates["personal_session_file"] = session_path

    ws_update_workspace(ws_id, **db_updates)
    context.user_data.pop("ws_flow", None)
    context.user_data.pop("ws_target_id", None)
    context.user_data.pop("ws_session_type", None)

    ws = ws_get_workspace(ws_id)
    ws_name = ws["name"] if ws else ws_id

    type_label = "WS Session (Workspace)" if actual_type == "ws" else "Personal Session"
    type_icon = "📤" if actual_type == "ws" else "👤"

    # Show token expiry info based on JWT exp
    token_exp = ws_decode_token_exp(access_token)
    if token_exp:
        now_utc = datetime.datetime.utcnow()
        delta_secs = (token_exp - now_utc).total_seconds()
        if delta_secs > 0:
            expiry_line = f"\n⏳ Token valid for: <b>{ws_format_expiry_delta(token_exp)}</b>"
        else:
            expiry_line = "\n🔴 Token expired! Please upload a fresh Session."
    else:
        expiry_line = ""

    # Clear expiry warned state for this session so it can re-alert when needed
    global _ws_expiry_warned
    _ws_expiry_warned = {(sf, k) for sf, k in _ws_expiry_warned if sf != session_path}

    back_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("⚙️ Workspace Settings", callback_data=f"ws_cfg_{ws_id}")
    ]])

    return await update.message.reply_text(
        f"✅ <b>Session uploaded successfully</b>\n\n"
        f"🏢 Workspace: <b>{ws_name}</b>\n"
        f"{type_icon} Type: <b>{type_label}</b>\n"
        f"👤 Account: <code>{owner_email or ''}</code>\n"
        f"📋 Plan: {plan_type} | Structure: {structure}\n"
        f"🏛 Account ID: <code>{account_uuid or ''}</code>\n"
        f"🔑 Org ID: <code>{org_id or ''}</code>"
        f"{expiry_line}{warning}",
        parse_mode="HTML",
        reply_markup=back_kb
    )

async def _ws_save_login_session(ws_id: str, session_data: dict, wait_msg, update, context):
    """Save a session obtained from ws_chatgpt_login() into file and DB, then show result."""
    ws = ws_get_workspace(ws_id)
    ws_name = ws["name"] if ws else ws_id
    back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("⚙️ Workspace Settings", callback_data=f"ws_cfg_{ws_id}")]])

    # Determine session type from account structure
    structure = (session_data.get("account") or {}).get("structure", "") or session_data.get("structure", "")
    session_type = "ws" if structure == "workspace" else "personal"

    # Build compact session file compatible with ws_save_session_file format
    acc = session_data.get("account") or {}
    account_uuid = acc.get("id") or session_data.get("accountId") or ""
    org_id = acc.get("organizationId") or acc.get("orgId") or session_data.get("organizationId") or ""
    owner_email = session_data.get("user", {}).get("email", "") if session_data.get("user") else ""
    plan_type = (session_data.get("user", {}).get("name") or "").lower()
    if "pro" in plan_type:
        plan_type = "pro"
    elif "team" in plan_type or structure == "workspace":
        plan_type = "team"
    else:
        plan_type = "free"

    access_token = session_data.get("accessToken", "")
    session_token = session_data.get("sessionToken", "")

    # Build JSON matching the expected session file format
    session_json = {
        "accessToken": access_token,
        "sessionToken": session_token,
        "account": {
            "id": account_uuid,
            "organizationId": org_id,
            "structure": structure,
        },
        "user": session_data.get("user") or {},
    }

    # Save to file via the standard helper
    try:
        import json as _json
        session_path = ws_save_session_file(ws_id, _json.dumps(session_json, indent=2), session_type)
    except Exception as e:
        try:
            await wait_msg.edit_text(f"❌ Failed to save session file:\n{e}", reply_markup=back_kb)
        except Exception:
            await update.message.reply_text(f"❌ Failed to save session file:\n{e}", reply_markup=back_kb)
        return

    # Update DB using the standard update function
    try:
        db_updates = {}
        if session_type == "ws":
            db_updates["session_file"] = session_path
            if org_id:
                db_updates["organization_id"] = org_id
            if account_uuid:
                db_updates["account_id"] = account_uuid
        else:
            db_updates["personal_session_file"] = session_path
        if db_updates:
            ws_update_workspace(ws_id, **db_updates)
    except Exception:
        pass  # DB error is non-fatal; file was saved

    # Clear temp login data
    for k in ("ws_login_email", "ws_login_password", "ws_login_mfa_state",
               "ws_login_otp_type", "ws_login_pending_id", "ws_login_totp_secret"):
        context.user_data.pop(k, None)

    # Report success
    type_label = "WS (Workspace)" if session_type == "ws" else "Personal"
    type_icon = "🏢" if session_type == "ws" else "👤"
    result_text = (
        f"✅ <b>Login and session saved successfully</b>\n\n"
        f"🏢 Workspace: <b>{html.escape(ws_name)}</b>\n"
        f"{type_icon} Type: <b>{type_label}</b>\n"
        f"📧 Account: <code>{html.escape(owner_email or '')}</code>\n"
        f"📋 Plan: {plan_type}\n"
        f"🏛 Account ID: <code>{account_uuid or ''}</code>\n"
        f"🔑 Org ID: <code>{org_id or ''}</code>"
    )
    try:
        await wait_msg.edit_text(result_text, parse_mode="HTML", reply_markup=back_kb)
    except Exception:
        await update.message.reply_text(result_text, parse_mode="HTML", reply_markup=back_kb)


async def handle_ws_admin_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Handle workspace admin text flows. Returns True if handled.
    This is called from the main text_handler for admin bot.
    """
    uid = update.effective_user.id
    if uid != OWNER_ID:
        return False
    flow = context.user_data.get("ws_flow")
    if not flow:
        return False
    text = (update.message.text or "").strip()

    if flow == "create_name":
        context.user_data["ws_new_name"] = text
        context.user_data["ws_flow"] = "create_max"
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data="ws_home")]])
        await update.message.reply_text(
            f"👥 Enter the <b>max members</b> for this Workspace:\nExample: <code>50</code>",
            parse_mode="HTML", reply_markup=back_kb
        )
        return True

    if flow == "create_max":
        try:
            max_inv = int(text)
        except ValueError:
            await update.message.reply_text("")
            return True
        name = context.user_data.pop("ws_new_name", "Workspace")
        context.user_data.pop("ws_flow", None)
        ws = ws_create_workspace(name, max_invites=max_inv)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("📤 Upload Session file", callback_data=f"ws_upload_{ws['id']}")],
            [InlineKeyboardButton("🔙 Workspaces List", callback_data="ws_home")],
        ])
        await update.message.reply_text(
            f"✅ <b>Workspace created</b>\n\n"
            f"🏢 Name: <b>{ws['name']}</b>\n"
            f"🆔 ID: <code>{ws['id']}</code>\n"
            f"👥 Max members: {ws['max_invites']}\n\n"
            f"⬇️ Upload the Session file now to get started:",
            parse_mode="HTML", reply_markup=kb
        )
        return True

    if flow == "set_url":
        ws_id = context.user_data.pop("ws_target_id", None)
        context.user_data.pop("ws_flow", None)
        if ws_id:
            ws_update_workspace(ws_id, url=text)
            back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]])
            await update.message.reply_text("", reply_markup=back_kb)
        return True

    if flow == "set_max":
        ws_id = context.user_data.pop("ws_target_id", None)
        context.user_data.pop("ws_flow", None)
        try:
            max_inv = int(text)
            if ws_id:
                ws_update_workspace(ws_id, max_invites=max_inv)
                back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]])
                await update.message.reply_text(f"✅ Max members updated to <b>{max_inv}</b>.", parse_mode="HTML", reply_markup=back_kb)
        except ValueError:
            await update.message.reply_text("")
        return True

    if flow == "add_protect":
        ws_id = context.user_data.pop("ws_target_id", None)
        context.user_data.pop("ws_flow", None)
        email = text.strip().lower()
        if "@" not in email or "." not in email:
            await update.message.reply_text("")
            context.user_data["ws_flow"] = "add_protect"
            context.user_data["ws_target_id"] = ws_id
            return True
        if ws_id:
            conn = db_connect()
            conn.execute(
                "INSERT OR IGNORE INTO chatgpt_protected_members (workspace_id, email, normalized_email, role) VALUES (?,?,?,?)",
                (ws_id, email, ws_normalize_email(email), "member")
            )
            conn.commit()
            conn.close()
            ws = ws_get_workspace(ws_id)
            ws_name = ws["name"] if ws else ws_id
            back_kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("", callback_data=f"ws_addprotect_{ws_id}"),
                 InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]
            ])
            await update.message.reply_text(
                f"🛡 <b>Added</b>\n\n"
                f"📧 <code>{email}</code>\n"
                f"🏢 <b>{ws_name}</b>",
                parse_mode="HTML", reply_markup=back_kb
            )
        return True

    # ── Login flow: email ─────────────────────────────────────────────────
    if flow == "login_email":
        ws_id = context.user_data.get("ws_target_id", "")
        context.user_data["ws_login_email"] = text.strip()
        context.user_data["ws_flow"] = "login_password"
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]])
        await update.message.reply_text(
            f"🔐 Email: <code>{html.escape(text.strip())}</code>\n\n"
            f"Now send the <b>password</b>:\n"
            f"<i>💡 If the account uses 2FA (TOTP), add the secret key on the second line:</i>\n"
            f"<code>password\nMFTSAY3BMNSTQMJW</code>\n\n"
            f"<i>Your message will be deleted automatically after processing for security</i>",
            parse_mode="HTML", reply_markup=back_kb
        )
        return True

    # ── Login flow: password (+ optional TOTP secret on line 2) ──────────
    if flow == "login_password":
        ws_id = context.user_data.get("ws_target_id", "")
        email = context.user_data.get("ws_login_email", "")
        # Parse: line 1 = password, line 2 (optional) = TOTP secret
        lines = text.strip().splitlines()
        password = lines[0].strip()
        totp_secret_raw = lines[1].strip().replace(" ", "").replace("-", "").upper() if len(lines) > 1 else ""
        # Validate TOTP secret if provided (Base32)
        totp_secret = totp_secret_raw if (totp_secret_raw and re.match(r'^[A-Z2-7]+=*$', totp_secret_raw)) else ""
        # Delete the message immediately for security
        try:
            await update.message.delete()
        except Exception:
            pass
        context.user_data["ws_flow"] = None
        context.user_data["ws_login_password"] = password
        context.user_data["ws_login_totp_secret"] = totp_secret
        ws = ws_get_workspace(ws_id)
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]])
        totp_hint = " + 🔑 TOTP" if totp_secret else ""
        wait_msg = await update.message.reply_text(
            f"⏳ Logging in to ChatGPT{totp_hint}...\n"
            "🔄 OTP code will be fetched automatically if available",
            reply_markup=back_kb
        )

        async def _do_login_with_auto_totp(em, pw, ts, pid=""):
            """Call ws_chatgpt_login, auto-generating TOTP if mfa_challenge arrives."""
            res = await ws_chatgpt_login(em, pw, pending_id=pid)
            if res.get("needs_otp") and res.get("otp_type") == "totp" and ts and _pyotp:
                # Auto-generate TOTP code from the stored secret
                try:
                    code = _pyotp.TOTP(ts).now()
                    res2 = await ws_chatgpt_login(em, pw, otp=code,
                                                  pending_id=res.get("pending_id", ""))
                    if res2.get("ok") or res2.get("needs_otp"):
                        return res2
                except Exception as e:
                    res["error"] = f"Failed to generate TOTP: {e}"
            return res

        result = await _do_login_with_auto_totp(email, password, totp_secret)

        if result.get("needs_otp"):
            context.user_data["ws_flow"] = "login_otp"
            context.user_data["ws_login_otp_type"] = result.get("otp_type", "totp")
            context.user_data["ws_login_pending_id"] = result.get("pending_id", "")
            otp_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]])
            otp_type = result.get("otp_type", "totp")
            auto_failed = result.get("auto_otp_failed", False)
            err_detail = result.get("error", "")
            if otp_type == "email":
                if auto_failed:
                    otp_msg = (
                        "⚠️ <b>Failed to fetch OTP automatically</b>\n\n"
                        f"<i>{html.escape(err_detail)}</i>\n\n"
                        ""
                    )
                else:
                    otp_msg = (
                        "📧 <b>Email verification code</b>\n\n"
                        ""
                        ""
                    )
            else:
                if totp_secret and _pyotp:
                    otp_msg = (
                        "⚠️ <b>Automatic TOTP failed</b>\n\n"
                        f"<i>{html.escape(err_detail)}</i>\n\n"
                        ""
                    )
                else:
                    otp_msg = (
                        "🔐 <b>Two-factor authentication code (2FA)</b>\n\n"
                        "Send TOTP code from authenticator app (Google Authenticator etc.):"
                    )
            try:
                await wait_msg.edit_text(otp_msg, parse_mode="HTML", reply_markup=otp_kb)
            except Exception:
                await update.message.reply_text(otp_msg, parse_mode="HTML", reply_markup=otp_kb)
            return True
        if not result.get("ok"):
            err = result.get("error", "")
            try:
                await wait_msg.edit_text(f"❌ Login failed:\n{err}", reply_markup=back_kb)
            except Exception:
                await update.message.reply_text(f"❌ Login failed:\n{err}", reply_markup=back_kb)
            return True
        # Success — save TOTP secret to DB for future auto-refresh
        if totp_secret and ws_id:
            try:
                conn = get_db()
                conn.execute("UPDATE chatgpt_workspaces SET chatgpt_totp_secret=? WHERE id=?",
                             (totp_secret, ws_id))
                conn.commit()
                conn.close()
            except Exception:
                pass
        await _ws_save_login_session(ws_id, result["session"], wait_msg, update, context)
        return True

    # ── Login flow: OTP ───────────────────────────────────────────────────
    if flow == "login_otp":
        ws_id = context.user_data.get("ws_target_id", "")
        email = context.user_data.get("ws_login_email", "")
        password_stored = context.user_data.get("ws_login_password", "")
        pending_id = context.user_data.get("ws_login_pending_id", "")
        otp = text.strip()
        context.user_data["ws_flow"] = None
        ws = ws_get_workspace(ws_id)
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("", callback_data=f"ws_cfg_{ws_id}")]])
        wait_msg = await update.message.reply_text("")
        # Resume login from stored auth session (avoids re-sending a new OTP email)
        result = await ws_chatgpt_login(email, password_stored, otp=otp, pending_id=pending_id)
        if not result.get("ok"):
            err = result.get("error", "")
            try:
                await wait_msg.edit_text(f"❌ Verification failed:\n{err}", reply_markup=back_kb)
            except Exception:
                await update.message.reply_text(f"❌ Verification failed:\n{err}", reply_markup=back_kb)
            return True
        await _ws_save_login_session(ws_id, result["session"], wait_msg, update, context)
        return True

    return False

# ── Admin /wsprotect command ──────────────────────────────────────────────

async def cmd_wsprotect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add/remove protected member: /wsprotect [ws_id] [email]"""
    uid = update.effective_user.id
    if uid != OWNER_ID:
        return await update.message.reply_text("❌ Owner only.")
    args = context.args
    if len(args) < 2:
        return await update.message.reply_text(
            "Usage: <code>/wsprotect [ws_id] [email]</code>\n"
            "Example: <code>/wsprotect ws-1234abcd admin@example.com</code>",
            parse_mode="HTML"
        )
    ws_id, email = args[0], args[1]
    ws = ws_get_workspace(ws_id)
    if not ws:
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Workspace Panel", callback_data="ws_home")]])
        return await update.message.reply_text("❌ Workspace not found.", reply_markup=back_kb)
    ws_add_protected_member(ws_id, email)
    back_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🛡 View protected in {ws['name']}", callback_data=f"ws_showprotect_{ws_id}")],
        [InlineKeyboardButton("🔙 Workspace Settings", callback_data=f"ws_cfg_{ws_id}"),
         InlineKeyboardButton("", callback_data="ws_home")],
    ])
    await update.message.reply_text(
        f"✅ Added <code>{email}</code> as a protected member in <b>{ws['name']}</b>.",
        parse_mode="HTML", reply_markup=back_kb
    )

async def cmd_wskeys(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List active invite keys: /wskeys [ws_id]"""
    uid = update.effective_user.id
    if uid != OWNER_ID:
        return await update.message.reply_text("❌ Owner only.")
    conn = db_connect()
    if context.args:
        rows = conn.execute(
            "SELECT * FROM chatgpt_invite_keys WHERE workspace_id=? AND status='active' ORDER BY created_at DESC",
            (context.args[0],)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM chatgpt_invite_keys WHERE status='active' ORDER BY created_at DESC LIMIT 20"
        ).fetchall()
    conn.close()
    ws_keys_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("", callback_data="ws_genkey")],
        [InlineKeyboardButton("🔙 Workspace Panel", callback_data="ws_home")],
    ])
    if not rows:
        return await update.message.reply_text("", reply_markup=ws_keys_kb)
    lines = []
    for r in rows:
        expiry = r["expires_at"][:16]
        ws = ws_get_workspace(r["workspace_id"])
        ws_name = ws["name"] if ws else (r["workspace_id"] or "any workspace")
        sub_h = int(r["subscription_hours"] or 720)
        dur_label = next((f"{la} ({le})" for h, la, le in WS_DURATION_OPTIONS if h == sub_h), f"{sub_h}h")
        lines.append(f"🔑 <code>{r['code']}</code>\n   🏢 {ws_name} | 🕒 {dur_label} | ⏰ {expiry}")
    await update.message.reply_text(
        f"🔑 <b>Active keys ({len(lines)})</b>\n\n" + "\n\n".join(lines),
        parse_mode="HTML", reply_markup=ws_keys_kb
    )

async def cmd_wsrefresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show session status and attempt refresh: /wsrefresh [ws_id]
    Shows remaining time for each session. Attempts auto-refresh if cookies present."""
    uid = update.effective_user.id
    if uid != OWNER_ID:
        return await update.message.reply_text("❌ Owner only.")

    ws_id_filter = context.args[0] if context.args else None
    workspaces = ws_list_workspaces()
    if ws_id_filter:
        workspaces = [w for w in workspaces if w["id"] == ws_id_filter]
        if not workspaces:
            return await update.message.reply_text("❌ Workspace not found.")

    if not workspaces:
        return await update.message.reply_text("ℹ️ No Workspaces.")

    msg = await update.message.reply_text("🔍 Checking Sessions...")
    results = []
    now_utc = datetime.datetime.utcnow()

    for ws in workspaces:
        ws_name = ws["name"]
        ws_id = ws["id"]
        for sf_key, label in [("session_file", "WS 🏢"), ("personal_session_file", "Personal 👤")]:
            sf = ws.get(sf_key)
            if not sf:
                continue
            session = ws_load_session(sf)
            if not session:
                results.append(f"❌ <b>{ws_name}</b> [{label}]\nFile not found — upload a new Session.")
                continue

            # Get expiry from JWT exp (actual API token lifetime)
            exp = ws_get_session_expiry(sf)
            delta_secs = int((exp - now_utc).total_seconds()) if exp else None

            # Detect if session can be auto-refreshed
            has_refresh = bool(
                session.get("sessionToken")
                or (isinstance(session.get("cookies"), dict) and (
                    session["cookies"].get("__Secure-next-auth.session-token")
                    or session["cookies"].get("next-auth.session-token")
                ))
                or (isinstance(session.get("cookies"), list) and any(
                    c.get("name") in ("__Secure-next-auth.session-token", "next-auth.session-token")
                    for c in session.get("cookies", []) if isinstance(c, dict)
                ))
            )

            if exp is None:
                status_icon = "❓"
                expiry_line = ""
            elif delta_secs <= 0:
                status_icon = "🔴"
                expiry_line = "⛔ Token expired — upload a new Session"
            elif delta_secs < 2 * 3600:
                status_icon = "🟠"
                expiry_line = f"⚠️ Expires in {ws_format_expiry_delta(exp)} — upload a new Session soon!"
            elif delta_secs < 24 * 3600:
                status_icon = "🟡"
                expiry_line = f"⏳ Expires in {ws_format_expiry_delta(exp)}"
            else:
                status_icon = "🟢"
                expiry_line = f"✅ Valid for {ws_format_expiry_delta(exp)}"

            # Attempt refresh if session token available
            refresh_line = ""
            if has_refresh:
                ok = await ws_try_refresh_token(sf)
                if ok:
                    new_exp = ws_get_session_expiry(sf)
                    refresh_line = f"\n🔄 Renewed — now valid for {ws_format_expiry_delta(new_exp)}"
                    status_icon = "🟢"
                else:
                    refresh_line = "\n⚠️ Renewal failed — sessionToken expired"
            else:
                refresh_line = "\nℹ️ No sessionToken found — manual renewal only"

            owner_email = session.get("user", {}).get("email", "—")
            plan = session.get("account", {}).get("planType", "—")

            results.append(
                f"{status_icon} <b>{ws_name}</b> [{label}]\n"
                f"   📧 {owner_email} | {plan}\n"
                f"   {expiry_line}"
                f"{refresh_line}"
            )

    if not results:
        text = "ℹ️ No Sessions uploaded for any Workspace."
    else:
        text = (
            "📋 <b>ChatGPT Workspace Sessions Status</b>\n\n"
            + "\n\n".join(results)
            + "\n\n<i>To upload a new Session: /ws → ⚙️ Settings → Upload Session</i>"
        )
    ws_refresh_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("", callback_data="ws_refresh_all"),
         InlineKeyboardButton("🔙 Workspace Panel", callback_data="ws_home")],
    ])
    await msg.edit_text(text, parse_mode="HTML", reply_markup=ws_refresh_kb)


# ── User workspace flow (main bot) ─────────────────────────────────────────

async def cmd_workspace(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """User: /workspace — enter invite key flow."""
    uid = update.effective_user.id
    if is_user_banned(uid):
        return await update.message.reply_text(t(uid, "banned"), parse_mode="HTML")
    is_ar = False
    context.user_data["ws_user_flow"] = "enter_key"
    cancel_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🚫 " + ("Cancel"), callback_data="user_ws_cancel"),
        InlineKeyboardButton("🏠 " + ("Home"), callback_data="start_home"),
    ]])
    await update.message.reply_text(
        "🏢 <b>ChatGPT Workspace</b>\n\n"
        "To join a Workspace, send the <b>invitation code</b> you received:",
        parse_mode="HTML", reply_markup=cancel_kb
    )

async def handle_ws_user_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Handle user workspace flows in the main bot.
    Returns True if handled.
    """
    uid = update.effective_user.id
    text = (update.message.text or "").strip()
    username = update.effective_user.username

    # ── Flow: waiting for invite key (triggered from Activate → ChatGPT Seat → by Key) ─────
    # Guard: don't intercept if another input flow is currently active
    if context.user_data.get("waiting_ws_invite_key") and not context.user_data.get("deposit_step") and not context.user_data.get("act_step"):
        context.user_data.pop("waiting_ws_invite_key", None)
        ok, reason, key_row = ws_validate_key(text)
        if not ok:
            is_ar = False
            reason_map = {
                "invalid": "❌ Invalid key.",
                "used": "❌ This key has already been used.",
                "expired": "❌ This key has expired.",
                "revoked": "❌ This key has been revoked.",
                "workspace_full": t(uid, "ws_seat_no_ws"),
                "workspace_disabled": t(uid, "ws_seat_no_ws"),
            }
            msg = reason_map.get(reason, "❌ Invalid key.")
            await update.message.reply_text(msg, parse_mode="HTML")
            return True
        context.user_data["ws_invite_code"] = text.upper()
        context.user_data["ws_workspace_id"] = key_row["workspace_id"]
        context.user_data["ws_key_sub_hours"] = int(key_row.get("subscription_hours") or 720)
        context.user_data["ws_user_flow"] = "enter_email"
        ws = ws_get_workspace(key_row["workspace_id"])
        ws_name = ws["name"] if ws else "Workspace"
        sub_hrs = context.user_data["ws_key_sub_hours"]
        is_ar = False
        dur_label = next((lbl_ar if is_ar else lbl_en for h, lbl_ar, lbl_en in WS_DURATION_OPTIONS if h == sub_hrs), f"{sub_hrs}h")
        await update.message.reply_text(
            f"✅ <b>{'Key accepted'}!</b>\n\n"
            f"🏢 Workspace: <b>{ws_name}</b>\n"
            f"🕒 {'Subscription'}: <b>{dur_label}</b>\n\n" +
            t(uid, "ws_seat_email_prompt"),
            parse_mode="HTML"
        )
        return True

    # ── Flow: paid seat — now enter email ─────────────────────────────────────────────────
    # Guard: don't intercept if another input flow is currently active
    if context.user_data.get("ws_seat_pending_paid") and not context.user_data.get("deposit_step") and not context.user_data.get("act_step"):
        email = text.strip().lower()
        if "@" not in email or "." not in email:
            await update.message.reply_text("❌ " + ("Invalid email. Try again:"))
            return True
        ws_id = context.user_data.pop("ws_seat_pending_ws_id", None)
        context.user_data.pop("ws_seat_pending_paid", None)
        sub_hours = context.user_data.pop("ws_seat_sub_hours", 720)
        if not ws_id:
            await update.message.reply_text("❌ Session expired. Please start again.")
            return True
        # Save email permanently so user doesn't have to re-enter it next time
        context.user_data["ws_last_email"] = email
        ws = _get_workspace_by_id(ws_id)
        ws_name = ws["name"] if ws else str(ws_id)
        price = ws_calc_price(sub_hours)
        req = ws_create_request(str(ws_id), email, "", uid, username, paid_amount=price)
        sub = ws_create_subscription(uid, email, str(ws_id), sub_hours, request_id=req["id"])
        dur_label = next((lbl_en for h, _, lbl_en in WS_DURATION_OPTIONS if h == sub_hours), f"{sub_hours}h")
        expires_str = sub["expires_at"][:16]
        is_ar = False
        done_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "💳 My Subscriptions",
                callback_data="user_my_subs"
            ),
            InlineKeyboardButton("🏠 Home", callback_data="start_home"),
        ]])
        await update.message.reply_text(
            (
                f"✅ <b>Subscribed!</b>\n\n"
                f"📧 Account: <code>{email}</code>\n"
                f"🏢 Workspace: <b>{ws_name}</b>\n"
                f"🕒 Duration: <b>{next(lbl_en for h, _, lbl_en in WS_DURATION_OPTIONS if h == sub_hours)}</b>\n"
                f"📅 Expires: <b>{expires_str}</b>\n\n"
                f""
                if is_ar else
                f"✅ <b>Subscribed!</b>\n\n"
                f"📧 Email: <code>{email}</code>\n"
                f"🏢 Workspace: <b>{ws_name}</b>\n"
                f"🕒 Duration: <b>{dur_label}</b>\n"
                f"📅 Expires: <b>{expires_str}</b>\n\n"
                f"⏳ You will be added shortly."
            ),
            parse_mode="HTML", reply_markup=done_kb
        )
        uname_display = f"@{username}" if username else f"#{uid}"
        log_msg = (
            f"💳 <b>Paid Workspace Seat</b>\n\n"
            f"👤 {uname_display} (<code>{uid}</code>)\n"
            f"📧 <code>{email}</code>\n"
            f"🏢 {ws_name}\n"
            f"🕒 {dur_label}\n"
            f"💰 ${price:.2f}\n"
            f"📅 Expires: {expires_str}"
        )
        try:
            await send_log_via_second_bot(log_msg)
        except Exception:
            pass
        return True

    flow = context.user_data.get("ws_user_flow")
    if not flow or context.user_data.get("deposit_step") or context.user_data.get("act_step"):
        return False

    if flow == "enter_key":
        ok, reason, key_row = ws_validate_key(text)
        if not ok:
            reason_map = {
                "invalid": "",
                "used": "",
                "expired": "",
                "revoked": "",
                "workspace_full": "❌ The Workspace is currently full.",
                "workspace_disabled": "❌ The Workspace is currently unavailable."
            }
            msg = reason_map.get(reason, "")
            context.user_data.pop("ws_user_flow", None)
            return await update.message.reply_text(msg, parse_mode="HTML") or True
        context.user_data["ws_invite_code"] = text.upper()
        context.user_data["ws_workspace_id"] = key_row["workspace_id"]
        context.user_data["ws_key_sub_hours"] = int(key_row.get("subscription_hours") or 720)
        context.user_data["ws_user_flow"] = "enter_email"
        ws = ws_get_workspace(key_row["workspace_id"])
        ws_name = ws["name"] if ws else "Workspace"
        sub_hrs = context.user_data["ws_key_sub_hours"]
        dur_label = next((lbl_ar for h, lbl_ar, _ in WS_DURATION_OPTIONS if h == sub_hrs), f"{sub_hrs}h")
        await update.message.reply_text(
            f"✅ Code accepted!\n\n"
            f"🏢 Workspace: <b>{ws_name}</b>\n"
            f"🕒 Subscription duration: <b>{dur_label}</b>\n\n"
            f"📧 Now send your <b>email address</b> to be added:",
            parse_mode="HTML"
        )
        return True

    if flow == "enter_email":
        email = text.strip().lower()
        if "@" not in email or "." not in email:
            await update.message.reply_text("")
            return True

        ws_id = context.user_data.get("ws_workspace_id")
        invite_code = context.user_data.get("ws_invite_code")
        sub_hours = context.user_data.pop("ws_key_sub_hours", 720)
        username = update.effective_user.username

        # Check if already requested
        conn = db_connect()
        existing = conn.execute(
            "SELECT * FROM chatgpt_requests WHERE workspace_id=? AND normalized_email=? AND status NOT IN ('failed','rejected')",
            (ws_id, ws_normalize_email(email))
        ).fetchone()
        conn.close()
        if existing:
            context.user_data.pop("ws_user_flow", None)
            return await update.message.reply_text(
                f"ℹ️ Email <code>{email}</code> is already registered in this Workspace.\n"
                f"Status: {dict(existing)['status']}",
                parse_mode="HTML"
            ) or True

        req = ws_create_request(ws_id, email, invite_code, uid, username)
        sub = ws_create_subscription(uid, email, ws_id, sub_hours, request_id=req["id"])
        # Save email permanently so user doesn't have to re-enter it next time
        context.user_data["ws_last_email"] = email
        context.user_data.pop("ws_user_flow", None)
        context.user_data.pop("ws_invite_code", None)
        context.user_data.pop("ws_workspace_id", None)

        ws = ws_get_workspace(ws_id)
        ws_name = ws["name"] if ws else ws_id
        expires_str = sub["expires_at"][:16]
        dur_label = next((lbl_ar for h, lbl_ar, _ in WS_DURATION_OPTIONS if h == sub_hours), f"{sub_hours}h")

        done_kb2 = InlineKeyboardMarkup([[
            InlineKeyboardButton("", callback_data="user_my_subs"),
            InlineKeyboardButton("", callback_data="start_home"),
        ]])
        await update.message.reply_text(
            f"✅ <b>Request received!</b>\n\n"
            f"📧 Email: <code>{email}</code>\n"
            f"🏢 Workspace: <b>{ws_name}</b>\n"
            f"🕒 Duration: <b>{dur_label}</b>\n"
            f"📅 Expires: <b>{expires_str}</b>\n\n"
            f"",
            parse_mode="HTML", reply_markup=done_kb2
        )

        # Notify owner about new request
        try:
            if OWNER_ID:
                await context.bot.send_message(
                    OWNER_ID,
                    f"📨 <b>New join request</b>\n\n"
                    f"👤 @{username or uid}\n"
                    f"📧 {email}\n"
                    f"🏢 {ws_name}",
                    parse_mode="HTML"
                )
        except Exception:
            pass
        return True

    return False


# =========================
# MAIN
# =========================
async def main():
    global global_log_bot, MAINTENANCE_MODE

    init_db()
    MAINTENANCE_MODE = get_maintenance_data()

    if not MAIN_BOT_TOKEN or not LOG_BOT_TOKEN:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or LOG_BOT_TOKEN")

    app_main = build_main_user_app()
    app_log = build_main_admin_app()

    global_log_bot = app_log.bot

    await app_main.initialize()
    await app_log.initialize()

    try:
        await app_main.bot.set_my_commands(main_user_commands())
        await app_log.bot.set_my_commands(basic_admin_commands())
    except Exception as e:
        logger.warning(f"Could not set default commands: {e}")

    await app_main.start()
    await app_log.start()

    await app_main.updater.start_polling()
    await app_log.updater.start_polling()

    for row in get_active_external_shops():
        ok, result = await start_external_shop_runtime(int(row["id"]))
        if ok:
            logger.info(f"Started external shop {row['id']}: {result}")
        else:
            logger.error(f"Failed to start external shop {row['id']}: {result}")

    asyncio.create_task(activation_poller(app_main.bot))
    asyncio.create_task(ws_worker_loop(app_main.bot))
    asyncio.create_task(scheduled_tasks())

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
