# -*- coding: utf-8 -*-
"""Pure utility helpers — no DB, no Telegram imports."""

import re
import datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN
import secrets

from config import (
    NETWORK_AMOUNT_DECIMALS,
    NETWORK_CHAIN_DECIMALS,
)

# ── Network helpers ───────────────────────────────────────────────────────

def normalize_network_name(network: str) -> str:
    name = (network or "TRC20").strip().upper()
    if "BEP" in name or "BSC" in name:
        return "BEP20"
    if "BINANCE" in name:
        return "BINANCE"
    return "TRC20"


def network_amount_decimals(network: str) -> int:
    return NETWORK_AMOUNT_DECIMALS.get(normalize_network_name(network), 6)


def network_chain_decimals(network: str) -> int:
    return NETWORK_CHAIN_DECIMALS.get(normalize_network_name(network), 6)


def network_quantizer(network: str) -> Decimal:
    return Decimal("1").scaleb(-network_amount_decimals(network))


# ── Amount parsing / formatting ──────────────────────────────────────────

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


# ── TXID / deposit helpers ───────────────────────────────────────────────

def canonicalize_txid(txid: str, network: str = "") -> str:
    raw = (txid or "").strip()
    if raw.lower().startswith("/claim"):
        parts = raw.split(maxsplit=1)
        raw = parts[1].strip() if len(parts) > 1 else ""
    if not raw:
        return ""
        
    net = normalize_network_name(network) if network else ""
    if net == "BINANCE":
        # Accept just about anything that looks like an ID for Binance
        return raw if len(raw) >= 5 else ""

    raw_hex = raw[2:] if raw.lower().startswith("0x") else raw
    if not re.fullmatch(r"[0-9a-fA-F]{64}", raw_hex):
        return ""
    raw_hex = raw_hex.lower()
    if net == "BEP20":
        return "0x" + raw_hex
    if net == "TRC20":
        return raw_hex
    return raw.lower() if raw.lower().startswith("0x") else raw_hex


def is_txid_like(text: str) -> bool:
    raw = (text or "").strip()
    if raw.lower().startswith("/claim"):
        return True
    if raw.upper().startswith("TOOLS-"):
        return True
    if canonicalize_txid(text):
        return True
    # If it's a long number, could be Binance Order ID
    if raw.isdigit() and len(raw) >= 8:
        return True
    return False


def pending_expected_amount_str(deposit: dict) -> str:
    stored = (deposit.get("expected_amount_str") or "").strip()
    if stored:
        return stored
    return format_amount_for_network(
        deposit.get("expected_amount", 0),
        deposit.get("network", "TRC20"),
        trim=False,
    )


def pending_expected_amount_decimal(deposit: dict) -> Decimal:
    return (
        normalize_amount_decimal(
            pending_expected_amount_str(deposit),
            deposit.get("network", "TRC20"),
        )
        or Decimal("0")
    )


# ── Datetime helpers ─────────────────────────────────────────────────────

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


# ── Token helpers ────────────────────────────────────────────────────────

def generate_tx_id() -> str:
    return secrets.token_hex(4).upper()


def is_probably_bot_token(token: str) -> bool:
    return bool(re.fullmatch(r"\d+:[A-Za-z0-9_-]{20,}", (token or "").strip()))
