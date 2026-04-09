# -*- coding: utf-8 -*-
"""Blockchain deposit verification — TRC20 / BEP20 USDT."""

import random
import logging
import datetime
from decimal import Decimal, ROUND_DOWN

import httpx

from database import db_connect
from config import (
    MY_TRC20_ADDRESS, MY_BEP20_ADDRESS, BSCSCAN_API_KEY, BSC_RPC_URL,
    USDT_TRC20_CONTRACT, USDT_BEP20_CONTRACT, TRANSFER_EVENT_TOPIC,
    logger,
)
from utils import (
    normalize_network_name, normalize_amount_decimal, network_amount_decimals,
    network_quantizer, format_amount_for_network, parse_amount_decimal,
    decimal_to_display, canonicalize_txid, network_chain_decimals,
    pending_expected_amount_decimal, pending_expected_amount_str,
    parse_db_datetime, is_txid_like,
)
from models.user import add_balance
from localization import t, get_user_lang


# ── Pending deposit helpers ───────────────────────────────────────────────

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
    fallback = (base_dec + Decimal(random.randint(1, max_noise)).scaleb(-digits)).quantize(
        network_quantizer(net), rounding=ROUND_DOWN
    )
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
    row = conn.execute("SELECT 1 FROM deposits WHERE lower(txid)=lower(?) LIMIT 1", (txid,)).fetchone()
    if not row:
        row = conn.execute(
            "SELECT 1 FROM pending_deposits WHERE status='confirmed' AND lower(tx_hash)=lower(?) LIMIT 1",
            (txid,)
        ).fetchone()
    conn.close()
    return bool(row)


# ── Chain queries ─────────────────────────────────────────────────────────

async def _check_trc20_deposits(wallet: str, pending: list) -> list:
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


async def _confirm_deposit(context, deposit: dict, tx_hash: str, amount):
    from config import OWNER_ID
    tx_hash_norm = canonicalize_txid(tx_hash, deposit.get("network", "")) or (tx_hash or "").strip().lower()
    amount_dec = parse_amount_decimal(amount) or Decimal("0")
    amount_str = decimal_to_display(amount_dec)

    conn = db_connect()
    already = conn.execute("SELECT 1 FROM deposits WHERE lower(txid)=lower(?) LIMIT 1", (tx_hash_norm,)).fetchone()
    if already:
        conn.close()
        return False

    row = conn.execute("SELECT status FROM pending_deposits WHERE id=?", (deposit["id"],)).fetchone()
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
            topics = [str(t_).lower() for t_ in log.get("topics", [])]
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


async def check_blockchain_deposits(context):
    """Lightweight cleanup job: only expire stale pending deposits."""
    expire_pending_deposits()
