import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode
from config import BINANCE_API_KEY, BINANCE_API_SECRET, BINANCE_PROXY, logger
from decimal import Decimal

def _binance_proxies():
    """Return proxy dict for requests if BINANCE_PROXY is set."""
    if BINANCE_PROXY:
        return {"http": BINANCE_PROXY, "https": BINANCE_PROXY}
    return None

def _get_binance_pay_history():
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        logger.error("Binance API keys not set in config.")
        return []

    base_url = "https://api.binance.com"
    endpoint = "/sapi/v1/pay/transactions"
    
    timestamp = int(time.time() * 1000)
    params = {
        "timestamp": timestamp,
        "limit": 100
    }
    
    query_string = urlencode(params)
    signature = hmac.new(
        BINANCE_API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    
    params["signature"] = signature
    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY
    }
    
    try:
        response = requests.get(base_url + endpoint, headers=headers, params=params, timeout=10, proxies=_binance_proxies())
        data = response.json()
        if response.status_code == 200 and (data.get("code") == "000000" or data.get("success")):
            return data.get("data", [])
        else:
            logger.error(f"Binance Pay API error: {data}")
            return []
    except Exception as e:
        logger.error(f"Binance Pay request exception: {e}")
        return []

def verify_binance_pay_order(order_id: str):
    """
    Checks if `order_id` exists in recent history.
    Returns a dict with 'amount' and 'transactionTime' if found, else None.
    """
    history = _get_binance_pay_history()
    if not history:
        return None
        
    for tx in history:
        if str(tx.get("orderId", "")) == str(order_id):
            funds = tx.get("fundsDetail", tx.get("fundDetail", []))
            for fund in funds:
                if fund.get("currency") == "USDT":
                    return {
                        "amount": str(fund.get("amount")),
                        "transactionTime": tx.get("transactionTime")
                    }
            # Fallback if amount is in root
            if "amount" in tx and tx.get("currency") == "USDT":
                return {
                    "amount": str(tx.get("amount")),
                    "transactionTime": tx.get("transactionTime")
                }
    return None

import asyncio

async def auto_verify_binance_pay(context):
    from services.blockchain import _confirm_deposit, is_deposit_txid_already_used
    from utils import parse_amount_decimal
    from database import db_connect

    history = await asyncio.to_thread(_get_binance_pay_history)
    if not history:
        return

    # Fetch pending Binance deposits
    conn = db_connect()
    try:
        rows = conn.execute("SELECT * FROM pending_deposits WHERE status='pending' AND network='BINANCE'").fetchall()
        if not rows:
            return
        pending_list = [dict(r) for r in rows]
    finally:
        conn.close()

    for tx in history:
        tx_amount_str = None
        funds = tx.get("fundsDetail", tx.get("fundDetail", []))
        for fund in funds:
            if fund.get("currency") == "USDT":
                tx_amount_str = fund.get("amount")
                break
        if not tx_amount_str and tx.get("currency") == "USDT":
            tx_amount_str = tx.get("amount")

        if not tx_amount_str:
            continue

        tx_amount = parse_amount_decimal(tx_amount_str)
        if not tx_amount or tx_amount <= 0:
            continue

        note = str(tx.get("note", "")).strip().upper()
        if not note:
            continue

        order_id = str(tx.get("orderId", ""))
        if not order_id:
            continue
            
        # Ensure this order_id isn't already used
        if is_deposit_txid_already_used(order_id):
            continue

        # Match against pending list
        for pending in list(pending_list):
            expected_code = str(pending.get("deposit_code", "")).strip().upper()
            if not expected_code:
                continue
                
            if note == expected_code:
                expected_amt = parse_amount_decimal(pending.get("expected_amount_str", pending.get("expected_amount")))
                if expected_amt and abs(tx_amount - expected_amt) < Decimal("0.00000002"):
                    # Success! Use the REAL orderId so it's blocked from double spending
                    await _confirm_deposit(context, pending, order_id, tx_amount)
                    pending_list.remove(pending)
                    break

def verify_binance_spot_deposit(txid: str, network: str):
    """
    Checks Binance Spot Deposit history for a specific network and txId.
    network is typically 'TRC20' or 'BEP20'.
    Returns amount as string if found and successful, else None.
    """
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        return None

    base_url = "https://api.binance.com"
    endpoint = "/sapi/v1/capital/deposit/hisrec"
    
    import time
    timestamp = int(time.time() * 1000)
    params = {
        "timestamp": timestamp,
        "coin": "USDT",
        "txId": txid,
        "status": 1  # 1 means Success
    }
    
    query_string = urlencode(params)
    signature = hmac.new(
        BINANCE_API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    
    params["signature"] = signature
    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY
    }
    
    try:
        response = requests.get(base_url + endpoint, headers=headers, params=params, timeout=10, proxies=_binance_proxies())
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                deposit = data[0]
                # Binance network field e.g. "TRX" or "BSC"
                net_match = deposit.get("network", "").upper()
                if network == "TRC20" and net_match == "TRX":
                    return str(deposit.get("amount"))
                elif network == "BEP20" and net_match == "BSC":
                    return str(deposit.get("amount"))
                elif network == "BINANCE":
                    return str(deposit.get("amount"))
        return None
    except Exception as e:
        logger.error(f"Binance Spot API error: {e}")
        return None

def _get_binance_spot_deposits_all():
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        return []

    base_url = "https://api.binance.com"
    endpoint = "/sapi/v1/capital/deposit/hisrec"
    
    timestamp = int(time.time() * 1000)
    params = {
        "timestamp": timestamp,
        "status": 1
    }
    
    query_string = urlencode(params)
    signature = hmac.new(
        BINANCE_API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    
    params["signature"] = signature
    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY
    }
    
    try:
        response = requests.get(base_url + endpoint, headers=headers, params=params, timeout=10, proxies=_binance_proxies())
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                return data
        return []
    except Exception as e:
        logger.error(f"Binance Spot API error: {e}")
        return []

async def notify_owner_of_deposits(context):
    from config import OWNER_ID
    from database import db_connect
    
    if not OWNER_ID:
        return

    pay_history = await asyncio.to_thread(_get_binance_pay_history)
    spot_history = await asyncio.to_thread(_get_binance_spot_deposits_all)
    
    conn = db_connect()
    
    try:
        already_init = conn.execute("SELECT value FROM config WHERE key='binance_notifications_init'").fetchone()
        if not already_init:
            conn.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('binance_notifications_init', '1')")
            for tx in pay_history:
                txid = str(tx.get("orderId", ""))
                if txid: conn.execute("INSERT OR IGNORE INTO binance_owner_notifications (txid) VALUES (?)", (txid,))
            for tx in spot_history:
                txid = str(tx.get("txId", ""))
                if txid: conn.execute("INSERT OR IGNORE INTO binance_owner_notifications (txid) VALUES (?)", (txid,))
            conn.commit()
            return
            
        # Process Pay history
        for tx in pay_history:
            txid = str(tx.get("orderId", ""))
            if not txid: continue
            
            row = conn.execute("SELECT txid FROM binance_owner_notifications WHERE txid=?", (txid,)).fetchone()
            if not row:
                amount = ""
                currency = tx.get("currency", "USDT")
                funds = tx.get("fundsDetail", tx.get("fundDetail", []))
                for fund in funds:
                    if fund.get("currency"):
                        amount = str(fund.get("amount"))
                        currency = fund.get("currency")
                        break
                if not amount:
                    amount = str(tx.get("amount", ""))
                    
                payer = str(tx.get("payerInfo", tx.get("note", "")))
                if payer:
                    note_str = f"📝 <b>Note:</b> {payer}\n"
                else:
                    note_str = ""
                    
                msg = (
                    f"🚨 <b>New Incoming Binance Transfer</b>\n\n"
                    f"🧾 <b>Source:</b> Binance Pay\n"
                    f"🆔 <b>Order ID:</b> <code>{txid}</code>\n"
                    f"💰 <b>Amount:</b> {amount} {currency}\n"
                    f"{note_str}\n"
                    f"<i>This was detected automatically.</i>"
                )
                try:
                    await context.bot.send_message(chat_id=OWNER_ID, text=msg, parse_mode="HTML")
                except Exception:
                    pass
                    
                conn.execute("INSERT OR IGNORE INTO binance_owner_notifications (txid, amount, currency, source) VALUES (?, ?, ?, ?)",
                             (txid, amount, currency, "Pay"))
                conn.commit()
                
        # Process Spot history
        for tx in spot_history:
            txid = str(tx.get("txId", ""))
            if not txid: continue
            
            row = conn.execute("SELECT txid FROM binance_owner_notifications WHERE txid=?", (txid,)).fetchone()
            if not row:
                amount = str(tx.get("amount", ""))
                currency = str(tx.get("coin", ""))
                network = str(tx.get("network", ""))
                if not network:
                    network = "Internal Transfer"
                    
                msg = (
                    f"🚨 <b>New Incoming Binance Transfer</b>\n\n"
                    f"🧾 <b>Source:</b> Spot / Crypto Deposit\n"
                    f"🆔 <b>TXID:</b> <code>{txid}</code>\n"
                    f"💰 <b>Amount:</b> {amount} {currency}\n"
                    f"🌐 <b>Network:</b> {network}\n\n"
                    f"<i>This was detected automatically.</i>"
                )
                try:
                    await context.bot.send_message(chat_id=OWNER_ID, text=msg, parse_mode="HTML")
                except Exception:
                    pass
                    
                conn.execute("INSERT OR IGNORE INTO binance_owner_notifications (txid, amount, currency, source) VALUES (?, ?, ?, ?)",
                             (txid, amount, currency, "Spot"))
                conn.commit()
                
    finally:
        conn.close()

