import imaplib
import email
import re
import asyncio
import logging
from email.header import decode_header
from decimal import Decimal
import os

from config import logger
from database import db_connect

IMAP_SERVER = "imap.gmail.com"
IMAP_EMAIL = os.environ.get("IMAP_EMAIL", "").strip()
IMAP_PASSWORD = os.environ.get("IMAP_PASSWORD", "").strip()

def _get_binance_email_amounts_sync():
    if not IMAP_EMAIL or not IMAP_PASSWORD:
        return []
        
    amounts = []
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER)
        mail.login(IMAP_EMAIL, IMAP_PASSWORD)
        mail.select("inbox")
        
        # Search for unseen emails from Binance
        _, messages = mail.search(None, '(UNSEEN FROM "binance.com")')
        
        if not messages or not messages[0]:
            mail.logout()
            return []

        for num in messages[0].split():
            _, msg_data = mail.fetch(num, "(RFC822)")
            for response_part in msg_data:
                if isinstance(response_part, tuple):
                    msg = email.message_from_bytes(response_part[1])
                    subject, encoding = decode_header(msg["Subject"])[0]
                    if isinstance(subject, bytes):
                        subject = subject.decode(encoding or "utf-8", errors="ignore")
                        
                    if "Payment Receive" in subject or "incoming transfer" in subject.lower():
                        body = ""
                        if msg.is_multipart():
                            for part in msg.walk():
                                if part.get_content_type() in ["text/plain", "text/html"]:
                                    body += part.get_payload(decode=True).decode(errors="ignore")
                        else:
                            body = msg.get_payload(decode=True).decode(errors="ignore")
                            
                        # Strip HTML tags
                        clean_body = re.sub(r'<[^>]+>', ' ', body)
                        match = re.search(r'Amount[:\s]+([\d.]+)\s*USDT', clean_body, re.IGNORECASE)
                        if not match:
                            # Fallback if "Amount:" isn't parsed neatly
                            match = re.search(r'([\d.]+)\s*USDT', clean_body, re.IGNORECASE)
                            
                        if match:
                            amount_str = match.group(1)
                            amounts.append(amount_str)
                            
        mail.logout()
    except Exception as e:
        logger.error(f"Email fetch error: {e}")
    return amounts

async def _process_binance_amount(context, amount_str: str):
    from services.blockchain import _confirm_deposit
    from utils import parse_amount_decimal
    
    amount_dec = parse_amount_decimal(amount_str)
    if not amount_dec:
        return
        
    conn = db_connect()
    rows = conn.execute("SELECT * FROM pending_deposits WHERE status='pending' AND network='BINANCE'").fetchall()
    conn.close()
    
    for row in rows:
        pending = dict(row)
        expected = parse_amount_decimal(pending.get("expected_amount_str", pending.get("expected_amount")))
        if expected and abs(amount_dec - expected) < Decimal("0.00000002"):
            # Found exact match for this amount, confirm it!
            txid_mock = f"BINANCE-PAY-{amount_str}-{pending['id']}"
            await _confirm_deposit(context, pending, txid_mock, amount_dec)
            break

async def auto_verify_binance_pay(context):
    """Called periodically by the JobQueue"""
    amounts = await asyncio.to_thread(_get_binance_email_amounts_sync)
    for amount_str in amounts:
        await _process_binance_amount(context, amount_str)
