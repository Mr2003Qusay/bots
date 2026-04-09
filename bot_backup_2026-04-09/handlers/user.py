# -*- coding: utf-8 -*-
"""User-facing handlers — start, profile, shop, deposit, history, invite, language."""

import re
import secrets
import time
import datetime
import asyncio
from decimal import Decimal

from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler

from config import (
    OWNER_ID, MAIN_BOT_TOKEN, MY_BOT_USERNAME, MAINTENANCE_MODE,
    MY_TRC20_ADDRESS, MY_BEP20_ADDRESS, MY_BARIDIMOB_RIB, logger,
)
from database import db_connect
from localization import t, get_user_lang, help_text_for
from models.user import (
    update_user_info, update_shop_user_info, get_user_data, is_user_banned,
    get_total_users, get_user_balance, get_shop_balance, get_user_history,
    add_balance, bind_referrer, set_lang, get_activate_price_for_user,
)
from models.reseller import is_reseller, get_reseller_profit, add_reseller_balance
from models.shop import (
    get_categories, get_products_by_cat, get_product,
    get_all_products, get_shop_user_count, get_shop_product_count,
    reduce_stock, record_purchase, claim_product_codes,
    sync_product_stock_from_codes, delivery_type_label,
    get_purchase_count,
)
from models.external_shop import get_active_external_shops
from services.iqless_api import iqless_pick_best_device, iqless_submit_job, iqless_poll_job
from services.blockchain import (
    get_user_pending_deposit, expire_pending_deposits,
    generate_unique_deposit_amount, verify_pending_deposit_tx,
    _confirm_deposit, check_blockchain_deposits,
)
from ui.keyboards import (
    build_main_user_keyboard, build_external_user_keyboard,
    build_help_keyboard,
)
from ui.menus import (
    current_bot_mode, current_shop_id, current_external_owner_id,
    current_external_title, current_external_admin_token,
    current_external_store_token, clear_all_user_flow_states,
    sync_commands_for_chat,
)
from handlers.common import (
    check_channel_join, send_join_alert, send_log_via_second_bot,
    send_to_external_admin,
)
from utils import (
    is_txid_like, parse_amount_decimal, canonicalize_txid,
    normalize_network_name, pending_expected_amount_str,
    parse_db_datetime, format_amount_for_network,
)


# ── Active jobs tracking ──────────────────────────────────────────────────
active_jobs = {}


def generate_tx_id() -> str:
    return secrets.token_hex(4).upper()


# ── Job DB persistence ───────────────────────────────────────────────────

def db_save_job(job_id, uid, email, cost, reseller_id, tx_id, submitted_at,
                status_msg_id=0, estimated_wait=0.0):
    conn = db_connect()
    conn.execute(
        "INSERT OR REPLACE INTO active_jobs_db (job_id, uid, email, cost, reseller_id, tx_id, submitted_at, status_msg_id, estimated_wait) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (job_id, uid, email, cost, reseller_id, tx_id, submitted_at, status_msg_id, estimated_wait)
    )
    conn.commit()
    conn.close()


def db_remove_job(job_id) -> bool:
    conn = db_connect()
    cur = conn.execute("DELETE FROM active_jobs_db WHERE job_id=?", (job_id,))
    conn.commit()
    conn.close()
    return (cur.rowcount or 0) > 0


def db_load_jobs():
    conn = db_connect()
    rows = conn.execute("SELECT * FROM active_jobs_db").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def db_update_job_msg(job_id, msg_id):
    conn = db_connect()
    conn.execute("UPDATE active_jobs_db SET status_msg_id=? WHERE job_id=?", (msg_id, job_id))
    conn.commit()
    conn.close()


# ── Error labels ──────────────────────────────────────────────────────────

ERROR_LABELS = {
    "WRONG_PASSWORD": "Wrong password",
    "WRONG_TOTP": "Wrong TOTP code",
    "INVALID_TOTP": "Invalid TOTP code",
    "ACCOUNT_LOCKED": "Account locked",
    "ACCOUNT_DISABLED": "Account disabled by Google",
    "INVALID_EMAIL": "Invalid email",
    "NO_GOOGLE_ONE": "Google One offer not found",
    "DEVICE_ERROR": "Device error",
    "TIMEOUT": "Timed out",
    "CAPTCHA": "Google captcha required",
    "UNKNOWN_ERROR": "Unknown error",
    "NETWORK_ERROR": "Network error",
    "2FA_REQUIRED": "2FA required",
    "WRONG_CREDENTIALS": "Wrong credentials",
    "SESSION_EXPIRED": "Session expired",
    "URL_CAPTURE_FAILED": "URL capture failed",
    "NOT_ELIGIBLE": "Not eligible",
    "PLAN_NOT_FOUND": "Plan not found",
    "ALREADY_SUBSCRIBED": "Already subscribed",
    "PAYMENT_FAILED": "Payment failed",
}


# ── Start ─────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mode = current_bot_mode(context)
    if mode == "main_admin":
        from handlers.admin import main_admin_start
        return await main_admin_start(update, context)
    if mode == "ext_admin":
        from handlers.external import external_admin_start
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
            await context.bot.send_message(
                OWNER_ID,
                f"🔔 <b>New User:</b>\n@{username} (ID: {uid})",
                parse_mode="HTML"
            )
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


# ── Language ──────────────────────────────────────────────────────────────

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


# ── Profile ───────────────────────────────────────────────────────────────

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

    if mode == "external_user":
        shop_id = current_shop_id(context)
        text = t(uid, "profile_ext_msg", uid=uid, name=display_name,
                 balance=get_user_balance(uid), orders=get_purchase_count(uid, shop_id))
        nav_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(t(uid, "btn_shop"), callback_data="user_shop")],
            [InlineKeyboardButton("🏠 Home", callback_data="user_home")],
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
                InlineKeyboardButton("💰 Deposit", callback_data="user_deposit"),
                InlineKeyboardButton("📋 History", callback_data="user_history"),
            ],
            [InlineKeyboardButton("🏠 Home", callback_data="user_home")],
        ])
    await msg.reply_text(text, parse_mode="HTML", reply_markup=nav_kb)


# ── Shop ──────────────────────────────────────────────────────────────────

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
    all_prods = get_all_products(shop_id)
    # Filter out hidden products for user view
    prods = [p for p in all_prods if not p.get("hidden")]
    if not prods:
        home_cb = "user_home"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(t(uid, "btn_home"), callback_data=home_cb)]])
        return await msg.reply_text(t(uid, "shop_empty"), parse_mode="HTML", reply_markup=kb)

    kb = []
    for p in prods:
        stock_label = f"[{int(p['stock'])}]" if int(p.get("stock", 0)) > 0 else "[Out]"
        kb.append([InlineKeyboardButton(
            f"✅ {p['name']} — ${float(p['price']):.2f} {stock_label}",
            callback_data=f"view_prod_{p['id']}"
        )])
    kb.append([InlineKeyboardButton(t(uid, "btn_home"), callback_data="user_home")])
    if update.callback_query:
        try:
            await msg.edit_text(t(uid, "shop_title"), parse_mode="HTML",
                                reply_markup=InlineKeyboardMarkup(kb))
            return
        except Exception:
            pass
    await msg.reply_text(t(uid, "shop_title"), parse_mode="HTML",
                         reply_markup=InlineKeyboardMarkup(kb))


# ── Help ──────────────────────────────────────────────────────────────────

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


# ── Deposit ───────────────────────────────────────────────────────────────

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


async def handle_deposit_amount_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        (uid, network, float(parse_amount_decimal(unique_amount_str) or 0),
         unique_amount_str, float(base_amount), wallet, expires_at)
    )
    conn.commit()
    conn.close()

    key = "deposit_pending_trc20" if network == "TRC20" else "deposit_pending_bep20"
    await update.message.reply_text(
        t(uid, key, amount=unique_amount_str, wallet=wallet),
        parse_mode="HTML"
    )


# ── Claim ─────────────────────────────────────────────────────────────────

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

    from services.blockchain import _claim_failure_text
    status_msg = await message.reply_text(t(uid, "claim_checking"), parse_mode="HTML")
    result = await verify_pending_deposit_tx(pending, txid_norm)
    if not result.get("ok"):
        failure_text = _claim_failure_text(
            uid, normalize_network_name(pending.get("network", "TRC20")),
            result, pending_expected_amount_str(pending)
        )
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


# ── Activate ──────────────────────────────────────────────────────────────

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
    cancel_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🚫 Cancel", callback_data="act_cancel_flow"),
    ]])
    await update.message.reply_text(t(uid, "send_activate_prompt"), parse_mode="HTML", reply_markup=cancel_kb)


# ── Daily / History / Invite / MyInvite ───────────────────────────────────

async def cmd_daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    msg = update.callback_query.message if update.callback_query else update.message
    if update.callback_query:
        try:
            await update.callback_query.answer()
        except Exception:
            pass
    home_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🏠 Home", callback_data="user_home"),
    ]])
    await msg.reply_text("⚠️ This feature is not available.", parse_mode="HTML", reply_markup=home_kb)


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
    nav_kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("💰 Deposit", callback_data="user_deposit"),
            InlineKeyboardButton("🏠 Home", callback_data="user_home"),
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
    invite_link = f"https://t.me/{bot_username}?start={uid}"
    nav_kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 Copy Link", url=invite_link)],
        [InlineKeyboardButton("🏠 Home", callback_data="user_home")],
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
        [InlineKeyboardButton("🏠 Home", callback_data="user_home")],
    ])
    await msg.reply_text(
        f"🔗 <b>Your Link:</b>\n<code>{invite_link}</code>",
        parse_mode="HTML", reply_markup=nav_kb
    )


# ── Activation result handler ────────────────────────────────────────────

async def handle_activation_result(bot, uid, job_id, email, cost, reseller_id, tx_id,
                                    url="", error="", success=False, msg_id=0):
    from models.user import increment_stats
    # Idempotency
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
        conn.execute(
            "INSERT INTO history (user_id, email, status, url, reason, tx_id) VALUES (?, ?, 'SUCCESS', ?, '', ?)",
            (uid, email, url, tx_id)
        )
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
        conn.execute(
            "INSERT INTO history (user_id, email, status, url, reason, tx_id) VALUES (?, ?, 'FAILED', '', ?, ?)",
            (uid, email, error[:200], tx_id)
        )
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
                await bot.edit_message_text(chat_id=uid, message_id=msg_id, text=fail_text, parse_mode="HTML")
            except Exception:
                await bot.send_message(uid, fail_text, parse_mode="HTML")
        else:
            await bot.send_message(uid, fail_text, parse_mode="HTML")

        await send_log_via_second_bot(
            f"❌ <b>Activation failed</b>\n\n"
            f"👤 User: <code>{uid}</code>\n📧 <code>{email}</code>\n"
            f"🆔 Job ID: <code>{job_id}</code>\n🧾 TX: <code>{tx_id}</code>\n"
            f"🔴 {error_label or error}\n💰 Balance refunded ${cost:.2f}"
        )


# ── Activation poller ─────────────────────────────────────────────────────

def _queue_msg(uid, email, job_id, pos, wait, tx_id=""):
    mins = int(wait // 60)
    secs = int(wait % 60)
    wait_str = f"{mins}m {secs}s" if mins else f"{secs}s"
    tx_line = f"🧾 <code>{tx_id}</code>\n" if tx_id else ""
    return (
        f"⏳ <b>Your job is queued...</b>\n\n"
        f"📧 <code>{email}</code>\n🆔 Job ID: <code>{job_id}</code>\n"
        f"{tx_line}📊 Queue position: <b>{pos}</b>\n⏱ Est. wait: ~<b>{wait_str}</b>"
    )


async def activation_poller(bot):
    recovered = db_load_jobs()
    for j in recovered:
        jid = j["job_id"]
        if jid not in active_jobs:
            active_jobs[jid] = {
                "uid": j["uid"], "email": j["email"], "cost": j["cost"],
                "reseller_id": j["reseller_id"], "tx_id": j["tx_id"],
                "submitted_at": j["submitted_at"],
                "status_msg_id": j.get("status_msg_id", 0),
                "estimated_wait": j.get("estimated_wait", 0.0),
                "last_pos": -1, "last_stage": -1,
            }
            logger.info(f"Recovered job {jid} for uid={j['uid']}")
            reconnect_text = (
                f"⏳ <b>Reconnecting!</b>\n"
                f"📧 <code>{j['email']}</code>\n🆔 Request: <code>{jid}</code>"
            )
            existing_msg_id = j.get("status_msg_id", 0)
            if existing_msg_id:
                try:
                    await bot.edit_message_text(
                        chat_id=j["uid"], message_id=existing_msg_id,
                        text=reconnect_text, parse_mode="HTML"
                    )
                except Exception:
                    try:
                        sent = await bot.send_message(j["uid"], reconnect_text, parse_mode="HTML")
                        active_jobs[jid]["status_msg_id"] = sent.message_id
                        db_update_job_msg(jid, sent.message_id)
                    except Exception:
                        pass
            else:
                try:
                    sent = await bot.send_message(j["uid"], reconnect_text, parse_mode="HTML")
                    active_jobs[jid]["status_msg_id"] = sent.message_id
                    db_update_job_msg(jid, sent.message_id)
                except Exception:
                    pass

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

                est = job_data.get("estimated_wait", 0.0) or wait
                job_timeout = max(est * 3, 300)
                elapsed = time.time() - job_data.get("submitted_at", time.time())

                if status == "success":
                    active_jobs.pop(job_id, None)
                    if db_remove_job(job_id):
                        await handle_activation_result(
                            bot, uid=uid, job_id=job_id, email=email,
                            cost=job_data["cost"], reseller_id=job_data["reseller_id"],
                            tx_id=job_data["tx_id"], url=data.get("url", ""),
                            success=True, msg_id=msg_id
                        )

                elif status == "failed":
                    active_jobs.pop(job_id, None)
                    if db_remove_job(job_id):
                        await handle_activation_result(
                            bot, uid=uid, job_id=job_id, email=email,
                            cost=job_data["cost"], reseller_id=job_data["reseller_id"],
                            tx_id=job_data["tx_id"], error=data.get("error", "UNKNOWN_ERROR"),
                            success=False, msg_id=msg_id
                        )

                elif status == "queued":
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
                    if elapsed > job_timeout and job_timeout > 0:
                        active_jobs.pop(job_id, None)
                        if db_remove_job(job_id):
                            await handle_activation_result(
                                bot, uid=uid, job_id=job_id, email=email,
                                cost=job_data["cost"], reseller_id=job_data["reseller_id"],
                                tx_id=job_data["tx_id"], error="TIMEOUT",
                                success=False, msg_id=msg_id
                            )

                elif status == "running":
                    last_stage = job_data.get("last_stage", -1)
                    if stage != last_stage:
                        job_data["last_stage"] = stage
                        tx_id_display = job_data.get("tx_id", "")
                        progress_text = (
                            f"⚙️ <b>Activating...</b>\n\n📧 <code>{email}</code>\n"
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
                    if elapsed > 360:
                        active_jobs.pop(job_id, None)
                        if db_remove_job(job_id):
                            await handle_activation_result(
                                bot, uid=uid, job_id=job_id, email=email,
                                cost=job_data["cost"], reseller_id=job_data["reseller_id"],
                                tx_id=job_data["tx_id"], error="TIMEOUT",
                                success=False, msg_id=msg_id
                            )

                else:
                    if elapsed > 300:
                        active_jobs.pop(job_id, None)
                        if db_remove_job(job_id):
                            await handle_activation_result(
                                bot, uid=uid, job_id=job_id, email=email,
                                cost=job_data["cost"], reseller_id=job_data["reseller_id"],
                                tx_id=job_data["tx_id"], error="TIMEOUT",
                                success=False, msg_id=msg_id
                            )

            except Exception as e:
                logger.error(f"Poller error for job {job_id}: {e}")


# ──  Confirm activation callback ─────────────────────────────────────────

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
            "❌ Service temporarily unavailable (API balance issue). Please contact support.\n\n💰 Your balance has been refunded.",
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
            msg_str = first.get("msg", str(detail))
            err_text = f"field '{field}': {msg_str}"
        else:
            err_text = str(detail or resp)
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
        "uid": uid, "email": email, "cost": cost,
        "reseller_id": reseller_id, "tx_id": tx_id,
        "submitted_at": submitted_at, "status_msg_id": status_msg_id,
        "estimated_wait": float(wait), "last_pos": pos,
    }
    db_save_job(job_id, uid, email, cost, reseller_id, tx_id, submitted_at,
                status_msg_id=status_msg_id, estimated_wait=float(wait))

    await send_log_via_second_bot(
        f"📤 <b>New Activation Request</b>\n\n"
        f"👤 User: <code>{uid}</code>\n📧 Email: <code>{email}</code>\n"
        f"🆔 Job ID: <code>{job_id}</code>\n🧾 TX: <code>{tx_id}</code>\n"
        f"📊 Queue position: <b>{pos}</b>\n💵 Amount: ${cost:.2f}"
    )

    if reseller_id and reseller_id != OWNER_ID:
        try:
            await context.bot.send_message(
                reseller_id,
                f"📤 <b>New request from your client</b>\n\n"
                f"👤 User: <code>{uid}</code>\n📧 <code>{email}</code>\n"
                f"🆔 Job ID: <code>{job_id}</code>\n🧾 TX: <code>{tx_id}</code>\n"
                f"💵 Amount: ${cost:.2f}",
                parse_mode="HTML"
            )
        except Exception:
            pass


# ── Finalize purchase ─────────────────────────────────────────────────────

async def finalize_purchase(update, context, user_id, shop_id, product, qty, cost):
    from models.user import add_shop_balance
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
    await update.effective_message.reply_text(
        t(user_id, "prod_bought", name=product["name"], qty=qty, total=float(cost)),
        parse_mode="HTML"
    )

    if mode == "codes" and delivery_data:
        await update.effective_message.reply_text(
            t(user_id, "prod_codes_delivered", codes=delivery_data), parse_mode="HTML"
        )
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
        await send_log_via_second_bot(
            f"🛒 <b>Sale</b>\nUser: {user_id}\nItem: {product['name']}\nQty: {qty}\nTotal: ${float(cost):.2f}"
        )
    return True, "", delivery_data
