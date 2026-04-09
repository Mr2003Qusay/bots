# -*- coding: utf-8 -*-
"""Callback dispatchers — shop callbacks, main menu callbacks, user text handler."""

import re
import html
import datetime
from decimal import Decimal

from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from config import (
    OWNER_ID, MAIN_BOT_TOKEN, MAINTENANCE_MODE, MY_BARIDIMOB_RIB,
    SUPPORT_USER, logger,
)
from database import db_connect
from localization import t, get_user_lang
from models.user import (
    is_user_banned, get_user_data, get_user_balance,
    get_activate_price_for_user, get_shop_balance, add_shop_balance,
)
from models.reseller import is_reseller
from models.shop import (
    get_categories, get_products_by_cat, get_product,
    get_all_products, reduce_stock, record_purchase,
    claim_product_codes, sync_product_stock_from_codes,
    delivery_type_label, get_shop_user_count, get_shop_product_count,
    get_purchase_count,
)
from services.blockchain import get_user_pending_deposit
from ui.menus import (
    current_bot_mode, current_shop_id, current_external_owner_id,
    current_external_title, current_external_admin_token,
    current_external_store_token, clear_all_user_flow_states,
)
from ui.keyboards import build_main_user_keyboard, build_external_user_keyboard
from handlers.common import (
    check_channel_join, send_join_alert, send_log_via_second_bot,
    send_to_external_admin,
)
from handlers.user import (
    start, cmd_profile, cmd_shop, cmd_help, cmd_deposit,
    cmd_daily, cmd_history, cmd_invite, cmd_myinvite,
    cmd_activate, cmd_claim, cmd_language, callback_lang,
    handle_confirm_activate, finalize_purchase,
    handle_deposit_amount_input, process_deposit_claim,
    active_jobs,
)
from utils import (
    is_txid_like, parse_amount_decimal, pending_expected_amount_str,
    normalize_network_name,
)


# ── Support entry (no ConversationHandler) ────────────────────────────────

async def cmd_support_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if is_user_banned(uid):
        return await update.message.reply_text(t(uid, "banned"), parse_mode="HTML")
    context.user_data["state"] = "support"
    await update.message.reply_text(t(uid, "support_welcome"), parse_mode="HTML")


# ── Shop callback ────────────────────────────────────────────────────────

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
            kb.append([InlineKeyboardButton(
                f"✅ {prod['name']} (${float(prod['price']):.2f})",
                callback_data=f"view_prod_{prod['id']}"
            )])
        kb.append([InlineKeyboardButton(t(uid, "btn_back"), callback_data="user_shop")])
        await query.message.edit_text(
            t(uid, "shop_cat", cat=cat), parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(kb)
        )
        return

    if data.startswith("view_prod_"):
        pid = int(data.split("_")[2])
        prod = get_product(shop_id, pid)
        if not prod:
            return await query.answer("Unavailable", show_alert=True)
        kb = [
            [InlineKeyboardButton(t(uid, "btn_buy"), callback_data=f"buy_ask_{pid}")],
            [InlineKeyboardButton(t(uid, "btn_back"), callback_data="user_shop")]
        ]
        text = t(uid, "shop_prod_view",
                 name=prod["name"], desc=prod["description"] or "-",
                 price=float(prod["price"]), stock=int(prod["stock"]),
                 delivery=delivery_type_label(uid, prod))
        if prod.get("image_id"):
            try:
                await query.message.delete()
                await context.bot.send_photo(uid, photo=prod["image_id"],
                                              caption=text, parse_mode="HTML",
                                              reply_markup=InlineKeyboardMarkup(kb))
            except Exception:
                await query.message.reply_text(text, parse_mode="HTML",
                                                reply_markup=InlineKeyboardMarkup(kb))
        else:
            try:
                await query.message.edit_text(text, parse_mode="HTML",
                                               reply_markup=InlineKeyboardMarkup(kb))
            except Exception:
                await query.message.reply_text(text, parse_mode="HTML",
                                                reply_markup=InlineKeyboardMarkup(kb))
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


# ── Main menu callback ───────────────────────────────────────────────────

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
        clear_all_user_flow_states(context)
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
        # Direct to Google One activation (simplified — no workspace)
        visible_prods = [p for p in get_all_products(0) if not p.get("hidden")]
        rows = [
            [InlineKeyboardButton(t(uid, "btn_google_one"), callback_data="user_google_one")],
        ]
        if visible_prods:
            rows.append([InlineKeyboardButton(
                "🛍️ More Products", callback_data="user_shop_direct"
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
        if context.user_data.get("act_step"):
            cancel_kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("🚫 Cancel", callback_data="act_cancel_flow"),
            ]])
            await query.answer("⚠️ Activation already in progress", show_alert=True)
            await query.message.reply_text(
                "⚠️ <b>You already have an activation in progress</b>\n\n"
                "Please complete or cancel it first.",
                parse_mode="HTML", reply_markup=cancel_kb
            )
            return
        clear_all_user_flow_states(context)
        context.user_data["act_step"] = "email"
        context.user_data.pop("waiting_for_credentials", None)
        await query.message.reply_text(t(uid, "send_activate_prompt"), parse_mode="HTML")
        await query.answer()
        return

    if data == "user_shop_direct":
        if mode != "main_user":
            return await query.answer(t(uid, "activate_not_available"), show_alert=True)
        prods = [p for p in get_all_products(0) if not p.get("hidden")]
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="user_activate")]])
        if not prods:
            return await query.message.reply_text("📭 No products available.", reply_markup=back_kb)
        rows = []
        for p in prods:
            rows.append([InlineKeyboardButton(
                f"✅ {p['name']} (${float(p['price']):.2f})",
                callback_data=f"user_prod_{p['id']}"
            )])
        rows.append([InlineKeyboardButton("🔙 Back", callback_data="user_activate")])
        await query.message.reply_text("🛍️ <b>Products</b>", parse_mode="HTML",
                                        reply_markup=InlineKeyboardMarkup(rows))
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
        kb = [
            [InlineKeyboardButton(t(uid, "btn_buy"), callback_data=f"buy_ask_{pid}")],
            [InlineKeyboardButton("🔙 Back", callback_data="user_shop_direct")]
        ]
        text = t(uid, "shop_prod_view",
                 name=prod["name"], desc=prod.get("description") or "-",
                 price=float(prod["price"]), stock=int(prod["stock"]),
                 delivery=delivery_type_label(uid, prod))
        kb_markup = InlineKeyboardMarkup(kb)
        if prod.get("image_id"):
            try:
                await query.message.delete()
                await context.bot.send_photo(uid, photo=prod["image_id"],
                                              caption=text, parse_mode="HTML",
                                              reply_markup=kb_markup)
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
        if is_user_banned(uid):
            return await query.answer(t(uid, "banned"), show_alert=True)
        context.user_data["state"] = "support"
        await query.message.reply_text(t(uid, "support_welcome"), parse_mode="HTML")
        return await query.answer()

    if data == "user_lang":
        await cmd_language(update, context)
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


# ── Text handler ──────────────────────────────────────────────────────────

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    text = (update.message.text or "").strip()
    mode = current_bot_mode(context)

    if is_user_banned(uid):
        return await update.message.reply_text(t(uid, "banned"), parse_mode="HTML")

    # ── Auto deposit amount input ─────────────────────────────────────
    if context.user_data.get("deposit_step") == "amount":
        await handle_deposit_amount_input(update, context)
        return

    if context.user_data.get("act_step"):
        if mode != "main_user":
            context.user_data.pop("act_step", None)
            return await update.message.reply_text(t(uid, "activate_not_available"), parse_mode="HTML")

        step = context.user_data["act_step"]
        _cancel_kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("🚫 Cancel", callback_data="act_cancel_flow")
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
            totp_secret = text.strip().replace(" ", "").replace("-", "").replace("\n", "").upper()
            if len(totp_secret) < 1:
                return await update.message.reply_text("❌ TOTP Secret cannot be empty. Please re-enter:", parse_mode="HTML")
            if len(totp_secret) > 64:
                return await update.message.reply_text("❌ TOTP Secret too long (max 64 characters). Please re-enter:", parse_mode="HTML")
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
                parse_mode="HTML", reply_markup=kb
            )
            return

    if mode == "main_user" and is_txid_like(text):
        await process_deposit_claim(update, context, text)
        return

    if context.user_data.get("state") == "support":
        context.user_data.pop("state", None)
        from handlers.common import handle_support_message
        await handle_support_message(update, context)
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

        total_cost = float(product["price"]) * qty
        if get_shop_balance(uid, shop_id) < total_cost:
            context.user_data.pop("state", None)
            return await update.message.reply_text(t(uid, "prod_no_bal", name=product["name"]), parse_mode="HTML")

        context.user_data["buy_final_qty"] = qty
        context.user_data["buy_final_cost"] = total_cost
        context.user_data["state"] = "WAIT_CONFIRM"
        return await update.message.reply_text(
            t(uid, "prod_buy_confirm", name=product["name"], qty=qty, total=float(total_cost)),
            parse_mode="HTML"
        )

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

        def _clear_buy_state():
            for k in ("state", "buying_pid", "buying_shop_id", "buy_final_qty",
                       "buy_final_cost", "buying_manual_delivery", "buying_max",
                       "buying_price", "buying_name"):
                context.user_data.pop(k, None)

        ok, err, _ = await finalize_purchase(update, context, uid, shop_id, product, qty, cost)
        _clear_buy_state()
        if not ok:
            return await update.message.reply_text(f"❌ {err}")
        return

    if context.user_data.get("addextshop_step"):
        from handlers.external import handle_addextshop_wizard
        return await handle_addextshop_wizard(update, context)

    if "admin_action" in context.user_data:
        from handlers.admin import main_admin_cmds_handler
        action = context.user_data.pop("admin_action")
        cmd_str = f"/{action} {text}"
        return await main_admin_cmds_handler(update, context, direct_cmd=cmd_str)

    if "ext_admin_action" in context.user_data:
        from handlers.external import ext_admin_cmds_handler
        action = context.user_data.pop("ext_admin_action")
        cmd_str = f"/{action} {text}"
        return await ext_admin_cmds_handler(update, context, direct_cmd=cmd_str)
