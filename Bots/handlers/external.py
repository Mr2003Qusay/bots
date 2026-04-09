# -*- coding: utf-8 -*-
"""External shop handlers — admin start/callbacks/cmds, runtime management, wizard."""

import re
import html
import asyncio

from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from config import OWNER_ID, MAIN_BOT_TOKEN, SUPPORT_USER, logger
from database import db_connect
from localization import t, get_user_lang
from models.user import get_user_data, get_shop_balance, add_shop_balance
from models.shop import (
    get_all_products, get_product, del_product,
    add_product_code, add_product_codes_bulk,
    get_shop_user_count, get_shop_product_count, get_shop_users,
    get_purchase_count,
)
from models.external_shop import (
    get_external_shops, get_external_shop_by_id,
    get_external_shop_by_shop_token, get_external_shop_by_admin_token,
    add_external_shop_db, remove_external_shop_db,
    update_external_shop_usernames, update_external_shop_title,
)
from ui.keyboards import build_ext_admin_keyboard, external_user_commands, ext_admin_commands
from ui.menus import (
    current_bot_mode, current_shop_id, current_external_owner_id,
    current_external_title, current_external_admin_token,
    current_external_store_token, sync_commands_for_chat,
)
from handlers.user import start, cmd_help, cmd_shop

# Runtime app tracking
EXTERNAL_USER_APPS = {}
EXTERNAL_ADMIN_APPS = {}


# ── External admin start ─────────────────────────────────────────────────

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


# ── External admin callback ──────────────────────────────────────────────

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
            "💰 <b>Store Wallet Control</b>\nUse <code>/addshop</code> and <code>/removeshop</code> from this bot.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(t(uid, "btn_back"), callback_data="user_home")]])
        )

    if data == "user_lang":
        from handlers.user import cmd_language
        return await cmd_language(update, context)

    if data == "user_home":
        return await external_admin_start(update, context)

    # ── Enhanced Product Manager (shared with main admin) ─────────
    if data.startswith("shop_mgr_"):
        from handlers.admin import callback_main_admin_menu as _admin_cb
        return await _admin_cb(update, context)

    if data.startswith("ext_act_"):
        action = data.split("_", 2)[2]
        if action == "listprod":
            # Use the enhanced product manager
            query.data = "shop_mgr_list"
            from handlers.admin import callback_main_admin_menu as _admin_cb
            return await _admin_cb(update, context)
        context.user_data["ext_admin_action"] = action
        prompts = {
            "addshop": "📝 <b>Send User ID and Amount:</b>\nExample: <code>123456789 10</code>",
            "removeshop": "📝 <b>Send User ID and Amount:</b>\nExample: <code>123456789 10</code>",
            "check": "📝 <b>Send User ID:</b>",
            "addcode": "📝 <b>Send Product ID and one code:</b>\nExample: <code>5 ABCD-1234</code>",
            "addcodes": "📝 <b>Send Product ID then codes:</b>",
            "broadcast": "📢 <b>Send Message:</b>",
            "reply": "📝 <b>Send User ID and Message:</b>\nExample: <code>12345 Hello</code>",
            "settitle": "🏷️ <b>Send New Store Title:</b>",
        }
        return await query.message.reply_text(prompts.get(action, "📝 <b>Enter Input:</b>"), parse_mode="HTML")


# ── External admin commands ───────────────────────────────────────────────

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

    async def reply(text, reply_markup=None):
        msg = update.callback_query.message if update.callback_query else update.message
        await msg.reply_text(text, parse_mode="HTML", reply_markup=reply_markup)

    if cmd == "/help": return await cmd_help(update, context)

    target_id = None
    if len(context.args) >= 1 and context.args[0].isdigit():
        target_id = int(context.args[0])

    if cmd == "/settitle":
        try:
            title = txt.split(maxsplit=1)[1]
            update_external_shop_title(shop_id, title)
            context.bot_data["external_title"] = title
            return await reply(f"✅ Title updated to <b>{title}</b>")
        except Exception: return await reply("❌ Usage: /settitle New Title")

    if cmd == "/addshop" and target_id:
        try:
            amt = float(context.args[1])
            add_shop_balance(target_id, amt, shop_id=shop_id)
            try: await Bot(current_external_store_token(context)).send_message(target_id, t(target_id, "shop_added_msg", amount=amt), parse_mode="HTML")
            except Exception: pass
            return await reply(f"✅ Added ${amt:.2f} to user {target_id}.")
        except Exception: return await reply("❌ Usage: /addshop ID Amount")

    if cmd == "/removeshop" and target_id:
        try:
            amt = float(context.args[1])
            add_shop_balance(target_id, -amt, shop_id=shop_id)
            return await reply(f"✅ Removed ${amt:.2f} from user {target_id}.")
        except Exception: return await reply("❌ Usage: /removeshop ID Amount")

    if cmd == "/check" and target_id:
        user_name = get_user_data(target_id).get("username", "")
        balance = get_shop_balance(target_id, shop_id)
        orders = get_purchase_count(target_id, shop_id)
        return await reply(f"👤 <b>User Info</b>\nID: <code>{target_id}</code>\n@{user_name or 'N/A'}\n🛒 Wallet: ${balance:.2f}\n🛍️ Orders: {orders}")

    if cmd == "/addcode":
        try:
            pid = int(context.args[0]); code_text = txt.split(maxsplit=2)[2]
            if not get_product(shop_id, pid): return await reply("❌ Product not found.")
            add_product_code(shop_id, pid, code_text)
            return await reply(f"✅ Code added. New stock: {get_product(shop_id, pid)['stock']}")
        except Exception: return await reply("❌ Usage: /addcode PRODUCT_ID CODE")

    if cmd == "/addcodes":
        try:
            raw = txt.split(maxsplit=2); pid = int(raw[1]); data_str = raw[2] if len(raw) > 2 else ""
            codes = [x.strip() for x in data_str.replace("||", "\n").splitlines() if x.strip()]
            if not codes: return await reply("❌ No codes provided.")
            if not get_product(shop_id, pid): return await reply("❌ Product not found.")
            inserted = add_product_codes_bulk(shop_id, pid, codes)
            return await reply(f"✅ Added {inserted} codes. Stock: {get_product(shop_id, pid)['stock']}")
        except Exception: return await reply("❌ Usage: /addcodes PRODUCT_ID then codes")

    if cmd == "/delprod":
        try:
            pid = int(context.args[0]); del_product(shop_id, pid)
            return await reply(f"✅ Product {pid} deleted.")
        except Exception: return await reply("❌ Usage: /delprod PRODUCT_ID")

    if cmd == "/listprod":
        prods = get_all_products(shop_id)
        if not prods: return await reply("📭 No products.")
        msg = f"🛒 <b>{current_external_title(context)} Products</b>\n"
        for p in prods:
            msg += f"ID: {p['id']} | {p['name']} | ${float(p['price']):.2f} | Stock: {int(p['stock'])}\n"
        return await reply(msg)

    if cmd == "/reply":
        try:
            target_id = int(context.args[0]); msg_text = " ".join(context.args[1:])
            conn = db_connect()
            conn.execute("UPDATE tickets SET status='closed' WHERE user_id=? AND shop_id=? AND status='open'", (target_id, shop_id))
            conn.commit(); conn.close()
            await Bot(current_external_store_token(context)).send_message(target_id, t(target_id, "support_reply", msg=msg_text), parse_mode="HTML")
            return await reply(f"✅ Reply sent to {target_id}")
        except Exception: return await reply("❌ Usage: /reply ID Message")

    if cmd == "/broadcast":
        msg_text = " ".join(context.args)
        if not msg_text: return await reply("❌ Usage: /broadcast MESSAGE")
        users = get_shop_users(shop_id)
        await reply(f"🚀 Broadcasting to {len(users)} users...")
        count = 0
        store_bot = Bot(current_external_store_token(context))
        for user_id in users:
            try: await store_bot.send_message(user_id, msg_text, parse_mode="HTML"); count += 1; await asyncio.sleep(0.04)
            except Exception: pass
        return await reply(f"✅ Done. Sent to {count} users.")

    return await reply("⚠️ Unknown command.")


# ── Bot token validation ─────────────────────────────────────────────────

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
        try: await bot.shutdown()
        except Exception: pass


# ── Add external shop wizard ─────────────────────────────────────────────

def clear_addextshop_wizard(context):
    for key in ["addextshop_step", "addextshop_shop_token", "addextshop_admin_token", "addextshop_owner_id"]:
        context.user_data.pop(key, None)


async def provision_external_shop(shop_token, admin_token, owner_id, title):
    try:
        add_external_shop_db(shop_token, admin_token, owner_id, title)
        row = get_external_shop_by_shop_token(shop_token)
        if not row: return False, "❌ Failed to save shop."
        ok, result = await start_external_shop_runtime(int(row["id"]))
        if ok: return True, f"✅ <b>External Shop Created!</b>\nTitle: <b>{title}</b>\n{result}"
        else: return False, f"❌ Saved but failed to start: {result}"
    except Exception as e:
        return False, f"❌ Error: {html.escape(str(e))}"


async def start_addextshop_wizard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.callback_query.message if update.callback_query else update.message
    context.user_data["addextshop_step"] = "shop_token"
    await msg.reply_text(
        "🌐 <b>External Shop Wizard — Step 1/3</b>\n\nSend <b>SHOP BOT TOKEN</b>.\n<i>Type /cancel to stop.</i>",
        parse_mode="HTML"
    )


async def handle_addextshop_wizard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    user_text = (msg.text or "").strip()
    step = context.user_data.get("addextshop_step")

    if user_text.lower() in {"cancel", "/cancel"}:
        clear_addextshop_wizard(context)
        return await msg.reply_text("🚫 Cancelled.")

    if step == "shop_token":
        if not is_probably_bot_token(user_text):
            return await msg.reply_text("❌ Invalid token.")
        if get_external_shop_by_shop_token(user_text) or get_external_shop_by_admin_token(user_text):
            return await msg.reply_text("⚠️ Token already in use.")
        ok, me, err = await inspect_bot_token(user_text)
        if not ok:
            return await msg.reply_text(f"❌ Could not validate: <code>{html.escape(str(err))}</code>", parse_mode="HTML")
        context.user_data["addextshop_shop_token"] = user_text
        context.user_data["addextshop_step"] = "admin_token"
        return await msg.reply_text(f"✅ Shop bot: @{me.username or me.id}\n\nStep 2/3\nSend <b>ADMIN BOT TOKEN</b>.", parse_mode="HTML")

    if step == "admin_token":
        if not is_probably_bot_token(user_text):
            return await msg.reply_text("❌ Invalid token.")
        if user_text == context.user_data.get("addextshop_shop_token"):
            return await msg.reply_text("⚠️ Must be different from shop token.")
        ok, me, err = await inspect_bot_token(user_text)
        if not ok:
            return await msg.reply_text(f"❌ Could not validate: <code>{html.escape(str(err))}</code>", parse_mode="HTML")
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


# ── External shop runtime management ─────────────────────────────────────

def _build_external_user_app(shop_row: dict):
    from handlers.app_builders import build_external_user_app
    return build_external_user_app(shop_row)


def _build_external_admin_app(shop_row: dict):
    from handlers.app_builders import build_external_admin_app
    return build_external_admin_app(shop_row)


async def start_external_shop_runtime(shop_id: int):
    row = get_external_shop_by_id(shop_id)
    if not row: return False, "Shop not found."
    if shop_id in EXTERNAL_USER_APPS or shop_id in EXTERNAL_ADMIN_APPS:
        return True, "Already active."
    try:
        user_app = _build_external_user_app(row)
        admin_app = _build_external_admin_app(row)
        await user_app.initialize()
        await admin_app.initialize()
        user_me = await user_app.bot.get_me()
        admin_me = await admin_app.bot.get_me()
        update_external_shop_usernames(shop_id, user_me.username or "", admin_me.username or "")
        await user_app.bot.set_my_commands(external_user_commands())
        await admin_app.bot.set_my_commands(ext_admin_commands())
        await user_app.start()
        await admin_app.start()
        _allowed = ["message", "callback_query", "channel_post", "my_chat_member"]
        await user_app.updater.start_polling(allowed_updates=_allowed)
        await admin_app.updater.start_polling(allowed_updates=_allowed)
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
                try: await app.stop(); await app.shutdown()
                except Exception: pass
        return False, str(e)


async def stop_external_shop_runtime(shop_id: int):
    user_app = EXTERNAL_USER_APPS.pop(shop_id, None)
    admin_app = EXTERNAL_ADMIN_APPS.pop(shop_id, None)
    for app in [user_app, admin_app]:
        if not app: continue
        try:
            if app.updater and app.updater.running: await app.updater.stop()
        except Exception: pass
        try: await app.stop()
        except Exception: pass
        try: await app.shutdown()
        except Exception: pass
