# -*- coding: utf-8 -*-
"""Common handlers — support, add-product wizard, cancel, logging helpers."""

import asyncio

from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ContextTypes, ConversationHandler, CommandHandler,
    CallbackQueryHandler, MessageHandler, filters,
)

from config import (
    OWNER_ID, MAIN_BOT_TOKEN, ADMIN_LOG_ID, SUPPORT_USER, logger,
)
from database import db_connect
from localization import t, get_user_lang, LANGS
from models.user import (
    is_user_banned, update_user_info, update_shop_user_info,
    get_all_users,
)
from models.shop import add_product_db
from ui.menus import (
    current_bot_mode, current_shop_id, current_external_owner_id,
    current_external_title, current_external_admin_token,
    current_external_store_token,
)

# Conversation states
SUPPORT_CHAT = 100
ADD_NAME, ADD_PRICE, ADD_STOCK, ADD_CAT, ADD_DESC, ADD_IMG, ADD_FILE = range(200, 207)

# Global log bot reference
global_log_bot = None


# ── Logging via second bot ────────────────────────────────────────────────

async def send_log_via_second_bot(text: str, document=None, filename=None):
    if not global_log_bot or not ADMIN_LOG_ID:
        return
    try:
        if document:
            await global_log_bot.send_document(
                chat_id=ADMIN_LOG_ID, document=document,
                filename=filename, caption=text[:1000]
            )
        else:
            await global_log_bot.send_message(
                chat_id=ADMIN_LOG_ID, text=text, parse_mode="HTML"
            )
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
            await bot.send_message(
                uid,
                LANGS.get(lang, LANGS["en"]).get(key_name, key_name),
                parse_mode="HTML"
            )
            count += 1
            await asyncio.sleep(0.04)
        except Exception:
            pass
    return count


# ── Channel join check ────────────────────────────────────────────────────

async def check_channel_join(user_id: int, bot: Bot, mode: str = "main_user") -> bool:
    from config import REQUIRED_CHANNEL
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
    from config import REQUIRED_CHANNEL
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(
            t(user_id, "btn_join_ch"),
            url=f"https://t.me/{REQUIRED_CHANNEL.replace('@', '')}"
        )],
        [InlineKeyboardButton(t(user_id, "btn_i_joined"), callback_data="check_join")],
    ])
    if update.callback_query:
        await update.callback_query.message.reply_text(
            t(user_id, "must_join"), parse_mode="HTML", reply_markup=kb
        )
    else:
        await update.message.reply_text(
            t(user_id, "must_join"), parse_mode="HTML", reply_markup=kb
        )


# ── Support conversation ─────────────────────────────────────────────────

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


def build_support_conversation():
    return ConversationHandler(
        entry_points=[
            CallbackQueryHandler(cmd_support, pattern="^user_support$"),
            CommandHandler("support", cmd_support),
        ],
        states={
            SUPPORT_CHAT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_support_message)
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )


# ── Add product conversation ─────────────────────────────────────────────

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


# ── Cancel ────────────────────────────────────────────────────────────────

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.callback_query.message if update.callback_query else update.message
    if context.user_data.get("act_step") or context.user_data.get("pending_activation"):
        await msg.reply_text(
            "⛔ Cannot cancel during activation.\n\nPlease complete the activation process.",
            parse_mode="HTML"
        )
        return ConversationHandler.END
    context.user_data.pop("admin_action", None)
    context.user_data.pop("ext_admin_action", None)
    context.user_data.pop("waiting_for_credentials", None)
    context.user_data.pop("state", None)
    context.user_data.pop("addextshop_step", None)
    await msg.reply_text("🚫 Cancelled.")
    return ConversationHandler.END
