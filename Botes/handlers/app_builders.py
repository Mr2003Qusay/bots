# -*- coding: utf-8 -*-
"""Application builders — construct Telegram Application instances for each bot mode."""

import datetime
from zoneinfo import ZoneInfo

from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, PicklePersistence,
)

from config import MAIN_BOT_TOKEN, LOG_BOT_TOKEN, PERSISTENCE_PATH
from services.blockchain import check_blockchain_deposits
from services.backup import scheduled_backup

from handlers.common import (
    build_support_conversation, build_add_product_conversation, cancel,
)
from handlers.user import (
    start, cmd_activate, cmd_shop, cmd_profile, cmd_help,
    cmd_invite, cmd_language, cmd_daily, cmd_history,
    cmd_deposit, cmd_claim, cmd_myinvite,
)
from handlers.callbacks import (
    callback_lang, callback_shop_handler, callback_main_menu, text_handler,
    cmd_support_entry,
)
from handlers.admin import (
    main_admin_cmds_handler, callback_main_admin_menu,
)
from handlers.external import (
    callback_ext_admin_menu, ext_admin_cmds_handler,
)


# ── Global error handler ─────────────────────────────────────────────────

async def error_handler(update, context):
    """Log errors and send to admin."""
    from config import OWNER_ID, logger
    import traceback
    tb = traceback.format_exception(type(context.error), context.error, context.error.__traceback__)
    tb_str = "".join(tb)[-2000:]
    logger.error(f"Exception: {context.error}\n{tb_str}")
    try:
        from handlers.common import global_log_bot
        if global_log_bot:
            await global_log_bot.send_message(
                OWNER_ID,
                f"⚠️ <b>Error</b>\n<pre>{tb_str[:3500]}</pre>",
                parse_mode="HTML"
            )
    except Exception:
        pass


async def unhandled_callback(update, context):
    """Catch-all for unmatched callbacks — log them for debugging."""
    if update.callback_query:
        from config import logger
        logger.warning(f"UNHANDLED CALLBACK: {update.callback_query.data} from user {update.callback_query.from_user.id}")
        try:
            await update.callback_query.answer()
        except Exception:
            pass

def build_main_user_app():
    app = (
        Application.builder()
        .token(MAIN_BOT_TOKEN)
        .connect_timeout(60).read_timeout(60)
        .write_timeout(60).pool_timeout(60)
        .build()
    )
    app.bot_data["bot_mode"] = "main_user"
    app.bot_data["shop_id"] = 0

    # Scheduled jobs
    app.job_queue.run_repeating(check_blockchain_deposits, interval=300, first=120)
    _beirut = ZoneInfo("Asia/Beirut")
    app.job_queue.run_daily(scheduled_backup, time=datetime.time(0, 0, 0, tzinfo=_beirut))
    app.job_queue.run_daily(scheduled_backup, time=datetime.time(12, 0, 0, tzinfo=_beirut))

    # Command handlers
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
    app.add_handler(CommandHandler("support", cmd_support_entry))
    app.add_handler(CommandHandler("cancel", cancel))

    # Callback handlers
    app.add_handler(CallbackQueryHandler(callback_lang, pattern="^lang_|^check_join$"))
    app.add_handler(CallbackQueryHandler(callback_shop_handler, pattern="^(shop_|view_prod_|buy_ask_)"))
    app.add_handler(CallbackQueryHandler(callback_main_menu, pattern="^user_|^dep_|^confirm_activate_|^cancel_activate$|^act_cancel_flow$|^cancel_deposit$"))

    # Text handler
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

    # Catch-all + error handler
    app.add_handler(CallbackQueryHandler(unhandled_callback))
    app.add_error_handler(error_handler)

    return app


def build_main_admin_app():
    app = (
        Application.builder()
        .token(LOG_BOT_TOKEN)
        .connect_timeout(60).read_timeout(60)
        .write_timeout(60).pool_timeout(60)
        .build()
    )
    app.bot_data["bot_mode"] = "main_admin"
    app.bot_data["shop_id"] = 0

    app.add_handler(build_add_product_conversation("^act_addprod$"))
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("myinvite", cmd_myinvite))
    app.add_handler(CommandHandler("language", cmd_language))
    app.add_handler(CommandHandler([
        "add", "remove", "addshop", "removeshop", "addreseller", "delreseller",
        "backup", "setprice", "setprofit", "addrc", "removerc",
        "rusers", "rlink", "runlink", "check",
        "reply", "broadcast", "broadcast_inactive", "maintenance", "ban", "unban",
        "listprod", "delprod", "addcode", "addcodes",
        "addextshop", "delextshop", "listextshops", "resellers"
    ], main_admin_cmds_handler))
    app.add_handler(CallbackQueryHandler(callback_main_admin_menu, pattern="^adm_|^act_|^api_|^maint_notify_|^user_lang$|^user_home$|^shop_mgr_"))
    app.add_handler(CallbackQueryHandler(callback_lang, pattern="^lang_"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_handler(CallbackQueryHandler(unhandled_callback))
    app.add_error_handler(error_handler)

    return app


def build_external_user_app(shop_row: dict):
    app = (
        Application.builder()
        .token(shop_row["shop_token"])
        .connect_timeout(30).read_timeout(30)
        .write_timeout(30).pool_timeout(30)
        .build()
    )
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
    app.add_handler(CallbackQueryHandler(callback_lang, pattern="^lang_|^check_join$"))
    app.add_handler(CallbackQueryHandler(callback_shop_handler, pattern="^(shop_|view_prod_|buy_ask_)"))
    app.add_handler(CallbackQueryHandler(callback_main_menu, pattern="^user_"))
    app.add_handler(build_support_conversation())
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_handler(CallbackQueryHandler(unhandled_callback))
    app.add_error_handler(error_handler)

    return app


def build_external_admin_app(shop_row: dict):
    app = (
        Application.builder()
        .token(shop_row["admin_token"])
        .connect_timeout(30).read_timeout(30)
        .write_timeout(30).pool_timeout(30)
        .build()
    )
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
        "addshop", "removeshop", "check", "delprod", "listprod",
        "addcode", "addcodes", "reply", "broadcast", "settitle"
    ], ext_admin_cmds_handler))
    app.add_handler(CallbackQueryHandler(callback_ext_admin_menu, pattern="^ext_|^user_home$|^user_lang$|^shop_mgr_"))
    app.add_handler(CallbackQueryHandler(callback_lang, pattern="^lang_"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_handler(CallbackQueryHandler(unhandled_callback))
    app.add_error_handler(error_handler)

    return app
