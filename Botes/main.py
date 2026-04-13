# -*- coding: utf-8 -*-
"""
Bot Entry Point — Modular Architecture
Starts main user bot, admin bot, external shop bots, and activation poller.
"""

import asyncio
import logging

from telegram import Bot

from config import MAIN_BOT_TOKEN, LOG_BOT_TOKEN, OWNER_ID, logger
from database import init_db
from models.external_shop import get_active_external_shops
from handlers.app_builders import build_main_user_app, build_main_admin_app
from handlers.external import start_external_shop_runtime
from handlers.user import activation_poller
from handlers.common import global_log_bot
import handlers.common as common_mod
from ui.keyboards import main_user_commands, owner_admin_commands


async def main():
    """Initialize database, build apps, start polling, and run forever."""
    logger.info("🚀 Initializing bot...")

    # ── Database ──────────────────────────────────────────────────────
    init_db()
    logger.info("✅ Database initialized")

    # ── Build main applications ───────────────────────────────────────
    user_app = build_main_user_app()
    admin_app = build_main_admin_app()

    # ── Initialize & start ────────────────────────────────────────────
    await user_app.initialize()
    await admin_app.initialize()

    # Set bot commands
    try:
        await user_app.bot.set_my_commands(main_user_commands())
    except Exception as e:
        logger.warning(f"Could not set user commands: {e}")

    try:
        await admin_app.bot.set_my_commands(owner_admin_commands())
    except Exception as e:
        logger.warning(f"Could not set admin commands: {e}")

    # Store log bot reference for global logging
    common_mod.global_log_bot = admin_app.bot

    await user_app.start()
    await admin_app.start()
    _allowed = ["message", "callback_query", "channel_post", "my_chat_member"]
    await user_app.updater.start_polling(drop_pending_updates=True, allowed_updates=_allowed)
    await admin_app.updater.start_polling(drop_pending_updates=True, allowed_updates=_allowed)

    user_me = await user_app.bot.get_me()
    admin_me = await admin_app.bot.get_me()
    logger.info(f"✅ User bot: @{user_me.username}")
    logger.info(f"✅ Admin bot: @{admin_me.username}")

    # ── External shops ────────────────────────────────────────────────
    ext_shops = get_active_external_shops()
    for shop in ext_shops:
        try:
            ok, info = await start_external_shop_runtime(int(shop["id"]))
            if ok:
                logger.info(f"✅ External shop #{shop['id']} ({shop['title']}): started")
            else:
                logger.warning(f"⚠️ External shop #{shop['id']}: {info}")
        except Exception as e:
            logger.error(f"❌ External shop #{shop['id']}: {e}")

    # ── Activation poller ─────────────────────────────────────────────
    poller_task = asyncio.create_task(activation_poller(user_app.bot))
    logger.info("✅ Activation poller started")

    # ── Binance Pay Auth Poller ───────────────────────────────────────
    from services.binance_pay_api import auto_verify_binance_pay, notify_owner_of_deposits
    async def binance_poller_loop(app):
        while True:
            try:
                await auto_verify_binance_pay(app)
            except Exception as e:
                pass
            try:
                await notify_owner_of_deposits(app)
            except Exception as e:
                pass
            await asyncio.sleep(60)

    binance_task = asyncio.create_task(binance_poller_loop(user_app))
    logger.info("✅ Binance Pay email poller started")

    # ── Startup notification ──────────────────────────────────────────
    try:
        await admin_app.bot.send_message(
            OWNER_ID,
            "🟢 <b>Bot Started (Modular)</b>\n\n"
            f"👤 User: @{user_me.username}\n"
            f"🛠️ Admin: @{admin_me.username}\n"
            f"🌐 External Shops: {len(ext_shops)}",
            parse_mode="HTML"
        )
    except Exception:
        pass

    logger.info("🟢 Bot is running. Press Ctrl+C to stop.")

    # ── Keep alive ────────────────────────────────────────────────────
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("🔴 Shutting down...")
    finally:
        poller_task.cancel()
        binance_task.cancel()
        try:
            await user_app.updater.stop()
            await admin_app.updater.stop()
        except Exception:
            pass
        try:
            await user_app.stop()
            await admin_app.stop()
        except Exception:
            pass
        try:
            await user_app.shutdown()
            await admin_app.shutdown()
        except Exception:
            pass
        logger.info("🔴 Bot stopped.")


if __name__ == "__main__":
    asyncio.run(main())
