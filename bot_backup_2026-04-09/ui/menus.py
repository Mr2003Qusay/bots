# -*- coding: utf-8 -*-
"""Context helper functions and command sync."""

from telegram import Bot, BotCommandScopeChat
from telegram.ext import ContextTypes

from config import logger
from ui.keyboards import (
    main_user_commands, external_user_commands, owner_admin_commands,
    reseller_admin_commands, basic_admin_commands, ext_admin_commands,
)


# ── Context helpers ───────────────────────────────────────────────────────

def current_shop_id(context: ContextTypes.DEFAULT_TYPE) -> int:
    return int(context.bot_data.get("shop_id", 0))


def current_bot_mode(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.bot_data.get("bot_mode", "main_user")


def current_external_owner_id(context: ContextTypes.DEFAULT_TYPE) -> int:
    return int(context.bot_data.get("external_owner_id", 0))


def current_external_title(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.bot_data.get("external_title", "External Shop")


def current_external_store_token(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.bot_data.get("external_store_token", "")


def current_external_admin_token(context: ContextTypes.DEFAULT_TYPE) -> str:
    return context.bot_data.get("external_admin_token", "")


# ── Command sync ─────────────────────────────────────────────────────────

async def sync_commands_for_chat(bot: Bot, chat_id: int, mode: str,
                                  is_owner: bool = False, is_reseller_user: bool = False):
    try:
        scope = BotCommandScopeChat(chat_id=chat_id)
        if mode == "main_user":
            from config import MAINTENANCE_MODE
            cmds = main_user_commands()
            if MAINTENANCE_MODE:
                cmds = [cmd for cmd in cmds if cmd.command != "activate"]
            await bot.set_my_commands(cmds, scope=scope)
        elif mode == "external_user":
            await bot.set_my_commands(external_user_commands(), scope=scope)
        elif mode == "main_admin":
            if is_owner:
                await bot.set_my_commands(owner_admin_commands(), scope=scope)
            elif is_reseller_user:
                await bot.set_my_commands(reseller_admin_commands(), scope=scope)
            else:
                await bot.set_my_commands(basic_admin_commands(), scope=scope)
        elif mode == "ext_admin":
            await bot.set_my_commands(ext_admin_commands(), scope=scope)
    except Exception as e:
        logger.warning(f"Could not sync commands for chat {chat_id}: {e}")


# ── Clear all user flow states ────────────────────────────────────────────

def clear_all_user_flow_states(context):
    """Clear all pending flow states to prevent cross-flow contamination.
    Does NOT clear permanent preferences like ws_last_email."""
    for key in [
        # Activation flow
        "act_step", "pending_activation", "act_email", "act_password",
        # Shop buy flow
        "state", "buying_pid", "buying_shop_id", "buy_final_qty",
        "buy_final_cost", "buying_manual_delivery",
        # Deposit flow
        "deposit_step", "deposit_network",
    ]:
        context.user_data.pop(key, None)
