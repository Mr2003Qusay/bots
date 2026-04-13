# -*- coding: utf-8 -*-
"""Inline keyboard builders — all bot menus."""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from localization import t


def build_main_user_keyboard(uid: int, show_activate: bool = True):
    rows = []
    if show_activate:
        rows.append([
            InlineKeyboardButton(t(uid, "btn_activate"), callback_data="user_activate"),
            InlineKeyboardButton(t(uid, "btn_shop"), callback_data="user_shop"),
        ])
    else:
        rows.append([InlineKeyboardButton(t(uid, "btn_shop"), callback_data="user_shop")])
    rows.extend([
        [
            InlineKeyboardButton(t(uid, "btn_profile"), callback_data="user_profile"),
            InlineKeyboardButton(t(uid, "btn_deposit"), callback_data="user_deposit"),
        ],
        [
            InlineKeyboardButton(t(uid, "btn_invite"), callback_data="user_invite"),
            InlineKeyboardButton(t(uid, "btn_daily"), callback_data="user_daily"),
        ],
        [
            InlineKeyboardButton(t(uid, "btn_support"), callback_data="user_support"),
            InlineKeyboardButton(t(uid, "btn_help"), callback_data="user_help"),
        ],
    ])
    return InlineKeyboardMarkup(rows)


def build_external_user_keyboard(uid: int):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(t(uid, "btn_shop"), callback_data="user_shop"),
            InlineKeyboardButton(t(uid, "btn_profile"), callback_data="user_profile"),
        ],
        [
            InlineKeyboardButton(t(uid, "btn_support"), callback_data="user_support"),
            InlineKeyboardButton(t(uid, "btn_help"), callback_data="user_help"),
        ],
    ])


def build_main_admin_keyboard(uid: int):
    from config import OWNER_ID
    from models.reseller import is_reseller

    rows = []
    if uid == OWNER_ID:
        rows.extend([
            [InlineKeyboardButton("📊 Stats", callback_data="adm_stats"), InlineKeyboardButton("💰 Balance", callback_data="adm_balance")],
            [InlineKeyboardButton("👥 Users", callback_data="adm_users"), InlineKeyboardButton("💾 Backup", callback_data="adm_backup")],
            [InlineKeyboardButton("📦 Data Export", callback_data="adm_data")],
            [InlineKeyboardButton("💼 Reseller Tools", callback_data="adm_cat_reseller"), InlineKeyboardButton("👑 Owner Mgmt", callback_data="adm_cat_owner")],
            [InlineKeyboardButton("🛒 Shop Manager", callback_data="adm_cat_shop"), InlineKeyboardButton("🌐 External Shops", callback_data="adm_cat_external")],
            [InlineKeyboardButton("⚙️ System Tools", callback_data="adm_cat_system"), InlineKeyboardButton("⚡ API Control", callback_data="adm_cat_api")],
        ])
    elif is_reseller(uid):
        rows.extend([
            [InlineKeyboardButton("📊 My Stats", callback_data="adm_stats"), InlineKeyboardButton("💰 My Balance", callback_data="adm_balance")],
            [InlineKeyboardButton("👥 My Users", callback_data="adm_users")],
            [InlineKeyboardButton("💼 Reseller Tools", callback_data="adm_cat_reseller")],
        ])
    rows.append([
        InlineKeyboardButton(t(uid, "btn_help"), callback_data="adm_help"),
        InlineKeyboardButton(t(uid, "btn_lang"), callback_data="user_lang"),
    ])
    return InlineKeyboardMarkup(rows)


def build_ext_admin_keyboard(uid: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Stats", callback_data="ext_stats"), InlineKeyboardButton("💰 Wallet", callback_data="ext_wallet")],
        [InlineKeyboardButton("📦 Add Product", callback_data="ext_act_addprod"), InlineKeyboardButton("📋 Products", callback_data="ext_act_listprod")],
        [InlineKeyboardButton("🔐 Add Code", callback_data="ext_act_addcode"), InlineKeyboardButton("📥 Bulk Codes", callback_data="ext_act_addcodes")],
        [InlineKeyboardButton("➕ Add User $", callback_data="ext_act_addshop"), InlineKeyboardButton("➖ Remove User $", callback_data="ext_act_removeshop")],
        [InlineKeyboardButton("🔍 Check User", callback_data="ext_act_check"), InlineKeyboardButton("📢 Broadcast", callback_data="ext_act_broadcast")],
        [InlineKeyboardButton("💬 Reply Ticket", callback_data="ext_act_reply"), InlineKeyboardButton("🏷️ Title", callback_data="ext_act_settitle")],
        [InlineKeyboardButton(t(uid, "btn_help"), callback_data="ext_help"), InlineKeyboardButton(t(uid, "btn_lang"), callback_data="user_lang")],
    ])


def build_help_keyboard(uid: int, mode: str):
    if mode == "main_admin":
        return build_main_admin_keyboard(uid)
    if mode == "ext_admin":
        return build_ext_admin_keyboard(uid)
    if mode == "external_user":
        return InlineKeyboardMarkup([
            [InlineKeyboardButton(t(uid, "btn_shop"), callback_data="user_shop"), InlineKeyboardButton(t(uid, "btn_profile"), callback_data="user_profile")],
            [InlineKeyboardButton(t(uid, "btn_support"), callback_data="user_support"), InlineKeyboardButton(t(uid, "btn_home"), callback_data="user_home")],
        ])
    # main_user
    from config import MAINTENANCE_MODE
    rows = []
    if not MAINTENANCE_MODE:
        rows.append([InlineKeyboardButton(t(uid, "btn_activate"), callback_data="user_activate"), InlineKeyboardButton(t(uid, "btn_shop"), callback_data="user_shop")])
    else:
        rows.append([InlineKeyboardButton(t(uid, "btn_shop"), callback_data="user_shop")])
    rows.extend([
        [InlineKeyboardButton(t(uid, "btn_profile"), callback_data="user_profile"), InlineKeyboardButton(t(uid, "btn_deposit"), callback_data="user_deposit")],
        [InlineKeyboardButton(t(uid, "btn_support"), callback_data="user_support"), InlineKeyboardButton(t(uid, "btn_home"), callback_data="user_home")],
    ])
    return InlineKeyboardMarkup(rows)


# ── Bot command menus ─────────────────────────────────────────────────────

def main_user_commands():
    return [
        BotCommand("start", "Start"),
        BotCommand("activate", "Activate Google One 5TB (12 Months)"),
        BotCommand("shop", "Shop"),
        BotCommand("profile", "Profile"),
        BotCommand("deposit", "Deposit"),
        BotCommand("claim", "Claim deposit"),
        BotCommand("invite", "Invite"),
        BotCommand("daily", "Daily"),
        BotCommand("history", "History"),
        BotCommand("language", "Language"),
        BotCommand("help", "Help"),
        BotCommand("support", "Support"),
    ]


def external_user_commands():
    return [
        BotCommand("start", "Store"),
        BotCommand("shop", "Shop"),
        BotCommand("profile", "Profile"),
        BotCommand("language", "Language"),
        BotCommand("help", "Help"),
        BotCommand("support", "Support"),
    ]


def owner_admin_commands():
    return [
        BotCommand("start", "Admin panel"),
        BotCommand("help", "Help"),
        BotCommand("myinvite", "My invite"),
        BotCommand("add", "Add balance"),
        BotCommand("remove", "Remove balance"),
        BotCommand("addshop", "Add shop $"),
        BotCommand("removeshop", "Remove shop $"),
        BotCommand("check", "Check user"),
        BotCommand("addreseller", "Add reseller"),
        BotCommand("delreseller", "Del reseller"),
        BotCommand("setprice", "Set price"),
        BotCommand("setprofit", "Set profit"),
        BotCommand("addrc", "Add reseller $"),
        BotCommand("removerc", "Remove reseller $"),
        BotCommand("rusers", "Reseller users"),
        BotCommand("rlink", "Link user"),
        BotCommand("runlink", "Unlink user"),
        BotCommand("addprod", "Add product"),
        BotCommand("delprod", "Del product"),
        BotCommand("listprod", "List products"),
        BotCommand("addcode", "Add code"),
        BotCommand("addcodes", "Bulk codes"),
        BotCommand("addextshop", "Add ext shop"),
        BotCommand("delextshop", "Del ext shop"),
        BotCommand("listextshops", "List ext shops"),
        BotCommand("broadcast", "Broadcast"),
        BotCommand("broadcast_inactive", "Broadcast inactive"),
        BotCommand("maintenance", "Toggle maint."),
        BotCommand("ban", "Ban user"),
        BotCommand("unban", "Unban user"),
        BotCommand("reply", "Reply ticket"),
        BotCommand("resellers", "Resellers report"),
        BotCommand("myinvite", "My invite"),
        BotCommand("language", "Language"),
    ]


def reseller_admin_commands():
    return [
        BotCommand("start", "Reseller panel"),
        BotCommand("help", "Help"),
        BotCommand("myinvite", "My invite"),
        BotCommand("add", "Add balance"),
        BotCommand("remove", "Remove balance"),
        BotCommand("check", "Check user"),
        BotCommand("resellers", "My report"),
        BotCommand("language", "Language"),
    ]


def basic_admin_commands():
    return [
        BotCommand("start", "Admin panel"),
        BotCommand("help", "Help"),
        BotCommand("language", "Language"),
    ]


def ext_admin_commands():
    return [
        BotCommand("start", "Store control"),
        BotCommand("help", "Help"),
        BotCommand("addshop", "Add wallet $"),
        BotCommand("removeshop", "Remove wallet $"),
        BotCommand("check", "Check user"),
        BotCommand("addprod", "Add product"),
        BotCommand("delprod", "Delete product"),
        BotCommand("listprod", "List products"),
        BotCommand("addcode", "Add one code"),
        BotCommand("addcodes", "Add bulk codes"),
        BotCommand("reply", "Reply ticket"),
        BotCommand("broadcast", "Broadcast"),
        BotCommand("settitle", "Set title"),
    ]
