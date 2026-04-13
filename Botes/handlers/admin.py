# -*- coding: utf-8 -*-
"""Admin handlers — main admin start, callback menu, command handler."""

import io
import os
import re
import html
import time
import asyncio
import zipfile
import datetime
import sqlite3

from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from config import (
    OWNER_ID, MAIN_BOT_TOKEN, LOG_BOT_TOKEN, MAINTENANCE_MODE,
    DB_PATH, IQLESS_API_KEY, IQLESS_BASE_URL, REQUIRED_CHANNEL,
    SUPPORT_USER, logger,
)
import config as _cfg
from database import db_connect
from localization import t, get_user_lang
from models.user import (
    is_user_banned, get_user_data, get_user_balance, get_total_users,
    add_balance, get_all_users, get_all_users_detailed,
    get_activate_price_for_user, set_activate_price, get_shop_balance,
    add_shop_balance, ban_user, set_user_owner, get_id_by_username,
    set_maintenance_mode, get_stats, get_user_invitees,
)
from models.reseller import (
    is_reseller, get_reseller_balance, get_reseller_stats,
    get_reseller_clients_detailed, get_reseller_clients,
    reseller_give_balance, reseller_remove_balance,
    add_reseller_balance, delete_reseller,
    get_reseller_profit, set_reseller_profit,
)
from models.shop import (
    get_all_products, get_product, del_product,
    add_product_code, add_product_codes_bulk,
)
from models.external_shop import (
    get_external_shops, get_external_shop_by_id,
    get_external_shop_by_shop_token, get_external_shop_by_admin_token,
    remove_external_shop_db,
)
from services.iqless_api import (
    iqless_get_balance, iqless_get_queue, iqless_poll_job,
    iqless_submit_job, iqless_cancel_job, iqless_pick_best_device,
)
from services.backup import do_backup
from ui.keyboards import build_main_admin_keyboard
from ui.menus import (
    current_bot_mode, current_shop_id, sync_commands_for_chat,
)
from handlers.common import (
    send_log_via_second_bot, broadcast_system_msg,
)
from handlers.user import (
    cmd_help, cmd_myinvite, active_jobs,
)

import httpx


# ── Main admin start ─────────────────────────────────────────────────────

async def main_admin_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    msg = update.callback_query.message if update.callback_query else update.message
    if uid != OWNER_ID and not is_reseller(uid):
        return await msg.reply_text("⛔ Access Denied")
    await sync_commands_for_chat(context.bot, uid, "main_admin",
                                  is_owner=(uid == OWNER_ID),
                                  is_reseller_user=is_reseller(uid))
    text = t(uid, "welcome_admin")
    kb = build_main_admin_keyboard(uid)
    if update.callback_query:
        try:
            await msg.edit_text(text, parse_mode="HTML", reply_markup=kb)
        except Exception:
            await msg.reply_text(text, parse_mode="HTML", reply_markup=kb)
    else:
        await msg.reply_text(text, parse_mode="HTML", reply_markup=kb)


# ── Admin callback menu ──────────────────────────────────────────────────

async def callback_main_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = query.from_user.id
    if uid != OWNER_ID and not is_reseller(uid):
        return

    data = query.data

    if data == "adm_home":
        return await main_admin_start(update, context)
    if data == "adm_help":
        return await cmd_help(update, context)
    if data == "user_lang":
        from handlers.user import cmd_language
        return await cmd_language(update, context)
    if data == "user_home":
        return await main_admin_start(update, context)

    # ── Enhanced Shop Management ──────────────────────────────────────
    if data.startswith("shop_mgr_"):
        from models.shop import (
            get_product_detailed, update_product, toggle_product_hidden,
            get_product_sales, get_product_total_sales, get_available_code_count,
        )
        parts = data.split("_", 3)
        action = parts[2] if len(parts) > 2 else ""
        shop_id = current_shop_id(context)

        if action == "list":
            prods = get_all_products(shop_id)
            if not prods:
                kb = [[InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_cat_shop" if shop_id == 0 else "user_home")]]
                return await query.message.edit_text("📭 No products.", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))
            rows = []
            for p in prods:
                hidden_icon = "🔴" if p.get("hidden") else "🟢"
                rows.append([InlineKeyboardButton(
                    f"{hidden_icon} {p['name']} (${float(p['price']):.2f}) [{int(p['stock'])}]",
                    callback_data=f"shop_mgr_view_{p['id']}"
                )])
            rows.append([InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_cat_shop" if shop_id == 0 else "user_home")])
            return await query.message.edit_text("🛒 <b>Products Manager</b>\n🟢 = Active  🔴 = Hidden", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows))

        if action == "view":
            pid = int(parts[3]) if len(parts) > 3 else 0
            p = get_product_detailed(shop_id, pid)
            if not p:
                return await query.answer("❌ Product not found", show_alert=True)
            hidden_label = "🔴 Hidden" if p["hidden"] else "🟢 Visible"
            delivery = p.get("delivery_type", "manual")
            txt = (
                f"📦 <b>{p['name']}</b>\n\n"
                f"💰 Price: <b>${float(p['price']):.2f}</b>\n"
                f"📂 Category: {p.get('category', 'General')}\n"
                f"📊 Stock: <b>{int(p['stock'])}</b> ({p['available_codes']} codes ready)\n"
                f"📋 Delivery: {delivery}\n"
                f"👁 Status: {hidden_label}\n"
                f"📝 Description: {p.get('description') or '—'}\n\n"
                f"📈 <b>Sales:</b> {p['total_orders']} orders | ${p['total_revenue']:.2f} revenue"
            )
            kb = [
                [
                    InlineKeyboardButton("✏️ Name", callback_data=f"shop_mgr_editname_{pid}"),
                    InlineKeyboardButton("💲 Price", callback_data=f"shop_mgr_editprice_{pid}"),
                ],
                [
                    InlineKeyboardButton("📝 Desc", callback_data=f"shop_mgr_editdesc_{pid}"),
                    InlineKeyboardButton("📂 Category", callback_data=f"shop_mgr_editcat_{pid}"),
                ],
                [
                    InlineKeyboardButton("🔐 Add Code", callback_data=f"shop_mgr_code_{pid}"),
                    InlineKeyboardButton("📥 Bulk Codes", callback_data=f"shop_mgr_bulk_{pid}"),
                ],
                [
                    InlineKeyboardButton("📊 Stock", callback_data=f"shop_mgr_editqty_{pid}"),
                    InlineKeyboardButton("📈 Sales", callback_data=f"shop_mgr_sales_{pid}"),
                ],
                [
                    InlineKeyboardButton(
                        "🟢 Show" if p["hidden"] else "🔴 Hide",
                        callback_data=f"shop_mgr_toggle_{pid}"
                    ),
                ],
                [InlineKeyboardButton("🗑️ Delete", callback_data=f"shop_mgr_delask_{pid}")],
                [InlineKeyboardButton("🔙 Products", callback_data="shop_mgr_list")],
            ]
            return await query.message.edit_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

        if action == "toggle":
            pid = int(parts[3]) if len(parts) > 3 else 0
            new_hidden = toggle_product_hidden(shop_id, pid)
            await query.answer(f"{'Hidden' if new_hidden else 'Visible'}", show_alert=True)
            # Inline refresh of product view
            p = get_product_detailed(shop_id, pid)
            if not p:
                return
            hidden_label = "🔴 Hidden" if p["hidden"] else "🟢 Visible"
            delivery = p.get("delivery_type", "manual")
            txt = (
                f"📦 <b>{p['name']}</b>\n\n"
                f"💰 Price: <b>${float(p['price']):.2f}</b>\n"
                f"📂 Category: {p.get('category', 'General')}\n"
                f"📊 Stock: <b>{int(p['stock'])}</b> ({p['available_codes']} codes ready)\n"
                f"📋 Delivery: {delivery}\n"
                f"👁 Status: {hidden_label}\n"
                f"📝 Description: {p.get('description') or '—'}\n\n"
                f"📈 <b>Sales:</b> {p['total_orders']} orders | ${p['total_revenue']:.2f} revenue"
            )
            kb = [
                [InlineKeyboardButton("✏️ Name", callback_data=f"shop_mgr_editname_{pid}"),
                 InlineKeyboardButton("💲 Price", callback_data=f"shop_mgr_editprice_{pid}")],
                [InlineKeyboardButton("📝 Desc", callback_data=f"shop_mgr_editdesc_{pid}"),
                 InlineKeyboardButton("📂 Category", callback_data=f"shop_mgr_editcat_{pid}")],
                [InlineKeyboardButton("🔐 Add Code", callback_data=f"shop_mgr_code_{pid}"),
                 InlineKeyboardButton("📥 Bulk Codes", callback_data=f"shop_mgr_bulk_{pid}")],
                [InlineKeyboardButton("📊 Stock", callback_data=f"shop_mgr_editqty_{pid}"),
                 InlineKeyboardButton("📈 Sales", callback_data=f"shop_mgr_sales_{pid}")],
                [InlineKeyboardButton("🟢 Show" if p["hidden"] else "🔴 Hide", callback_data=f"shop_mgr_toggle_{pid}")],
                [InlineKeyboardButton("🗑️ Delete", callback_data=f"shop_mgr_delask_{pid}")],
                [InlineKeyboardButton("🔙 Products", callback_data="shop_mgr_list")],
            ]
            return await query.message.edit_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

        if action == "sales":
            pid = int(parts[3]) if len(parts) > 3 else 0
            sales = get_product_sales(shop_id, pid, limit=15)
            p = get_product(shop_id, pid)
            if not sales:
                txt = f"📈 <b>{p['name'] if p else 'Product'}</b>\n\nNo sales yet."
            else:
                orders, revenue = get_product_total_sales(shop_id, pid)
                txt = f"📈 <b>{p['name'] if p else 'Product'} Sales</b>\n📊 {orders} orders | ${revenue:.2f} total\n\n"
                for s in sales:
                    txt += f"👤 @{s.get('username', 'N/A')} (<code>{s['user_id']}</code>)\n   x{s['qty']} = ${float(s['price']):.2f} | {s.get('ts', '')}\n"
            kb = [[InlineKeyboardButton("🔙 Back", callback_data=f"shop_mgr_view_{pid}")]]
            return await query.message.edit_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

        if action == "delask":
            pid = int(parts[3]) if len(parts) > 3 else 0
            p = get_product(shop_id, pid)
            kb = [
                [
                    InlineKeyboardButton("✅ Yes, Delete", callback_data=f"shop_mgr_delyes_{pid}"),
                    InlineKeyboardButton("❌ Cancel", callback_data=f"shop_mgr_view_{pid}"),
                ]
            ]
            return await query.message.edit_text(
                f"⚠️ <b>Delete '{p['name'] if p else pid}'?</b>\n\nThis will remove the product and all its codes. This cannot be undone.",
                parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb)
            )

        if action == "delyes":
            pid = int(parts[3]) if len(parts) > 3 else 0
            p = get_product(shop_id, pid)
            del_product(shop_id, pid)
            await query.answer(f"✅ Deleted {p['name'] if p else pid}", show_alert=True)
            # Inline refresh of product list
            products = get_all_products(shop_id)
            if not products:
                kb = [[InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_cat_shop" if shop_id == 0 else "user_home")]]
                return await query.message.edit_text("📭 No products.", reply_markup=InlineKeyboardMarkup(kb))
            rows = []
            for prod in products:
                rows.append([InlineKeyboardButton(
                    f"{'🔴' if prod.get('hidden') else '✅'} {prod['name']} (${float(prod['price']):.2f}) [{int(prod['stock'])}]",
                    callback_data=f"shop_mgr_view_{prod['id']}"
                )])
            rows.append([InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_cat_shop" if shop_id == 0 else "user_home")])
            return await query.message.edit_text("📋 <b>Products Manager</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(rows))

        # Edit actions — set admin_action and prompt
        edit_actions = {
            "editname": ("shop_mgr_setname", "✏️ <b>New Product Name:</b>"),
            "editprice": ("shop_mgr_setprice", "💲 <b>New Price ($):</b>"),
            "editdesc": ("shop_mgr_setdesc", "📝 <b>New Description:</b>"),
            "editcat": ("shop_mgr_setcat", "📂 <b>New Category:</b>"),
            "editqty": ("shop_mgr_setstock", "📊 <b>New Stock Quantity:</b>\n(Enter a number)"),
            "code": ("shop_mgr_addcode", "🔐 <b>Send one code:</b>"),
            "bulk": ("shop_mgr_addbulk", "📥 <b>Send codes (one per line):</b>"),
        }
        if action in edit_actions:
            pid = int(parts[3]) if len(parts) > 3 else 0
            admin_key, prompt = edit_actions[action]
            context.user_data["admin_action"] = admin_key
            context.user_data["shop_mgr_pid"] = pid
            return await query.message.reply_text(prompt, parse_mode="HTML")

    if data == "adm_backup" and uid == OWNER_ID:
        await query.answer("")
        msg = await query.message.reply_text("💾 Creating and sending backup...")
        result = await do_backup(bot=context.bot)
        await msg.edit_text(f"💾 <b>Backup</b>\n\n{result}", parse_mode="HTML")
        return

    if data == "adm_stats":
        kb = [[InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")]]
        if uid == OWNER_ID:
            u, d, r = get_stats()
            txt = f"📊 <b>Stats</b>\n👥 Users: {u}\n💰 Deposits: ${d:.2f}\n💼 Resellers: {r}"
        else:
            txt = f"📊 <b>My Stats</b>\n👥 My Users: {get_reseller_stats(uid)}"
        return await query.message.edit_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

    if data == "adm_balance":
        kb = [[InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")]]
        res_bal = get_reseller_balance(uid) if is_reseller(uid) else 0
        user_bal = get_user_balance(uid)
        txt = f"💰 <b>My Balance:</b> ${user_bal:.2f}\n💼 <b>Reseller Wallet:</b> ${res_bal:.2f}"
        return await query.message.edit_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

    if data == "adm_users":
        kb = [[InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")]]
        if uid == OWNER_ID:
            total = get_total_users()
            all_users = get_all_users_detailed()
            header = f"Total Users: {total}\n"
            header += f"{'ID':<15} {'Username':<25} {'Balance':>8} {'Acts':>5} {'Invites':>8}\n"
            header += "-" * 65 + "\n"
            content = header + "\n".join([
                f"{u['user_id']:<15} {('@'+u['username']):<25} ${u['balance']:>7.2f} {u['activations']:>5} {u['invites']:>8}"
                for u in all_users
            ])
            bio = io.BytesIO(content.encode("utf-8"))
            bio.name = "users.txt"
            try:
                await query.message.reply_document(document=bio, caption=f"👥 <b>Total Users: {total}</b>", parse_mode="HTML")
            except Exception:
                await query.message.reply_text(f"👥 <b>Total Users:</b> {total}", parse_mode="HTML")
        else:
            clients = get_reseller_clients_detailed(uid)
            if not clients:
                await query.message.edit_text("👥 <b>My Clients</b>\n\nNo clients.", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))
            elif len(clients) <= 30:
                rows_txt = []
                for c in clients:
                    rows_txt.append(f"🆔 <code>{c['user_id']}</code> | @{c['username']}\n   💰 ${c['balance']:.2f} | ✅ {c['activations']} acts | 👥 {c['invites']} invites")
                txt = f"👥 <b>My Clients ({len(clients)} total)</b>\n\n" + "\n\n".join(rows_txt)
                await query.message.edit_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))
            else:
                header = f"{'ID':<15} {'Username':<25} {'Balance':>8} {'Acts':>5} {'Invites':>8}\n" + "-" * 65 + "\n"
                lines = header + "\n".join([
                    f"{c['user_id']:<15} {('@'+c['username']):<25} ${c['balance']:>7.2f} {c['activations']:>5} {c['invites']:>8}"
                    for c in clients
                ])
                bio = io.BytesIO(lines.encode("utf-8"))
                bio.name = f"clients_{uid}.txt"
                await query.message.reply_document(document=bio, caption=f"👥 <b>My Clients — {len(clients)} total</b>", parse_mode="HTML")
        return

    if data == "adm_data" and uid == OWNER_ID:
        try:
            bio = io.BytesIO()
            today = str(datetime.date.today())
            essential_files = ["bot.py", "bot.db", "pyproject.toml", "uv.lock", "main.py"]
            with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as zf:
                for fname in essential_files:
                    if os.path.isfile(fname):
                        try: zf.write(fname, fname)
                        except Exception: pass
                for dname in ["storage", "models", "services", "handlers", "ui"]:
                    if os.path.isdir(dname):
                        for root, dirs, files in os.walk(dname):
                            dirs[:] = [d for d in dirs if d not in ["__pycache__"]]
                            for fn in files:
                                if fn.endswith((".db-journal", ".db-wal", ".db-shm")): continue
                                try: zf.write(os.path.join(root, fn), os.path.join(root, fn))
                                except Exception: pass
            bio.seek(0)
            size_kb = bio.getbuffer().nbytes // 1024
            return await query.message.reply_document(
                document=bio, filename=f"bot_backup_{today}.zip",
                caption=f"📦 <b>Bot Backup — {today}</b>\n📏 Size: {size_kb} KB", parse_mode="HTML"
            )
        except Exception as e:
            return await query.message.reply_text(f"❌ Error: {e}")

    # ── Category menus ────────────────────────────────────────────────
    if data == "adm_cat_reseller":
        kb = [
            [InlineKeyboardButton("➕ Add Balance", callback_data="act_add"), InlineKeyboardButton("➖ Remove Balance", callback_data="act_remove")],
            [InlineKeyboardButton("🔍 Check User", callback_data="act_check"), InlineKeyboardButton("🔗 My Invite", callback_data="act_myinvite")],
            [InlineKeyboardButton("📊 Report", callback_data="act_resellers")],
            [InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")]
        ]
        return await query.message.edit_text("💼 <b>Reseller Tools</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

    if data == "adm_cat_owner" and uid == OWNER_ID:
        kb = [
            [InlineKeyboardButton("➕ Add Reseller", callback_data="act_addreseller"), InlineKeyboardButton("🗑️ Del Reseller", callback_data="act_delreseller")],
            [InlineKeyboardButton("💲 Set Activate Price", callback_data="act_setprice")],
            [InlineKeyboardButton("💲 Set Reseller Profit", callback_data="act_setprofit")],
            [InlineKeyboardButton("💳 Add R. Balance", callback_data="act_addrc"), InlineKeyboardButton("➖ Rem R. Balance", callback_data="act_removerc")],
            [InlineKeyboardButton("👥 View R. Users", callback_data="act_rusers"), InlineKeyboardButton("🔗 Link User", callback_data="act_rlink")],
            [InlineKeyboardButton("⛓️ Unlink User", callback_data="act_runlink")],
            [InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")]
        ]
        return await query.message.edit_text("👑 <b>Owner Management</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

    if data == "adm_cat_shop" and uid == OWNER_ID:
        kb = [
            [InlineKeyboardButton("📋 Products Manager", callback_data="shop_mgr_list")],
            [InlineKeyboardButton("📦 Add Product", callback_data="act_addprod"), InlineKeyboardButton("🔐 Add Code", callback_data="act_addcode")],
            [InlineKeyboardButton("📥 Bulk Codes", callback_data="act_addcodes")],
            [InlineKeyboardButton("➕ Add Shop $", callback_data="act_addshop"), InlineKeyboardButton("➖ Remove Shop $", callback_data="act_removeshop")],
            [InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")]
        ]
        return await query.message.edit_text("🛒 <b>Main Shop Manager</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

    if data == "adm_cat_external" and uid == OWNER_ID:
        kb = [
            [InlineKeyboardButton("➕ Add External Shop", callback_data="act_addextshop"), InlineKeyboardButton("📋 List External Shops", callback_data="act_listextshops")],
            [InlineKeyboardButton("🗑️ Delete External Shop", callback_data="act_delextshop")],
            [InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")]
        ]
        return await query.message.edit_text("🌐 <b>External Shops</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

    if data == "adm_cat_system" and uid == OWNER_ID:
        kb = [
            [InlineKeyboardButton("🚧 Toggle Maint.", callback_data="act_maintenance"), InlineKeyboardButton("📢 Broadcast", callback_data="act_broadcast")],
            [InlineKeyboardButton("📢 Broadcast Inactive", callback_data="act_broadcastinactive"), InlineKeyboardButton("💬 Reply Ticket", callback_data="act_reply")],
            [InlineKeyboardButton("🚫 Ban User", callback_data="act_ban"), InlineKeyboardButton("✅ Unban User", callback_data="act_unban")],
            [InlineKeyboardButton("💰 API Balance", callback_data="act_apibalance"), InlineKeyboardButton("⚡ Active Jobs", callback_data="act_activejobs")],
            [InlineKeyboardButton("🔍 Check TX ID", callback_data="act_checktx")],
            [InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")]
        ]
        return await query.message.edit_text("⚙️ <b>System Tools</b>", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

    if data == "adm_cat_api" and uid == OWNER_ID:
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Health Check", callback_data="api_health"), InlineKeyboardButton("💰 Balance", callback_data="api_balance")],
            [InlineKeyboardButton("📋 Queue Status", callback_data="api_queue"), InlineKeyboardButton("📜 History", callback_data="api_history")],
            [InlineKeyboardButton("➕ Submit Job", callback_data="api_submit"), InlineKeyboardButton("🔎 Track Job", callback_data="api_trackjob")],
            [InlineKeyboardButton("🚫 Cancel Job", callback_data="api_canceljob"), InlineKeyboardButton("⚡ Active Jobs", callback_data="act_activejobs")],
            [InlineKeyboardButton("🔍 Check TX", callback_data="act_checktx")],
            [InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_home")],
        ])
        return await query.message.edit_text("⚡ <b>API Control Panel</b>\nManage Google One 5TB — 12 Months API", parse_mode="HTML", reply_markup=kb)

    # ── API direct callbacks ──────────────────────────────────────────
    if data == "api_health" and uid == OWNER_ID:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{IQLESS_BASE_URL}/api/health")
            h = resp.json()
            devices = h.get("pools", {}).get("unified", {}).get("devices", [])
            dev_lines = ""
            for d in devices:
                status = "🟡 Busy" if d.get("busy") else ("🟢 Ready" if d.get("ready") else "🔴 Not Ready")
                dev_lines += f"\n  📱 <code>{d['serial']}</code> — {status}"
            txt = (f"🔍 <b>Health Check</b>\n\n🌐 Status: <b>{'✅ OK' if h.get('status') == 'ok' else '❌ Down'}</b>\n"
                   f"📱 Devices: {h.get('devices_connected', 0)}/{h.get('device_count', 0)} connected\n\n<b>Devices:</b>{dev_lines}")
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm_cat_api")]])
            return await query.message.reply_text(txt, parse_mode="HTML", reply_markup=kb)
        except Exception as e:
            return await query.message.reply_text(f"❌ Error: {e}")

    if data == "api_balance" and uid == OWNER_ID:
        try:
            bal = await iqless_get_balance()
            key_display = IQLESS_API_KEY[:12] + "..." if len(IQLESS_API_KEY) > 12 else IQLESS_API_KEY
            txt = (f"💰 <b>API Balance</b>\n\n"
                   f"👤 Account: <b>{bal.get('name', 'N/A')}</b>\n"
                   f"🔑 Key: <code>{key_display}</code>\n"
                   f"💳 Balance: <b>{bal.get('balance', 0)} credits</b>\n"
                   f"💵 Cost/Job: {bal.get('cost_per_job', 'N/A')}\n"
                   f"📊 Total Used: {bal.get('total_used', 0)}")
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm_cat_api")]])
            return await query.message.reply_text(txt, parse_mode="HTML", reply_markup=kb)
        except Exception as e:
            return await query.message.reply_text(f"❌ Error: {e}")

    if data == "api_queue" and uid == OWNER_ID:
        try:
            q = await iqless_get_queue()
            txt = (f"📋 <b>Queue Status</b>\n\n⏳ Pending: <b>{q.get('pending_count', 0)}</b> jobs\n"
                   f"📱 Devices: {q.get('devices_connected', 0)} connected | {q.get('devices_ready', 0)} ready")
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm_cat_api")]])
            return await query.message.reply_text(txt, parse_mode="HTML", reply_markup=kb)
        except Exception as e:
            return await query.message.reply_text(f"❌ Error: {e}")

    if data == "api_history" and uid == OWNER_ID:
        try:
            headers = {"X-API-Key": IQLESS_API_KEY}
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{IQLESS_BASE_URL}/api/history?limit=10", headers=headers)
            h = resp.json()
            records = h.get("records", [])
            txt = f"📜 <b>Success History</b>\nTotal: {h.get('total', 0)} records\n\n"
            for r in records:
                txt += f"📧 <code>{r['email']}</code>\n🔗 <a href='{r['url']}'>Link</a> | {r.get('created_at', '')}\n\n"
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="adm_cat_api")]])
            return await query.message.reply_text(txt, parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
        except Exception as e:
            return await query.message.reply_text(f"❌ Error: {e}")

    if data == "api_submit" and uid == OWNER_ID:
        context.user_data["admin_action"] = "api_submit_email"
        cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="api_submit_cancel")]])
        return await query.message.reply_text("➕ <b>Submit Activation Job</b>\n\nStep 1/3 — Send Gmail address:", parse_mode="HTML", reply_markup=cancel_kb)

    if data == "api_submit_cancel" and uid == OWNER_ID:
        for k in ["admin_action", "api_submit_email", "api_submit_pass"]:
            context.user_data.pop(k, None)
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(t(uid, "btn_back"), callback_data="adm_cat_api")]])
        return await query.message.edit_text("❌ <b>Operation cancelled</b>", parse_mode="HTML", reply_markup=kb)

    if data == "api_trackjob" and uid == OWNER_ID:
        context.user_data["admin_action"] = "api_trackjob"
        return await query.message.reply_text("🔎 <b>Track Job</b>\n\nSend the Job ID:", parse_mode="HTML")

    if data == "api_canceljob" and uid == OWNER_ID:
        context.user_data["admin_action"] = "api_canceljob"
        return await query.message.reply_text("🚫 <b>Cancel Job</b>\n\nSend the Job ID:", parse_mode="HTML")

    if data.startswith("api_cancel_confirm:") and uid == OWNER_ID:
        job_id = data.split(":", 1)[1]
        await query.answer("")
        try:
            status_code, resp = await iqless_cancel_job(job_id)
            txt = f"✅ <b>Cancelled</b>\n🆔 <code>{job_id}</code>" if status_code in (200, 204) else f"❌ <b>Failed</b>\n{resp}"
        except Exception as e:
            txt = f"❌ Error: {e}"
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 API Control", callback_data="adm_cat_api")]])
        return await query.message.edit_text(txt, parse_mode="HTML", reply_markup=kb)

    if data.startswith("api_cancel_abort:") and uid == OWNER_ID:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 API Control", callback_data="adm_cat_api")]])
        return await query.message.edit_text("↩️ <b>Reverted</b>", parse_mode="HTML", reply_markup=kb)

    if data == "act_apibalance" and uid == OWNER_ID:
        try:
            bal_data = await iqless_get_balance()
            txt = (f"🔑 <b>API Balance</b>\n\n"
                   f"👤 Account: <b>{bal_data.get('name', 'N/A')}</b>\n"
                   f"💳 Balance: <b>{bal_data.get('balance', 0)}</b> credits\n"
                   f"💵 Cost/Job: {bal_data.get('cost_per_job', 'N/A')}\n"
                   f"📊 Total Used: {bal_data.get('total_used', 0)}")
            return await query.message.reply_text(txt, parse_mode="HTML")
        except Exception as e:
            return await query.message.reply_text(f"❌ Error: {e}")

    if data == "act_activejobs" and uid == OWNER_ID:
        if not active_jobs:
            return await query.message.reply_text("✅ <b>No active activation jobs currently.</b>", parse_mode="HTML")
        lines = [f"⚡ <b>Active Jobs ({len(active_jobs)})</b>\n"]
        now = time.time()
        for jid, jdata in active_jobs.items():
            elapsed = int(now - jdata.get("submitted_at", now))
            mins, secs = divmod(elapsed, 60)
            lines.append(f"🆔 <code>{jid}</code>\n👤 UID: <code>{jdata.get('uid')}</code>\n📧 {jdata.get('email', 'N/A')}\n💰 ${jdata.get('cost', 0):.2f}\n⏱️ Since: {mins}m {secs}s\n")
        return await query.message.reply_text("\n".join(lines), parse_mode="HTML")

    # ── act_ action dispatchers ───────────────────────────────────────
    if data.startswith("act_"):
        action = data.split("_", 1)[1]
        if action == "myinvite":
            return await cmd_myinvite(update, context)
        if action == "resellers":
            return await main_admin_cmds_handler(update, context, direct_cmd="/resellers")
        if action == "listprod":
            return await main_admin_cmds_handler(update, context, direct_cmd="/listprod")
        if action == "listextshops":
            return await main_admin_cmds_handler(update, context, direct_cmd="/listextshops")
        if action == "maintenance":
            return await main_admin_cmds_handler(update, context, direct_cmd="/maintenance")
        if action == "addprod":
            return
        if action == "addextshop":
            from handlers.external import start_addextshop_wizard
            return await start_addextshop_wizard(update, context)

        context.user_data["admin_action"] = action
        mapping = {
            "add": "📝 <b>Send User ID and Amount ($):</b>\nExample: <code>123456789 5</code>",
            "remove": "📝 <b>Send User ID and Amount ($):</b>\nExample: <code>123456789 5</code>",
            "addshop": "📝 <b>Send User ID and Amount ($):</b>\nExample: <code>123456789 10</code>",
            "removeshop": "📝 <b>Send User ID and Amount ($):</b>\nExample: <code>123456789 10</code>",
            "addrc": "📝 <b>Send Reseller ID and Amount ($):</b>\nExample: <code>123456789 10</code>",
            "removerc": "📝 <b>Send Reseller ID and Amount ($):</b>\nExample: <code>123456789 10</code>",
            "check": "📝 <b>Send User ID:</b>",
            "addreseller": "📝 <b>Send User ID:</b>",
            "delreseller": "📝 <b>Send User ID:</b>",
            "ban": "📝 <b>Send User ID:</b>",
            "unban": "📝 <b>Send User ID:</b>",
            "delprod": "📝 <b>Send Product ID:</b>",
            "addcode": "📝 <b>Send Product ID and one code:</b>\nExample: <code>5 ABCD-1234</code>",
            "addcodes": "📝 <b>Send Product ID then codes on new lines:</b>\nExample:\n<code>5\nCODE-1\nCODE-2</code>",
            "setprice": "💰 <b>Send New Activation Price ($):</b>",
            "setprofit": "💰 <b>Send Reseller Profit Per Activation ($):</b>",
            "broadcast": "📢 <b>Send Message:</b>",
            "broadcastinactive": "📢 <b>Send Message for Inactive Users:</b>",
            "rusers": "📝 <b>Send Reseller ID:</b>",
            "rlink": "📝 <b>Send User ID and Reseller ID:</b>\nExample: <code>12345 67890</code>",
            "runlink": "📝 <b>Send User ID:</b>",
            "reply": "📝 <b>Send User ID and Message:</b>\nExample: <code>12345 Hello</code>",
            "delextshop": "📝 <b>Send External Shop ID:</b>",
            "checktx": "🔍 <b>Enter TX ID to search:</b>",
        }
        prompt = mapping.get(action, "📝 <b>Enter Input:</b>")
        return await query.message.reply_text(prompt, parse_mode="HTML")

    if data == "maint_notify_yes":
        return await _apply_maintenance_notify(update, context, notify=True)
    if data == "maint_notify_no":
        return await _apply_maintenance_notify(update, context, notify=False)


async def _apply_maintenance_notify(update, context, notify):
    msg = f"✅ Maintenance set to <b>{'ON' if MAINTENANCE_MODE else 'OFF'}</b>."
    if notify:
        key = "maint_start_broadcast" if MAINTENANCE_MODE else "maint_end_broadcast"
        count = await broadcast_system_msg(context, key)
        msg += f"\n📢 Sent to {count} users."
    await update.callback_query.message.edit_text(msg, parse_mode="HTML")


# ── Main admin command handler ────────────────────────────────────────────

async def main_admin_cmds_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, direct_cmd: str = None):
    uid = update.effective_user.id
    if uid != OWNER_ID and not is_reseller(uid):
        msg = update.callback_query.message if update.callback_query else update.message
        return await msg.reply_text("⛔ Access Denied.")

    txt = direct_cmd if direct_cmd else (update.message.text or "")
    parts = txt.split(maxsplit=2)
    cmd = parts[0] if parts else ""
    context.args = txt.split()[1:] if txt else []

    async def reply(text, reply_markup=None):
        msg = update.callback_query.message if update.callback_query else update.message
        await msg.reply_text(text, parse_mode="HTML", reply_markup=reply_markup)

    if cmd == "/help": return await cmd_help(update, context)
    if cmd == "/myinvite": return await cmd_myinvite(update, context)

    # ── Shop manager text input handlers ──────────────────────────────
    if cmd.startswith("/shop_mgr_"):
        from models.shop import update_product, add_product_code, add_product_codes_bulk, get_product
        shop_id = current_shop_id(context)
        pid = context.user_data.pop("shop_mgr_pid", 0)
        input_text = " ".join(context.args).strip()
        back_kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data=f"shop_mgr_view_{pid}")]])

        if cmd == "/shop_mgr_setname":
            if not input_text:
                return await reply("❌ Name cannot be empty.", reply_markup=back_kb)
            update_product(shop_id, pid, name=input_text)
            return await reply(f"✅ Name updated to <b>{input_text}</b>", reply_markup=back_kb)

        if cmd == "/shop_mgr_setprice":
            try:
                price = float(input_text)
                if price < 0: raise ValueError
                update_product(shop_id, pid, price=price)
                return await reply(f"✅ Price updated to <b>${price:.2f}</b>", reply_markup=back_kb)
            except Exception:
                return await reply("❌ Invalid price. Enter a number.", reply_markup=back_kb)

        if cmd == "/shop_mgr_setdesc":
            update_product(shop_id, pid, description=input_text or "")
            return await reply("✅ Description updated.", reply_markup=back_kb)

        if cmd == "/shop_mgr_setcat":
            if not input_text:
                return await reply("❌ Category cannot be empty.", reply_markup=back_kb)
            update_product(shop_id, pid, category=input_text)
            return await reply(f"✅ Category updated to <b>{input_text}</b>", reply_markup=back_kb)

        if cmd == "/shop_mgr_addcode":
            if not input_text:
                return await reply("❌ Code cannot be empty.", reply_markup=back_kb)
            p = get_product(shop_id, pid)
            if not p:
                return await reply("❌ Product not found.", reply_markup=back_kb)
            add_product_code(shop_id, pid, input_text)
            return await reply(f"✅ Code added. Stock: {get_product(shop_id, pid)['stock']}", reply_markup=back_kb)

        if cmd == "/shop_mgr_addbulk":
            full_text = txt.split(maxsplit=1)[1] if len(txt.split(maxsplit=1)) > 1 else ""
            codes = [x.strip() for x in full_text.replace("||", "\n").splitlines() if x.strip()]
            if not codes:
                return await reply("❌ No codes provided.", reply_markup=back_kb)
            p = get_product(shop_id, pid)
            if not p:
                return await reply("❌ Product not found.", reply_markup=back_kb)
            inserted = add_product_codes_bulk(shop_id, pid, codes)
            return await reply(f"✅ Added {inserted} codes. Stock: {get_product(shop_id, pid)['stock']}", reply_markup=back_kb)

        if cmd == "/shop_mgr_setstock":
            try:
                new_stock = int(input_text)
                if new_stock < 0: raise ValueError
                update_product(shop_id, pid, stock=new_stock)
                return await reply(f"✅ Stock updated to <b>{new_stock}</b>", reply_markup=back_kb)
            except Exception:
                return await reply("❌ Invalid number. Enter a positive integer.", reply_markup=back_kb)


    target_id = None
    if len(context.args) >= 1:
        if context.args[0].isdigit(): target_id = int(context.args[0])
        elif context.args[0].startswith("@"): target_id = get_id_by_username(context.args[0])

    if cmd == "/reply":
        try:
            target_id = int(context.args[0])
            msg_text = " ".join(context.args[1:])
            conn = db_connect()
            ticket = conn.execute("SELECT shop_id, bot_token FROM tickets WHERE user_id=? ORDER BY id DESC LIMIT 1", (target_id,)).fetchone()
            conn.execute("UPDATE tickets SET status='closed' WHERE user_id=? AND status='open'", (target_id,))
            conn.commit(); conn.close()
            bot_token = ticket["bot_token"] if ticket and ticket["bot_token"] else MAIN_BOT_TOKEN
            await Bot(bot_token).send_message(target_id, t(target_id, "support_reply", msg=msg_text), parse_mode="HTML")
            return await reply(f"✅ Reply sent to {target_id}")
        except Exception:
            return await reply("❌ Usage: /reply ID Message")

    if cmd in ["/broadcast_inactive", "/broadcastinactive"]:
        conn = db_connect()
        users = conn.execute("SELECT user_id FROM users WHERE last_activity < datetime('now', '-7 days')").fetchall()
        conn.close()
        msg_text = " ".join(context.args)
        await reply(f"📣 Sending to {len(users)} inactive users...")
        for row in users:
            try: await Bot(MAIN_BOT_TOKEN).send_message(int(row["user_id"]), msg_text, parse_mode="HTML")
            except Exception: pass
        return await reply("✅ Done.")

    if cmd == "/add" and target_id:
        try:
            amt = float(context.args[1])
            if uid == OWNER_ID:
                add_balance(target_id, amt)
                try: await Bot(MAIN_BOT_TOKEN).send_message(target_id, t(target_id, "balance_added_msg", amount=amt), parse_mode="HTML")
                except Exception: pass
                return await reply("✅ Added.")
            ok, res = reseller_give_balance(uid, target_id, amt)
            if ok:
                try: await Bot(MAIN_BOT_TOKEN).send_message(target_id, t(target_id, "balance_added_msg", amount=amt), parse_mode="HTML")
                except Exception: pass
            return await reply(res)
        except Exception:
            return await reply("❌ Usage: /add ID Amount")

    if cmd == "/remove" and target_id:
        try:
            amt = float(context.args[1])
            if uid == OWNER_ID:
                add_balance(target_id, -amt)
                try: await Bot(MAIN_BOT_TOKEN).send_message(target_id, t(target_id, "balance_removed_msg", amount=amt), parse_mode="HTML")
                except Exception: pass
                return await reply("✅ Removed.")
            ok, res = reseller_remove_balance(uid, target_id, amt)
            return await reply(res)
        except Exception:
            return await reply("❌ Usage: /remove ID Amount")

    if cmd == "/addshop" and uid == OWNER_ID and target_id:
        try:
            amt = float(context.args[1]); add_shop_balance(target_id, amt, shop_id=0)
            return await reply(f"✅ Added ${amt:.2f} to main shop wallet.")
        except Exception: return await reply("❌ Usage: /addshop ID Amount")

    if cmd == "/removeshop" and uid == OWNER_ID and target_id:
        try:
            amt = float(context.args[1]); add_shop_balance(target_id, -amt, shop_id=0)
            return await reply(f"✅ Removed ${amt:.2f} from main shop wallet.")
        except Exception: return await reply("❌ Usage: /removeshop ID Amount")

    if cmd == "/addcode" and uid == OWNER_ID:
        try:
            pid = int(context.args[0]); code_text = txt.split(maxsplit=2)[2]
            if not get_product(0, pid): return await reply("❌ Product not found.")
            add_product_code(0, pid, code_text)
            return await reply(f"✅ Code added. New stock: {get_product(0, pid)['stock']}")
        except Exception: return await reply("❌ Usage: /addcode PRODUCT_ID CODE")

    if cmd == "/addcodes" and uid == OWNER_ID:
        try:
            raw = txt.split(maxsplit=2); pid = int(raw[1]); data_str = raw[2] if len(raw) > 2 else ""
            codes = [x.strip() for x in data_str.replace("||", "\n").splitlines() if x.strip()]
            if not codes: return await reply("❌ No codes provided.")
            if not get_product(0, pid): return await reply("❌ Product not found.")
            inserted = add_product_codes_bulk(0, pid, codes)
            return await reply(f"✅ Added {inserted} codes. New stock: {get_product(0, pid)['stock']}")
        except Exception: return await reply("❌ Usage: /addcodes PRODUCT_ID then codes on new lines")

    if cmd == "/addreseller" and uid == OWNER_ID and target_id:
        conn = db_connect(); conn.execute("INSERT OR IGNORE INTO resellers (user_id) VALUES (?)", (target_id,)); conn.commit(); conn.close()
        return await reply(f"✅ User {target_id} is now a Reseller.")

    if cmd == "/delreseller" and uid == OWNER_ID and target_id:
        delete_reseller(target_id); return await reply(f"🗑️ Removed {target_id} from resellers.")

    if cmd == "/ban" and uid == OWNER_ID and target_id: ban_user(target_id, 1); return await reply(f"🚫 Banned {target_id}")
    if cmd == "/unban" and uid == OWNER_ID and target_id: ban_user(target_id, 0); return await reply(f"✅ Unbanned {target_id}")

    if cmd == "/check" and target_id:
        res = get_user_data(target_id)
        msg = f"👤 <b>User Info</b>\nID: <code>{target_id}</code>\nUsername: @{res.get('username', 'N/A')}\n💰 Balance: ${float(res.get('balance', 0)):.2f}\n🛒 Shop Wallet: ${float(get_shop_balance(target_id, 0)):.2f}"
        return await reply(msg)

    if cmd == "/backup" and uid == OWNER_ID:
        prog = await update.message.reply_text("💾 Creating backup...")
        result = await do_backup(bot=context.bot)
        await prog.edit_text(f"💾 <b>Backup</b>\n\n{result}", parse_mode="HTML"); return

    if cmd == "/setprice" and uid == OWNER_ID:
        try:
            price = float(context.args[0]); set_activate_price(price)
            return await reply(f"✅ Activation Price set to <b>${price:.2f}</b>")
        except Exception: return await reply("❌ Usage: /setprice PRICE")

    if cmd == "/setprofit" and uid == OWNER_ID:
        try:
            profit = float(context.args[0]); set_reseller_profit(profit)
            return await reply(f"✅ Reseller Profit set to <b>${profit:.2f}</b> per activation.")
        except Exception: return await reply("❌ Usage: /setprofit AMOUNT")

    if cmd == "/addrc" and uid == OWNER_ID and target_id:
        try:
            amt = float(context.args[1]); add_reseller_balance(target_id, amt)
            return await reply(f"✅ Added ${amt:.2f} to reseller wallet.")
        except Exception: return await reply("❌ Usage: /addrc RID AMOUNT")

    if cmd == "/removerc" and uid == OWNER_ID and target_id:
        try:
            amt = float(context.args[1]); add_reseller_balance(target_id, -amt)
            return await reply(f"✅ Removed ${amt:.2f} from reseller wallet.")
        except Exception: return await reply("❌ Usage: /removerc RID AMOUNT")

    if cmd == "/resellers":
        conn = db_connect()
        rows = conn.execute("SELECT r.user_id, r.balance, r.total_sold, r.profit_per_activation, u.username, u.first_name FROM resellers r LEFT JOIN users u ON r.user_id=u.user_id").fetchall()
        conn.close()
        msg = "💼 <b>Resellers Report</b>\n\n"
        for r in rows:
            msg += f"👤 <b>{r['first_name'] or 'No Name'}</b> | @{r['username'] or 'No Username'}\n🆔 <code>{int(r['user_id'])}</code>\n💰 Wallet: ${float(r['balance'] or 0):.2f}\n📉 Sold: {int(r['total_sold'] or 0)}\n\n"
        return await reply(msg)

    if cmd == "/rusers" and uid == OWNER_ID:
        try:
            rid = int(context.args[0]); clients = get_reseller_clients_detailed(rid)
            if not clients: return await reply(f"📭 Reseller {rid} has no users.")
            msg = f"👥 <b>Users of Reseller {rid}</b> ({len(clients)} total)\n\n"
            for c in clients[:25]:
                msg += f"🆔 <code>{c['user_id']}</code> | @{c['username']}\n   💰 ${c['balance']:.2f} | ✅ {c['activations']} acts\n\n"
            return await reply(msg)
        except Exception: return await reply("❌ Usage: /rusers RID")

    if cmd == "/rlink" and uid == OWNER_ID:
        try:
            u_id = int(context.args[0]); r_id = int(context.args[1])
            return await reply(f"✅ Linked User {u_id} to Reseller {r_id}" if set_user_owner(u_id, r_id) else "✅ Done.")
        except Exception: return await reply("❌ Usage: /rlink USER_ID RESELLER_ID")

    if cmd == "/runlink" and uid == OWNER_ID:
        try:
            u_id = int(context.args[0]); conn = db_connect()
            conn.execute("UPDATE users SET owner_id=0 WHERE user_id=?", (u_id,)); conn.commit(); conn.close()
            return await reply(f"✅ Unlinked User {u_id}.")
        except Exception: return await reply("❌ Usage: /runlink USER_ID")

    if cmd == "/listprod":
        prods = get_all_products(0)
        if not prods: return await reply("📭 No products found.")
        msg = "🛒 <b>Main Shop Products</b>\n"
        for p in prods:
            msg += f"ID: {p['id']} | {p['name']} | ${float(p['price']):.2f} | Stock: {int(p['stock'])} | {p['delivery_type']} | Cat: {p['category']}\n"
        return await reply(msg)

    if cmd == "/delprod" and uid == OWNER_ID:
        try:
            pid = int(context.args[0]); del_product(0, pid)
            return await reply(f"✅ Product {pid} deleted.")
        except Exception: return await reply("❌ Usage: /delprod PRODUCT_ID")

    if cmd == "/maintenance" and uid == OWNER_ID:
        _cfg.MAINTENANCE_MODE = not _cfg.MAINTENANCE_MODE
        set_maintenance_mode(_cfg.MAINTENANCE_MODE)
        state_label = "ON 🔴" if _cfg.MAINTENANCE_MODE else "OFF 🟢"
        kb = [[InlineKeyboardButton("✅ Yes, Broadcast", callback_data="maint_notify_yes")],
              [InlineKeyboardButton("🔕 No, Silent", callback_data="maint_notify_no")]]
        return await reply(f"🚧 <b>Maintenance: {state_label}</b>\n\nNotify all users?", reply_markup=InlineKeyboardMarkup(kb))

    if cmd == "/broadcast" and uid == OWNER_ID:
        msg_text = " ".join(context.args)
        if not msg_text: return await reply("⚠️ Usage: /broadcast MESSAGE")
        users = get_all_users()
        await reply("🚀 <b>Broadcasting...</b>")
        count = 0
        temp_bot = Bot(MAIN_BOT_TOKEN)
        for user_id in users:
            try: await temp_bot.send_message(user_id, msg_text, parse_mode="HTML"); count += 1; await asyncio.sleep(0.04)
            except Exception: pass
        return await reply(f"✅ <b>Broadcast Complete.</b>\nSent to: {count} users.")

    if cmd == "/addextshop" and uid == OWNER_ID:
        from handlers.external import start_addextshop_wizard
        return await start_addextshop_wizard(update, context)

    if cmd == "/delextshop" and uid == OWNER_ID:
        try:
            shop_id = int(context.args[0]); row = get_external_shop_by_id(shop_id)
            if not row: return await reply("❌ External shop not found.")
            from handlers.external import stop_external_shop_runtime
            await stop_external_shop_runtime(shop_id)
            remove_external_shop_db(shop_id)
            return await reply(f"✅ External shop <b>{row['title']}</b> deleted.")
        except Exception: return await reply("❌ Usage: /delextshop SHOP_ID")

    if cmd == "/listextshops" and uid == OWNER_ID:
        rows = get_external_shops()
        if not rows: return await reply("📭 No external shops found.")
        msg = "🌐 <b>External Shops</b>\n\n"
        for r in rows:
            msg += f"ID: <code>{r['id']}</code>\nTitle: <b>{r['title']}</b>\nOwner: <code>{r['owner_id']}</code>\nStatus: {'🟢 Active' if int(r['is_active']) == 1 else '🔴 Stopped'}\n\n"
        return await reply(msg)

    # ── API admin submit/track/cancel commands ────────────────────────
    if cmd == "/api_submit_email" and uid == OWNER_ID:
        email = " ".join(context.args).strip()
        if not email or "@" not in email:
            context.user_data["admin_action"] = "api_submit_email"
            return await reply("❌ Invalid email. Try again.")
        context.user_data["api_submit_email"] = email
        context.user_data["admin_action"] = "api_submit_pass"
        return await reply(f"✅ Email: <code>{email}</code>\n\nStep 2/3 — Send password:")

    if cmd == "/api_submit_pass" and uid == OWNER_ID:
        password = " ".join(context.args).strip()
        if not password:
            context.user_data["admin_action"] = "api_submit_pass"
            return await reply("❌ Empty password. Try again.")
        context.user_data["api_submit_pass"] = password
        context.user_data["admin_action"] = "api_submit_totp"
        return await reply("✅ Password saved.\n\nStep 3/3 — Send 2FA key (TOTP Secret):")

    if cmd == "/api_submit_totp" and uid == OWNER_ID:
        totp = " ".join(context.args).strip()
        if not totp: return await reply("❌ TOTP key is empty.")
        email = context.user_data.pop("api_submit_email", None)
        password = context.user_data.pop("api_submit_pass", None)
        if not email or not password: return await reply("❌ Session expired. Start again.")
        await reply("⏳ <b>Submitting...</b>")
        try:
            device, dev_status = await iqless_pick_best_device()
            if dev_status == "all_unavailable": return await reply("❌ All devices offline.")
            status_code, resp = await iqless_submit_job(email, password, totp, device=device)
            if status_code == 200:
                return await reply(f"✅ <b>Submitted!</b>\n🆔 Job ID: <code>{resp.get('job_id')}</code>\n📊 Queue: {resp.get('queue_position')}")
            else:
                return await reply(f"❌ <b>Failed</b>\n{resp}")
        except Exception as e:
            return await reply(f"❌ Error: {e}")

    if cmd == "/api_trackjob" and uid == OWNER_ID:
        job_id = " ".join(context.args).strip()
        if not job_id: return await reply("❌ Enter a Job ID.")
        try:
            data = await iqless_poll_job(job_id)
            status = data.get("status", "unknown")
            txt = f"🆔 <code>{job_id}</code>\n📊 Status: <b>{status}</b>\n🔄 Stage: {data.get('stage', 0)}/{data.get('total_stages', 8)}"
            if status == "success" and data.get("url"): txt += f"\n🔗 {data['url']}"
            elif status == "failed" and data.get("error"): txt += f"\n⚠️ {data['error']}"
            return await reply(txt)
        except Exception as e: return await reply(f"❌ Error: {e}")

    if cmd == "/api_canceljob" and uid == OWNER_ID:
        job_id = " ".join(context.args).strip()
        if not job_id: return await reply("❌ Enter a Job ID.")
        try:
            data = await iqless_poll_job(job_id)
            status = data.get("status", "unknown")
            if status in ("success", "failed"):
                return await reply(f"⚠️ Cannot cancel — status: <b>{status}</b>")
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Confirm", callback_data=f"api_cancel_confirm:{job_id}"),
                InlineKeyboardButton("❌ Abort", callback_data=f"api_cancel_abort:{job_id}"),
            ]])
            return await reply(f"⚠️ <b>Confirm cancellation</b>\n🆔 <code>{job_id}</code>\n📊 {status}", reply_markup=kb)
        except Exception as e: return await reply(f"❌ Error: {e}")

    if cmd == "/checktx" and uid == OWNER_ID:
        tx_input = " ".join(context.args).strip()
        if not tx_input: return await reply("❌ Enter the TX ID.")
        for jid, jdata in active_jobs.items():
            if jdata.get("tx_id", "") == tx_input or jid == tx_input:
                return await reply(f"⚡ <b>Active</b>\n🆔 Job: <code>{jid}</code>\n👤 UID: <code>{jdata.get('uid')}</code>\n📧 {jdata.get('email', 'N/A')}")
        conn = db_connect()
        row = conn.execute("SELECT * FROM history WHERE tx_id=? ORDER BY id DESC LIMIT 1", (tx_input,)).fetchone()
        conn.close()
        if row:
            return await reply(f"🔍 TX: <code>{row['tx_id']}</code>\n👤 UID: <code>{row['user_id']}</code>\n📧 {row['email']}\n{'✅' if row['status']=='success' else '❌'} {row['status']}")
        return await reply(f"⚠️ No transaction found: <code>{tx_input}</code>")

    return await reply("⚠️ Unknown or unauthorized command.")
