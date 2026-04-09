# -*- coding: utf-8 -*-
"""Localisation texts and helpers — stripped of all ChatGPT Workspace keys."""

from database import db_connect


# ── Translation lookup ────────────────────────────────────────────────────

def get_user_lang(user_id: int) -> str:
    try:
        conn = db_connect()
        row = conn.execute(
            "SELECT COALESCE(lang, 'en') AS lang FROM users WHERE user_id=?", (user_id,)
        ).fetchone()
        conn.close()
        if row and (row["lang"] or "").strip():
            return (row["lang"] or "en").strip().lower()
    except Exception:
        pass
    return "en"


def t(user_id: int, key: str, **kwargs):
    lang = get_user_lang(user_id)
    d = LANGS.get(lang, LANGS["en"])
    template = d.get(key, LANGS["en"].get(key, key))
    fmt = {}
    for k, v in kwargs.items():
        if isinstance(v, float):
            fmt[k] = "{:.2f}".format(v)
        else:
            fmt[k] = v
    try:
        return template.format(**fmt)
    except Exception:
        return template


def help_text_for(user_id: int, key: str) -> str:
    items = HELP_SETS.get(key, {}).get("en", [])
    titles = {
        "main_user": "📚 <b>User Commands</b>",
        "external_user": "📚 <b>Store Commands</b>",
        "admin_owner": "👑 <b>Owner Commands</b>",
        "admin_reseller": "💼 <b>Reseller Commands</b>",
        "ext_admin": "🌐 <b>Store Control Commands</b>",
    }
    title = titles.get(key, "📚 <b>Commands</b>")
    lines = [title, ""]
    for cmd_name, desc in items:
        lines.append(f"<code>{cmd_name}</code> - {desc}")
    return "\n".join(lines)


# ── English texts ─────────────────────────────────────────────────────────

EN_TEXTS = {
    "lang_set": "✅ Language set to <b>English</b>",
    "welcome": (
        "👋 <b>Welcome!</b>\n\n"
        "🆔 ID: <code>{uid}</code>\n"
        "👥 <b>Users:</b> {users_count}\n"
        "💰 <b>Balance:</b> ${balance}\n"
        "💵 <b>Activate Price:</b> ${price}\n\n"
        "{service_line}"
        "<b>Status:</b> {status}\n\n"
        "👇 <b>Select an option below:</b>"
    ),
    "welcome_external": (
        "🛒 <b>{shop_title}</b>\n\n"
        "🆔 ID: <code>{uid}</code>\n"
        "👥 <b>Users:</b> {users_count}\n"
        "💰 <b>Balance:</b> ${balance}\n"
        "📦 <b>Products:</b> {products}\n\n"
        "🛍️ <b>This bot is store-only.</b>\n\n"
        "👇 <b>Select an option below:</b>"
    ),
    "welcome_admin": "👋 <b>Admin Panel</b>\n\nSelect a category to manage:",
    "welcome_ext_admin": "🌐 <b>{shop_title} - Control Panel</b>\n\nSelect a section:",
    "status_active": "🟢 <b>Online</b>",
    "status_maint": "🚧 <b>Maintenance</b>",
    "maint_msg": "⚠️ <b>System is under maintenance.</b>\nPlease try again later.",
    "maint_start_broadcast": "🚧 <b>Maintenance Alert</b>\n\nThe bot is currently under maintenance.",
    "maint_end_broadcast": "✅ <b>System Online</b>\n\nThe bot is back online.",
    "must_join": "⚠️ <b>Access Denied!</b>\n\nYou must join our updates channel to use this bot.",
    "btn_join_ch": "📢 Join Channel",
    "btn_i_joined": "✅ I have Joined",
    "join_success": "✅ <b>Thanks for joining!</b>",
    "still_not_joined": "⚠️ You still have not joined the channel.",
    "lang_select": "🌐 <b>Select Language:</b>",
    "banned": "🚫 <b>BANNED.</b>",
    "send_activate_prompt": (
        "⚡ <b>Google One Activation</b>\n\n"
        "📧 Please enter your <b>Gmail address</b>:"
    ),
    "act_ask_password": "🔑 Now enter your <b>Gmail password</b>:",
    "act_ask_totp": (
        "🔐 Now enter your <b>TOTP Secret</b>:\n\n"
        "📌 This is your 2FA secret key (Base32 encoded, e.g. <code>JBSWY3DPEHPK3PXP</code>)"
    ),
    "act_bad_email": "❌ Invalid email address. Please enter a valid Gmail address:",
    "activate_not_available": "⚠️ Activation is not available in this bot.",
    "bad_credentials": "❌ Invalid format. Please send:\n<code>email\npassword\ntotp_secret</code>",
    "activate_cost": "💵 <b>Activation Cost:</b> ${price}\n\nYour Balance: ${balance}\n\n✅ Confirm to proceed.",
    "activate_no_bal": "❌ Insufficient balance! You need ${price} to activate.\n\nYour balance: ${balance}",
    "activate_queued": (
        "⏳ <b>Job Submitted!</b>\n\n"
        "📧 Email: <code>{email}</code>\n"
        "🆔 Job ID: <code>{job_id}</code>\n"
        "🧾 TX: <code>{tx_id}</code>\n"
        "🔢 Queue Position: {pos}\n"
        "⏱ Est. Wait: ~{wait}s\n\n"
        "I will notify you when done."
    ),
    "activate_success": (
        "✅ <b>Google One Activation Successful!</b>\n\n"
        "📧 Email: <code>{email}</code>\n"
        "🆔 Job ID: <code>{job_id}</code>\n"
        "🔗 Link: {url}\n\n"
        "🧾 Transaction: <code>#{tx}</code>"
    ),
    "activate_failed": (
        "❌ <b>Activation Failed</b>\n\n"
        "📧 Email: <code>{email}</code>\n"
        "Reason: <b>{reason}</b>\n\n"
        "💰 <b>${cost} refunded to your balance.</b>"
    ),
    "activate_already_queued": "⚠️ This email is already in the queue or being processed.",
    "activate_already_done": "⚠️ This email has already been successfully activated.",
    "activate_no_devices": "⚠️ No devices available right now. Please try again in a few minutes.",
    "activate_service_paused": "⚠️ Activation service is temporarily paused. Please try again later.",
    "activate_api_error": "❌ API Error: {error}",
    "checkin_success": "📅 <b>Daily Check-in:</b>\n✅ You received +${amount}!",
    "checkin_fail": "⏳ <b>Already Checked-in!</b>\nCome back tomorrow.",
    "referral_bonus": "🎉 <b>New Referral!</b>\nYou got +${amount} for inviting a friend.",
    "deposit_menu": (
        "💰 <b>Deposit</b>\n\n"
        "🔹 <b>USDT TRC20:</b>\n<code>{trc20}</code>\n\n"
        "🔹 <b>USDT BEP20:</b>\n<code>{bep20}</code>\n\n"
        "🇩🇿 <b>BaridiMob (Algeria):</b>\n<code>{baridimob}</code>\n"
        "📌 Rate: 1000 DA = $4 | 630 DA = $2.5\n\n"
        "🆘 <b>Support:</b> Use /support\n"
        "👇 <b>After payment, contact admin.</b>"
    ),
    "deposit_choose_network": (
        "💰 <b>Deposit — Choose Payment Method</b>\n\n"
        "Select the network you will use to send USDT,\n"
        "or choose BaridiMob for Algeria:"
    ),
    "deposit_ask_amount": (
        "💵 <b>Enter the amount you want to deposit (USD):</b>\n\n"
        "Example: <code>10</code> or <code>5.5</code>\n"
        "Minimum: $1"
    ),
    "deposit_pending_trc20": (
        "✅ <b>Deposit Request Created!</b>\n\n"
        "🔹 Network: <b>USDT TRC20 (TRON)</b>\n\n"
        "💰 Send EXACTLY:\n<code>{amount}</code> USDT\n\n"
        "📬 To this address:\n<code>{wallet}</code>\n\n"
        "⚠️ <b>Important:</b> Send exactly this amount — even a tiny difference will prevent confirmation.\n\n"
        "⏰ This request expires in <b>30 minutes</b>.\n"
        "📨 After payment, send <code>/claim TXID</code> or just send the TXID alone."
    ),
    "deposit_pending_bep20": (
        "✅ <b>Deposit Request Created!</b>\n\n"
        "🔹 Network: <b>USDT BEP20 (BSC)</b>\n\n"
        "💰 Send EXACTLY:\n<code>{amount}</code> USDT\n\n"
        "📬 To this address:\n<code>{wallet}</code>\n\n"
        "⚠️ <b>Important:</b> Send exactly this amount — even a tiny difference will prevent confirmation.\n\n"
        "⏰ This request expires in <b>30 minutes</b>.\n"
        "📨 After payment, send <code>/claim TXID</code> or just send the TXID alone."
    ),
    "deposit_confirmed": (
        "🎉 <b>Deposit Confirmed!</b>\n\n"
        "✅ <b>+${amount} USDT</b> added to your balance.\n"
        "🔗 TX: <code>{txhash}</code>"
    ),
    "deposit_already_pending": (
        "⚠️ <b>You already have a pending deposit!</b>\n\n"
        "💰 Amount: <code>{amount}</code> USDT\n"
        "🔹 Network: {network}\n"
        "📬 Address: <code>{wallet}</code>\n\n"
        "⏰ Expires: {expires}\n\n"
        "After payment, send <code>/claim TXID</code> or just send the TXID."
    ),
    "deposit_no_pending": "⚠️ You do not have any active deposit request. Use /deposit first.",
    "claim_usage": "❌ Usage: <code>/claim TXID</code>\nYou can also send the TXID alone.",
    "claim_invalid_txid": "❌ Invalid TXID format. Send the full blockchain transaction hash.",
    "claim_checking": "🔎 Checking your transaction...",
    "claim_expired": "⚠️ This deposit request expired. Please create a new one with /deposit.",
    "claim_already_used": "⚠️ This transaction has already been claimed.",
    "claim_not_found": "❌ I could not find this TXID for your pending {network} deposit to our wallet.",
    "claim_not_confirmed": "⏳ This transaction is not confirmed yet. Try again in a moment.",
    "claim_amount_mismatch": (
        "❌ Amount mismatch.\n"
        "Expected: <code>{expected}</code> USDT\n"
        "Found: <code>{found}</code> USDT\n"
        "Only an exact match is accepted."
    ),
    "claim_error": "❌ Could not verify this TXID right now. Please try again shortly.",
    "deposit_invalid_amount": "❌ Invalid amount. Please enter a valid number (e.g. <code>10</code> or <code>5.5</code>):",
    "deposit_min_amount": "❌ Minimum deposit is $1. Please enter a higher amount:",
    "deposit_baridimob_info": (
        "🇩🇿 <b>BaridiMob Deposit</b>\n\n"
        "RIB: <code>{rib}</code>\n\n"
        "📌 Rate: 1000 DA = $4 | 630 DA = $2.5\n\n"
        "After payment, send screenshot to /support and admin will credit your balance."
    ),
    "balance_msg": (
        "💰 <b>Your Balance:</b> ${balance}\n"
        "💵 <b>Activate Price:</b> ${price}\n"
        "✅ <b>Successes:</b> {succ}\n"
        "❌ <b>Failed:</b> {fail}"
    ),
    "profile_msg": (
        "👤 <b>Your Profile</b>\n\n"
        "🆔 <b>ID:</b> <code>{uid}</code>\n"
        "👤 <b>Name:</b> {name}\n"
        "💰 <b>Balance:</b> ${balance}\n"
        "💵 <b>Activate Price:</b> ${price}\n"
        "✅ <b>Successes:</b> {succ}\n"
        "❌ <b>Failed:</b> {fail}"
    ),
    "profile_ext_msg": (
        "👤 <b>Your Store Profile</b>\n\n"
        "🆔 <b>ID:</b> <code>{uid}</code>\n"
        "👤 <b>Name:</b> {name}\n"
        "💰 <b>Balance:</b> ${balance}\n"
        "🛍️ <b>Purchases:</b> {orders}"
    ),
    "shop_title": "🛒 <b>Digital Store</b>\nSelect a Category:",
    "shop_empty": "📭 The shop is currently empty.",
    "shop_cat": "📂 <b>Category: {cat}</b>\nSelect a product:",
    "shop_prod_view": (
        "📦 <b>{name}</b>\n\n"
        "📝 {desc}\n\n"
        "💵 Price: ${price}\n"
        "🚚 Delivery: {delivery}\n\n"
        "👇 Click Buy to purchase."
    ),
    "shop_ask_qty": "🔢 <b>How many do you want to buy?</b>\n(Send a number)",
    "prod_buy_confirm": (
        "📝 <b>Confirm Purchase</b>\n"
        "📦 Product: {name}\n"
        "🔢 Quantity: {qty}\n"
        "💵 Total Cost: ${total}\n\n"
        "Type 'yes' to confirm or 'cancel'."
    ),
    "prod_bought": (
        "✅ <b>Purchase Successful!</b>\n"
        "📦 <b>Product:</b> {name}\n"
        "🔢 <b>Qty:</b> {qty}\n"
        "💵 <b>Cost:</b> ${total}"
    ),
    "prod_codes_delivered": "🔐 <b>Your codes:</b>\n\n<code>{codes}</code>",
    "prod_file_delivered": "📎 <b>Your file was delivered automatically.</b>",
    "prod_no_stock": "❌ This product is out of stock or the quantity is unavailable.",
    "prod_no_bal": "❌ You do not have enough shop balance ($) to buy <b>{name}</b>.",
    "buy_cancelled": "🚫 Purchase cancelled.",
    "btn_buy": "🛒 Buy",
    "btn_activate": "⚡ Activate",
    "btn_shop": "🛒 Shop",
    "btn_deposit": "💰 Deposit",
    "btn_profile": "👤 Profile",
    "btn_history": "📜 History",
    "btn_daily": "📅 Check-in",
    "btn_check": "💰 Balance",
    "btn_lang": "🌐 Language",
    "btn_help": "❓ Help",
    "btn_support": "📞 Support",
    "btn_invite": "🤝 Invite",
    "btn_back": "🔙 Back",
    "btn_home": "🏠 Home",
    "btn_confirm": "✅ Confirm",
    "btn_cancel": "❌ Cancel",
    "btn_adm_stats": "📊 Stats",
    "btn_adm_bal": "💰 My Wallet",
    "btn_adm_users": "👥 Users",
    "btn_adm_reseller": "💼 Reseller Tools",
    "btn_adm_owner": "👑 Owner Tools",
    "btn_adm_shop": "🛒 Shop Manager",
    "btn_adm_sys": "⚙️ System",
    "btn_adm_data": "📦 Data Backup",
    "btn_adm_external": "🌐 External Shops",
    "support_welcome": "📞 <b>Support Center</b>\n\nPlease describe your issue in one message.",
    "support_sent": "✅ <b>Message Sent!</b> Please wait for a reply.",
    "support_reply": "📩 <b>Admin Reply:</b>\n{msg}",
    "history_title": "📜 <b>Last 5 Activations:</b>\n\n{log}",
    "history_empty": "📭 No history found.",
    "invite_msg_user": (
        "🤝 <b>Invitation Link:</b>\n\n"
        "Share this link with others:\n"
        "<code>https://t.me/{bot}?start={uid}</code>"
    ),
    "balance_added_msg": "💰 <b>Balance Updated!</b>\nAdmin added +${amount} to your account.",
    "balance_removed_msg": "💰 <b>Balance Updated!</b>\nAdmin removed ${amount} from your account.",
    "shop_added_msg": "🛒 <b>Shop Wallet Updated!</b>\nAdmin added +${amount} to your shop wallet.",
    "shop_removed_msg": "🛒 <b>Shop Wallet Updated!</b>\nAdmin removed ${amount} from your shop wallet.",
    "reseller_notify": (
        "✅ <b>Activation successful for your client!</b>\n\n"
        "👤 User: <code>{uid}</code>\n"
        "📧 <code>{email}</code>\n"
        "🆔 Job ID: <code>{job_id}</code>\n"
        "🧾 TX: <code>#{tx}</code>\n"
        "💵 Amount: ${amount}\n"
        "💰 Your profit: +${profit}"
    ),
    "owner_notify": (
        "🔔 <b>Activation Notification</b>\n\n"
        "🧾 Transaction: <code>#{tx}</code>\n"
        "👤 User: <code>{uid}</code>\n"
        "📧 Email: <code>{email}</code>\n"
        "💵 Amount: ${amount}\n"
        "🔗 Link: {url}"
    ),
}

LANGS = {"en": EN_TEXTS}


# ── Help command sets ─────────────────────────────────────────────────────

HELP_SETS = {
    "main_user": {
        "en": [
            ("/start", "Open the main menu."),
            ("/activate", "Activate Google One with your account."),
            ("/shop", "Browse products and buy with your shop wallet."),
            ("/profile", "View your balance and stats."),
            ("/deposit", "Create a deposit request."),
            ("/claim TXID", "Confirm a paid deposit by transaction hash."),
            ("/invite", "Get your referral link."),
            ("/daily", "Claim your daily bonus."),
            ("/history", "View your activation history."),
            ("/language", "Change bot language."),
            ("/help", "Show this command guide."),
            ("/support", "Send a support ticket."),
        ],
    },
    "external_user": {
        "en": [
            ("/start", "Open the store home."),
            ("/shop", "Browse products and buy."),
            ("/profile", "View your wallet and purchase count."),
            ("/language", "Change bot language."),
            ("/help", "Show this command guide."),
            ("/support", "Send a support ticket."),
        ],
    },
    "admin_owner": {
        "en": [
            ("/start", "Open owner control panel."),
            ("/help", "Show owner commands."),
            ("/add", "Add $ balance to a user."),
            ("/remove", "Remove $ balance from a user."),
            ("/addshop", "Add main-shop wallet $ to a user."),
            ("/removeshop", "Remove main-shop wallet $ from a user."),
            ("/check", "Check user details."),
            ("/addreseller", "Promote a user to reseller."),
            ("/delreseller", "Remove reseller role."),
            ("/setprice", "Set global activation price."),
            ("/addrc", "Add reseller wallet."),
            ("/removerc", "Remove reseller wallet."),
            ("/setprofit", "Set reseller profit per activation."),
            ("/rusers", "View reseller clients."),
            ("/uinvites", "View invites of a user + channel subscription stats."),
            ("/rlink", "Link user to reseller."),
            ("/runlink", "Unlink user."),
            ("/addprod", "Add main shop product."),
            ("/delprod", "Delete main shop product."),
            ("/listprod", "List main shop products."),
            ("/addcode", "Add one instant-delivery code to a product."),
            ("/addcodes", "Add multiple instant-delivery codes to a product."),
            ("/addextshop", "Create external shop step by step."),
            ("/delextshop", "Delete an external shop."),
            ("/listextshops", "List external shops."),
            ("/broadcast", "Broadcast to main bot users."),
            ("/broadcast_inactive", "Broadcast to inactive users."),
            ("/maintenance", "Toggle activation maintenance."),
            ("/ban", "Ban a user."),
            ("/unban", "Unban a user."),
            ("/reply", "Reply to a support ticket."),
            ("/resellers", "View resellers report."),
            ("/myinvite", "Your invite link."),
            ("/language", "Change language."),
        ],
    },
    "admin_reseller": {
        "en": [
            ("/start", "Open reseller panel."),
            ("/help", "Show commands."),
            ("/myinvite", "My invite link."),
            ("/add", "Add balance to a user."),
            ("/remove", "Remove balance from a user."),
            ("/check", "Check user details."),
            ("/resellers", "My report."),
            ("/language", "Change language."),
        ],
    },
    "ext_admin": {
        "en": [
            ("/start", "Store control."),
            ("/help", "Help."),
            ("/addshop", "Add wallet $"),
            ("/removeshop", "Remove wallet $"),
            ("/check", "Check user"),
            ("/addprod", "Add product"),
            ("/delprod", "Delete product"),
            ("/listprod", "List products"),
            ("/addcode", "Add one code"),
            ("/addcodes", "Add bulk codes"),
            ("/reply", "Reply ticket"),
            ("/broadcast", "Broadcast"),
            ("/settitle", "Set title"),
        ],
    },
}
