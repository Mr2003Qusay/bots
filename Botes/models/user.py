# -*- coding: utf-8 -*-
"""User model — CRUD, balance, bans, stats, history."""

from database import db_connect
from config import (
    DEFAULT_ACTIVATE_PRICE,
    DEFAULT_RESELLER_PROFIT,
)


# ── Config DB ─────────────────────────────────────────────────────────────

def get_config_float(key: str, default: float) -> float:
    conn = db_connect()
    row = conn.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
    conn.close()
    return float(row["value"]) if row and row["value"] is not None else float(default)


def set_config(key: str, value):
    conn = db_connect()
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()


def get_activate_price() -> float:
    return get_config_float("activate_price", DEFAULT_ACTIVATE_PRICE)


def set_activate_price(price: float):
    set_config("activate_price", price)


def get_reseller_profit() -> float:
    return get_config_float("reseller_profit", DEFAULT_RESELLER_PROFIT)


def set_reseller_profit(profit: float):
    set_config("reseller_profit", profit)


def get_maintenance_data():
    conn = db_connect()
    status = conn.execute("SELECT value FROM config WHERE key='maintenance'").fetchone()
    conn.close()
    return bool(status and status["value"] == "1")


def set_maintenance_mode(state: bool):
    conn = db_connect()
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('maintenance', ?)", ("1" if state else "0",))
    conn.commit()
    conn.close()


def get_activate_price_for_user(user_id: int) -> float:
    return get_activate_price()


# ── User CRUD ─────────────────────────────────────────────────────────────

def get_total_users() -> int:
    conn = db_connect()
    row = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()
    conn.close()
    return int(row["c"] if row else 0)


def get_all_users():
    conn = db_connect()
    rows = conn.execute("SELECT user_id FROM users").fetchall()
    conn.close()
    return [int(r["user_id"]) for r in rows]


def get_all_users_detailed():
    conn = db_connect()
    rows = conn.execute("""
        SELECT u.user_id, u.username, u.balance,
               COALESCE(u.success_count, 0) AS activations,
               (SELECT COUNT(*) FROM users inv WHERE inv.referrer_id = u.user_id) AS invites
        FROM users u
        ORDER BY u.rowid ASC
    """).fetchall()
    conn.close()
    return [
        {
            "user_id": int(r["user_id"]),
            "username": r["username"] or "No Username",
            "balance": float(r["balance"] or 0),
            "activations": int(r["activations"] or 0),
            "invites": int(r["invites"] or 0),
        }
        for r in rows
    ]


def update_user_info(user_id: int, username: str, first_name: str = "") -> bool:
    conn = db_connect()
    exists = conn.execute("SELECT 1 FROM users WHERE user_id=?", (user_id,)).fetchone()
    if not exists:
        conn.execute(
            "INSERT INTO users (user_id, username, first_name, last_activity) VALUES (?, ?, ?, datetime('now'))",
            (user_id, username or "", first_name or "")
        )
        new_user = True
    else:
        conn.execute(
            "UPDATE users SET username=?, first_name=?, last_activity=datetime('now') WHERE user_id=?",
            (username or "", first_name or "", user_id)
        )
        new_user = False
    conn.commit()
    conn.close()
    return new_user


def update_shop_user_info(shop_id: int, user_id: int, username: str, first_name: str = "", lang: str = "en"):
    if shop_id <= 0:
        return
    conn = db_connect()
    conn.execute("""
        INSERT INTO shop_users (shop_id, user_id, username, first_name, lang, last_activity)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(shop_id, user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            lang=excluded.lang,
            last_activity=datetime('now')
    """, (shop_id, user_id, username or "", first_name or "", lang or "en"))
    conn.commit()
    conn.close()


def get_user_data(user_id: int) -> dict:
    conn = db_connect()
    conn.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
    conn.commit()
    cols = [r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
    if "p_credits" in cols:
        balance_expr = "COALESCE(balance, COALESCE(p_credits, 0.0)) AS balance"
    else:
        balance_expr = "COALESCE(balance, 0.0) AS balance"
    row = conn.execute(f"""
        SELECT
            {balance_expr},
            COALESCE(shop_balance, 0.0) AS shop_balance,
            COALESCE(lang, 'en') AS lang,
            COALESCE(is_banned, 0) AS is_banned,
            COALESCE(owner_id, 0) AS owner_id,
            last_daily,
            COALESCE(success_count, 0) AS success_count,
            COALESCE(fail_count, 0) AS fail_count,
            COALESCE(first_name, '') AS first_name,
            COALESCE(username, '') AS username,
            COALESCE(referrer_id, 0) AS referrer_id
        FROM users
        WHERE user_id=?
    """, (user_id,)).fetchone()
    conn.close()
    return dict(row) if row else {
        "balance": 0.0, "shop_balance": 0.0, "lang": "en",
        "is_banned": 0, "owner_id": 0, "last_daily": None,
        "success_count": 0, "fail_count": 0, "first_name": "", "username": "", "referrer_id": 0
    }


def is_user_banned(user_id: int) -> bool:
    return int(get_user_data(user_id).get("is_banned", 0)) == 1


def increment_stats(user_id: int, success: bool = True):
    conn = db_connect()
    if success:
        conn.execute("UPDATE users SET success_count=COALESCE(success_count,0)+1 WHERE user_id=?", (user_id,))
    else:
        conn.execute("UPDATE users SET fail_count=COALESCE(fail_count,0)+1 WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()


def set_last_checkin(user_id: int, date_str: str):
    conn = db_connect()
    conn.execute("UPDATE users SET last_daily=? WHERE user_id=?", (date_str, user_id))
    conn.commit()
    conn.close()


def get_id_by_username(username: str):
    conn = db_connect()
    uname = username.replace("@", "").strip()
    row = conn.execute("SELECT user_id FROM users WHERE username LIKE ?", (uname,)).fetchone()
    conn.close()
    return int(row["user_id"]) if row else None


def ban_user(user_id: int, status: int = 1):
    conn = db_connect()
    conn.execute("UPDATE users SET is_banned=? WHERE user_id=?", (int(status), user_id))
    conn.commit()
    conn.close()


def set_lang(user_id: int, lang_code: str):
    conn = db_connect()
    conn.execute("UPDATE users SET lang=? WHERE user_id=?", (lang_code, user_id))
    conn.commit()
    conn.close()


def add_balance(user_id: int, amount: float = 0.0):
    conn = db_connect()
    conn.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
    if amount != 0:
        conn.execute("UPDATE users SET balance=MAX(0, COALESCE(balance,0)+?) WHERE user_id=?", (float(amount), user_id))
    conn.commit()
    conn.close()


def get_user_balance(user_id: int) -> float:
    return float(get_user_data(user_id).get("balance", 0.0))


def get_shop_balance(user_id: int, shop_id: int = 0) -> float:
    return get_user_balance(user_id)


def add_shop_balance(user_id: int, amount: float = 0.0, shop_id: int = 0):
    add_balance(user_id, amount)
    if shop_id != 0:
        conn = db_connect()
        conn.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        conn.execute("INSERT OR IGNORE INTO shop_users (shop_id, user_id) VALUES (?, ?)", (shop_id, user_id))
        conn.commit()
        conn.close()


def bind_referrer(user_id: int, referrer_id: int) -> bool:
    if not referrer_id or referrer_id == user_id:
        return False
    conn = db_connect()
    row = conn.execute("SELECT COALESCE(referrer_id,0) AS rid FROM users WHERE user_id=?", (user_id,)).fetchone()
    if row and int(row["rid"] or 0) != 0:
        conn.close()
        return False
    conn.execute("UPDATE users SET referrer_id=? WHERE user_id=?", (referrer_id, user_id))
    conn.commit()
    conn.close()
    return True


def get_stats():
    conn = db_connect()
    users = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
    deps = conn.execute("SELECT COALESCE(SUM(amount),0) AS s FROM deposits").fetchone()["s"]
    resellers = conn.execute("SELECT COUNT(*) AS c FROM resellers").fetchone()["c"]
    conn.close()
    return int(users), float(deps), int(resellers)


def get_user_history(user_id: int):
    conn = db_connect()
    rows = conn.execute(
        "SELECT email, status, url, reason, tx_id, ts FROM history WHERE user_id=? ORDER BY id DESC LIMIT 5",
        (user_id,)
    ).fetchall()
    conn.close()
    return rows


def set_user_owner(user_id: int, owner_id: int) -> bool:
    if user_id == owner_id:
        return False
    conn = db_connect()
    conn.execute("UPDATE users SET owner_id=? WHERE user_id=?", (owner_id, user_id))
    conn.commit()
    conn.close()
    return True


def get_user_invitees(user_id: int):
    conn = db_connect()
    rows = conn.execute("""
        SELECT user_id, username, COALESCE(success_count,0) AS activations
        FROM users
        WHERE referrer_id = ?
        ORDER BY rowid ASC
    """, (user_id,)).fetchall()
    conn.close()
    return [
        {
            "user_id": int(r["user_id"]),
            "username": r["username"] or "No Username",
            "activations": int(r["activations"] or 0),
        }
        for r in rows
    ]
