# -*- coding: utf-8 -*-
"""Reseller model — balance, clients, profit logic."""

from database import db_connect
from config import OWNER_ID


def is_reseller(user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    conn = db_connect()
    row = conn.execute("SELECT 1 FROM resellers WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return row is not None


def get_reseller_balance(user_id: int) -> float:
    conn = db_connect()
    row = conn.execute("SELECT COALESCE(balance,0) AS b FROM resellers WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    return float(row["b"]) if row else 0.0


def add_reseller_balance(user_id: int, amount: float):
    conn = db_connect()
    conn.execute("INSERT OR IGNORE INTO resellers (user_id) VALUES (?)", (user_id,))
    conn.execute("UPDATE resellers SET balance=COALESCE(balance,0)+? WHERE user_id=?", (float(amount), user_id))
    conn.commit()
    conn.close()


def get_reseller_stats(reseller_id: int) -> int:
    conn = db_connect()
    row = conn.execute("SELECT COUNT(*) AS c FROM users WHERE owner_id=?", (reseller_id,)).fetchone()
    conn.close()
    return int(row["c"]) if row else 0


def get_reseller_clients(reseller_id: int):
    conn = db_connect()
    rows = conn.execute("SELECT user_id, username FROM users WHERE owner_id=?", (reseller_id,)).fetchall()
    conn.close()
    return [(int(r["user_id"]), r["username"]) for r in rows]


def get_reseller_clients_detailed(reseller_id: int):
    conn = db_connect()
    rows = conn.execute("""
        SELECT u.user_id, u.username, u.balance,
               COALESCE(u.success_count, 0) AS activations,
               (SELECT COUNT(*) FROM users inv WHERE inv.referrer_id = u.user_id) AS invites
        FROM users u
        WHERE u.owner_id = ?
        ORDER BY u.user_id ASC
    """, (reseller_id,)).fetchall()
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


def delete_reseller(user_id: int):
    conn = db_connect()
    conn.execute("DELETE FROM resellers WHERE user_id=?", (user_id,))
    conn.commit()
    conn.close()


def reseller_give_balance(reseller_id: int, customer_id: int, amount: float):
    conn = db_connect()
    bal = conn.execute("SELECT COALESCE(balance,0) AS b FROM resellers WHERE user_id=?", (reseller_id,)).fetchone()
    if not bal or float(bal["b"]) < amount:
        conn.close()
        return False, "⚠️ Insufficient Reseller Balance."
    conn.execute(
        "UPDATE resellers SET balance=balance-?, total_sold=COALESCE(total_sold,0)+? WHERE user_id=?",
        (float(amount), float(amount), reseller_id)
    )
    conn.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (customer_id,))
    conn.execute(
        "UPDATE users SET balance=COALESCE(balance,0)+?, "
        "owner_id=CASE WHEN COALESCE(owner_id,0)=0 THEN ? ELSE owner_id END WHERE user_id=?",
        (float(amount), reseller_id, customer_id)
    )
    conn.commit()
    conn.close()
    return True, "Success"


def reseller_remove_balance(reseller_id: int, customer_id: int, amount: float):
    conn = db_connect()
    row = conn.execute(
        "SELECT COALESCE(owner_id,0) AS owner_id, COALESCE(balance,0) AS b FROM users WHERE user_id=?",
        (customer_id,)
    ).fetchone()
    if not row:
        conn.close()
        return False, "User not found."
    if int(row["owner_id"] or 0) != reseller_id and reseller_id != OWNER_ID:
        conn.close()
        return False, "⛔ Not your user!"
    if float(row["b"] or 0) < amount:
        conn.close()
        return False, "Insufficient balance."
    conn.execute("UPDATE users SET balance=MAX(0,balance-?) WHERE user_id=?", (float(amount), customer_id))
    conn.execute("UPDATE resellers SET balance=COALESCE(balance,0)+? WHERE user_id=?", (float(amount), reseller_id))
    conn.commit()
    conn.close()
    return True, "Success"


def get_reseller_profit() -> float:
    from config import DEFAULT_RESELLER_PROFIT
    conn = db_connect()
    row = conn.execute("SELECT value FROM config WHERE key='reseller_profit'").fetchone()
    conn.close()
    if row:
        try:
            return float(row["value"])
        except Exception:
            pass
    return DEFAULT_RESELLER_PROFIT


def set_reseller_profit(amount: float):
    conn = db_connect()
    conn.execute(
        "INSERT OR REPLACE INTO config (key, value) VALUES ('reseller_profit', ?)",
        (str(amount),)
    )
    conn.commit()
    conn.close()

