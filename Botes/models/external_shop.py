# -*- coding: utf-8 -*-
"""External shop model — DB CRUD for external shop rows."""

from database import db_connect


def add_external_shop_db(shop_token: str, admin_token: str, owner_id: int, title: str = "External Shop"):
    conn = db_connect()
    conn.execute("""
        INSERT INTO external_shops (shop_token, admin_token, owner_id, title, is_active)
        VALUES (?, ?, ?, ?, 1)
    """, (shop_token.strip(), admin_token.strip(), int(owner_id), title.strip() or "External Shop"))
    conn.commit()
    conn.close()


def get_external_shop_by_shop_token(shop_token: str):
    conn = db_connect()
    row = conn.execute("SELECT * FROM external_shops WHERE shop_token=?", (shop_token.strip(),)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_external_shop_by_admin_token(admin_token: str):
    conn = db_connect()
    row = conn.execute("SELECT * FROM external_shops WHERE admin_token=?", (admin_token.strip(),)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_external_shop_by_id(shop_id: int):
    conn = db_connect()
    row = conn.execute("SELECT * FROM external_shops WHERE id=?", (shop_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_external_shops():
    conn = db_connect()
    rows = conn.execute("SELECT * FROM external_shops ORDER BY id ASC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_active_external_shops():
    conn = db_connect()
    rows = conn.execute("SELECT * FROM external_shops WHERE is_active=1 ORDER BY id ASC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_external_shop_usernames(shop_id: int, shop_username: str = "", admin_username: str = ""):
    conn = db_connect()
    conn.execute(
        "UPDATE external_shops SET shop_username=?, admin_username=? WHERE id=?",
        (shop_username or "", admin_username or "", shop_id)
    )
    conn.commit()
    conn.close()


def update_external_shop_title(shop_id: int, title: str):
    conn = db_connect()
    conn.execute("UPDATE external_shops SET title=? WHERE id=?", (title.strip(), shop_id))
    conn.commit()
    conn.close()


def remove_external_shop_db(shop_id: int):
    row = get_external_shop_by_id(shop_id)
    if not row:
        return None
    conn = db_connect()
    conn.execute("DELETE FROM external_shops WHERE id=?", (shop_id,))
    conn.execute("DELETE FROM products WHERE shop_id=?", (shop_id,))
    conn.execute("DELETE FROM product_codes WHERE shop_id=?", (shop_id,))
    conn.execute("DELETE FROM purchases WHERE shop_id=?", (shop_id,))
    conn.execute("DELETE FROM shop_wallets WHERE shop_id=?", (shop_id,))
    conn.execute("DELETE FROM shop_users WHERE shop_id=?", (shop_id,))
    conn.execute("DELETE FROM tickets WHERE shop_id=?", (shop_id,))
    conn.commit()
    conn.close()
    return row
