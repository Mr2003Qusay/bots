# -*- coding: utf-8 -*-
"""Shop / Product model — CRUD, codes, categories, purchases."""

from database import db_connect
from localization import get_user_lang


# ── Stock sync ────────────────────────────────────────────────────────────

def sync_product_stock_from_codes(shop_id: int, product_id: int):
    conn = db_connect()
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM product_codes WHERE shop_id=? AND product_id=? AND is_sold=0",
        (shop_id, product_id)
    ).fetchone()
    available = int(row["c"]) if row else 0
    conn.execute(
        "UPDATE products SET stock=?, delivery_type='codes', auto_delivery=1 WHERE id=? AND shop_id=?",
        (available, product_id, shop_id)
    )
    conn.commit()
    conn.close()


# ── Product CRUD ──────────────────────────────────────────────────────────

def add_product_db(shop_id: int, name: str, price: float, stock: int,
                   category: str = "General", desc: str = "",
                   file_id: str = None, image_id: str = None):
    delivery_type = "file" if file_id else "manual"
    auto_delivery = 1 if file_id else 0
    conn = db_connect()
    conn.execute("""
        INSERT INTO products (shop_id, name, price, stock, category, description,
                              file_id, image_id, delivery_type, auto_delivery)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (shop_id, name, float(price), int(stock), category or "General",
          desc or "", file_id, image_id, delivery_type, auto_delivery))
    conn.commit()
    conn.close()


def del_product(shop_id: int, product_id: int):
    conn = db_connect()
    conn.execute("DELETE FROM product_codes WHERE shop_id=? AND product_id=?", (shop_id, product_id))
    conn.execute("DELETE FROM products WHERE id=? AND shop_id=?", (product_id, shop_id))
    conn.commit()
    conn.close()


def get_product(shop_id: int, product_id: int):
    conn = db_connect()
    row = conn.execute("""
        SELECT id, shop_id, name, price, stock, file_id, description,
               image_id, category, delivery_type, auto_delivery
        FROM products WHERE id=? AND shop_id=?
    """, (product_id, shop_id)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_products(shop_id: int):
    conn = db_connect()
    rows = conn.execute("""
        SELECT id, name, price, stock, category, delivery_type, auto_delivery,
               COALESCE(hidden, 0) AS hidden
        FROM products WHERE shop_id=? ORDER BY category, name
    """, (shop_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_categories(shop_id: int):
    conn = db_connect()
    rows = conn.execute(
        "SELECT DISTINCT category FROM products WHERE shop_id=? AND COALESCE(hidden, 0)=0 ORDER BY category",
        (shop_id,)
    ).fetchall()
    conn.close()
    return [r["category"] for r in rows]


def get_products_by_cat(shop_id: int, cat: str):
    conn = db_connect()
    rows = conn.execute("""
        SELECT id, name, price, stock, delivery_type, auto_delivery
        FROM products WHERE shop_id=? AND category=? AND COALESCE(hidden, 0)=0 ORDER BY name
    """, (shop_id, cat)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def reduce_stock(shop_id: int, product_id: int, qty: int = 1):
    conn = db_connect()
    conn.execute(
        "UPDATE products SET stock=MAX(0, stock-?) WHERE id=? AND shop_id=?",
        (int(qty), product_id, shop_id)
    )
    conn.commit()
    conn.close()


def record_purchase(shop_id: int, user_id: int, product_id: int,
                    price: float, qty: int, input_data: str, delivery_data: str = ""):
    conn = db_connect()
    conn.execute("""
        INSERT INTO purchases (shop_id, user_id, product_id, input_data, price, qty, delivery_data)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (shop_id, user_id, product_id, input_data, float(price), int(qty), delivery_data or ""))
    conn.commit()
    conn.close()


def get_purchase_count(user_id: int, shop_id: int = 0) -> int:
    conn = db_connect()
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM purchases WHERE user_id=? AND shop_id=?",
        (user_id, shop_id)
    ).fetchone()
    conn.close()
    return int(row["c"]) if row else 0


def get_shop_user_count(shop_id: int) -> int:
    if shop_id == 0:
        from models.user import get_total_users
        return get_total_users()
    conn = db_connect()
    row = conn.execute("SELECT COUNT(*) AS c FROM shop_users WHERE shop_id=?", (shop_id,)).fetchone()
    conn.close()
    return int(row["c"]) if row else 0


def get_shop_product_count(shop_id: int) -> int:
    conn = db_connect()
    row = conn.execute("SELECT COUNT(*) AS c FROM products WHERE shop_id=?", (shop_id,)).fetchone()
    conn.close()
    return int(row["c"]) if row else 0


# ── Codes ─────────────────────────────────────────────────────────────────

def add_product_code(shop_id: int, product_id: int, code_text: str):
    conn = db_connect()
    conn.execute("""
        INSERT OR IGNORE INTO product_codes (shop_id, product_id, code_text, is_sold)
        VALUES (?, ?, ?, 0)
    """, (shop_id, product_id, code_text.strip()))
    conn.commit()
    conn.close()
    sync_product_stock_from_codes(shop_id, product_id)


def add_product_codes_bulk(shop_id: int, product_id: int, codes):
    conn = db_connect()
    inserted = 0
    for code in codes:
        code = code.strip()
        if not code:
            continue
        try:
            conn.execute("""
                INSERT OR IGNORE INTO product_codes (shop_id, product_id, code_text, is_sold)
                VALUES (?, ?, ?, 0)
            """, (shop_id, product_id, code))
            inserted += 1
        except Exception:
            pass
    conn.commit()
    conn.close()
    sync_product_stock_from_codes(shop_id, product_id)
    return inserted


def get_available_code_count(shop_id: int, product_id: int) -> int:
    conn = db_connect()
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM product_codes WHERE shop_id=? AND product_id=? AND is_sold=0",
        (shop_id, product_id)
    ).fetchone()
    conn.close()
    return int(row["c"]) if row else 0


def claim_product_codes(shop_id: int, product_id: int, user_id: int, qty: int):
    """Claim up to `qty` codes. Returns whatever is available (may be less than qty)."""
    conn = db_connect()
    rows = conn.execute("""
        SELECT id, code_text FROM product_codes
        WHERE shop_id=? AND product_id=? AND is_sold=0
        ORDER BY id ASC LIMIT ?
    """, (shop_id, product_id, int(qty))).fetchall()
    if not rows:
        conn.close()
        return []
    ids = [int(r["id"]) for r in rows]
    placeholders = ",".join(["?"] * len(ids))
    conn.execute(
        f"UPDATE product_codes SET is_sold=1, sold_to=?, sold_at=datetime('now') WHERE id IN ({placeholders})",
        [user_id] + ids
    )
    conn.commit()
    conn.close()
    sync_product_stock_from_codes(shop_id, product_id)
    return [r["code_text"] for r in rows]


def delivery_type_label(user_id: int, product: dict) -> str:
    mode = (product.get("delivery_type") or "manual").lower()
    lang = get_user_lang(user_id)
    if lang == "ar":
        return {"codes": "كود تلقائي", "file": "ملف تلقائي"}.get(mode, "يدوي")
    return {"codes": "Auto Code", "file": "Auto File"}.get(mode, "Manual")


def get_shop_users(shop_id: int):
    if shop_id == 0:
        from models.user import get_all_users
        return get_all_users()
    conn = db_connect()
    rows = conn.execute("SELECT user_id FROM shop_users WHERE shop_id=?", (shop_id,)).fetchall()
    conn.close()
    return [int(r["user_id"]) for r in rows]


# ── Enhanced product management ───────────────────────────────────────────

def update_product(shop_id: int, product_id: int, **kwargs):
    """Update product fields. Allowed keys: name, price, description, category, image_id, hidden, stock."""
    allowed = {"name", "price", "description", "category", "image_id", "hidden", "stock"}
    fields = {k: v for k, v in kwargs.items() if k in allowed}
    if not fields:
        return False
    set_clause = ", ".join(f"{k}=?" for k in fields)
    values = list(fields.values()) + [product_id, shop_id]
    conn = db_connect()
    conn.execute(f"UPDATE products SET {set_clause} WHERE id=? AND shop_id=?", values)
    conn.commit()
    conn.close()
    return True


def toggle_product_hidden(shop_id: int, product_id: int) -> bool:
    """Toggle hidden flag. Returns new hidden state."""
    conn = db_connect()
    row = conn.execute("SELECT COALESCE(hidden, 0) AS h FROM products WHERE id=? AND shop_id=?",
                       (product_id, shop_id)).fetchone()
    if not row:
        conn.close()
        return False
    new_val = 0 if int(row["h"]) else 1
    conn.execute("UPDATE products SET hidden=? WHERE id=? AND shop_id=?", (new_val, product_id, shop_id))
    conn.commit()
    conn.close()
    return bool(new_val)


def get_product_sales(shop_id: int, product_id: int, limit: int = 10):
    """Get recent purchase records for a product."""
    conn = db_connect()
    rows = conn.execute("""
        SELECT p.user_id, p.qty, p.price, p.ts, u.username
        FROM purchases p
        LEFT JOIN users u ON p.user_id = u.user_id
        WHERE p.shop_id=? AND p.product_id=?
        ORDER BY p.id DESC LIMIT ?
    """, (shop_id, product_id, limit)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_product_total_sales(shop_id: int, product_id: int):
    """Get total orders and revenue for a product."""
    conn = db_connect()
    row = conn.execute(
        "SELECT COUNT(*) AS orders, COALESCE(SUM(price),0) AS revenue FROM purchases WHERE shop_id=? AND product_id=?",
        (shop_id, product_id)
    ).fetchone()
    conn.close()
    return int(row["orders"]) if row else 0, float(row["revenue"]) if row else 0.0


def get_product_detailed(shop_id: int, product_id: int):
    """Get full product info including hidden, codes count, sales."""
    conn = db_connect()
    row = conn.execute("""
        SELECT id, shop_id, name, price, stock, file_id, description,
               image_id, category, delivery_type, auto_delivery,
               COALESCE(hidden, 0) AS hidden
        FROM products WHERE id=? AND shop_id=?
    """, (product_id, shop_id)).fetchone()
    if not row:
        conn.close()
        return None
    p = dict(row)
    codes = conn.execute(
        "SELECT COUNT(*) AS c FROM product_codes WHERE shop_id=? AND product_id=? AND is_sold=0",
        (shop_id, product_id)
    ).fetchone()
    p["available_codes"] = int(codes["c"]) if codes else 0
    sales = conn.execute(
        "SELECT COUNT(*) AS orders, COALESCE(SUM(price),0) AS revenue FROM purchases WHERE shop_id=? AND product_id=?",
        (shop_id, product_id)
    ).fetchone()
    p["total_orders"] = int(sales["orders"]) if sales else 0
    p["total_revenue"] = float(sales["revenue"]) if sales else 0.0
    conn.close()
    return p

