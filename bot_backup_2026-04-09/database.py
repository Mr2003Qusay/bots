# -*- coding: utf-8 -*-
"""Database connection, schema initialisation, and migrations.

ChatGPT Workspace tables are **not** created or modified here. Existing
data in the database is untouched — only new code paths no longer interact
with workspace tables.
"""

import sqlite3

from config import (
    DB_PATH,
    DEFAULT_ACTIVATE_PRICE,
    DEFAULT_RESELLER_PROFIT,
    logger,
)
from utils import format_amount_for_network


# ── Connection ────────────────────────────────────────────────────────────

def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── Schema & migrations ──────────────────────────────────────────────────

def init_db():
    conn = db_connect()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            balance REAL DEFAULT 0.0,
            shop_balance REAL DEFAULT 0.0,
            last_daily TEXT,
            referrer_id INTEGER,
            lang TEXT DEFAULT 'en',
            is_banned INTEGER DEFAULT 0,
            owner_id INTEGER DEFAULT 0,
            last_activity DATETIME,
            success_count INTEGER DEFAULT 0,
            fail_count INTEGER DEFAULT 0,
            first_name TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            email TEXT,
            status TEXT,
            url TEXT,
            reason TEXT,
            tx_id TEXT,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS deposits (
            txid TEXT PRIMARY KEY,
            user_id INTEGER,
            amount REAL,
            network TEXT,
            ts DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS pending_deposits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            network TEXT,
            expected_amount REAL,
            base_amount REAL,
            wallet_address TEXT,
            status TEXT DEFAULT 'pending',
            tx_hash TEXT DEFAULT '',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            expires_at DATETIME
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS resellers (
            user_id INTEGER PRIMARY KEY,
            balance REAL DEFAULT 0.0,
            total_sold INTEGER DEFAULT 0,
            profit_per_activation REAL DEFAULT 0.5,
            amount_paid REAL DEFAULT 0.0
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shop_id INTEGER DEFAULT 0,
            name TEXT,
            price REAL DEFAULT 0.0,
            stock INTEGER DEFAULT 0,
            description TEXT DEFAULT '',
            category TEXT DEFAULT 'General',
            file_id TEXT DEFAULT NULL,
            image_id TEXT DEFAULT NULL,
            delivery_type TEXT DEFAULT 'manual',
            auto_delivery INTEGER DEFAULT 0
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS product_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shop_id INTEGER DEFAULT 0,
            product_id INTEGER NOT NULL,
            code_text TEXT NOT NULL,
            is_sold INTEGER DEFAULT 0,
            sold_to INTEGER DEFAULT NULL,
            sold_at DATETIME DEFAULT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(shop_id, product_id, code_text, is_sold)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shop_id INTEGER DEFAULT 0,
            user_id INTEGER,
            product_id INTEGER,
            input_data TEXT,
            price REAL,
            qty INTEGER DEFAULT 1,
            delivery_data TEXT DEFAULT '',
            ts DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS tickets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shop_id INTEGER DEFAULT 0,
            user_id INTEGER,
            status TEXT DEFAULT 'open',
            bot_token TEXT DEFAULT '',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS external_shops (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shop_token TEXT UNIQUE,
            admin_token TEXT UNIQUE,
            owner_id INTEGER NOT NULL,
            title TEXT DEFAULT 'External Shop',
            shop_username TEXT DEFAULT '',
            admin_username TEXT DEFAULT '',
            is_active INTEGER DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS shop_wallets (
            shop_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            balance REAL DEFAULT 0.0,
            PRIMARY KEY (shop_id, user_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS shop_users (
            shop_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            username TEXT DEFAULT '',
            first_name TEXT DEFAULT '',
            lang TEXT DEFAULT 'en',
            last_activity DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (shop_id, user_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS active_jobs_db (
            job_id TEXT PRIMARY KEY,
            uid INTEGER NOT NULL,
            email TEXT NOT NULL,
            cost REAL NOT NULL,
            reseller_id INTEGER NOT NULL DEFAULT 0,
            tx_id TEXT NOT NULL,
            submitted_at REAL NOT NULL,
            status_msg_id INTEGER DEFAULT 0,
            estimated_wait REAL DEFAULT 0
        )
    """)

    # ── Column migrations ─────────────────────────────────────────────────
    migrations = [
        "ALTER TABLE users ADD COLUMN balance REAL DEFAULT 0.0",
        "ALTER TABLE users ADD COLUMN shop_balance REAL DEFAULT 0.0",
        "ALTER TABLE users ADD COLUMN first_name TEXT",
        "ALTER TABLE products ADD COLUMN shop_id INTEGER DEFAULT 0",
        "ALTER TABLE products ADD COLUMN description TEXT DEFAULT ''",
        "ALTER TABLE products ADD COLUMN category TEXT DEFAULT 'General'",
        "ALTER TABLE products ADD COLUMN image_id TEXT DEFAULT NULL",
        "ALTER TABLE products ADD COLUMN delivery_type TEXT DEFAULT 'manual'",
        "ALTER TABLE products ADD COLUMN auto_delivery INTEGER DEFAULT 0",
        "ALTER TABLE purchases ADD COLUMN shop_id INTEGER DEFAULT 0",
        "ALTER TABLE purchases ADD COLUMN qty INTEGER DEFAULT 1",
        "ALTER TABLE purchases ADD COLUMN delivery_data TEXT DEFAULT ''",
        "ALTER TABLE tickets ADD COLUMN shop_id INTEGER DEFAULT 0",
        "ALTER TABLE tickets ADD COLUMN bot_token TEXT DEFAULT ''",
        "ALTER TABLE resellers ADD COLUMN profit_per_activation REAL DEFAULT 0.5",
        "ALTER TABLE resellers ADD COLUMN amount_paid REAL DEFAULT 0.0",
        "ALTER TABLE active_jobs_db ADD COLUMN status_msg_id INTEGER DEFAULT 0",
        "ALTER TABLE active_jobs_db ADD COLUMN estimated_wait REAL DEFAULT 0",
        "ALTER TABLE history ADD COLUMN email TEXT DEFAULT ''",
        "ALTER TABLE history ADD COLUMN tx_id TEXT DEFAULT ''",
        "ALTER TABLE history ADD COLUMN reason TEXT DEFAULT ''",
        "ALTER TABLE pending_deposits ADD COLUMN expected_amount_str TEXT DEFAULT ''",
        "ALTER TABLE products ADD COLUMN hidden INTEGER DEFAULT 0",
        "ALTER TABLE purchases ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP",
    ]
    for q in migrations:
        try:
            c.execute(q)
        except Exception:
            pass

    # Backfill exact expected amount strings for legacy pending deposits
    try:
        rows = c.execute(
            "SELECT id, network, expected_amount FROM pending_deposits "
            "WHERE COALESCE(expected_amount_str, '')='' "
        ).fetchall()
        for row in rows:
            c.execute(
                "UPDATE pending_deposits SET expected_amount_str=? WHERE id=?",
                (format_amount_for_network(row[2] or 0, row[1] or "TRC20", trim=False), row[0]),
            )
    except Exception:
        pass

    # Migrate p_credits -> balance for existing databases (ONE-TIME only)
    try:
        already = c.execute("SELECT value FROM config WHERE key='p_credits_migrated'").fetchone()
        if not already:
            cols = [r[1] for r in c.execute("PRAGMA table_info(users)").fetchall()]
            if "p_credits" in cols and "balance" in cols:
                c.execute(
                    "UPDATE users SET balance=COALESCE(p_credits,0.0) "
                    "WHERE balance=0.0 AND p_credits IS NOT NULL AND p_credits > 0"
                )
            c.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('p_credits_migrated', '1')")
    except Exception:
        pass

    # Retroactively link users invited by resellers
    try:
        already = c.execute("SELECT value FROM config WHERE key='reseller_owner_linked'").fetchone()
        if not already:
            c.execute("""
                UPDATE users SET owner_id = referrer_id
                WHERE referrer_id IS NOT NULL AND referrer_id != 0
                  AND (owner_id IS NULL OR owner_id = 0)
                  AND referrer_id IN (SELECT user_id FROM resellers)
            """)
            c.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('reseller_owner_linked', '1')")
    except Exception:
        pass

    # ONE-TIME: link ALL users inserted after position 271 to reseller 6914433826
    LEGACY_RESELLER_ID = 6914433826
    try:
        already = c.execute("SELECT value FROM config WHERE key='legacy_reseller_link_6914433826_v2'").fetchone()
        if not already:
            c.execute("INSERT OR IGNORE INTO users (user_id, lang) VALUES (?, 'en')", (LEGACY_RESELLER_ID,))
            c.execute("INSERT OR IGNORE INTO resellers (user_id) VALUES (?)", (LEGACY_RESELLER_ID,))
            c.execute("""
                UPDATE users SET owner_id = ?
                WHERE user_id != ?
                  AND user_id IN (SELECT user_id FROM users ORDER BY rowid LIMIT -1 OFFSET 271)
            """, (LEGACY_RESELLER_ID, LEGACY_RESELLER_ID))
            c.execute("INSERT OR REPLACE INTO config (key, value) VALUES ('legacy_reseller_link_6914433826_v2', '1')")
    except Exception:
        pass

    c.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('activate_price', ?)", (str(DEFAULT_ACTIVATE_PRICE),))
    c.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('reseller_profit', ?)", (str(DEFAULT_RESELLER_PROFIT),))
    c.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('maintenance', '0')")

    conn.commit()
    conn.close()


# ── Job persistence helpers ───────────────────────────────────────────────

def db_save_job(job_id: str, uid: int, email: str, cost: float,
                reseller_id: int, tx_id: str, submitted_at: float,
                status_msg_id: int = 0, estimated_wait: float = 0.0):
    try:
        conn = db_connect()
        conn.execute(
            "INSERT OR REPLACE INTO active_jobs_db "
            "(job_id, uid, email, cost, reseller_id, tx_id, submitted_at, status_msg_id, estimated_wait) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (job_id, uid, email, cost, reseller_id, tx_id, submitted_at, status_msg_id, estimated_wait),
        )
        conn.commit()
        conn.close()
        logger.info(f"JOB_SAVED uid={uid} email={email} job_id={job_id} tx_id={tx_id}")
    except Exception as e:
        logger.error(f"db_save_job error: {e}")


def db_update_job_msg(job_id: str, status_msg_id: int):
    try:
        conn = db_connect()
        conn.execute("UPDATE active_jobs_db SET status_msg_id=? WHERE job_id=?", (status_msg_id, job_id))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"db_update_job_msg error: {e}")


def db_remove_job(job_id: str) -> bool:
    """Delete job from DB. Returns True only if THIS call actually deleted it."""
    try:
        conn = db_connect()
        cur = conn.execute("DELETE FROM active_jobs_db WHERE job_id=?", (job_id,))
        affected = cur.rowcount
        conn.commit()
        conn.close()
        return affected > 0
    except Exception as e:
        logger.error(f"db_remove_job error: {e}")
        return False


def db_load_jobs() -> list:
    try:
        conn = db_connect()
        rows = conn.execute(
            "SELECT job_id, uid, email, cost, reseller_id, tx_id, submitted_at, "
            "COALESCE(status_msg_id,0), COALESCE(estimated_wait,0) FROM active_jobs_db"
        ).fetchall()
        conn.close()
        return [
            {
                "job_id": r[0], "uid": r[1], "email": r[2], "cost": r[3],
                "reseller_id": r[4], "tx_id": r[5], "submitted_at": r[6],
                "status_msg_id": r[7], "estimated_wait": r[8],
            }
            for r in rows
        ]
    except Exception as e:
        logger.error(f"db_load_jobs error: {e}")
        return []
