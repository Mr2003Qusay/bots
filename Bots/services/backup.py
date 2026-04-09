# -*- coding: utf-8 -*-
"""Backup service — daily DB backup via admin bot."""

import os
import datetime
import zipfile
import io

from config import DB_PATH, ADMIN_LOG_ID, logger


async def do_backup(bot=None, send_log_fn=None):
    """Create a backup zip and send via log bot. Returns result text."""
    today = str(datetime.date.today())
    try:
        bio = io.BytesIO()
        essential_files = ["bot.py", "bot.db", "pyproject.toml", "uv.lock", "main.py",
                           "config.py", "database.py", "localization.py", "utils.py"]
        essential_dirs = ["storage", "models", "services", "handlers", "ui"]
        with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as zf:
            for fname in essential_files:
                if os.path.isfile(fname):
                    try:
                        zf.write(fname, fname)
                    except Exception:
                        pass
            for dname in essential_dirs:
                if os.path.isdir(dname):
                    for root, dirs, files in os.walk(dname):
                        dirs[:] = [d for d in dirs if d not in ["__pycache__"]]
                        for file_name in files:
                            if file_name.endswith((".db-journal", ".db-wal", ".db-shm")):
                                continue
                            path = os.path.join(root, file_name)
                            try:
                                zf.write(path, path)
                            except Exception:
                                pass
        bio.seek(0)
        size_kb = bio.getbuffer().nbytes // 1024

        if bot and ADMIN_LOG_ID:
            caption = (
                f"📦 <b>Bot Backup — {today}</b>\n\n"
                f"📁 Contains: All bot modules + database\n"
                f"📏 Size: {size_kb} KB"
            )
            await bot.send_document(
                chat_id=ADMIN_LOG_ID,
                document=bio,
                filename=f"bot_backup_{today}.zip",
                caption=caption,
                parse_mode="HTML"
            )
        return f"✅ Backup created ({size_kb} KB)"
    except Exception as e:
        logger.error(f"Backup error: {e}")
        return f"❌ Error: {e}"


async def scheduled_backup(context):
    """Job queue callback for daily backup."""
    try:
        await do_backup(bot=context.bot)
    except Exception as e:
        logger.error(f"Scheduled backup failed: {e}")
