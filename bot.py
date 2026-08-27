import asyncio
import csv
import io
import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    Update,
)
from telegram.error import (
    BadRequest,
    Forbidden,
    NetworkError,
    RetryAfter,
    TelegramError,
    TimedOut,
)
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ============================================================
# CONFIG
# ============================================================

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_ID_RAW = os.getenv("OWNER_ID", "8753914631").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///bot_data.db").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing.")

try:
    OWNER_ID = int(OWNER_ID_RAW)
except (TypeError, ValueError):
    raise RuntimeError("OWNER_ID must be a numeric Telegram user ID.")

if DATABASE_URL.startswith("sqlite:///"):
    DB_PATH = Path(DATABASE_URL[len("sqlite:///"):])
elif DATABASE_URL.startswith("sqlite://"):
    DB_PATH = Path(DATABASE_URL[len("sqlite://"):])
else:
    raise RuntimeError(
        "This build supports SQLite only. "
        "Set DATABASE_URL=sqlite:///bot_data.db"
    )

DB_PATH.parent.mkdir(parents=True, exist_ok=True)
BACKUP_DIR = DB_PATH.parent / "backups"
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

MAX_RETRIES = 3
BROADCAST_DELAY = 0.08
MAX_BUTTONS = 100
MAX_TEXT_LENGTH = 4096

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("telegram_join_request_bot")


# ============================================================
# HELPERS
# IMPORTANT: defined BEFORE Database is instantiated.
# This fixes the original NameError during startup.
# ============================================================

def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def safe_int(value, default=0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def valid_http_url(url: str) -> bool:
    return bool(
        re.fullmatch(
            r"https?://[^\s]+",
            url.strip(),
            flags=re.IGNORECASE,
        )
    )


def parse_json(value, fallback):
    try:
        result = json.loads(value)
        return result
    except (TypeError, ValueError, json.JSONDecodeError):
        return fallback


def clean_error(exc: Exception) -> str:
    return str(exc)[:4000]


# ============================================================
# DATABASE
# ============================================================

class Database:
    def __init__(self, path: Path):
        self.path = path
        self.conn: Optional[sqlite3.Connection] = None
        self.connect_lock = asyncio.Lock()

    def connect(self):
        if self.conn is not None:
            return

        self.path.parent.mkdir(parents=True, exist_ok=True)

        self.conn = sqlite3.connect(
            str(self.path),
            timeout=30,
            check_same_thread=False,
        )
        self.conn.row_factory = sqlite3.Row

        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=30000")

        self.create_schema()
        self.seed_defaults()

    def close(self):
        if self.conn is not None:
            try:
                self.conn.commit()
            except Exception:
                pass
            try:
                self.conn.close()
            except Exception:
                logger.exception("Database close failed")
            finally:
                self.conn = None

    def execute(self, query, params=(), commit=False):
        self.connect()
        cursor = self.conn.execute(query, params)
        if commit:
            self.conn.commit()
        return cursor

    def executemany(self, query, rows, commit=False):
        self.connect()
        cursor = self.conn.executemany(query, rows)
        if commit:
            self.conn.commit()
        return cursor

    def fetchone(self, query, params=()):
        return self.execute(query, params).fetchone()

    def fetchall(self, query, params=()):
        return self.execute(query, params).fetchall()

    def create_schema(self):
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                language_code TEXT,
                is_bot INTEGER NOT NULL DEFAULT 0,
                is_blocked INTEGER NOT NULL DEFAULT 0,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_users_last_seen
            ON users(last_seen);

            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY,
                role TEXT NOT NULL DEFAULT 'admin',
                permissions TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS channels (
                channel_id INTEGER PRIMARY KEY,
                username TEXT,
                title TEXT,
                type TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                required INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                media_type TEXT NOT NULL DEFAULT 'none',
                file_id TEXT,
                caption TEXT NOT NULL DEFAULT '',
                parse_mode TEXT NOT NULL DEFAULT 'HTML',
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS message_buttons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                url TEXT NOT NULL,
                row_number INTEGER NOT NULL DEFAULT 0,
                position INTEGER NOT NULL DEFAULT 0,
                enabled INTEGER NOT NULL DEFAULT 1,
                FOREIGN KEY(message_id)
                    REFERENCES messages(id)
                    ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_message_buttons_message
            ON message_buttons(message_id);

            CREATE TABLE IF NOT EXISTS join_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                requested_at TEXT NOT NULL,
                message_sent INTEGER NOT NULL DEFAULT 0,
                message_sent_at TEXT,
                error TEXT,
                status TEXT NOT NULL DEFAULT 'received',
                event_key TEXT UNIQUE,
                FOREIGN KEY(user_id)
                    REFERENCES users(user_id)
            );

            CREATE INDEX IF NOT EXISTS idx_join_requests_user
            ON join_requests(user_id);

            CREATE INDEX IF NOT EXISTS idx_join_requests_channel
            ON join_requests(channel_id);

            CREATE INDEX IF NOT EXISTS idx_join_requests_requested
            ON join_requests(requested_at);

            CREATE TABLE IF NOT EXISTS broadcasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id INTEGER NOT NULL,
                text TEXT,
                media_type TEXT NOT NULL DEFAULT 'none',
                file_id TEXT,
                caption TEXT,
                parse_mode TEXT NOT NULL DEFAULT 'HTML',
                buttons_json TEXT NOT NULL DEFAULT '[]',
                status TEXT NOT NULL DEFAULT 'pending',
                total INTEGER NOT NULL DEFAULT 0,
                sent INTEGER NOT NULL DEFAULT 0,
                failed INTEGER NOT NULL DEFAULT 0,
                blocked INTEGER NOT NULL DEFAULT 0,
                next_user_id INTEGER,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT
            );

            CREATE TABLE IF NOT EXISTS broadcast_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                broadcast_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                status TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                UNIQUE(broadcast_id, user_id),
                FOREIGN KEY(broadcast_id)
                    REFERENCES broadcasts(id)
                    ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_broadcast_logs_broadcast
            ON broadcast_logs(broadcast_id);

            CREATE TABLE IF NOT EXISTS bot_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT,
                user_id INTEGER,
                channel_id INTEGER,
                details TEXT,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_bot_events_created
            ON bot_events(created_at);

            CREATE TABLE IF NOT EXISTS error_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                level TEXT,
                module TEXT,
                event TEXT,
                exception TEXT,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_error_logs_created
            ON error_logs(created_at);

            CREATE TABLE IF NOT EXISTS backups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT,
                created_at TEXT NOT NULL,
                created_by INTEGER,
                size INTEGER
            );
            """
        )
        self.conn.commit()

    def seed_defaults(self):
        now = utc_now()

        defaults = {
            "maintenance_mode": "0",
            "auto_message_enabled": "1",
            "start_message": "Please join our channel to continue.",
            "start_button_text": "JOIN NOW",
            "check_join_enabled": "0",
            "bot_name": "Join Request Bot",
        }

        for key, value in defaults.items():
            self.execute(
                """
                INSERT OR IGNORE INTO bot_settings(key, value)
                VALUES (?, ?)
                """,
                (key, value),
            )

        self.execute(
            """
            INSERT OR IGNORE INTO admins(
                user_id, role, permissions, created_at
            )
            VALUES (?, 'owner', ?, ?)
            """,
            (
                OWNER_ID,
                json.dumps({"all": True}),
                now,
            ),
        )

        self.conn.commit()

    def get_setting(self, key, default=""):
        row = self.fetchone(
            "SELECT value FROM bot_settings WHERE key=?",
            (key,),
        )
        return row["value"] if row else default

    def set_setting(self, key, value):
        self.execute(
            """
            INSERT INTO bot_settings(key,value)
            VALUES(?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, str(value)),
            commit=True,
        )

    def upsert_user(self, user):
        now = utc_now()

        self.execute(
            """
            INSERT INTO users(
                user_id, username, first_name, last_name,
                language_code, is_bot, first_seen, last_seen
            )
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                language_code=excluded.language_code,
                is_bot=excluded.is_bot,
                last_seen=excluded.last_seen
            """,
            (
                user.id,
                user.username,
                user.first_name,
                user.last_name,
                user.language_code,
                int(bool(user.is_bot)),
                now,
                now,
            ),
            commit=True,
        )

    def save_join_request(
        self,
        user_id,
        channel_id,
        event_key,
        requested_at,
    ):
        try:
            cursor = self.execute(
                """
                INSERT INTO join_requests(
                    user_id, channel_id, requested_at, event_key
                )
                VALUES(?,?,?,?)
                """,
                (
                    user_id,
                    channel_id,
                    requested_at,
                    event_key,
                ),
                commit=True,
            )
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None

    def update_join_request(
        self,
        row_id,
        sent,
        status,
        error=None,
    ):
        self.execute(
            """
            UPDATE join_requests
            SET message_sent=?,
                message_sent_at=?,
                status=?,
                error=?
            WHERE id=?
            """,
            (
                int(bool(sent)),
                utc_now() if sent else None,
                status,
                error,
                row_id,
            ),
            commit=True,
        )

    def log_event(
        self,
        event_type,
        user_id=None,
        channel_id=None,
        details="",
    ):
        self.execute(
            """
            INSERT INTO bot_events(
                event_type,user_id,channel_id,details,created_at
            )
            VALUES(?,?,?,?,?)
            """,
            (
                event_type,
                user_id,
                channel_id,
                str(details)[:4000],
                utc_now(),
            ),
            commit=True,
        )

    def log_error(
        self,
        level,
        module,
        event,
        exception,
    ):
        try:
            self.execute(
                """
                INSERT INTO error_logs(
                    level,module,event,exception,created_at
                )
                VALUES(?,?,?,?,?)
                """,
                (
                    level,
                    module,
                    event,
                    str(exception)[:4000],
                    utc_now(),
                ),
                commit=True,
            )
        except Exception:
            logger.exception("Could not save error log")

    def ensure_join_message(self):
        now = utc_now()

        self.execute(
            """
            INSERT OR IGNORE INTO messages(
                name,media_type,file_id,caption,parse_mode,
                enabled,created_at,updated_at
            )
            VALUES(
                'join_request','none','',
                '','HTML',1,?,?
            )
            """,
            (now, now),
            commit=True,
        )

    def get_join_message(self):
        self.ensure_join_message()
        return self.fetchone(
            """
            SELECT * FROM messages
            WHERE name='join_request'
            """
        )

    def get_message_buttons(self, message_id):
        return self.fetchall(
            """
            SELECT * FROM message_buttons
            WHERE message_id=? AND enabled=1
            ORDER BY row_number, position, id
            """,
            (message_id,),
        )

    def clear_message_buttons(self, message_id):
        self.execute(
            "DELETE FROM message_buttons WHERE message_id=?",
            (message_id,),
            commit=True,
        )

    def add_message_button(
        self,
        message_id,
        text,
        url,
        row_number,
        position,
    ):
        self.execute(
            """
            INSERT INTO message_buttons(
                message_id,text,url,row_number,position,enabled
            )
            VALUES(?,?,?,?,?,1)
            """,
            (
                message_id,
                text,
                url,
                row_number,
                position,
            ),
            commit=True,
        )

    def get_channels(self, enabled_only=False):
        if enabled_only:
            return self.fetchall(
                """
                SELECT * FROM channels
                WHERE enabled=1
                ORDER BY sort_order, title
                """
            )

        return self.fetchall(
            """
            SELECT * FROM channels
            ORDER BY sort_order, title
            """
        )

    def stats(self):
        queries = {
            "users": "SELECT COUNT(*) c FROM users",
            "active": """
                SELECT COUNT(*) c FROM users
                WHERE is_blocked=0
            """,
            "blocked": """
                SELECT COUNT(*) c FROM users
                WHERE is_blocked=1
            """,
            "requests": """
                SELECT COUNT(*) c FROM join_requests
            """,
            "today": """
                SELECT COUNT(*) c FROM join_requests
                WHERE date(requested_at)=date('now')
            """,
            "week": """
                SELECT COUNT(*) c FROM join_requests
                WHERE requested_at >= datetime('now','-7 days')
            """,
            "month": """
                SELECT COUNT(*) c FROM join_requests
                WHERE requested_at >= datetime('now','-30 days')
            """,
            "sent": """
                SELECT COUNT(*) c FROM join_requests
                WHERE message_sent=1
            """,
            "failed": """
                SELECT COUNT(*) c FROM join_requests
                WHERE status='failed'
            """,
            "channels": """
                SELECT COUNT(*) c FROM channels
            """,
        }

        result = {}

        for key, query in queries.items():
            row = self.fetchone(query)
            result[key] = int(row["c"]) if row else 0

        return result


db = Database(DB_PATH)
db.connect()


# ============================================================
# ACCESS CONTROL
# ============================================================

def is_admin(user_id: Optional[int]) -> bool:
    if not user_id:
        return False

    row = db.fetchone(
        """
        SELECT role,permissions
        FROM admins
        WHERE user_id=?
        """,
        (user_id,),
    )

    if not row:
        return False

    if row["role"] == "owner":
        return True

    permissions = parse_json(
        row["permissions"],
        {},
    )

    return bool(
        permissions.get("all")
        or any(bool(v) for v in permissions.values())
    )


def is_owner(user_id: Optional[int]) -> bool:
    return bool(user_id and user_id == OWNER_ID)


# ============================================================
# KEYBOARDS
# ============================================================

def build_keyboard(buttons):
    if not isinstance(buttons, list):
        return None

    rows = {}

    for index, button in enumerate(buttons[:MAX_BUTTONS]):
        if not isinstance(button, dict):
            continue

        text = str(button.get("text", "")).strip()
        url = str(button.get("url", "")).strip()

        if not text or not valid_http_url(url):
            continue

        row = max(0, safe_int(button.get("row"), 0))
        position = max(
            0,
            safe_int(
                button.get("position"),
                index,
            ),
        )

        rows.setdefault(row, []).append(
            (
                position,
                InlineKeyboardButton(
                    text=text[:64],
                    url=url,
                ),
            )
        )

    keyboard_rows = []

    for row_number in sorted(rows):
        row = sorted(
            rows[row_number],
            key=lambda item: item[0],
        )
        keyboard_rows.append(
            [button for _, button in row]
        )

    if not keyboard_rows:
        return None

    return InlineKeyboardMarkup(keyboard_rows)


def start_keyboard():
    rows = []

    for channel in db.get_channels(enabled_only=True):
        username = (channel["username"] or "").lstrip("@")

        if username:
            url = f"https://t.me/{username}"
        else:
            url = db.get_setting(
                f"channel_url_{channel['channel_id']}",
                "",
            )

        if url and valid_http_url(url):
            rows.append(
                [
                    InlineKeyboardButton(
                        db.get_setting(
                            "start_button_text",
                            "JOIN NOW",
                        )[:64],
                        url=url,
                    )
                ]
            )

    if db.get_setting("check_join_enabled", "0") == "1":
        rows.append(
            [
                InlineKeyboardButton(
                    "I HAVE JOINED",
                    callback_data="check_join",
                )
            ]
        )

    return (
        InlineKeyboardMarkup(rows)
        if rows
        else None
    )


def admin_menu():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📊 Dashboard",
                    callback_data="admin_dashboard",
                ),
                InlineKeyboardButton(
                    "⚙️ Settings",
                    callback_data="admin_settings",
                ),
            ],
            [
                InlineKeyboardButton(
                    "📩 Join Request",
                    callback_data="admin_join",
                ),
                InlineKeyboardButton(
                    "💬 Message Builder",
                    callback_data="admin_message",
                ),
            ],
            [
                InlineKeyboardButton(
                    "📢 Channels",
                    callback_data="admin_channels",
                ),
                InlineKeyboardButton(
                    "👥 Users",
                    callback_data="admin_users",
                ),
            ],
            [
                InlineKeyboardButton(
                    "📢 Broadcast",
                    callback_data="admin_broadcast",
                ),
                InlineKeyboardButton(
                    "📈 Statistics",
                    callback_data="admin_stats",
                ),
            ],
            [
                InlineKeyboardButton(
                    "💾 Backup",
                    callback_data="admin_backup",
                ),
                InlineKeyboardButton(
                    "📤 Export",
                    callback_data="admin_export",
                ),
            ],
            [
                InlineKeyboardButton(
                    "🧪 Test Message",
                    callback_data="admin_test",
                ),
                InlineKeyboardButton(
                    "📝 Logs",
                    callback_data="admin_logs",
                ),
            ],
            [
                InlineKeyboardButton(
                    "🔐 Admins",
                    callback_data="admin_admins",
                )
            ],
        ]
    )


def back_keyboard():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⬅️ Back",
                    callback_data="admin_home",
                )
            ]
        ]
    )


# ============================================================
# TELEGRAM SEND HELPERS
# ============================================================

async def send_configured_message(bot, chat_id: int):
    message = db.get_join_message()

    if not message or not message["enabled"]:
        return None

    caption = message["caption"] or ""
    parse_mode = message["parse_mode"] or None
    media_type = message["media_type"] or "none"
    file_id = message["file_id"] or ""

    buttons = []

    for row in db.get_message_buttons(message["id"]):
        buttons.append(
            {
                "text": row["text"],
                "url": row["url"],
                "row": row["row_number"],
                "position": row["position"],
            }
        )

    keyboard = build_keyboard(buttons)

    for attempt in range(MAX_RETRIES):
        try:
            if media_type == "photo" and file_id:
                return await bot.send_photo(
                    chat_id=chat_id,
                    photo=file_id,
                    caption=caption[:1024],
                    parse_mode=parse_mode,
                    reply_markup=keyboard,
                )

            if media_type == "document" and file_id:
                return await bot.send_document(
                    chat_id=chat_id,
                    document=file_id,
                    caption=caption[:1024],
                    parse_mode=parse_mode,
                    reply_markup=keyboard,
                )

            text = caption or " "

            return await bot.send_message(
                chat_id=chat_id,
                text=text[:MAX_TEXT_LENGTH],
                parse_mode=parse_mode,
                reply_markup=keyboard,
                disable_web_page_preview=True,
            )

        except RetryAfter as exc:
            await asyncio.sleep(
                float(exc.retry_after) + 1
            )

        except (NetworkError, TimedOut):
            if attempt >= MAX_RETRIES - 1:
                raise

            await asyncio.sleep(
                2 ** attempt
            )

        except BadRequest:
            # BadRequest is normally a permanent content/configuration
            # error. Retrying it only creates unnecessary API traffic.
            raise

    raise RuntimeError(
        "Telegram send retry limit reached."
    )


# ============================================================
# START
# ============================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    if not user or not update.message:
        return

    try:
        db.upsert_user(user)

        if (
            db.get_setting("maintenance_mode", "0") == "1"
            and not is_admin(user.id)
        ):
            return

        keyboard = start_keyboard()

        if keyboard:
            await update.message.reply_text(
                db.get_setting(
                    "start_message",
                    "Please join our channel to continue.",
                )[:MAX_TEXT_LENGTH],
                reply_markup=keyboard,
            )
        elif is_admin(user.id):
            await update.message.reply_text(
                "No channel is configured.",
                reply_markup=admin_menu(),
            )

    except Exception as exc:
        logger.exception("/start failed")
        db.log_error(
            "ERROR",
            "start",
            "handler",
            repr(exc),
        )


# ============================================================
# JOIN REQUEST
# ============================================================

async def handle_join_request(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    request = update.chat_join_request

    if not request:
        return

    user = request.from_user
    chat = request.chat

    try:
        db.upsert_user(user)

        channel = db.fetchone(
            """
            SELECT * FROM channels
            WHERE channel_id=? AND enabled=1
            """,
            (chat.id,),
        )

        if not channel:
            db.log_event(
                "join_request_ignored",
                user.id,
                chat.id,
                "Channel not configured or disabled",
            )
            return

        request_time = (
            request.date.strftime("%Y-%m-%d %H:%M:%S")
            if request.date
            else utc_now()
        )

        # Stable enough for duplicate protection during repeated update
        # delivery: channel + user + Telegram update id.
        update_id = getattr(update, "update_id", 0)
        event_key = (
            f"join:{chat.id}:{user.id}:{update_id}"
        )

        row_id = db.save_join_request(
            user.id,
            chat.id,
            event_key,
            request_time,
        )

        if row_id is None:
            db.log_event(
                "duplicate_join_request",
                user.id,
                chat.id,
            )
            return

        db.log_event(
            "join_request_received",
            user.id,
            chat.id,
        )

        if db.get_setting(
            "auto_message_enabled",
            "1",
        ) != "1":
            db.update_join_request(
                row_id,
                sent=False,
                status="disabled",
            )
            return

        try:
            await send_configured_message(
                context.bot,
                user.id,
            )

            db.update_join_request(
                row_id,
                sent=True,
                status="sent",
            )

            db.log_event(
                "join_request_message_sent",
                user.id,
                chat.id,
            )

        except Forbidden as exc:
            error = clean_error(exc)

            db.execute(
                """
                UPDATE users
                SET is_blocked=1
                WHERE user_id=?
                """,
                (user.id,),
                commit=True,
            )

            db.update_join_request(
                row_id,
                sent=False,
                status="blocked",
                error=error,
            )

            db.log_error(
                "WARNING",
                "join_request",
                "forbidden",
                error,
            )

        except Exception as exc:
            error = clean_error(exc)

            db.update_join_request(
                row_id,
                sent=False,
                status="failed",
                error=error,
            )

            db.log_error(
                "ERROR",
                "join_request",
                "send_failed",
                error,
            )

    except Exception as exc:
        logger.exception("Join request handler failed")
        db.log_error(
            "EXCEPTION",
            "join_request",
            "handler",
            repr(exc),
        )


# ============================================================
# ADMIN COMMANDS
# ============================================================

async def admin_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not update.message:
        return

    user = update.effective_user

    if not user or not is_admin(user.id):
        await update.message.reply_text("Access Denied")
        return

    db.upsert_user(user)

    await update.message.reply_text(
        "🔐 Admin Panel",
        reply_markup=admin_menu(),
    )


async def cancel_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not update.message:
        return

    user = update.effective_user

    if not user or not is_admin(user.id):
        return

    context.user_data.clear()

    await update.message.reply_text(
        "Cancelled.",
        reply_markup=admin_menu(),
    )


# ============================================================
# ADMIN PAGES
# ============================================================

async def show_dashboard(query):
    s = db.stats()

    try:
        db_size = DB_PATH.stat().st_size
    except OSError:
        db_size = 0

    text = (
        "📊 BOT DASHBOARD\n\n"
        f"👥 Total Users: {s['users']}\n"
        f"🟢 Active: {s['active']}\n"
        f"🚫 Blocked: {s['blocked']}\n\n"
        f"📩 Join Requests: {s['requests']}\n"
        f"📅 Today: {s['today']}\n"
        f"📆 Week: {s['week']}\n"
        f"🗓 Month: {s['month']}\n\n"
        f"📤 Sent: {s['sent']}\n"
        f"❌ Failed: {s['failed']}\n"
        f"📢 Channels: {s['channels']}\n\n"
        f"🗄 DB: {DB_PATH.name}\n"
        f"📦 Size: {db_size:,} bytes"
    )

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🔄 Refresh",
                        callback_data="admin_dashboard",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Back",
                        callback_data="admin_home",
                    )
                ],
            ]
        ),
    )


async def show_statistics(query):
    s = db.stats()

    total = s["sent"] + s["failed"]
    success_rate = (
        (s["sent"] / total) * 100
        if total
        else 0
    )

    await query.edit_message_text(
        (
            "📈 STATISTICS\n\n"
            f"Users: {s['users']}\n"
            f"Active: {s['active']}\n"
            f"Blocked: {s['blocked']}\n\n"
            f"Requests Today: {s['today']}\n"
            f"Requests 7 Days: {s['week']}\n"
            f"Requests 30 Days: {s['month']}\n\n"
            f"Messages Sent: {s['sent']}\n"
            f"Messages Failed: {s['failed']}\n"
            f"Success Rate: {success_rate:.2f}%"
        ),
        reply_markup=back_keyboard(),
    )


async def show_settings(query):
    maintenance = db.get_setting(
        "maintenance_mode",
        "0",
    )

    check = db.get_setting(
        "check_join_enabled",
        "0",
    )

    await query.edit_message_text(
        (
            "⚙️ BOT SETTINGS\n\n"
            f"Bot Name: {db.get_setting('bot_name','Join Request Bot')}\n"
            f"Maintenance: {'ON' if maintenance == '1' else 'OFF'}\n"
            f"Check Join Button: {'ON' if check == '1' else 'OFF'}"
        ),
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "Toggle Maintenance",
                        callback_data="toggle_maintenance",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "Toggle Check Join",
                        callback_data="toggle_check",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Back",
                        callback_data="admin_home",
                    )
                ],
            ]
        ),
    )


async def show_join_settings(query):
    enabled = db.get_setting(
        "auto_message_enabled",
        "1",
    )

    await query.edit_message_text(
        (
            "📩 JOIN REQUEST SETTINGS\n\n"
            f"Auto Message: {'ON' if enabled == '1' else 'OFF'}\n\n"
            "Only enabled/configured channels are processed."
        ),
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "Toggle Auto Message",
                        callback_data="toggle_auto",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Back",
                        callback_data="admin_home",
                    )
                ],
            ]
        ),
    )


async def show_message_builder(query):
    message = db.get_join_message()

    buttons = db.get_message_buttons(
        message["id"]
    )

    media = message["media_type"] or "none"
    parse_mode = message["parse_mode"] or "HTML"
    caption = message["caption"] or "(empty)"

    await query.edit_message_text(
        (
            "💬 MESSAGE BUILDER\n\n"
            f"Media: {media}\n"
            f"Parse Mode: {parse_mode}\n"
            f"Buttons: {len(buttons)}\n\n"
            f"Caption:\n{caption[:1000]}"
        ),
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "📝 Caption",
                        callback_data="set_caption",
                    ),
                    InlineKeyboardButton(
                        "🔤 Parse",
                        callback_data="toggle_parse",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "🖼 Photo",
                        callback_data="set_photo",
                    ),
                    InlineKeyboardButton(
                        "🗑 Remove Media",
                        callback_data="remove_media",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "🔘 Buttons",
                        callback_data="set_buttons",
                    ),
                    InlineKeyboardButton(
                        "🗑 Clear Buttons",
                        callback_data="clear_buttons",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "👁 Preview",
                        callback_data="preview",
                    ),
                    InlineKeyboardButton(
                        "🧪 Test",
                        callback_data="admin_test",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Back",
                        callback_data="admin_home",
                    )
                ],
            ]
        ),
    )


async def show_channels(query):
    channels = db.get_channels()

    lines = ["📢 CHANNEL MANAGER\n"]

    if not channels:
        lines.append("No channels configured.")
    else:
        for channel in channels:
            status = (
                "ON"
                if channel["enabled"]
                else "OFF"
            )

            title = (
                channel["title"]
                or channel["username"]
                or str(channel["channel_id"])
            )

            lines.append(
                f"• {title}\n"
                f"  ID: {channel['channel_id']}\n"
                f"  Status: {status}\n"
            )

    rows = [
        [
            InlineKeyboardButton(
                "➕ Add Channel",
                callback_data="add_channel",
            )
        ]
    ]

    for channel in channels:
        rows.append(
            [
                InlineKeyboardButton(
                    (
                        "Disable "
                        if channel["enabled"]
                        else "Enable "
                    ) + str(channel["channel_id"]),
                    callback_data=(
                        f"channel_toggle:{channel['channel_id']}"
                    ),
                ),
                InlineKeyboardButton(
                    "🗑",
                    callback_data=(
                        f"remove_channel:{channel['channel_id']}"
                    ),
                ),
            ]
        )

    rows.append(
        [
            InlineKeyboardButton(
                "⬅️ Back",
                callback_data="admin_home",
            )
        ]
    )

    await query.edit_message_text(
        "\n".join(lines)[:4000],
        reply_markup=InlineKeyboardMarkup(rows),
    )


async def show_users(query):
    s = db.stats()

    latest = db.fetchall(
        """
        SELECT user_id,username,first_name,last_seen
        FROM users
        ORDER BY last_seen DESC
        LIMIT 10
        """
    )

    lines = [
        "👥 USERS",
        "",
        f"Total: {s['users']}",
        f"Active: {s['active']}",
        f"Blocked: {s['blocked']}",
        "",
        "Latest:",
    ]

    for row in latest:
        name = (
            row["username"]
            or row["first_name"]
            or str(row["user_id"])
        )
        lines.append(
            f"• {name} — {row['user_id']}"
        )

    await query.edit_message_text(
        "\n".join(lines)[:4000],
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "📤 Export CSV",
                        callback_data="export_users",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Back",
                        callback_data="admin_home",
                    )
                ],
            ]
        ),
    )


async def show_broadcast_menu(query):
    await query.edit_message_text(
        (
            "📢 BROADCAST\n\n"
            "Send a text or photo to active users.\n"
            "The system handles Telegram rate limits and blocked users."
        ),
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "➕ New Broadcast",
                        callback_data="broadcast_start",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Back",
                        callback_data="admin_home",
                    )
                ],
            ]
        ),
    )


async def show_backup_menu(query):
    backups = db.fetchall(
        """
        SELECT filename,created_at,size
        FROM backups
        ORDER BY id DESC
        LIMIT 5
        """
    )

    lines = ["💾 BACKUP\n"]

    if backups:
        for row in backups:
            lines.append(
                f"• {row['filename']}\n"
                f"  {row['size']:,} bytes\n"
                f"  {row['created_at']}"
            )
    else:
        lines.append("No backups yet.")

    await query.edit_message_text(
        "\n".join(lines)[:4000],
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "💾 Create Backup",
                        callback_data="backup_create",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Back",
                        callback_data="admin_home",
                    )
                ],
            ]
        ),
    )


async def show_export_menu(query):
    await query.edit_message_text(
        "📤 DATABASE EXPORT",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "👥 Users CSV",
                        callback_data="export_users",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "📩 Join Requests CSV",
                        callback_data="export_requests",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "📢 Broadcast Logs CSV",
                        callback_data="export_broadcasts",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Back",
                        callback_data="admin_home",
                    )
                ],
            ]
        ),
    )


async def show_logs(query):
    rows = db.fetchall(
        """
        SELECT level,module,event,exception,created_at
        FROM error_logs
        ORDER BY id DESC
        LIMIT 15
        """
    )

    if not rows:
        text = "📝 LOGS\n\nNo error records."
    else:
        parts = ["📝 LOGS\n"]

        for row in rows:
            parts.append(
                f"[{row['created_at']}] "
                f"{row['level']} "
                f"{row['module']}\n"
                f"{row['event']}\n"
                f"{(row['exception'] or '')[:300]}\n"
            )

        text = "\n".join(parts)

    await query.edit_message_text(
        text[:4000],
        reply_markup=back_keyboard(),
    )


async def show_admins(query):
    rows = db.fetchall(
        """
        SELECT user_id,role,created_at
        FROM admins
        ORDER BY role,user_id
        """
    )

    lines = ["🔐 ADMINS\n"]

    for row in rows:
        mark = " 👑" if row["role"] == "owner" else ""
        lines.append(
            f"{row['user_id']} — {row['role']}{mark}"
        )

    lines.append(
        "\nOwner is controlled by OWNER_ID."
    )

    await query.edit_message_text(
        "\n".join(lines)[:4000],
        reply_markup=back_keyboard(),
    )


# ============================================================
# ADMIN CALLBACK ROUTER
# ============================================================

async def admin_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    query = update.callback_query

    if not query:
        return

    user = query.from_user

    if not user or not is_admin(user.id):
        try:
            await query.answer(
                "Access Denied",
                show_alert=True,
            )
        except TelegramError:
            pass
        return

    data = query.data or ""

    try:
        await query.answer()

        if data == "admin_home":
            await query.edit_message_text(
                "🔐 Admin Panel",
                reply_markup=admin_menu(),
            )
            return

        if data == "admin_dashboard":
            await show_dashboard(query)
            return

        if data == "admin_stats":
            await show_statistics(query)
            return

        if data == "admin_settings":
            await show_settings(query)
            return

        if data == "admin_join":
            await show_join_settings(query)
            return

        if data == "admin_message":
            await show_message_builder(query)
            return

        if data == "admin_channels":
            await show_channels(query)
            return

        if data == "admin_users":
            await show_users(query)
            return

        if data == "admin_broadcast":
            await show_broadcast_menu(query)
            return

        if data == "admin_backup":
            await show_backup_menu(query)
            return

        if data == "admin_export":
            await show_export_menu(query)
            return

        if data == "admin_logs":
            await show_logs(query)
            return

        if data == "admin_admins":
            await show_admins(query)
            return

        if data == "admin_test":
            await test_message(query, context)
            return

        if data == "toggle_auto":
            current = db.get_setting(
                "auto_message_enabled",
                "1",
            )

            db.set_setting(
                "auto_message_enabled",
                "0" if current == "1" else "1",
            )

            await show_join_settings(query)
            return

        if data == "toggle_maintenance":
            current = db.get_setting(
                "maintenance_mode",
                "0",
            )

            db.set_setting(
                "maintenance_mode",
                "0" if current == "1" else "1",
            )

            await show_settings(query)
            return

        if data == "toggle_check":
            current = db.get_setting(
                "check_join_enabled",
                "0",
            )

            db.set_setting(
                "check_join_enabled",
                "0" if current == "1" else "1",
            )

            await show_settings(query)
            return

        if data == "set_caption":
            context.user_data["awaiting"] = "caption"

            await query.message.reply_text(
                "Send the new caption.\n\n"
                "Use the selected Telegram parse mode.\n"
                "Use /cancel to cancel."
            )
            return

        if data == "toggle_parse":
            message = db.get_join_message()

            current = message["parse_mode"] or "HTML"

            new_mode = (
                "MarkdownV2"
                if current == "HTML"
                else "HTML"
            )

            db.execute(
                """
                UPDATE messages
                SET parse_mode=?,updated_at=?
                WHERE id=?
                """,
                (
                    new_mode,
                    utc_now(),
                    message["id"],
                ),
                commit=True,
            )

            await show_message_builder(query)
            return

        if data == "set_photo":
            context.user_data["awaiting"] = "photo"

            await query.message.reply_text(
                "Send the photo now.\n\n"
                "Use /cancel to cancel."
            )
            return

        if data == "remove_media":
            message = db.get_join_message()

            db.execute(
                """
                UPDATE messages
                SET media_type='none',
                    file_id='',
                    updated_at=?
                WHERE id=?
                """,
                (
                    utc_now(),
                    message["id"],
                ),
                commit=True,
            )

            await show_message_builder(query)
            return

        if data == "set_buttons":
            context.user_data["awaiting"] = "buttons"

            await query.message.reply_text(
                "Send buttons as JSON.\n\n"
                '[{"text":"JOIN NOW",'
                '"url":"https://t.me/example",'
                '"row":0,"position":0},'
                '{"text":"CHANNEL",'
                '"url":"https://t.me/example2",'
                '"row":0,"position":1}]\n\n'
                "Use /cancel to cancel."
            )
            return

        if data == "clear_buttons":
            message = db.get_join_message()

            db.clear_message_buttons(
                message["id"]
            )

            await show_message_builder(query)
            return

        if data == "preview":
            await preview_message(query, context)
            return

        if data == "add_channel":
            context.user_data["awaiting"] = "channel"

            await query.message.reply_text(
                "Send the numeric channel ID.\n\n"
                "Example: -1001234567890\n\n"
                "The bot must already be an administrator "
                "in that channel."
            )
            return

        if data.startswith("channel_toggle:"):
            channel_id = safe_int(
                data.split(":", 1)[1],
                None,
            )

            if channel_id is None:
                await query.message.reply_text(
                    "Invalid channel action."
                )
                return

            db.execute(
                """
                UPDATE channels
                SET enabled=
                    CASE enabled
                        WHEN 1 THEN 0
                        ELSE 1
                    END,
                    updated_at=?
                WHERE channel_id=?
                """,
                (
                    utc_now(),
                    channel_id,
                ),
                commit=True,
            )

            await show_channels(query)
            return

        if data.startswith("remove_channel:"):
            channel_id = safe_int(
                data.split(":", 1)[1],
                None,
            )

            if channel_id is None:
                await query.message.reply_text(
                    "Invalid channel action."
                )
                return

            if not is_owner(user.id):
                await query.message.reply_text(
                    "Only Owner can remove channels."
                )
                return

            db.execute(
                "DELETE FROM channels WHERE channel_id=?",
                (channel_id,),
                commit=True,
            )

            await show_channels(query)
            return

        if data == "broadcast_start":
            context.user_data["awaiting"] = "broadcast"

            await query.message.reply_text(
                "Send the broadcast text or photo with caption.\n\n"
                "Use /cancel to cancel."
            )
            return

        if data == "backup_create":
            await create_backup(query)
            return

        if data == "export_users":
            await export_csv(query, "users")
            return

        if data == "export_requests":
            await export_csv(query, "join_requests")
            return

        if data == "export_broadcasts":
            await export_csv(query, "broadcast_logs")
            return

        if data == "check_join":
            await answer_query(
                query,
                "Verification requires the user to join "
                "the configured channel(s).",
            )
            return

        await answer_query(
            query,
            "Unknown or expired action.",
            True,
        )

    except Exception as exc:
        logger.exception(
            "Admin callback failed: %s",
            data,
        )

        db.log_error(
            "EXCEPTION",
            "admin_callback",
            data,
            repr(exc),
        )

        try:
            await query.message.reply_text(
                "⚠️ Operation failed safely.\n"
                "Check /admin → Logs."
            )
        except Exception:
            pass


async def answer_query(
    query,
    text="",
    show_alert=False,
):
    try:
        await query.answer(
            text=text[:200],
            show_alert=show_alert,
        )
    except TelegramError:
        pass


# ============================================================
# MESSAGE PREVIEW / TEST
# ============================================================

async def preview_message(
    query,
    context: ContextTypes.DEFAULT_TYPE,
):
    try:
        await send_configured_message(
            context.bot,
            query.from_user.id,
        )

        await query.message.reply_text(
            "👁 Preview sent."
        )
    except Exception as exc:
        await query.message.reply_text(
            f"Preview failed: {clean_error(exc)[:700]}"
        )


async def test_message(
    query,
    context: ContextTypes.DEFAULT_TYPE,
):
    try:
        await send_configured_message(
            context.bot,
            query.from_user.id,
        )

        await query.message.reply_text(
            "🧪 Test message sent."
        )
    except Exception as exc:
        await query.message.reply_text(
            f"Test failed: {clean_error(exc)[:700]}"
        )


# ============================================================
# ADMIN INPUT
# ============================================================

async def admin_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user = update.effective_user
    message = update.message

    if not user or not message or not is_admin(user.id):
        return

    state = context.user_data.get("awaiting")

    if not state:
        return

    try:
        if state == "caption":
            text = message.text or ""

            if len(text) > MAX_TEXT_LENGTH:
                await message.reply_text(
                    "Caption is too long."
                )
                return

            join_message = db.get_join_message()

            db.execute(
                """
                UPDATE messages
                SET caption=?,updated_at=?
                WHERE id=?
                """,
                (
                    text,
                    utc_now(),
                    join_message["id"],
                ),
                commit=True,
            )

            context.user_data.pop(
                "awaiting",
                None,
            )

            await message.reply_text(
                "✅ Caption saved.",
                reply_markup=admin_menu(),
            )
            return

        if state == "photo":
            if not message.photo:
                await message.reply_text(
                    "Please send a photo."
                )
                return

            photo = message.photo[-1]

            join_message = db.get_join_message()

            db.execute(
                """
                UPDATE messages
                SET media_type='photo',
                    file_id=?,
                    updated_at=?
                WHERE id=?
                """,
                (
                    photo.file_id,
                    utc_now(),
                    join_message["id"],
                ),
                commit=True,
            )

            context.user_data.pop(
                "awaiting",
                None,
            )

            await message.reply_text(
                "✅ Photo saved.",
                reply_markup=admin_menu(),
            )
            return

        if state == "buttons":
            raw = message.text or ""
            buttons = parse_json(raw, None)

            if not isinstance(buttons, list):
                await message.reply_text(
                    "Invalid JSON array."
                )
                return

            if len(buttons) > MAX_BUTTONS:
                await message.reply_text(
                    f"Maximum {MAX_BUTTONS} buttons."
                )
                return

            validated = []

            for index, button in enumerate(buttons):
                if not isinstance(button, dict):
                    raise ValueError(
                        f"Button {index + 1} is not an object."
                    )

                text = str(
                    button.get("text", "")
                ).strip()

                url = str(
                    button.get("url", "")
                ).strip()

                if not text:
                    raise ValueError(
                        f"Button {index + 1}: text missing."
                    )

                if not valid_http_url(url):
                    raise ValueError(
                        f"Button {index + 1}: invalid URL."
                    )

                validated.append(
                    {
                        "text": text[:64],
                        "url": url,
                        "row": max(
                            0,
                            safe_int(
                                button.get("row"),
                                0,
                            ),
                        ),
                        "position": max(
                            0,
                            safe_int(
                                button.get("position"),
                                index,
                            ),
                        ),
                    }
                )

            join_message = db.get_join_message()

            db.clear_message_buttons(
                join_message["id"]
            )

            for button in validated:
                db.add_message_button(
                    join_message["id"],
                    button["text"],
                    button["url"],
                    button["row"],
                    button["position"],
                )

            context.user_data.pop(
                "awaiting",
                None,
            )

            await message.reply_text(
                f"✅ {len(validated)} button(s) saved.",
                reply_markup=admin_menu(),
            )
            return

        if state == "channel":
            raw_id = (message.text or "").strip()

            try:
                channel_id = int(raw_id)
            except ValueError:
                await message.reply_text(
                    "Invalid numeric channel ID."
                )
                return

            try:
                chat = await context.bot.get_chat(
                    channel_id
                )

                if chat.type != "channel":
                    await message.reply_text(
                        "Please provide a Telegram channel ID."
                    )
                    return

                member = await context.bot.get_chat_member(
                    chat.id,
                    context.bot.id,
                )

                status = str(
                    getattr(member, "status", "")
                )

                if status not in (
                    "administrator",
                    "creator",
                ):
                    await message.reply_text(
                        "Bot is not an administrator in this channel."
                    )
                    return

                now = utc_now()

                db.execute(
                    """
                    INSERT INTO channels(
                        channel_id,username,title,type,
                        enabled,required,sort_order,
                        created_at,updated_at
                    )
                    VALUES(?,?,?,?,1,1,0,?,?)
                    ON CONFLICT(channel_id) DO UPDATE SET
                        username=excluded.username,
                        title=excluded.title,
                        type=excluded.type,
                        updated_at=excluded.updated_at
                    """,
                    (
                        chat.id,
                        chat.username,
                        chat.title or "",
                        chat.type,
                        now,
                        now,
                    ),
                    commit=True,
                )

                context.user_data.pop(
                    "awaiting",
                    None,
                )

                await message.reply_text(
                    "✅ Channel configured.\n\n"
                    f"Title: {chat.title or '-'}\n"
                    f"ID: {chat.id}\n"
                    f"Username: @{chat.username}"
                    if chat.username
                    else
                    "✅ Channel configured.\n\n"
                    f"Title: {chat.title or '-'}\n"
                    f"ID: {chat.id}",
                    reply_markup=admin_menu(),
                )

            except TelegramError as exc:
                await message.reply_text(
                    "Could not access the channel.\n\n"
                    f"{clean_error(exc)[:700]}"
                )
            return

        if state == "broadcast":
            await create_broadcast(
                update,
                context,
            )
            return

    except Exception as exc:
        logger.exception(
            "Admin input failed: %s",
            state,
        )

        db.log_error(
            "EXCEPTION",
            "admin_input",
            state,
            repr(exc),
        )

        await message.reply_text(
            f"Operation failed safely:\n"
            f"{clean_error(exc)[:700]}"
        )


# ============================================================
# BROADCAST
# ============================================================

async def create_broadcast(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    user = update.effective_user
    message = update.message

    if not user or not message:
        return

    media_type = "none"
    file_id = None
    text = ""
    caption = ""

    if message.photo:
        media_type = "photo"
        file_id = message.photo[-1].file_id
        caption = message.caption or ""
    else:
        text = message.text or ""
        caption = text

    if not text and not caption:
        await message.reply_text(
            "Broadcast content cannot be empty."
        )
        return

    if len(text or caption) > MAX_TEXT_LENGTH:
        await message.reply_text(
            "Broadcast text is too long."
        )
        return

    cursor = db.execute(
        """
        INSERT INTO broadcasts(
            admin_id,text,media_type,file_id,caption,
            parse_mode,buttons_json,status,created_at
        )
        VALUES(?,?,?,?,?,?,?,?,?)
        """,
        (
            user.id,
            text,
            media_type,
            file_id,
            caption,
            db.get_join_message()["parse_mode"] or "HTML",
            "[]",
            "pending",
            utc_now(),
        ),
        commit=True,
    )

    broadcast_id = cursor.lastrowid

    context.user_data[
        "pending_broadcast_id"
    ] = broadcast_id

    await message.reply_text(
        (
            f"📢 Broadcast #{broadcast_id} created.\n\n"
            "Review the content and send:\n"
            "/broadcast_confirm\n\n"
            "Or /cancel"
        )
    )


async def broadcast_confirm(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    if not update.message:
        return

    user = update.effective_user

    if not user or not is_admin(user.id):
        await update.message.reply_text(
            "Access Denied"
        )
        return

    broadcast_id = context.user_data.get(
        "pending_broadcast_id"
    )

    if not broadcast_id:
        await update.message.reply_text(
            "No pending broadcast."
        )
        return

    row = db.fetchone(
        """
        SELECT * FROM broadcasts
        WHERE id=? AND status='pending'
        """,
        (broadcast_id,),
    )

    if not row:
        await update.message.reply_text(
            "Pending broadcast not found."
        )
        return

    db.execute(
        """
        UPDATE broadcasts
        SET status='running',
            started_at=?
        WHERE id=?
        """,
        (
            utc_now(),
            broadcast_id,
        ),
        commit=True,
    )

    context.user_data.pop(
        "pending_broadcast_id",
        None,
    )

    await update.message.reply_text(
        f"📢 Broadcast #{broadcast_id} started."
    )

    context.application.create_task(
        run_broadcast(
            context.application,
            broadcast_id,
        )
    )


async def send_broadcast_to_user(
    bot,
    row,
    user_id,
):
    buttons = parse_json(
        row["buttons_json"],
        [],
    )

    keyboard = build_keyboard(buttons)

    if row["media_type"] == "photo" and row["file_id"]:
        return await bot.send_photo(
            chat_id=user_id,
            photo=row["file_id"],
            caption=(row["caption"] or "")[:1024] or None,
            parse_mode=row["parse_mode"] or None,
            reply_markup=keyboard,
        )

    return await bot.send_message(
        chat_id=user_id,
        text=(
            row["text"]
            or row["caption"]
            or " "
        )[:MAX_TEXT_LENGTH],
        parse_mode=row["parse_mode"] or None,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


async def run_broadcast(
    application: Application,
    broadcast_id: int,
):
    try:
        row = db.fetchone(
            """
            SELECT * FROM broadcasts
            WHERE id=?
            """,
            (broadcast_id,),
        )

        if not row:
            return

        users = db.fetchall(
            """
            SELECT user_id FROM users
            WHERE is_blocked=0
            ORDER BY user_id
            """
        )

        total = len(users)

        db.execute(
            """
            UPDATE broadcasts
            SET total=?
            WHERE id=?
            """,
            (
                total,
                broadcast_id,
            ),
            commit=True,
        )

        sent = 0
        failed = 0
        blocked = 0

        for item in users:
            user_id = item["user_id"]

            existing = db.fetchone(
                """
                SELECT status
                FROM broadcast_logs
                WHERE broadcast_id=? AND user_id=?
                """,
                (
                    broadcast_id,
                    user_id,
                ),
            )

            if existing and existing["status"] in (
                "sent",
                "blocked",
            ):
                continue

            try:
                await send_broadcast_to_user(
                    application.bot,
                    row,
                    user_id,
                )

                sent += 1

                db.execute(
                    """
                    INSERT INTO broadcast_logs(
                        broadcast_id,user_id,status,error,created_at
                    )
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(broadcast_id,user_id)
                    DO UPDATE SET
                        status=excluded.status,
                        error=excluded.error,
                        created_at=excluded.created_at
                    """,
                    (
                        broadcast_id,
                        user_id,
                        "sent",
                        None,
                        utc_now(),
                    ),
                    commit=True,
                )

            except Forbidden as exc:
                blocked += 1

                db.execute(
                    """
                    UPDATE users
                    SET is_blocked=1
                    WHERE user_id=?
                    """,
                    (user_id,),
                    commit=True,
                )

                db.execute(
                    """
                    INSERT INTO broadcast_logs(
                        broadcast_id,user_id,status,error,created_at
                    )
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(broadcast_id,user_id)
                    DO UPDATE SET
                        status=excluded.status,
                        error=excluded.error,
                        created_at=excluded.created_at
                    """,
                    (
                        broadcast_id,
                        user_id,
                        "blocked",
                        clean_error(exc),
                        utc_now(),
                    ),
                    commit=True,
                )

            except RetryAfter as exc:
                try:
                    await asyncio.sleep(
                        float(exc.retry_after) + 1
                    )

                    await send_broadcast_to_user(
                        application.bot,
                        row,
                        user_id,
                    )

                    sent += 1

                except Forbidden as retry_exc:
                    blocked += 1

                    db.execute(
                        """
                        UPDATE users
                        SET is_blocked=1
                        WHERE user_id=?
                        """,
                        (user_id,),
                        commit=True,
                    )

                    db.log_error(
                        "WARNING",
                        "broadcast",
                        "retry_forbidden",
                        repr(retry_exc),
                    )

                except Exception as retry_exc:
                    failed += 1

                    db.log_error(
                        "WARNING",
                        "broadcast",
                        "retry_failed",
                        repr(retry_exc),
                    )

            except (NetworkError, TimedOut) as exc:
                failed += 1

                db.execute(
                    """
                    INSERT INTO broadcast_logs(
                        broadcast_id,user_id,status,error,created_at
                    )
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(broadcast_id,user_id)
                    DO UPDATE SET
                        status=excluded.status,
                        error=excluded.error,
                        created_at=excluded.created_at
                    """,
                    (
                        broadcast_id,
                        user_id,
                        "failed",
                        clean_error(exc),
                        utc_now(),
                    ),
                    commit=True,
                )

            except Exception as exc:
                failed += 1

                db.execute(
                    """
                    INSERT INTO broadcast_logs(
                        broadcast_id,user_id,status,error,created_at
                    )
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(broadcast_id,user_id)
                    DO UPDATE SET
                        status=excluded.status,
                        error=excluded.error,
                        created_at=excluded.created_at
                    """,
                    (
                        broadcast_id,
                        user_id,
                        "failed",
                        clean_error(exc),
                        utc_now(),
                    ),
                    commit=True,
                )

            db.execute(
                """
                UPDATE broadcasts
                SET sent=?,failed=?,blocked=?,
                    next_user_id=?
                WHERE id=?
                """,
                (
                    sent,
                    failed,
                    blocked,
                    user_id,
                    broadcast_id,
                ),
                commit=True,
            )

            await asyncio.sleep(
                BROADCAST_DELAY
            )

        db.execute(
            """
            UPDATE broadcasts
            SET status='completed',
                sent=?,failed=?,blocked=?,
                next_user_id=NULL,
                finished_at=?
            WHERE id=?
            """,
            (
                sent,
                failed,
                blocked,
                utc_now(),
                broadcast_id,
            ),
            commit=True,
        )

        db.log_event(
            "broadcast_completed",
            details=(
                f"id={broadcast_id};"
                f"sent={sent};"
                f"failed={failed};"
                f"blocked={blocked}"
            ),
        )

    except Exception as exc:
        logger.exception(
            "Broadcast crashed safely"
        )

        db.execute(
            """
            UPDATE broadcasts
            SET status='paused'
            WHERE id=? AND status='running'
            """,
            (broadcast_id,),
            commit=True,
        )

        db.log_error(
            "EXCEPTION",
            "broadcast",
            "worker",
            repr(exc),
        )


# ============================================================
# BACKUP
# ============================================================

async def create_backup(query):
    if not DB_PATH.exists():
        await query.message.reply_text(
            "Database file does not exist."
        )
        return

    filename = (
        "backup_"
        + datetime.now().strftime(
            "%Y_%m_%d_%H%M%S"
        )
        + ".db"
    )

    destination = BACKUP_DIR / filename

    source = None
    target = None

    try:
        # SQLite online backup API avoids copying an active WAL file
        # incorrectly.
        source = sqlite3.connect(
            str(DB_PATH),
            timeout=30,
        )

        target = sqlite3.connect(
            str(destination),
            timeout=30,
        )

        source.backup(target)
        target.commit()

        integrity = target.execute(
            "PRAGMA integrity_check"
        ).fetchone()[0]

        if integrity != "ok":
            raise RuntimeError(
                f"Backup integrity check failed: {integrity}"
            )

        size = destination.stat().st_size

        db.execute(
            """
            INSERT INTO backups(
                filename,created_at,created_by,size
            )
            VALUES(?,?,?,?)
            """,
            (
                filename,
                utc_now(),
                query.from_user.id,
                size,
            ),
            commit=True,
        )

        with destination.open("rb") as file:
            await query.message.reply_document(
                document=InputFile(
                    file,
                    filename=filename,
                ),
                caption=(
                    "💾 Backup created successfully.\n"
                    f"Size: {size:,} bytes"
                ),
            )

    except Exception as exc:
        logger.exception("Backup creation failed")
        db.log_error(
            "ERROR",
            "backup",
            "create",
            repr(exc),
        )

        await query.message.reply_text(
            f"Backup failed:\n{clean_error(exc)[:700]}"
        )

    finally:
        if source:
            source.close()

        if target:
            target.close()


# ============================================================
# CSV EXPORT
# ============================================================

async def export_csv(
    query,
    export_type,
):
    if export_type == "users":
        rows = db.fetchall(
            "SELECT * FROM users ORDER BY user_id"
        )
        filename = "users.csv"

    elif export_type == "join_requests":
        rows = db.fetchall(
            """
            SELECT * FROM join_requests
            ORDER BY id
            """
        )
        filename = "join_requests.csv"

    elif export_type == "broadcast_logs":
        rows = db.fetchall(
            """
            SELECT * FROM broadcast_logs
            ORDER BY id
            """
        )
        filename = "broadcast_logs.csv"

    else:
        await query.message.reply_text(
            "Invalid export."
        )
        return

    output = io.StringIO()
    writer = csv.writer(output)

    if rows:
        writer.writerow(rows[0].keys())

        for row in rows:
            writer.writerow(
                [row[key] for key in row.keys()]
            )
    else:
        writer.writerow(["No records"])

    data = io.BytesIO(
        output.getvalue().encode("utf-8-sig")
    )
    data.name = filename

    await query.message.reply_document(
        document=InputFile(
            data,
            filename=filename,
        ),
        caption=f"📤 {filename}",
    )


# ============================================================
# GLOBAL ERROR HANDLER
# ============================================================

async def global_error_handler(
    update: object,
    context: ContextTypes.DEFAULT_TYPE,
):
    error = context.error

    if isinstance(error, RetryAfter):
        logger.warning(
            "Telegram rate limit: %s seconds",
            error.retry_after,
        )
        return

    if isinstance(error, Forbidden):
        logger.warning(
            "Telegram forbidden error: %s",
            error,
        )
        return

    if isinstance(
        error,
        (NetworkError, TimedOut),
    ):
        logger.warning(
            "Telegram network error: %s",
            error,
        )
        return

    logger.exception(
        "Unhandled application error",
        exc_info=error,
    )

    try:
        db.log_error(
            "EXCEPTION",
            "application",
            "global_error",
            repr(error),
        )
    except Exception:
        logger.exception(
            "Could not save global error"
        )


# ============================================================
# STARTUP / SHUTDOWN
# ============================================================

async def post_init(
    application: Application,
):
    try:
        db.connect()

        integrity = db.fetchone(
            "PRAGMA integrity_check"
        )

        if not integrity or integrity[0] != "ok":
            raise RuntimeError(
                f"SQLite integrity check failed: {integrity}"
            )

        me = await application.bot.get_me()

        logger.info(
            "Connected as @%s (%s)",
            me.username,
            me.id,
        )

        await application.bot.set_my_commands(
            [
                ("start", "Start"),
                ("admin", "Admin panel"),
                ("cancel", "Cancel current action"),
                (
                    "broadcast_confirm",
                    "Confirm pending broadcast",
                ),
            ]
        )

        db.log_event(
            "startup",
            details=(
                f"bot_id={me.id};"
                f"username={me.username}"
            ),
        )

        logger.info(
            "Database initialized: %s",
            DB_PATH.resolve(),
        )

    except Exception:
        logger.exception(
            "Startup validation failed"
        )
        raise


async def post_shutdown(
    application: Application,
):
    try:
        db.log_event("shutdown")
    except Exception:
        pass

    db.close()
    logger.info("Shutdown complete.")


# ============================================================
# APPLICATION
# ============================================================

def build_application() -> Application:
    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .concurrent_updates(False)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )

    application.add_handler(
        CommandHandler(
            "admin",
            admin_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "cancel",
            cancel_command,
        )
    )

    application.add_handler(
        CommandHandler(
            "broadcast_confirm",
            broadcast_confirm,
        )
    )

    application.add_handler(
        ChatJoinRequestHandler(
            handle_join_request
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            admin_callback
        )
    )

    application.add_handler(
        MessageHandler(
            (
                filters.ChatType.PRIVATE
                & (
                    filters.TEXT
                    | filters.PHOTO
                )
                & ~filters.COMMAND
            ),
            admin_input,
        )
    )

    application.add_error_handler(
        global_error_handler
    )

    return application


def main():
    logger.info(
        "Initializing database at: %s",
        DB_PATH.resolve(),
    )

    # Explicit startup initialization before polling.
    # utc_now() is already defined at this point.
    db.connect()

    application = build_application()

    logger.info(
        "Starting Telegram polling..."
    )

    application.run_polling(
        allowed_updates=[
            "message",
            "callback_query",
            "chat_join_request",
        ],
        drop_pending_updates=False,
        close_loop=False,
    )


if __name__ == "__main__":
    main()
