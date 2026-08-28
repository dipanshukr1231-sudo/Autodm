import asyncio
import csv
import io
import json
import logging
import os
import re
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    MessageEntity,
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

RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "").strip().rstrip("/")
try:
    RENDER_PORT = int(os.getenv("PORT", "10000"))
except (TypeError, ValueError):
    RENDER_PORT = 10000
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", f"telegram-{BOT_TOKEN.split(':', 1)[0]}").strip("/")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

MAX_RETRIES = 3
BROADCAST_DELAY = 0.08
MAX_BUTTONS = 100
MAX_TEXT_LENGTH = 4096
MAX_CAPTION_LENGTH = 1024

# Automatic disaster-recovery backup interval. Backups are sent to the
# configured Telegram backup/log channel, so they survive Render disk resets.
AUTO_BACKUP_INTERVAL_SECONDS = 60
AUTO_BACKUP_LOCAL_RETENTION = 10

# Telegram custom emoji are message entities, not Unicode emoji.
# The original source message is copied whenever possible; otherwise the
# exact stored entities are sent explicitly.
CUSTOM_EMOJI_ENTITY_TYPE = "custom_emoji"

# Telegram button styles supported by Bot API / python-telegram-bot 22.7+
# primary=blue, success=green, danger=red.
BUTTON_STYLES = ("primary", "success", "danger")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("telegram_join_request_bot")


# ============================================================
# HELPERS
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


def normalize_button_style(style: Optional[str]) -> str:
    style = (style or "primary").strip().lower()
    return style if style in BUTTON_STYLES else "primary"


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
                auto_approve INTEGER NOT NULL DEFAULT 0,
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
                style TEXT NOT NULL DEFAULT 'primary',
                icon_custom_emoji_id TEXT,
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
                source_chat_id INTEGER,
                source_message_id INTEGER,
                entities_json TEXT NOT NULL DEFAULT '[]',
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
        # Add style column to message_buttons if upgrading from old schema
        try:
            self.conn.execute(
                "ALTER TABLE message_buttons ADD COLUMN style TEXT NOT NULL DEFAULT 'primary'"
            )
            self.conn.commit()
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Add custom emoji icon support to buttons when upgrading an existing DB.
        try:
            self.conn.execute(
                "ALTER TABLE message_buttons ADD COLUMN icon_custom_emoji_id TEXT"
            )
            self.conn.commit()
        except sqlite3.OperationalError:
            pass

        # Add per-channel auto-approve setting when upgrading an existing DB.
        try:
            self.conn.execute(
                "ALTER TABLE channels ADD COLUMN auto_approve INTEGER NOT NULL DEFAULT 0"
            )
            self.conn.commit()
        except sqlite3.OperationalError:
            pass  # Column already exists

        # Add source fields / entity storage to broadcasts when upgrading
        for col in (
            "source_chat_id INTEGER",
            "source_message_id INTEGER",
            "entities_json TEXT NOT NULL DEFAULT '[]'",
        ):
            try:
                self.conn.execute(f"ALTER TABLE broadcasts ADD COLUMN {col}")
                self.conn.commit()
            except sqlite3.OperationalError:
                pass

        self.conn.commit()

    def seed_defaults(self):
        now = utc_now()

        defaults = {
            "maintenance_mode": "0",
            "auto_message_enabled": "1",
            "start_message": "Please join our channel to continue.",
            "start_button_text": "JOIN NOW",
            "start_button_style": "primary",
            "check_join_enabled": "0",
            "bot_name": "Join Request Bot",
            "join_msg_source_entities": "[]",
            # Public/private Telegram channel used as an off-server backup vault.
            "backup_channel_id": "",
            "backup_channel_username": "",
            "backup_channel_title": "",
            "backup_channel_enabled": "0",
            "join_msg_source_entities": "[]",
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
        style="primary",
        icon_custom_emoji_id=None,
    ):
        self.execute(
            """
            INSERT INTO message_buttons(
                message_id,text,url,style,icon_custom_emoji_id,row_number,position,enabled
            )
            VALUES(?,?,?,?,?,?,?,1)
            """,
            (
                message_id,
                text,
                url,
                style if style in BUTTON_STYLES else "primary",
                str(icon_custom_emoji_id) if icon_custom_emoji_id else None,
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

AUTO_BACKUP_TASK = None


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

def _make_inline_button(
    text: str,
    url: str,
    style: Optional[str] = None,
    icon_custom_emoji_id: Optional[str] = None,
) -> InlineKeyboardButton:
    """Create a styled URL button using current Bot API fields."""
    kwargs = {
        "text": text[:64],
        "url": url,
        "style": normalize_button_style(style),
    }
    if icon_custom_emoji_id:
        kwargs["icon_custom_emoji_id"] = str(icon_custom_emoji_id)
    return InlineKeyboardButton(**kwargs)


def _make_callback_button(
    text: str,
    callback_data: str,
    style: Optional[str] = None,
    icon_custom_emoji_id: Optional[str] = None,
) -> InlineKeyboardButton:
    """Create a styled callback button."""
    kwargs = {
        "text": text[:64],
        "callback_data": callback_data,
        "style": normalize_button_style(style),
    }
    if icon_custom_emoji_id:
        kwargs["icon_custom_emoji_id"] = str(icon_custom_emoji_id)
    return InlineKeyboardButton(**kwargs)


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

        style = normalize_button_style(button.get("style", "primary"))
        icon_id = str(button.get("icon_custom_emoji_id", "") or "").strip() or None
        row = max(0, safe_int(button.get("row", 0), 0))
        position = max(0, safe_int(button.get("position", index), index))
        rows.setdefault(row, []).append(
            (
                position,
                _make_inline_button(text, url, style, icon_id),
            )
        )

    keyboard_rows = []
    for row_number in sorted(rows):
        keyboard_rows.append([button for _, button in sorted(rows[row_number], key=lambda item: item[0])])

    return InlineKeyboardMarkup(keyboard_rows) if keyboard_rows else None


def start_keyboard():
    rows = []

    button_text = db.get_setting("start_button_text", "JOIN NOW")
    button_style = db.get_setting("start_button_style", "primary")

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
                    _make_inline_button(
                        button_text[:64],
                        url,
                        button_style,
                    )
                ]
            )

    if db.get_setting("check_join_enabled", "0") == "1":
        rows.append(
            [
                _make_callback_button("I HAVE JOINED", "check_join", "success")
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
                _make_callback_button("📊 Dashboard", "admin_dashboard", "primary"),
                _make_callback_button("⚙️ Settings", "admin_settings", "primary"),
            ],
            [
                _make_callback_button("📩 Join Request", "admin_join", "primary"),
                _make_callback_button("💬 Message Builder", "admin_message", "primary"),
            ],
            [
                _make_callback_button("📢 Channels", "admin_channels", "primary"),
                _make_callback_button("👥 Users", "admin_users", "primary"),
            ],
            [
                _make_callback_button("📢 Broadcast", "admin_broadcast", "primary"),
                _make_callback_button("📈 Statistics", "admin_stats", "primary"),
            ],
            [
                _make_callback_button("💾 Backup", "admin_backup", "primary"),
                _make_callback_button("📤 Export", "admin_export", "primary"),
            ],
            [
                _make_callback_button("☁️ Backup Channel", "admin_backup_channel", "primary"),
            ],
            [
                _make_callback_button("🧪 Test Message", "admin_test", "success"),
                _make_callback_button("📝 Logs", "admin_logs", "danger"),
            ],
            [
                _make_callback_button("🔐 Admins", "admin_admins", "primary")
            ],
        ]
    )


def back_keyboard():
    return InlineKeyboardMarkup(
        [
            [
                _make_callback_button("⬅️ Back", "admin_home", "primary")
            ]
        ]
    )


# ============================================================
# TEMPLATE + ENTITY HELPERS
# ============================================================

USERNAME_PLACEHOLDERS = (
    "{Username}",
    "{username}",
    "{UserName}",
    "{USERNAME}",
)


def display_name_for_user(user) -> str:
    """Return a safe display name for per-user message templates."""
    first_name = (getattr(user, "first_name", None) or "").strip()
    if first_name:
        return first_name

    username = (getattr(user, "username", None) or "").strip().lstrip("@")
    if username:
        return f"@{username}"

    return "there"


def serialize_message_entities(entities) -> str:
    """Serialize Telegram MessageEntity objects without losing custom emoji IDs."""
    result = []
    for entity in entities or ():
        try:
            entity_type = getattr(entity, "type", None)
            offset = int(getattr(entity, "offset", 0))
            length = int(getattr(entity, "length", 0))
        except (TypeError, ValueError):
            continue

        if not entity_type or length <= 0:
            continue

        data = {
            "type": entity_type,
            "offset": offset,
            "length": length,
        }

        for key in ("url", "language", "custom_emoji_id", "date_time_format"):
            value = getattr(entity, key, None)
            if value is not None:
                data[key] = value

        # Keep mention-name users when present.
        entity_user = getattr(entity, "user", None)
        if entity_user is not None:
            try:
                data["user"] = entity_user.to_dict()
            except Exception:
                pass

        # DATE_TIME is uncommon, but preserve its timestamp if present.
        unix_time = getattr(entity, "unix_time", None)
        if unix_time is not None:
            try:
                data["unix_time"] = unix_time.isoformat()
            except AttributeError:
                pass

        result.append(data)

    return json.dumps(result, ensure_ascii=False, separators=(",", ":"))


def count_custom_emoji(entities) -> int:
    return sum(
        1
        for entity in (entities or ())
        if getattr(entity, "type", "") == CUSTOM_EMOJI_ENTITY_TYPE
    )


def deserialize_message_entities(value: str, bot=None):
    """Restore MessageEntity objects, explicitly preserving custom_emoji_id."""
    raw = parse_json(value, [])
    if not isinstance(raw, list):
        return []

    entities = []
    for data in raw:
        if not isinstance(data, dict):
            continue

        try:
            entity_type = str(data.get("type", ""))
            offset = int(data.get("offset", 0))
            length = int(data.get("length", 0))
            if not entity_type or length <= 0 or offset < 0:
                continue

            # Construct manually first. This is the most deterministic path
            # for custom emoji entities across PTB versions.
            kwargs = {
                "type": entity_type,
                "offset": offset,
                "length": length,
            }
            for key in ("url", "language", "custom_emoji_id", "date_time_format"):
                if data.get(key) is not None:
                    kwargs[key] = data[key]

            try:
                entity = MessageEntity(**kwargs)
            except TypeError:
                # Older PTB builds may not accept newer optional fields.
                # Keep custom_emoji_id whenever the installed PTB supports it.
                kwargs.pop("date_time_format", None)
                try:
                    entity = MessageEntity(**kwargs)
                except TypeError:
                    # Last compatibility fallback: construct with the fields
                    # guaranteed by the Bot API, while never silently converting
                    # a custom emoji entity into Unicode text.
                    minimal = {
                        "type": entity_type,
                        "offset": offset,
                        "length": length,
                    }
                    if entity_type == CUSTOM_EMOJI_ENTITY_TYPE and data.get("custom_emoji_id") is not None:
                        minimal["custom_emoji_id"] = str(data["custom_emoji_id"])
                    for key in ("url", "language"):
                        if data.get(key) is not None:
                            minimal[key] = data[key]
                    entity = MessageEntity(**minimal)

            # Custom emoji MUST retain its Telegram ID. If a malformed/legacy
            # record has no ID, skip it instead of sending a fake Unicode emoji.
            if entity_type == CUSTOM_EMOJI_ENTITY_TYPE:
                custom_id = getattr(entity, "custom_emoji_id", None)
                if not custom_id:
                    logger.warning("Skipping custom_emoji entity without custom_emoji_id: %r", data)
                    continue

            entities.append(entity)
        except Exception:
            logger.exception("Could not restore message entity: %r", data)

    return entities


def _utf16_len(value: str) -> int:
    return len(value.encode("utf-16-le")) // 2


def render_template_with_entities(text: str, entities, user):
    """Replace {Username} placeholders while keeping Telegram entities aligned.

    Telegram entity offsets are UTF-16 code-unit offsets, so all offset changes
    are calculated in UTF-16 rather than Python code-point positions.
    """
    if not text:
        return text, list(entities or [])

    replacement = display_name_for_user(user)
    matches = []
    for placeholder in USERNAME_PLACEHOLDERS:
        start = 0
        while True:
            pos = text.find(placeholder, start)
            if pos < 0:
                break
            matches.append((pos, pos + len(placeholder), replacement))
            start = pos + len(placeholder)

    if not matches:
        return text, list(entities or [])

    matches.sort(key=lambda item: item[0])

    pieces = []
    cursor = 0
    for start, end, value in matches:
        pieces.append(text[cursor:start])
        pieces.append(value)
        cursor = end
    pieces.append(text[cursor:])
    rendered = "".join(pieces)

    # Convert Python string positions of replacements into UTF-16 positions.
    replacements_utf16 = []
    for start, end, value in matches:
        replacements_utf16.append(
            (
                _utf16_len(text[:start]),
                _utf16_len(text[:end]),
                _utf16_len(value),
            )
        )

    def map_offset(old_offset: int) -> int:
        delta = 0
        for old_start, old_end, new_len in replacements_utf16:
            old_len = old_end - old_start
            if old_offset >= old_end:
                delta += new_len - old_len
            elif old_offset > old_start:
                # An entity boundary inside a placeholder is unusual. Map it
                # to the end of the replacement so Telegram receives valid
                # non-negative offsets.
                return old_start + delta + new_len
            else:
                break
        return old_offset + delta

    shifted = []
    for entity in entities or []:
        old_start = int(entity.offset)
        old_end = old_start + int(entity.length)
        new_start = map_offset(old_start)
        new_end = map_offset(old_end)

        if new_end < new_start:
            continue

        try:
            entity_kwargs = {
                "type": getattr(entity, "type", ""),
                "offset": new_start,
                "length": new_end - new_start,
            }
            # Only pass optional fields when they actually exist. This keeps
            # template rendering compatible across PTB versions and, most
            # importantly, preserves custom_emoji_id instead of dropping it.
            for key in ("url", "user", "language", "custom_emoji_id", "date_time_format", "unix_time"):
                value = getattr(entity, key, None)
                if value is not None:
                    entity_kwargs[key] = value
            try:
                shifted.append(MessageEntity(**entity_kwargs))
            except TypeError:
                entity_kwargs.pop("date_time_format", None)
                entity_kwargs.pop("unix_time", None)
                shifted.append(MessageEntity(**entity_kwargs))
        except (TypeError, ValueError):
            logger.warning("Could not shift message entity safely: %r", entity)

    return rendered, shifted


# ============================================================
# TELEGRAM SEND HELPERS
# Premium emoji fix: use copy_message when we have a saved source so that
# Telegram-side entities (including custom_emoji) are preserved verbatim.
# For the join-request message we store the admin's original message_id and
# chat_id so we can copy_message instead of re-sending the text.
# ============================================================

async def send_media_content(
    bot,
    chat_id: int,
    media_type: str,
    file_id: str,
    caption: str,
    entities,
    parse_mode: Optional[str],
    keyboard=None,
):
    """Send content while preserving Telegram entities/custom emoji exactly."""
    media_type = (media_type or "none").lower()
    caption = caption or ""
    entities = list(entities or [])

    # Keep Telegram custom emoji as entities. Never replace a custom emoji
    # with its visually similar Unicode character. Malformed legacy records
    # are ignored so old databases remain usable without crashing delivery.
    valid_entities = []
    for entity in entities:
        if getattr(entity, "type", None) == CUSTOM_EMOJI_ENTITY_TYPE and not getattr(entity, "custom_emoji_id", None):
            logger.warning("Ignoring custom_emoji entity without custom_emoji_id during send")
            continue
        valid_entities.append(entity)
    entities = valid_entities

    if media_type == "photo" and file_id:
        kwargs = {"chat_id": chat_id, "photo": file_id, "reply_markup": keyboard}
        if caption:
            kwargs["caption"] = caption[:MAX_CAPTION_LENGTH]
            if entities:
                kwargs["caption_entities"] = entities
            elif parse_mode:
                kwargs["parse_mode"] = parse_mode
        return await bot.send_photo(**kwargs)

    if media_type == "video" and file_id:
        kwargs = {"chat_id": chat_id, "video": file_id, "reply_markup": keyboard}
        if caption:
            kwargs["caption"] = caption[:MAX_CAPTION_LENGTH]
            if entities:
                kwargs["caption_entities"] = entities
            elif parse_mode:
                kwargs["parse_mode"] = parse_mode
        return await bot.send_video(**kwargs)

    if media_type == "document" and file_id:
        kwargs = {"chat_id": chat_id, "document": file_id, "reply_markup": keyboard}
        if caption:
            kwargs["caption"] = caption[:MAX_CAPTION_LENGTH]
            if entities:
                kwargs["caption_entities"] = entities
            elif parse_mode:
                kwargs["parse_mode"] = parse_mode
        return await bot.send_document(**kwargs)

    if media_type == "animation" and file_id:
        kwargs = {"chat_id": chat_id, "animation": file_id, "reply_markup": keyboard}
        if caption:
            kwargs["caption"] = caption[:MAX_CAPTION_LENGTH]
            if entities:
                kwargs["caption_entities"] = entities
            elif parse_mode:
                kwargs["parse_mode"] = parse_mode
        return await bot.send_animation(**kwargs)

    if media_type == "audio" and file_id:
        kwargs = {"chat_id": chat_id, "audio": file_id, "reply_markup": keyboard}
        if caption:
            kwargs["caption"] = caption[:MAX_CAPTION_LENGTH]
            if entities:
                kwargs["caption_entities"] = entities
            elif parse_mode:
                kwargs["parse_mode"] = parse_mode
        return await bot.send_audio(**kwargs)

    if media_type == "voice" and file_id:
        kwargs = {"chat_id": chat_id, "voice": file_id, "reply_markup": keyboard}
        if caption:
            kwargs["caption"] = caption[:MAX_CAPTION_LENGTH]
            if entities:
                kwargs["caption_entities"] = entities
            elif parse_mode:
                kwargs["parse_mode"] = parse_mode
        return await bot.send_voice(**kwargs)

    # Text messages MUST contain actual text. The previous implementation
    # used a single space as a fallback, which caused Telegram's
    # "Text must be non-empty" error shown in the screenshot.
    text = caption[:MAX_TEXT_LENGTH].strip("\u0000")
    if not text.strip():
        raise ValueError("Text must be non-empty. Add text/caption before previewing or broadcasting.")

    kwargs = {
        "chat_id": chat_id,
        "text": text,
        "reply_markup": keyboard,
        "disable_web_page_preview": True,
    }
    if entities:
        kwargs["entities"] = entities
    elif parse_mode:
        kwargs["parse_mode"] = parse_mode
    return await bot.send_message(**kwargs)


async def send_configured_message(bot, chat_id: int, user=None):
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
                "style": normalize_button_style(row["style"] if "style" in row.keys() else "primary"),
                "icon_custom_emoji_id": row["icon_custom_emoji_id"] if "icon_custom_emoji_id" in row.keys() else None,
                "row": row["row_number"],
                "position": row["position"],
            }
        )

    keyboard = build_keyboard(buttons)
    stored_entities = deserialize_message_entities(
        db.get_setting("join_msg_source_entities", "[]"), bot
    )
    rendered_caption, rendered_entities = render_template_with_entities(
        caption, stored_entities, user
    )

    has_template = any(token in caption for token in USERNAME_PLACEHOLDERS)
    source_chat = safe_int(db.get_setting("join_msg_source_chat", "0"), 0)
    source_msg = safe_int(db.get_setting("join_msg_source_id", "0"), 0)
    source_is_exact = db.get_setting("join_msg_source_exact", "0") == "1"

    for attempt in range(MAX_RETRIES):
        try:
            # Exact-copy path is the strongest custom-emoji preservation path.
            # It is only used when the stored source message represents the
            # complete configured message, not a separately supplied caption.
            if source_chat and source_msg and source_is_exact and not has_template:
                try:
                    return await bot.copy_message(
                        chat_id=chat_id,
                        from_chat_id=source_chat,
                        message_id=source_msg,
                        reply_markup=keyboard,
                    )
                except TelegramError as copy_exc:
                    logger.warning(
                        "Configured copy_message fallback: %s",
                        clean_error(copy_exc)[:500],
                    )

            # Explicit-entity path. When a template is used we cannot copy the
            # original message because {Username} must be rendered first.
            return await send_media_content(
                bot,
                chat_id,
                media_type,
                file_id,
                rendered_caption,
                rendered_entities,
                parse_mode,
                keyboard,
            )
        except RetryAfter as exc:
            await asyncio.sleep(float(exc.retry_after) + 1)
        except (NetworkError, TimedOut):
            if attempt >= MAX_RETRIES - 1:
                raise
            await asyncio.sleep(2 ** attempt)
        except BadRequest:
            raise

    raise RuntimeError("Telegram send retry limit reached.")


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
        db.log_error("ERROR", "start", "handler", repr(exc))


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

        update_id = getattr(update, "update_id", 0)
        event_key = f"join:{chat.id}:{user.id}:{update_id}"

        row_id = db.save_join_request(
            user.id,
            chat.id,
            event_key,
            request_time,
        )

        if row_id is None:
            db.log_event("duplicate_join_request", user.id, chat.id)
            return

        db.log_event("join_request_received", user.id, chat.id)

        dm_sent = False

        if db.get_setting("auto_message_enabled", "1") == "1":
            try:
                dm_chat_id = getattr(request, "user_chat_id", None) or user.id
                await send_configured_message(context.bot, dm_chat_id, user)
                dm_sent = True
                db.log_event("join_request_message_sent", user.id, chat.id)
            except Forbidden as exc:
                error = clean_error(exc)
                db.execute(
                    "UPDATE users SET is_blocked=1 WHERE user_id=?",
                    (user.id,),
                    commit=True,
                )
                db.log_error("WARNING", "join_request", "forbidden", error)
                db.update_join_request(row_id, sent=False, status="blocked", error=error)
            except Exception as exc:
                error = clean_error(exc)
                db.log_error("ERROR", "join_request", "send_failed", error)
                db.update_join_request(row_id, sent=False, status="failed", error=error)

        if channel["auto_approve"]:
            try:
                await context.bot.approve_chat_join_request(
                    chat_id=chat.id,
                    user_id=user.id,
                )
                db.log_event("join_request_auto_approved", user.id, chat.id)
                final_status = "auto_approved" if dm_sent else "auto_approved_dm_failed"
                db.update_join_request(row_id, sent=dm_sent, status=final_status)
            except TelegramError as exc:
                error = clean_error(exc)
                db.update_join_request(
                    row_id, sent=dm_sent, status="approve_failed", error=error
                )
                db.log_error("ERROR", "join_request", "approve_failed", error)
        elif dm_sent:
            db.update_join_request(row_id, sent=True, status="sent")

    except Exception as exc:
        logger.exception("Join request handler failed")
        db.log_error("EXCEPTION", "join_request", "handler", repr(exc))


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
                    _make_callback_button("🔄 Refresh", "admin_dashboard", "primary")
                ],
                [
                    _make_callback_button("⬅️ Back", "admin_home", "primary")
                ],
            ]
        ),
    )


async def show_statistics(query):
    s = db.stats()

    total = s["sent"] + s["failed"]
    success_rate = (s["sent"] / total) * 100 if total else 0

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
    maintenance = db.get_setting("maintenance_mode", "0")
    check = db.get_setting("check_join_enabled", "0")
    style = db.get_setting("start_button_style", "primary")

    await query.edit_message_text(
        (
            "⚙️ BOT SETTINGS\n\n"
            f"Bot Name: {db.get_setting('bot_name','Join Request Bot')}\n"
            f"Maintenance: {'ON' if maintenance == '1' else 'OFF'}\n"
            f"Check Join Button: {'ON' if check == '1' else 'OFF'}\n"
            f"Join Button Style: {style.upper()}"
        ),
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    _make_callback_button("Toggle Maintenance", "toggle_maintenance", "primary")
                ],
                [
                    _make_callback_button("Toggle Check Join", "toggle_check", "primary")
                ],
                [
                    _make_callback_button("🔵 Primary", "btn_style:primary", "primary"),
                    _make_callback_button("🟢 Success", "btn_style:success", "success"),
                    _make_callback_button("🔴 Danger", "btn_style:danger", "danger"),
                ],
                [
                    _make_callback_button("⬅️ Back", "admin_home", "primary")
                ],
            ]
        ),
    )


async def show_join_settings(query):
    enabled = db.get_setting("auto_message_enabled", "1")

    await query.edit_message_text(
        (
            "📩 JOIN REQUEST SETTINGS\n\n"
            f"Auto Message: {'ON' if enabled == '1' else 'OFF'}\n\n"
            "Only enabled/configured channels are processed.\n"
            "Auto Approve is controlled per channel from Channel Manager."
        ),
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    _make_callback_button("Toggle Auto Message", "toggle_auto", "primary")
                ],
                [
                    _make_callback_button("⬅️ Back", "admin_home", "primary")
                ],
            ]
        ),
    )


async def show_message_builder(query):
    message = db.get_join_message()
    buttons = db.get_message_buttons(message["id"])
    media = message["media_type"] or "none"
    parse_mode = message["parse_mode"] or "HTML"
    caption = message["caption"] or "(empty)"

    btn_lines = []
    for b in buttons:
        style = b["style"] if "style" in b.keys() else "primary"
        icon = " ⭐" if ("icon_custom_emoji_id" in b.keys() and b["icon_custom_emoji_id"]) else ""
        btn_lines.append(f"  [{style}]{icon} {b['text']} → {b['url']}")
    btn_preview = "\n".join(btn_lines) if btn_lines else "None"

    await query.edit_message_text(
        (
            "💬 MESSAGE BUILDER\n\n"
            f"Media: {media}\n"
            f"Parse Mode: {parse_mode}\n"
            f"Buttons: {len(buttons)}\n"
            f"{btn_preview}\n\n"
            f"Caption:\n{caption[:800]}"
        )[:4000],
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    _make_callback_button("📝 Caption", "set_caption", "primary"),
                    _make_callback_button("🔤 Parse", "toggle_parse", "primary"),
                ],
                [
                    _make_callback_button("🖼/🎥 Media", "set_media", "primary"),
                    _make_callback_button("🗑 Remove Media", "remove_media", "danger"),
                ],
                [
                    _make_callback_button("➕ Add Button", "add_button", "success"),
                    _make_callback_button("🗑 Clear Buttons", "clear_buttons", "danger"),
                ],
                [
                    _make_callback_button("👁 Preview", "preview", "primary"),
                    _make_callback_button("🧪 Test", "admin_test", "success"),
                ],
                [
                    _make_callback_button("⬅️ Back", "admin_home", "primary")
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
            status = "ON" if channel["enabled"] else "OFF"
            title = (
                channel["title"]
                or channel["username"]
                or str(channel["channel_id"])
            )
            auto = "ON" if channel["auto_approve"] else "OFF"
            lines.append(
                f"• {title}\n"
                f"  ID: {channel['channel_id']}\n"
                f"  Status: {status}\n"
                f"  Auto Approve: {auto}\n"
            )

    rows = [
        [
            _make_callback_button("➕ Add Channel", "add_channel", "success")
        ]
    ]

    for channel in channels:
        rows.append(
            [
                _make_callback_button(
                    ("Disable " if channel["enabled"] else "Enable ")
                    + str(channel["channel_id"]),
                    f"channel_toggle:{channel['channel_id']}",
                    "primary",
                ),
                _make_callback_button(
                    ("✅ Auto Approve" if channel["auto_approve"] else "⏸ Auto Approve"),
                    f"channel_auto:{channel['channel_id']}",
                    "success" if channel["auto_approve"] else "danger",
                ),
            ]
        )
        rows.append(
            [
                _make_callback_button(
                    "🗑 Remove Channel",
                    f"remove_channel:{channel['channel_id']}",
                    "danger",
                )
            ]
        )

    rows.append(
        [
            _make_callback_button("⬅️ Back", "admin_home", "primary")
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
        lines.append(f"• {name} — {row['user_id']}")

    await query.edit_message_text(
        "\n".join(lines)[:4000],
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    _make_callback_button("📤 Export CSV", "export_users", "primary")
                ],
                [
                    _make_callback_button("⬅️ Back", "admin_home", "primary")
                ],
            ]
        ),
    )


async def show_broadcast_menu(query):
    recent = db.fetchall(
        "SELECT id,status,total,sent,failed,blocked,created_at FROM broadcasts ORDER BY id DESC LIMIT 5"
    )
    lines = [
        "📢 BROADCAST CENTER",
        "",
        "Supports text, photo, video, document, animation, audio and voice.",
        "Premium/custom emoji are kept from the original Telegram message.",
        "Buttons support blue/green/red styles and optional custom-emoji icons.",
        "",
        "Recent:",
    ]
    if recent:
        for row in recent:
            lines.append(
                f"#{row['id']} — {row['status']} — {row['sent']}/{row['total']} sent, "
                f"{row['failed']} failed, {row['blocked']} blocked"
            )
    else:
        lines.append("No broadcasts yet.")

    await query.edit_message_text(
        "\n".join(lines)[:4000],
        reply_markup=InlineKeyboardMarkup(
            [
                [_make_callback_button("➕ New Broadcast", "broadcast_start", "success")],
                [_make_callback_button("⬅️ Back", "admin_home", "primary")],
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

    channel_id = db.get_setting("backup_channel_id", "")
    channel_title = db.get_setting("backup_channel_title", "")
    channel_username = db.get_setting("backup_channel_username", "")
    channel_enabled = db.get_setting("backup_channel_enabled", "0") == "1"

    lines = ["💾 BACKUP\n"]

    if channel_enabled and channel_id:
        channel_line = channel_title or channel_id
        if channel_username:
            channel_line += f" (@{channel_username})"
        lines.extend([
            "☁️ Auto Backup: ON",
            f"📢 Backup Channel: {channel_line}",
            f"🆔 ID: {channel_id}",
            "⏱ Interval: 1 minute",
            "",
        ])
    else:
        lines.extend([
            "☁️ Auto Backup: OFF",
            "📢 Backup Channel: Not configured",
            "",
        ])

    if backups:
        lines.append("Recent local backups:")
        for row in backups:
            lines.append(
                f"• {row['filename']}\n"
                f"  {row['size']:,} bytes\n"
                f"  {row['created_at']}"
            )
    else:
        lines.append("No local backups yet.")

    lines.append(
        "\n📥 RESTORE: Send a .db/.sqlite/.sqlite3 backup file directly here."
    )

    rows = [
        [_make_callback_button("💾 Create Backup", "backup_create", "success")],
        [_make_callback_button("☁️ Backup Channel Settings", "admin_backup_channel", "primary")],
        [_make_callback_button("⬅️ Back", "admin_home", "primary")],
    ]

    await query.edit_message_text(
        "\n".join(lines)[:4000],
        reply_markup=InlineKeyboardMarkup(rows),
    )


async def show_export_menu(query):
    await query.edit_message_text(
        "📤 DATABASE EXPORT",
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    _make_callback_button("👥 Users CSV", "export_users", "primary")
                ],
                [
                    _make_callback_button("📩 Join Requests CSV", "export_requests", "primary")
                ],
                [
                    _make_callback_button("📢 Broadcast Logs CSV", "export_broadcasts", "primary")
                ],
                [
                    _make_callback_button("⬅️ Back", "admin_home", "primary")
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
        lines.append(f"{row['user_id']} — {row['role']}{mark}")

    lines.append("\nOwner is controlled by OWNER_ID.")

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
            await query.answer("Access Denied", show_alert=True)
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

        if data == "admin_backup_channel":
            await show_backup_channel(query)
            return

        if data == "set_backup_channel":
            context.user_data["awaiting"] = "backup_channel"
            await query.message.reply_text(
                "Send the backup channel ID, @username, or link.\n\n"
                "Private channel: send its numeric ID (for example -1001234567890).\n"
                "Public channel: @MyBackupChannel or https://t.me/MyBackupChannel\n\n"
                "The bot must already be an administrator with permission to post messages.\n"
                "Private backup channels are kept hidden from normal users.\n\n"
                "Use /cancel to cancel."
            )
            return

        if data == "disable_backup_channel":
            db.set_setting("backup_channel_enabled", "0")
            await show_backup_channel(query)
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
            current = db.get_setting("auto_message_enabled", "1")
            db.set_setting("auto_message_enabled", "0" if current == "1" else "1")
            await show_join_settings(query)
            return

        if data == "toggle_maintenance":
            current = db.get_setting("maintenance_mode", "0")
            db.set_setting("maintenance_mode", "0" if current == "1" else "1")
            await show_settings(query)
            return

        if data == "toggle_check":
            current = db.get_setting("check_join_enabled", "0")
            db.set_setting("check_join_enabled", "0" if current == "1" else "1")
            await show_settings(query)
            return

        # Button color style selection
        if data.startswith("btn_style:"):
            style = data.split(":", 1)[1]
            if style in BUTTON_STYLES:
                db.set_setting("start_button_style", style)
            await show_settings(query)
            return

        if data == "set_caption":
            context.user_data["awaiting"] = "caption"
            await query.message.reply_text(
                "Send the caption/text now.\n\n"
                "Use Telegram's Custom Emoji picker for Premium Emoji. "
                "The bot stores Telegram entities, so they are sent as custom emoji instead of normal emoji.\n\n"
                "Use /cancel to cancel."
            )
            return

        if data == "toggle_parse":
            message = db.get_join_message()
            current = message["parse_mode"] or "HTML"
            new_mode = "MarkdownV2" if current == "HTML" else "HTML"
            db.execute(
                "UPDATE messages SET parse_mode=?,updated_at=? WHERE id=?",
                (new_mode, utc_now(), message["id"]),
                commit=True,
            )
            await show_message_builder(query)
            return

        if data in ("set_photo", "set_media"):
            context.user_data["awaiting"] = "media"
            await query.message.reply_text(
                "Send photo, video, document, animation, audio or voice now.\n"
                "You may include a Premium/custom-emoji caption.\n\n"
                "Use /cancel to cancel."
            )
            return

        if data == "remove_media":
            message = db.get_join_message()
            db.execute(
                "UPDATE messages SET media_type='none',file_id='',updated_at=? WHERE id=?",
                (utc_now(), message["id"]),
                commit=True,
            )
            # Clear copy_message source so we don't copy a deleted photo
            db.set_setting("join_msg_source_chat", "0")
            db.set_setting("join_msg_source_id", "0")
            db.set_setting("join_msg_source_exact", "0")
            db.set_setting("join_msg_source_entities", "[]")
            await show_message_builder(query)
            return

        # FIXED: Guided button add flow instead of raw JSON
        if data == "add_button":
            context.user_data["button_target"] = "join"
            context.user_data["awaiting"] = "btn_link"
            await query.message.reply_text(
                "Step 1/4 — Send the button URL.\n"
                "Example: https://t.me/yourchannel\n\n"
                "Use /cancel to cancel."
            )
            return

        if data == "clear_buttons":
            message = db.get_join_message()
            db.clear_message_buttons(message["id"])
            await show_message_builder(query)
            return

        if data == "preview":
            await preview_message(query, context)
            return

        if data.startswith("channel_toggle:"):
            channel_id = safe_int(data.split(":", 1)[1], None)

            if channel_id is None:
                await query.message.reply_text("Invalid channel action.")
                return

            db.execute(
                """
                UPDATE channels
                SET enabled=CASE enabled WHEN 1 THEN 0 ELSE 1 END,
                    updated_at=?
                WHERE channel_id=?
                """,
                (utc_now(), channel_id),
                commit=True,
            )
            await show_channels(query)
            return

        if data.startswith("channel_auto:"):
            channel_id = safe_int(data.split(":", 1)[1], None)

            if channel_id is None:
                await query.message.reply_text("Invalid channel action.")
                return

            row = db.fetchone(
                "SELECT * FROM channels WHERE channel_id=?",
                (channel_id,),
            )
            if not row:
                await query.message.reply_text("Channel not found.")
                return

            # Enabling auto-approve requires the bot to be an administrator
            # with permission to invite users. Check before saving the setting.
            if not row["auto_approve"]:
                try:
                    member = await context.bot.get_chat_member(
                        chat_id=channel_id,
                        user_id=context.bot.id,
                    )
                    can_invite = bool(getattr(member, "can_invite_users", False))
                    if getattr(member, "status", None) not in ("administrator", "creator") or not can_invite:
                        await query.answer(
                            "Bot needs admin + Invite Users permission for auto-approve.",
                            show_alert=True,
                        )
                        return
                except TelegramError as exc:
                    await query.answer(
                        f"Cannot verify channel permissions: {clean_error(exc)[:120]}",
                        show_alert=True,
                    )
                    return

            db.execute(
                """
                UPDATE channels
                SET auto_approve=CASE auto_approve WHEN 1 THEN 0 ELSE 1 END,
                    updated_at=?
                WHERE channel_id=?
                """,
                (utc_now(), channel_id),
                commit=True,
            )
            await show_channels(query)
            return

        if data.startswith("remove_channel:"):
            channel_id = safe_int(data.split(":", 1)[1], None)

            if channel_id is None:
                await query.message.reply_text("Invalid channel action.")
                return

            if not is_owner(user.id):
                await query.message.reply_text("Only Owner can remove channels.")
                return

            db.execute(
                "DELETE FROM channels WHERE channel_id=?",
                (channel_id,),
                commit=True,
            )
            await show_channels(query)
            return

        if data == "add_channel":
            context.user_data["awaiting"] = "channel"
            await query.message.reply_text(
                "Send the channel link or @username.\n\n"
                "The bot must be an admin in the channel.\n"
                "Use /cancel to cancel."
            )
            return

        if data == "broadcast_start":
            context.user_data["awaiting"] = "broadcast"
            context.user_data["pending_broadcast_buttons"] = []
            await query.message.reply_text(
                "Send the broadcast content now.\n\n"
                "Text, photo, video, document, animation, audio or voice are supported. "
                "Premium/custom emoji are preserved from Telegram entities.\n\n"
                "Use /cancel to cancel."
            )
            return

        if data == "broadcast_add_button":
            if not context.user_data.get("pending_broadcast_id"):
                await query.message.reply_text("No pending broadcast draft.")
                return
            context.user_data["button_target"] = "broadcast"
            context.user_data["awaiting"] = "btn_link"
            await query.message.reply_text(
                "Step 1/4 — Send the button URL.\nExample: https://t.me/yourchannel\n\nUse /cancel to cancel."
            )
            return

        if data == "broadcast_clear_buttons":
            broadcast_id = context.user_data.get("pending_broadcast_id")
            if broadcast_id:
                context.user_data["pending_broadcast_buttons"] = []
                db.execute(
                    "UPDATE broadcasts SET buttons_json='[]' WHERE id=? AND status='pending'",
                    (broadcast_id,),
                    commit=True,
                )
            await query.message.reply_text("🗑 Broadcast buttons cleared.", reply_markup=broadcast_draft_keyboard())
            return

        if data == "broadcast_preview":
            broadcast_id = context.user_data.get("pending_broadcast_id")
            if not broadcast_id:
                await query.message.reply_text("No pending broadcast draft.")
                return
            try:
                row = db.fetchone("SELECT * FROM broadcasts WHERE id=? AND status='pending'", (broadcast_id,))
                if not row:
                    await query.message.reply_text("Broadcast draft not found.")
                    return
                await send_broadcast_to_user(context.bot, row, query.from_user.id)
                await query.message.reply_text("👁 Broadcast preview sent.", reply_markup=broadcast_draft_keyboard())
            except Exception as exc:
                await query.message.reply_text(f"Preview failed: {clean_error(exc)[:700]}", reply_markup=broadcast_draft_keyboard())
            return

        if data == "broadcast_send":
            broadcast_id = context.user_data.get("pending_broadcast_id")
            if not broadcast_id:
                await query.message.reply_text("No pending broadcast draft.")
                return
            await start_broadcast_send(query.message, context, broadcast_id)
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
                "Verification requires the user to join the configured channel(s).",
            )
            return

        await answer_query(query, "Unknown or expired action.", True)

    except Exception as exc:
        logger.exception("Admin callback failed: %s", data)
        db.log_error("EXCEPTION", "admin_callback", data, repr(exc))

        try:
            await query.message.reply_text(
                "⚠️ Operation failed safely.\n"
                "Check Admin → Logs."
            )
        except Exception:
            pass


async def answer_query(query, text="", show_alert=False):
    try:
        await query.answer(text=text[:200], show_alert=show_alert)
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
        await send_configured_message(context.bot, query.from_user.id, query.from_user)
        await query.message.reply_text("👁 Preview sent.")
    except Exception as exc:
        await query.message.reply_text(f"Preview failed: {clean_error(exc)[:700]}")


async def test_message(
    query,
    context: ContextTypes.DEFAULT_TYPE,
):
    try:
        await send_configured_message(context.bot, query.from_user.id, query.from_user)
        await query.message.reply_text("🧪 Test message sent.")
    except Exception as exc:
        await query.message.reply_text(f"Test failed: {clean_error(exc)[:700]}")


# ============================================================
# USER MESSAGE -> CONFIGURED JOIN MESSAGE
# ============================================================

async def handle_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send the configured join message/button when a normal user starts or types."""
    user = update.effective_user
    message = update.message
    if not user or not message or is_admin(user.id):
        return

    # /start is handled by the dedicated start handler so it keeps the configured
    # welcome text. For every other private user message, send the same configured
    # join message with the admin-configured join button(s).
    try:
        db.upsert_user(user)
        if db.get_setting("maintenance_mode", "0") == "1":
            return
        await send_configured_message(context.bot, user.id, user)
    except Forbidden:
        db.execute("UPDATE users SET is_blocked=1 WHERE user_id=?", (user.id,), commit=True)
    except Exception as exc:
        logger.exception("User message handler failed")
        db.log_error("ERROR", "user_message", "send_configured", repr(exc))


async def private_message_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Route private non-command messages to admin flow or normal-user flow."""
    user = update.effective_user
    if not user:
        return
    if is_admin(user.id):
        await admin_input(update, context)
    else:
        await handle_user_message(update, context)


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

    # Owner-only backup restore.
    if (
        message.document
        and message.document.file_name
        and Path(message.document.file_name).suffix.lower() in {".db", ".sqlite", ".sqlite3"}
    ):
        if is_owner(user.id):
            await restore_backup_from_document(message, context)
        else:
            await message.reply_text("Only the owner can restore backups.")
        return

    if not state:
        return

    try:
        if state == "caption":
            text = message.text if message.text is not None else (message.caption or "")
            if len(text) > MAX_TEXT_LENGTH:
                await message.reply_text("Caption is too long (maximum 4096 characters).")
                return

            join_msg = db.get_join_message()
            db.execute(
                "UPDATE messages SET caption=?,updated_at=? WHERE id=?",
                (text, utc_now(), join_msg["id"]),
                commit=True,
            )

            entities = (message.entities if message.text is not None else message.caption_entities) or ()
            db.set_setting("join_msg_source_entities", serialize_message_entities(entities))

            # A separately supplied caption is NOT an exact copy of the media
            # message. Clear the exact-copy source whenever media is configured.
            if (join_msg["media_type"] or "none") != "none" or message.caption:
                db.set_setting("join_msg_source_chat", "0")
                db.set_setting("join_msg_source_id", "0")
                db.set_setting("join_msg_source_exact", "0")
            else:
                # Text-only message can be copied exactly, preserving all entities.
                db.set_setting("join_msg_source_chat", str(message.chat_id))
                db.set_setting("join_msg_source_id", str(message.message_id))
                db.set_setting("join_msg_source_exact", "1")

            custom_count = count_custom_emoji(entities)
            context.user_data.pop("awaiting", None)
            await message.reply_text(
                f"✅ Caption saved. {custom_count} Premium/custom emoji entity(ies) detected.\n"
                "Preview/Test will use Telegram entities directly.",
                reply_markup=admin_menu(),
            )
            return

        if state in ("media", "photo"):
            media_type = None
            file_id = None
            caption = message.caption or ""
            entities = message.caption_entities or ()

            if message.photo:
                media_type, file_id = "photo", message.photo[-1].file_id
            elif message.video:
                media_type, file_id = "video", message.video.file_id
            elif message.animation:
                media_type, file_id = "animation", message.animation.file_id
            elif message.document:
                media_type, file_id = "document", message.document.file_id
            elif message.audio:
                media_type, file_id = "audio", message.audio.file_id
            elif message.voice:
                media_type, file_id = "voice", message.voice.file_id

            if not media_type or not file_id:
                await message.reply_text(
                    "Send a photo, video, document, animation, audio or voice message."
                )
                return

            if len(caption) > MAX_CAPTION_LENGTH:
                await message.reply_text(f"Media caption is too long. Telegram allows up to {MAX_CAPTION_LENGTH} characters.")
                return

            join_msg = db.get_join_message()
            db.execute(
                "UPDATE messages SET media_type=?,file_id=?,caption=?,updated_at=? WHERE id=?",
                (media_type, file_id, caption, utc_now(), join_msg["id"]),
                commit=True,
            )
            db.set_setting("join_msg_source_entities", serialize_message_entities(entities))

            if caption:
                # This exact source contains both media and caption.
                db.set_setting("join_msg_source_chat", str(message.chat_id))
                db.set_setting("join_msg_source_id", str(message.message_id))
                db.set_setting("join_msg_source_exact", "1")
            else:
                # Do not accidentally copy a previous text/caption source.
                db.set_setting("join_msg_source_chat", "0")
                db.set_setting("join_msg_source_id", "0")
                db.set_setting("join_msg_source_exact", "0")

            context.user_data.pop("awaiting", None)
            await message.reply_text(
                f"✅ {media_type.title()} saved. {count_custom_emoji(entities)} Premium/custom emoji entity(ies) detected.",
                reply_markup=admin_menu(),
            )
            return

        if state == "btn_link":
            raw = (message.text or "").strip()
            if raw.startswith("@"):
                raw = f"https://t.me/{raw.lstrip('@')}"
            elif raw.startswith("t.me/"):
                raw = "https://" + raw
            if not valid_http_url(raw):
                await message.reply_text(
                    "Invalid URL. Send a full http(s) URL, e.g. https://t.me/yourchannel"
                )
                return
            context.user_data["btn_pending_url"] = raw
            context.user_data["awaiting"] = "btn_name"
            await message.reply_text(
                "Step 2/4 — Send the button label.\n\n"
                "You can include one Premium/custom emoji. It will be used as the button icon.\n"
                "Use /cancel to cancel."
            )
            return

        if state == "btn_name":
            name = (message.text or message.caption or "").strip()
            if not name:
                await message.reply_text("Button label cannot be empty.")
                return

            icon_id = None
            entities = message.entities or message.caption_entities or ()
            for entity in entities:
                if getattr(entity, "type", None) == MessageEntity.CUSTOM_EMOJI:
                    icon_id = getattr(entity, "custom_emoji_id", None)
                    break

            context.user_data["btn_pending_name"] = name[:64]
            context.user_data["btn_pending_icon"] = icon_id
            context.user_data["awaiting"] = "btn_style_choice"
            await message.reply_text(
                "Step 3/4 — Choose button color:",
                reply_markup=InlineKeyboardMarkup(
                    [[
                        _make_callback_button("🔵 Primary", "btn_add_style:primary", "primary"),
                        _make_callback_button("🟢 Success", "btn_add_style:success", "success"),
                        _make_callback_button("🔴 Danger", "btn_add_style:danger", "danger"),
                    ]]
                ),
            )
            return

        if state == "btn_style_choice":
            await message.reply_text(
                "Please tap a color button above.",
                reply_markup=InlineKeyboardMarkup(
                    [[
                        _make_callback_button("🔵 Primary", "btn_add_style:primary", "primary"),
                        _make_callback_button("🟢 Success", "btn_add_style:success", "success"),
                        _make_callback_button("🔴 Danger", "btn_add_style:danger", "danger"),
                    ]]
                ),
            )
            return

        if state == "broadcast_buttons":
            await message.reply_text(
                "Broadcast draft is ready. Use the buttons below to add buttons or send it.",
                reply_markup=broadcast_draft_keyboard(),
            )
            return

        if state == "backup_channel":
            raw = (message.text or "").strip()
            if not raw:
                await message.reply_text(
                    "Send the private/public backup channel ID, @username, or t.me link."
                )
                return

            # Private channels normally have no @username.  The owner can use
            # the numeric chat ID (for example -1001234567890). Public channels
            # can still be configured by @username or t.me link.
            if raw.startswith("@"):
                lookup = raw
            elif "t.me/" in raw:
                tail = raw.split("t.me/", 1)[1].split("/", 1)[0].strip()
                lookup = f"@{tail}"
            else:
                try:
                    lookup = int(raw)
                except ValueError:
                    lookup = f"@{raw.lstrip('@')}"

            try:
                chat = await context.bot.get_chat(lookup)
                if chat.type != "channel":
                    await message.reply_text("That is not a Telegram channel.")
                    return

                member = await context.bot.get_chat_member(chat.id, context.bot.id)
                status = str(getattr(member, "status", ""))
                if status not in ("administrator", "creator"):
                    await message.reply_text(
                        "❌ Bot is not an administrator in this channel."
                    )
                    return

                can_post = getattr(member, "can_post_messages", None)
                if can_post is False:
                    await message.reply_text(
                        "❌ Bot is admin, but it does not have permission to post messages."
                    )
                    return

                # Do not require a username: this intentionally supports private
                # backup vault channels. Username is stored only if Telegram has one.
                username = getattr(chat, "username", None) or ""
                db.set_setting("backup_channel_id", str(chat.id))
                db.set_setting("backup_channel_username", username)
                db.set_setting("backup_channel_title", chat.title or "")
                db.set_setting("backup_channel_enabled", "1")
                context.user_data.pop("awaiting", None)

                await message.reply_text(
                    "✅ Private backup channel configured.\n\n"
                    f"📢 {chat.title or 'Private Backup Vault'}\n"
                    f"🆔 {chat.id}\n\n"
                    "🔒 This backup channel is admin-only and is never shown to normal users.\n"
                    "☁️ Automatic full backup is now ON.\n"
                    "⏱ A new backup will be uploaded every 1 minute.",
                    reply_markup=admin_menu(),
                )

                # Test the exact publishing path immediately so a wrong
                # permission is discovered before the first scheduled backup.
                try:
                    await message.reply_text("🧪 Sending a test backup to the channel...")
                    await automatic_backup_once(context.bot)
                    await message.reply_text("✅ Test backup uploaded successfully.")
                except Exception as exc:
                    db.log_error("ERROR", "backup", "channel_test", repr(exc))
                    await message.reply_text(
                        "⚠️ Channel saved, but the test upload failed:\n"
                        f"{clean_error(exc)[:500]}\n\n"
                        "Check that the bot can post in the channel."
                    )
            except TelegramError as exc:
                await message.reply_text(
                    f"❌ Could not access the backup channel:\n{clean_error(exc)[:700]}"
                )
            return

        if state == "channel":
            raw = (message.text or "").strip()
            if raw.startswith("@"):
                raw = raw.lstrip("@")
            elif "t.me/" in raw:
                raw = raw.split("t.me/", 1)[1].split("/", 1)[0]
            try:
                try:
                    channel_id_or_username = int(raw)
                except ValueError:
                    channel_id_or_username = f"@{raw}"

                chat = await context.bot.get_chat(channel_id_or_username)
                if chat.type != "channel":
                    await message.reply_text("Please provide a Telegram channel username/link or numeric ID.")
                    return

                member = await context.bot.get_chat_member(chat.id, context.bot.id)
                if str(getattr(member, "status", "")) not in ("administrator", "creator"):
                    await message.reply_text("Bot is not an administrator in this channel.")
                    return

                now = utc_now()
                db.execute(
                    """
                    INSERT INTO channels(
                        channel_id,username,title,type,enabled,required,auto_approve,sort_order,created_at,updated_at
                    ) VALUES(?,?,?,?,1,1,0,0,?,?)
                    ON CONFLICT(channel_id) DO UPDATE SET
                        username=excluded.username,title=excluded.title,type=excluded.type,updated_at=excluded.updated_at
                    """,
                    (chat.id, chat.username, chat.title or "", chat.type, now, now),
                    commit=True,
                )
                context.user_data.pop("awaiting", None)
                await message.reply_text(
                    f"✅ Channel configured.\n\nTitle: {chat.title or '-'}\nID: {chat.id}\n"
                    + (f"Username: @{chat.username}" if chat.username else ""),
                    reply_markup=admin_menu(),
                )
            except TelegramError as exc:
                await message.reply_text(f"Could not access the channel.\n\n{clean_error(exc)[:700]}")
            return

        if state == "broadcast":
            await create_broadcast(update, context)
            return

    except Exception as exc:
        logger.exception("Admin input failed: %s", state)
        db.log_error("EXCEPTION", "admin_input", state, repr(exc))
        await message.reply_text(f"Operation failed safely:\n{clean_error(exc)[:700]}")


# Callback handler for the button-style picker (step 3 of guided flow)
async def btn_add_style_callback(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
    query = update.callback_query
    if not query:
        return

    user = query.from_user
    if not user or not is_admin(user.id):
        await query.answer("Access Denied", show_alert=True)
        return

    data = query.data or ""
    if not data.startswith("btn_add_style:"):
        return

    style = normalize_button_style(data.split(":", 1)[1])
    url = context.user_data.pop("btn_pending_url", None)
    name = context.user_data.pop("btn_pending_name", None)
    icon_id = context.user_data.pop("btn_pending_icon", None)
    target = context.user_data.get("button_target", "join")
    context.user_data.pop("awaiting", None)

    await query.answer()
    if not url or not name:
        await query.message.reply_text("Button data expired. Start Add Button again.")
        return

    button = {
        "text": name[:64],
        "url": url,
        "style": style,
        "icon_custom_emoji_id": icon_id,
        "row": 0,
        "position": 0,
    }

    if target == "broadcast":
        buttons = context.user_data.setdefault("pending_broadcast_buttons", [])
        button["row"] = len(buttons)
        button["position"] = 0
        buttons.append(button)
        broadcast_id = context.user_data.get("pending_broadcast_id")
        if broadcast_id:
            db.execute(
                "UPDATE broadcasts SET buttons_json=? WHERE id=? AND status='pending'",
                (json.dumps(buttons, ensure_ascii=False), broadcast_id),
                commit=True,
            )
        await query.message.reply_text(
            f"✅ Broadcast button added: [{style.upper()}] {name}\n"
            + ("⭐ Premium button icon saved." if icon_id else ""),
            reply_markup=broadcast_draft_keyboard(),
        )
        return

    join_msg = db.get_join_message()
    existing = db.get_message_buttons(join_msg["id"])
    next_row = len(existing)
    db.add_message_button(
        join_msg["id"],
        name,
        url,
        row_number=next_row,
        position=0,
        style=style,
        icon_custom_emoji_id=icon_id,
    )
    await query.message.reply_text(
        f"✅ Button added: [{style.upper()}] {name}\n"
        + ("⭐ Premium button icon saved." if icon_id else ""),
        reply_markup=admin_menu(),
    )


# ============================================================
# BROADCAST
# ============================================================

def broadcast_draft_keyboard():
    return InlineKeyboardMarkup(
        [
            [_make_callback_button("➕ Add Button", "broadcast_add_button", "success")],
            [
                _make_callback_button("👁 Preview", "broadcast_preview", "primary"),
                _make_callback_button("🚀 Send Now", "broadcast_send", "success"),
            ],
            [
                _make_callback_button("🗑 Clear Buttons", "broadcast_clear_buttons", "danger"),
            ],
            [_make_callback_button("⬅️ Broadcast Center", "admin_broadcast", "primary")],
        ]
    )


async def create_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    if not user or not message:
        return

    media_type = "none"
    file_id = None
    text = ""
    caption = ""
    source_entities = message.entities or ()

    if message.photo:
        media_type, file_id = "photo", message.photo[-1].file_id
        caption = message.caption or ""
        source_entities = message.caption_entities or ()
    elif message.video:
        media_type, file_id = "video", message.video.file_id
        caption = message.caption or ""
        source_entities = message.caption_entities or ()
    elif message.animation:
        media_type, file_id = "animation", message.animation.file_id
        caption = message.caption or ""
        source_entities = message.caption_entities or ()
    elif message.document:
        media_type, file_id = "document", message.document.file_id
        caption = message.caption or ""
        source_entities = message.caption_entities or ()
    elif message.audio:
        media_type, file_id = "audio", message.audio.file_id
        caption = message.caption or ""
        source_entities = message.caption_entities or ()
    elif message.voice:
        media_type, file_id = "voice", message.voice.file_id
        caption = message.caption or ""
        source_entities = message.caption_entities or ()
    else:
        text = message.text or ""
        caption = text
        source_entities = message.entities or ()

    content = caption if media_type != "none" else text
    max_len = 1024 if media_type != "none" else MAX_TEXT_LENGTH
    if not content and media_type == "none":
        await message.reply_text("Broadcast content cannot be empty.")
        return
    if len(content) > max_len:
        await message.reply_text(f"Broadcast content is too long. Maximum: {max_len} characters.")
        return

    buttons = context.user_data.get("pending_broadcast_buttons", [])
    entities_json = serialize_message_entities(source_entities)
    cursor = db.execute(
        """
        INSERT INTO broadcasts(
            admin_id,text,media_type,file_id,caption,parse_mode,
            source_chat_id,source_message_id,entities_json,buttons_json,status,created_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            user.id,
            text,
            media_type,
            file_id,
            caption,
            db.get_join_message()["parse_mode"] or "HTML",
            message.chat_id,
            message.message_id,
            entities_json,
            json.dumps(buttons, ensure_ascii=False),
            "pending",
            utc_now(),
        ),
        commit=True,
    )
    broadcast_id = cursor.lastrowid
    context.user_data["pending_broadcast_id"] = broadcast_id
    context.user_data["awaiting"] = "broadcast_buttons"

    custom_count = count_custom_emoji(source_entities)
    await message.reply_text(
        f"📢 Broadcast #{broadcast_id} ready.\n\n"
        f"Type: {media_type}\n"
        f"Premium/custom emoji: {custom_count}\n"
        f"Buttons: {len(buttons)}\n\n"
        "Add buttons or tap Send Now. The original Telegram message is copied "
        "when possible, preserving its entities exactly.",
        reply_markup=broadcast_draft_keyboard(),
    )


async def broadcast_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    user = update.effective_user
    if not user or not is_admin(user.id):
        await update.message.reply_text("Access Denied")
        return

    broadcast_id = context.user_data.get("pending_broadcast_id")
    if not broadcast_id:
        await update.message.reply_text("No pending broadcast.")
        return

    await start_broadcast_send(update.message, context, broadcast_id)


async def start_broadcast_send(message, context, broadcast_id):
    row = db.fetchone("SELECT * FROM broadcasts WHERE id=? AND status='pending'", (broadcast_id,))
    if not row:
        await message.reply_text("Pending broadcast not found or already sent.")
        return

    db.execute(
        "UPDATE broadcasts SET status='running',started_at=? WHERE id=?",
        (utc_now(), broadcast_id),
        commit=True,
    )
    context.user_data.pop("pending_broadcast_id", None)
    context.user_data.pop("pending_broadcast_buttons", None)
    context.user_data.pop("button_target", None)
    context.user_data.pop("awaiting", None)
    await message.reply_text(f"📢 Broadcast #{broadcast_id} started.")
    context.application.create_task(run_broadcast(context.application, broadcast_id))


async def send_broadcast_to_user(bot, row, user_id: int):
    """Copy the original Telegram message first, then use an entity-preserving fallback."""
    source_chat = safe_int(row["source_chat_id"] if "source_chat_id" in row.keys() else 0, 0)
    source_msg = safe_int(row["source_message_id"] if "source_message_id" in row.keys() else 0, 0)
    buttons = parse_json(row["buttons_json"], [])
    keyboard = build_keyboard(buttons)

    if source_chat and source_msg:
        try:
            return await bot.copy_message(
                chat_id=user_id,
                from_chat_id=source_chat,
                message_id=source_msg,
                reply_markup=keyboard,
            )
        except TelegramError as copy_exc:
            logger.warning(
                "Broadcast copy_message fallback for user %s: %s",
                user_id,
                clean_error(copy_exc)[:500],
            )

    entities = deserialize_message_entities(row["entities_json"] if "entities_json" in row.keys() else "[]", bot)
    return await send_media_content(
        bot,
        user_id,
        row["media_type"] or "none",
        row["file_id"] or "",
        row["caption"] if row["media_type"] != "none" else (row["text"] or row["caption"] or ""),
        entities,
        row["parse_mode"] or None,
        keyboard,
    )


async def run_broadcast(application: Application, broadcast_id: int):
    try:
        row = db.fetchone("SELECT * FROM broadcasts WHERE id=?", (broadcast_id,))
        if not row:
            return

        users = db.fetchall("SELECT user_id FROM users WHERE is_blocked=0 ORDER BY user_id")
        total_row = db.fetchone("SELECT COUNT(*) AS c FROM users")
        total = int(total_row["c"]) if total_row else len(users)
        db.execute("UPDATE broadcasts SET total=? WHERE id=?", (total, broadcast_id), commit=True)

        count_rows = db.fetchall(
            "SELECT status,COUNT(*) AS c FROM broadcast_logs WHERE broadcast_id=? GROUP BY status",
            (broadcast_id,),
        )
        counts = {row["status"]: int(row["c"]) for row in count_rows}
        sent = counts.get("sent", 0)
        failed = counts.get("failed", 0)
        blocked = counts.get("blocked", 0)
        for item in users:
            user_id = item["user_id"]
            existing = db.fetchone(
                "SELECT status FROM broadcast_logs WHERE broadcast_id=? AND user_id=?",
                (broadcast_id, user_id),
            )
            if existing and existing["status"] in ("sent", "blocked"):
                continue

            try:
                await send_broadcast_to_user(application.bot, row, user_id)
                sent += 1
                status, error = "sent", None
            except Forbidden as exc:
                blocked += 1
                status, error = "blocked", clean_error(exc)
                db.execute("UPDATE users SET is_blocked=1 WHERE user_id=?", (user_id,), commit=True)
            except RetryAfter as exc:
                await asyncio.sleep(float(exc.retry_after) + 1)
                try:
                    await send_broadcast_to_user(application.bot, row, user_id)
                    sent += 1
                    status, error = "sent", None
                except Forbidden as retry_exc:
                    blocked += 1
                    status, error = "blocked", clean_error(retry_exc)
                    db.execute("UPDATE users SET is_blocked=1 WHERE user_id=?", (user_id,), commit=True)
                except Exception as retry_exc:
                    failed += 1
                    status, error = "failed", clean_error(retry_exc)
            except (NetworkError, TimedOut) as exc:
                retry_ok = False
                last_error = exc
                for retry_index in range(2):
                    try:
                        await asyncio.sleep(2 ** retry_index)
                        await send_broadcast_to_user(application.bot, row, user_id)
                        sent += 1
                        status, error = "sent", None
                        retry_ok = True
                        break
                    except Forbidden as retry_exc:
                        blocked += 1
                        status, error = "blocked", clean_error(retry_exc)
                        db.execute("UPDATE users SET is_blocked=1 WHERE user_id=?", (user_id,), commit=True)
                        retry_ok = True
                        break
                    except Exception as retry_exc:
                        last_error = retry_exc
                if not retry_ok:
                    failed += 1
                    status, error = "failed", clean_error(last_error)
            except Exception as exc:
                failed += 1
                status, error = "failed", clean_error(exc)

            db.execute(
                """
                INSERT INTO broadcast_logs(broadcast_id,user_id,status,error,created_at)
                VALUES(?,?,?,?,?)
                ON CONFLICT(broadcast_id,user_id) DO UPDATE SET
                    status=excluded.status,error=excluded.error,created_at=excluded.created_at
                """,
                (broadcast_id, user_id, status, error, utc_now()),
                commit=True,
            )
            db.execute(
                "UPDATE broadcasts SET sent=?,failed=?,blocked=?,next_user_id=? WHERE id=?",
                (sent, failed, blocked, user_id, broadcast_id),
                commit=True,
            )
            await asyncio.sleep(BROADCAST_DELAY)

        db.execute(
            """
            UPDATE broadcasts SET status='completed',sent=?,failed=?,blocked=?,next_user_id=NULL,finished_at=?
            WHERE id=?
            """,
            (sent, failed, blocked, utc_now(), broadcast_id),
            commit=True,
        )
        db.log_event(
            "broadcast_completed",
            details=f"id={broadcast_id};sent={sent};failed={failed};blocked={blocked}",
        )
    except Exception as exc:
        logger.exception("Broadcast crashed safely")
        db.execute(
            "UPDATE broadcasts SET status='paused' WHERE id=? AND status='running'",
            (broadcast_id,),
            commit=True,
        )
        db.log_error("EXCEPTION", "broadcast", "worker", repr(exc))


# ============================================================
# BACKUP / DISASTER RECOVERY
# ============================================================

def backup_channel_id() -> Optional[int]:
    """Return the configured Telegram backup channel ID, if enabled."""
    if db.get_setting("backup_channel_enabled", "0") != "1":
        return None
    raw = db.get_setting("backup_channel_id", "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def prune_local_backups() -> None:
    """Keep only a small local rolling set; Telegram channel is the durable vault."""
    try:
        files = sorted(
            (
                f for f in BACKUP_DIR.glob("backup_*.db")
                if f.is_file()
            ),
            key=lambda f: f.stat().st_mtime,
            reverse=True,
        )
        for old_file in files[AUTO_BACKUP_LOCAL_RETENTION:]:
            try:
                old_file.unlink()
            except OSError:
                logger.warning("Could not prune local backup: %s", old_file)
    except Exception:
        logger.exception("Local backup pruning failed")


def create_backup_file(created_by: Optional[int] = None):
    """
    Create a consistent SQLite backup containing ALL persisted bot state:
    users, admins, channels, bot_settings, messages, buttons, join requests,
    broadcasts, broadcast logs, events, errors and backup metadata.
    """
    if not DB_PATH.exists():
        raise FileNotFoundError("Database file does not exist.")

    filename = (
        "backup_"
        + datetime.now(timezone.utc).strftime("%Y_%m_%d_%H%M%S_%f")
        + ".db"
    )
    destination = BACKUP_DIR / filename

    source = None
    target = None
    try:
        # SQLite's backup API gives us a consistent snapshot even while the bot
        # is receiving join requests or writing settings.
        source = sqlite3.connect(str(DB_PATH), timeout=30)
        target = sqlite3.connect(str(destination), timeout=30)
        source.backup(target)
        target.commit()

        integrity = target.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise RuntimeError(f"Backup integrity check failed: {integrity}")

        size = destination.stat().st_size

        db.execute(
            "INSERT INTO backups(filename,created_at,created_by,size) VALUES(?,?,?,?)",
            (filename, utc_now(), created_by, size),
            commit=True,
        )

        prune_local_backups()
        return destination, filename, size

    finally:
        if source:
            source.close()
        if target:
            target.close()


async def send_backup_to_channel(bot, destination: Path, filename: str, size: int, automatic: bool):
    """Upload a backup file to the configured Telegram backup channel."""
    channel_id = backup_channel_id()
    if not channel_id:
        return False

    caption = (
        "🔐 AUTOMATIC BOT BACKUP\n" if automatic else "💾 MANUAL BOT BACKUP\n"
    )
    caption += (
        f"📁 {filename}\n"
        f"📦 Size: {size:,} bytes\n"
        f"🕐 UTC: {utc_now()}\n\n"
        "This .db file contains the bot's persisted database state and can be "
        "uploaded to the bot by the owner to restore it."
    )

    with destination.open("rb") as file:
        await bot.send_document(
            chat_id=channel_id,
            document=InputFile(file, filename=filename),
            caption=caption[:1024],
        )
    return True


async def create_backup(query):
    """Manual backup from Admin → Backup."""
    if not DB_PATH.exists():
        await query.message.reply_text("Database file does not exist.")
        return

    destination = None
    try:
        destination, filename, size = create_backup_file(query.from_user.id)

        # Always give the owner the downloadable copy.
        with destination.open("rb") as file:
            await query.message.reply_document(
                document=InputFile(file, filename=filename),
                caption=(
                    "💾 Backup created successfully.\n"
                    f"Size: {size:,} bytes\n\n"
                    "To restore: send this .db file to me."
                ),
            )

        # If a backup channel is configured, keep an off-server copy too.
        channel_id = backup_channel_id()
        if channel_id:
            try:
                await send_backup_to_channel(
                    query.get_bot(),
                    destination,
                    filename,
                    size,
                    automatic=False,
                )
                await query.message.reply_text(
                    "☁️ A copy was also uploaded to the configured backup channel."
                )
            except Exception as channel_exc:
                logger.exception("Manual backup channel upload failed")
                db.log_error(
                    "ERROR",
                    "backup",
                    "manual_channel_upload",
                    repr(channel_exc),
                )
                await query.message.reply_text(
                    "⚠️ Local/manual backup succeeded, but channel upload failed:\n"
                    f"{clean_error(channel_exc)[:500]}"
                )

    except Exception as exc:
        logger.exception("Backup creation failed")
        db.log_error("ERROR", "backup", "create", repr(exc))
        await query.message.reply_text(f"Backup failed:\n{clean_error(exc)[:700]}")


async def automatic_backup_once(bot) -> bool:
    """Create and upload one disaster-recovery backup."""
    channel_id = backup_channel_id()
    if not channel_id:
        return False

    destination = None
    try:
        destination, filename, size = create_backup_file(None)
        await send_backup_to_channel(
            bot,
            destination,
            filename,
            size,
            automatic=True,
        )
        db.log_event(
            "automatic_backup_sent",
            details=f"channel_id={channel_id};filename={filename};size={size}",
        )
        logger.info(
            "Automatic backup uploaded: %s (%s bytes) -> %s",
            filename,
            size,
            channel_id,
        )
        return True
    except Exception as exc:
        logger.exception("Automatic backup failed")
        db.log_error("ERROR", "backup", "automatic", repr(exc))
        return False


async def automatic_backup_loop(application: Application):
    """
    Run forever in the bot process. Every minute, if a backup channel is
    configured, create a complete SQLite snapshot and upload it there.
    """
    logger.info("Automatic backup worker started (interval=%ss).", AUTO_BACKUP_INTERVAL_SECONDS)

    # Small startup delay avoids racing with startup DB/schema initialization.
    await asyncio.sleep(5)

    while True:
        try:
            await automatic_backup_once(application.bot)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Automatic backup loop iteration failed")
            db.log_error("ERROR", "backup", "loop", repr(exc))

        try:
            await asyncio.sleep(AUTO_BACKUP_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            logger.info("Automatic backup worker stopped.")
            raise


async def show_backup_channel(query):
    channel_id = db.get_setting("backup_channel_id", "")
    username = db.get_setting("backup_channel_username", "")
    title = db.get_setting("backup_channel_title", "")
    enabled = db.get_setting("backup_channel_enabled", "0") == "1"

    if enabled and channel_id:
        status_lines = [
            "🟢 ENABLED",
            f"Channel: {title or '-'}",
            f"ID: {channel_id}",
        ]
        if username:
            status_lines.append(f"Username: @{username}")
        status = "\n".join(status_lines)
    else:
        status = "🔴 NOT CONFIGURED"

    await query.edit_message_text(
        "☁️ BACKUP CHANNEL\n\n"
        f"{status}\n\n"
        "Automatic backup: EVERY 1 MINUTE\n"
        "Each backup is a complete SQLite database snapshot.\n"
        "The file is uploaded to this Telegram channel so it survives "
        "Render/local disk loss.\n\n"
        "Bot requirements:\n"
        "• Bot must be an administrator in the channel.\n"
        "• Bot must have permission to post messages.\n"
        "• Use a channel you control as the backup vault.",
        reply_markup=InlineKeyboardMarkup(
            [
                [_make_callback_button("➕ Set / Change Channel", "set_backup_channel", "success")],
                [_make_callback_button("⏸ Disable Auto Backup", "disable_backup_channel", "danger")],
                [_make_callback_button("⬅️ Back", "admin_home", "primary")],
            ]
        ),
    )


async def restore_backup_from_document(message, context):
    """
    Owner sends a .db/.sqlite/.sqlite3 backup file -> validate -> restore.
    Before replacement, make a local emergency snapshot of the current DB.
    """
    try:
        await message.reply_text("⏳ Validating backup file...")

        tg_file = await context.bot.get_file(message.document.file_id)
        suffix = Path(message.document.file_name or ".db").suffix.lower()
        if suffix not in {".db", ".sqlite", ".sqlite3"}:
            await message.reply_text(
                "❌ Unsupported backup file. Send a .db, .sqlite or .sqlite3 file."
            )
            return

        tmp_path = BACKUP_DIR / (
            f"restore_tmp_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S_%f')}.db"
        )
        await tg_file.download_to_drive(str(tmp_path))

        # Validate the uploaded SQLite database.
        try:
            check_conn = sqlite3.connect(str(tmp_path), timeout=10)
            integrity = check_conn.execute("PRAGMA integrity_check").fetchone()[0]
            check_conn.close()
        except Exception as exc:
            tmp_path.unlink(missing_ok=True)
            await message.reply_text(
                f"❌ Invalid backup file:\n{clean_error(exc)[:500]}"
            )
            return

        if integrity != "ok":
            tmp_path.unlink(missing_ok=True)
            await message.reply_text(
                f"❌ Backup integrity check failed: {integrity}"
            )
            return

        # Ensure this is a backup produced by this bot, or at minimum a valid
        # SQLite DB containing the bot's core schema.
        check_conn = sqlite3.connect(str(tmp_path), timeout=10)
        tables = {
            row[0]
            for row in check_conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        check_conn.close()

        required_tables = {"users", "admins", "channels", "bot_settings", "messages"}
        missing = required_tables - tables
        if missing:
            tmp_path.unlink(missing_ok=True)
            await message.reply_text(
                "❌ This is a valid SQLite file, but it is not a compatible bot backup.\n"
                f"Missing tables: {', '.join(sorted(missing))}"
            )
            return

        # Emergency snapshot of the currently running DB before overwrite.
        emergency = None
        try:
            emergency, emergency_name, emergency_size = create_backup_file(message.from_user.id)
            logger.info("Pre-restore emergency backup created: %s", emergency_name)
        except Exception:
            logger.exception("Could not create pre-restore emergency backup")

        # Close current DB connection and atomically replace the live database.
        db.close()
        os.replace(str(tmp_path), str(DB_PATH))
        db.connect()

        db.log_event(
            "backup_restored",
            user_id=message.from_user.id,
            details=f"filename={message.document.file_name};pre_restore={bool(emergency)}",
        )

        await message.reply_text(
            "✅ FULL BACKUP RESTORED SUCCESSFULLY.\n\n"
            "All persisted users, settings, channels, messages, buttons, "
            "join requests, broadcasts and logs from that backup are now active.\n\n"
            "Automatic backup will continue using the restored backup-channel setting.",
            reply_markup=admin_menu(),
        )

    except Exception as exc:
        logger.exception("Backup restore failed")
        try:
            db.connect()
        except Exception:
            pass
        await message.reply_text(f"❌ Restore failed:\n{clean_error(exc)[:700]}")


# ============================================================
# CSV EXPORT
# ============================================================

async def export_csv(query, export_type):
    if export_type == "users":
        rows = db.fetchall("SELECT * FROM users ORDER BY user_id")
        filename = "users.csv"
    elif export_type == "join_requests":
        rows = db.fetchall("SELECT * FROM join_requests ORDER BY id")
        filename = "join_requests.csv"
    elif export_type == "broadcast_logs":
        rows = db.fetchall("SELECT * FROM broadcast_logs ORDER BY id")
        filename = "broadcast_logs.csv"
    else:
        await query.message.reply_text("Invalid export.")
        return

    output = io.StringIO()
    writer = csv.writer(output)

    if rows:
        writer.writerow(rows[0].keys())
        for row in rows:
            writer.writerow([row[key] for key in row.keys()])
    else:
        writer.writerow(["No records"])

    data = io.BytesIO(output.getvalue().encode("utf-8-sig"))
    data.name = filename

    await query.message.reply_document(
        document=InputFile(data, filename=filename),
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
        logger.warning("Telegram rate limit: %s seconds", error.retry_after)
        return

    if isinstance(error, Forbidden):
        logger.warning("Telegram forbidden error: %s", error)
        return

    if isinstance(error, (NetworkError, TimedOut)):
        logger.warning("Telegram network error: %s", error)
        return

    logger.exception("Unhandled application error", exc_info=error)

    try:
        db.log_error("EXCEPTION", "application", "global_error", repr(error))
    except Exception:
        logger.exception("Could not save global error")


# ============================================================
# STARTUP / SHUTDOWN
# ============================================================

async def post_init(application: Application):
    try:
        db.connect()

        integrity = db.fetchone("PRAGMA integrity_check")

        if not integrity or integrity[0] != "ok":
            raise RuntimeError(f"SQLite integrity check failed: {integrity}")

        me = await application.bot.get_me()
        logger.info("Connected as @%s (%s)", me.username, me.id)
        logger.info(
            "Custom emoji support uses Telegram MessageEntity(custom_emoji); "
            "bot-owner Premium is required by Bot API for direct custom emoji sending."
        )

        # Only /start in the menu — clean, no extra commands shown to users
        await application.bot.set_my_commands(
            [
                BotCommand("start", "Start"),
            ]
        )

        # Resume broadcasts that were interrupted by a Render restart/deploy.
        pending = db.fetchall(
            "SELECT id FROM broadcasts WHERE status IN ('running','paused') ORDER BY id"
        )
        for row in pending:
            db.execute(
                "UPDATE broadcasts SET status='running' WHERE id=?",
                (row["id"],),
                commit=True,
            )
            application.create_task(run_broadcast(application, int(row["id"])))

        db.log_event(
            "startup",
            details=f"bot_id={me.id};username={me.username}",
        )

        # Start the disaster-recovery worker inside the same process. This does
        # not require a paid Render Background Worker.
        global AUTO_BACKUP_TASK
        AUTO_BACKUP_TASK = application.create_task(
            automatic_backup_loop(application)
        )

        logger.info("Database initialized: %s", DB_PATH.resolve())

    except Exception:
        logger.exception("Startup validation failed")
        raise


async def post_shutdown(application: Application):
    global AUTO_BACKUP_TASK

    if AUTO_BACKUP_TASK:
        try:
            AUTO_BACKUP_TASK.cancel()
        except Exception:
            pass
        AUTO_BACKUP_TASK = None

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

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(CommandHandler("broadcast_confirm", broadcast_confirm))

    application.add_handler(ChatJoinRequestHandler(handle_join_request))

    # btn_add_style: must be before the generic admin_callback so it catches first
    application.add_handler(
        CallbackQueryHandler(btn_add_style_callback, pattern=r"^btn_add_style:")
    )
    application.add_handler(CallbackQueryHandler(admin_callback))

    # One private-message router keeps admin workflows working while normal
    # users receive the configured join message/button on any message.
    application.add_handler(
        MessageHandler(
            (
                filters.ChatType.PRIVATE
                & (
                    filters.TEXT
                    | filters.PHOTO
                    | filters.VIDEO
                    | filters.ANIMATION
                    | filters.AUDIO
                    | filters.VOICE
                    | filters.Document.ALL
                )
                & ~filters.COMMAND
            ),
            private_message_router,
        )
    )

    application.add_error_handler(global_error_handler)

    return application


def main():
    logger.info("Initializing database at: %s", DB_PATH.resolve())
    db.connect()

    application = build_application()

    allowed_updates = [
        "message",
        "callback_query",
        "chat_join_request",
    ]

    # Render web services expose RENDER_EXTERNAL_URL automatically. When it is
    # present, use Telegram webhooks instead of getUpdates polling. This is
    # important for Render because Free web services can spin down while idle;
    # Telegram webhook traffic can wake the service back up. Local/non-Render
    # runs continue to use polling.
    if RENDER_EXTERNAL_URL:
        webhook_url = f"{RENDER_EXTERNAL_URL}/{WEBHOOK_PATH}"
        logger.info("Starting Telegram webhook on 0.0.0.0:%s", RENDER_PORT)
        logger.info("Webhook URL: %s", webhook_url)
        application.run_webhook(
            listen="0.0.0.0",
            port=RENDER_PORT,
            url_path=WEBHOOK_PATH,
            webhook_url=webhook_url,
            allowed_updates=allowed_updates,
            drop_pending_updates=False,
            secret_token=WEBHOOK_SECRET or None,
        )
    else:
        logger.info("Starting Telegram polling...")
        application.run_polling(
            allowed_updates=allowed_updates,
            drop_pending_updates=False,
        )


if __name__ == "__main__":
    main()
