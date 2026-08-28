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

MAX_RETRIES = 3
BROADCAST_DELAY = 0.08
MAX_BUTTONS = 100
MAX_TEXT_LENGTH = 4096

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

        # Add source_chat_id / source_message_id to broadcasts if upgrading
        for col in ("source_chat_id INTEGER", "source_message_id INTEGER"):
            col_name = col.split()[0]
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
    ):
        self.execute(
            """
            INSERT INTO message_buttons(
                message_id,text,url,style,row_number,position,enabled
            )
            VALUES(?,?,?,?,?,?,1)
            """,
            (
                message_id,
                text,
                url,
                style if style in BUTTON_STYLES else "primary",
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

def _make_inline_button(
    text: str,
    url: str,
    style: Optional[str] = None,
) -> InlineKeyboardButton:
    """Create a Telegram inline URL button using the Bot API `style` field."""
    return InlineKeyboardButton(
        text=text[:64],
        url=url,
        style=normalize_button_style(style),
    )


def _make_callback_button(
    text: str,
    callback_data: str,
    style: Optional[str] = None,
) -> InlineKeyboardButton:
    """Create a styled Telegram callback button."""
    return InlineKeyboardButton(
        text=text[:64],
        callback_data=callback_data,
        style=normalize_button_style(style),
    )


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

        style = str(button.get("style", "primary")).strip()
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
                _make_inline_button(text, url, style),
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
    """Serialize Telegram MessageEntity objects for persistent storage."""
    result = []
    for entity in entities or ():
        data = {
            "type": entity.type,
            "offset": int(entity.offset),
            "length": int(entity.length),
        }

        if entity.url:
            data["url"] = entity.url
        if entity.language:
            data["language"] = entity.language
        if entity.custom_emoji_id:
            data["custom_emoji_id"] = entity.custom_emoji_id
        if getattr(entity, "date_time_format", None):
            data["date_time_format"] = entity.date_time_format
        if getattr(entity, "user", None):
            try:
                data["user"] = entity.user.to_dict()
            except Exception:
                pass

        # DATE_TIME is uncommon, but keep its unix timestamp when available.
        unix_time = getattr(entity, "unix_time", None)
        if unix_time is not None:
            try:
                data["unix_time"] = unix_time.isoformat()
            except AttributeError:
                pass

        result.append(data)

    return json.dumps(result, ensure_ascii=False)


def deserialize_message_entities(value: str, bot=None):
    """Restore MessageEntity objects from stored JSON."""
    raw = parse_json(value, [])
    if not isinstance(raw, list):
        return []

    entities = []
    for data in raw:
        if not isinstance(data, dict):
            continue
        item = dict(data)

        # PTB can reconstruct nested User objects through de_json.
        try:
            entities.append(MessageEntity.de_json(item, bot))
        except Exception:
            try:
                allowed = {
                    key: item[key]
                    for key in (
                        "type",
                        "offset",
                        "length",
                        "url",
                        "language",
                        "custom_emoji_id",
                        "date_time_format",
                    )
                    if key in item
                }
                entities.append(MessageEntity(**allowed))
            except Exception:
                continue

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
            shifted.append(
                MessageEntity(
                    type=entity.type,
                    offset=new_start,
                    length=new_end - new_start,
                    url=entity.url,
                    user=entity.user,
                    language=entity.language,
                    custom_emoji_id=entity.custom_emoji_id,
                    date_time_format=getattr(entity, "date_time_format", None),
                    unix_time=getattr(entity, "unix_time", None),
                )
            )
        except TypeError:
            shifted.append(
                MessageEntity(
                    type=entity.type,
                    offset=new_start,
                    length=new_end - new_start,
                    url=entity.url,
                    user=entity.user,
                    language=entity.language,
                    custom_emoji_id=entity.custom_emoji_id,
                )
            )

    return rendered, shifted


# ============================================================
# TELEGRAM SEND HELPERS
# Premium emoji fix: use copy_message when we have a saved source so that
# Telegram-side entities (including custom_emoji) are preserved verbatim.
# For the join-request message we store the admin's original message_id and
# chat_id so we can copy_message instead of re-sending the text.
# ============================================================

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
                "style": normalize_button_style(
                    row["style"] if "style" in row.keys() else "primary"
                ),
                "row": row["row_number"],
                "position": row["position"],
            }
        )

    keyboard = build_keyboard(buttons)

    # If an admin supplied a message containing custom/premium emoji, store
    # Telegram's original entities and reuse them directly. This avoids losing
    # custom emoji while still allowing {Username} to be personalized.
    stored_entities = deserialize_message_entities(
        db.get_setting("join_msg_source_entities", "[]"),
        bot,
    )

    rendered_caption, rendered_entities = render_template_with_entities(
        caption,
        stored_entities,
        user,
    )

    has_template = any(token in caption for token in USERNAME_PLACEHOLDERS)
    source_chat = safe_int(db.get_setting("join_msg_source_chat", "0"), 0)
    source_msg = safe_int(db.get_setting("join_msg_source_id", "0"), 0)

    for attempt in range(MAX_RETRIES):
        try:
            # When there is no per-user template, copy_message remains the best
            # fallback because Telegram preserves every original entity exactly.
            # With a template, send the stored entities after substitution.
            if source_chat and source_msg and not has_template and not stored_entities:
                try:
                    return await bot.copy_message(
                        chat_id=chat_id,
                        from_chat_id=source_chat,
                        message_id=source_msg,
                        reply_markup=keyboard,
                    )
                except TelegramError:
                    db.set_setting("join_msg_source_chat", "0")
                    db.set_setting("join_msg_source_id", "0")

            if media_type == "photo" and file_id:
                kwargs = {
                    "chat_id": chat_id,
                    "photo": file_id,
                    "caption": (rendered_caption or "")[:1024] or None,
                    "reply_markup": keyboard,
                }
                if rendered_entities:
                    kwargs["caption_entities"] = rendered_entities
                else:
                    kwargs["parse_mode"] = parse_mode
                return await bot.send_photo(**kwargs)

            if media_type == "document" and file_id:
                kwargs = {
                    "chat_id": chat_id,
                    "document": file_id,
                    "caption": (rendered_caption or "")[:1024] or None,
                    "reply_markup": keyboard,
                }
                if rendered_entities:
                    kwargs["caption_entities"] = rendered_entities
                else:
                    kwargs["parse_mode"] = parse_mode
                return await bot.send_document(**kwargs)

            text = rendered_caption or " "
            kwargs = {
                "chat_id": chat_id,
                "text": text[:MAX_TEXT_LENGTH],
                "reply_markup": keyboard,
                "disable_web_page_preview": True,
            }
            if rendered_entities:
                kwargs["entities"] = rendered_entities
            else:
                kwargs["parse_mode"] = parse_mode

            return await bot.send_message(**kwargs)

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

        if db.get_setting("auto_message_enabled", "1") != "1":
            db.update_join_request(row_id, sent=False, status="disabled")
            return

        try:
            await send_configured_message(context.bot, user.id, user)
            db.update_join_request(row_id, sent=True, status="sent")
            db.log_event("join_request_message_sent", user.id, chat.id)

        except Forbidden as exc:
            error = clean_error(exc)
            db.execute(
                "UPDATE users SET is_blocked=1 WHERE user_id=?",
                (user.id,),
                commit=True,
            )
            db.update_join_request(row_id, sent=False, status="blocked", error=error)
            db.log_error("WARNING", "join_request", "forbidden", error)

        except Exception as exc:
            error = clean_error(exc)
            db.update_join_request(row_id, sent=False, status="failed", error=error)
            db.log_error("ERROR", "join_request", "send_failed", error)

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
            "Only enabled/configured channels are processed."
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
        s = b["style"] if "style" in b.keys() else "primary"
        btn_lines.append(f"  [{s}] {b['text']} → {b['url']}")

    btn_preview = "\n".join(btn_lines) if btn_lines else "None"

    await query.edit_message_text(
        (
            "💬 MESSAGE BUILDER\n\n"
            f"Media: {media}\n"
            f"Parse Mode: {parse_mode}\n"
            f"Buttons: {len(buttons)}\n"
            f"{btn_preview}\n\n"
            f"Caption:\n{caption[:800]}"
        ),
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    _make_callback_button("📝 Caption", "set_caption", "primary"),
                    _make_callback_button("🔤 Parse", "toggle_parse", "primary"),
                ],
                [
                    _make_callback_button("🖼 Photo", "set_photo", "primary"),
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
            lines.append(
                f"• {title}\n"
                f"  ID: {channel['channel_id']}\n"
                f"  Status: {status}\n"
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
                    "🗑",
                    f"remove_channel:{channel['channel_id']}",
                    "danger",
                ),
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
    await query.edit_message_text(
        (
            "📢 BROADCAST\n\n"
            "Send text or photo (with caption) to all active users.\n"
            "Premium emoji in captions are preserved automatically.\n\n"
            "Handles rate limits and blocked users."
        ),
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    _make_callback_button("➕ New Broadcast", "broadcast_start", "success")
                ],
                [
                    _make_callback_button("⬅️ Back", "admin_home", "primary")
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

    lines.append(
        "\n📥 To restore: send the backup .db file here."
    )

    await query.edit_message_text(
        "\n".join(lines)[:4000],
        reply_markup=InlineKeyboardMarkup(
            [
                [
                    _make_callback_button("💾 Create Backup", "backup_create", "success")
                ],
                [
                    _make_callback_button("⬅️ Back", "admin_home", "primary")
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
                "Send the new caption.\n\n"
                "Tip: If you want premium emoji in messages, send the caption with emoji here.\n"
                "The bot will copy your message exactly so premium emoji are preserved.\n\n"
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
                "UPDATE messages SET media_type='none',file_id='',updated_at=? WHERE id=?",
                (utc_now(), message["id"]),
                commit=True,
            )
            # Clear copy_message source so we don't copy a deleted photo
            db.set_setting("join_msg_source_chat", "0")
            db.set_setting("join_msg_source_id", "0")
            db.set_setting("join_msg_source_entities", "[]")
            await show_message_builder(query)
            return

        # FIXED: Guided button add flow instead of raw JSON
        if data == "add_button":
            context.user_data["awaiting"] = "btn_link"
            await query.message.reply_text(
                "Step 1/3 — Send the channel or group link for this button.\n"
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
            await query.message.reply_text(
                "Send the broadcast message.\n\n"
                "Text, or photo with caption — premium emoji are preserved.\n\n"
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

    # ── Backup restore: admin sends a .db document ──────────────────────────
    if message.document and message.document.file_name and message.document.file_name.endswith(".db"):
        if is_owner(user.id):
            await restore_backup_from_document(message, context)
        else:
            await message.reply_text("Only the owner can restore backups.")
        return

    if not state:
        return

    try:
        # ── Caption ──────────────────────────────────────────────────────────
        if state == "caption":
            text = message.text or message.caption or ""

            if len(text) > MAX_TEXT_LENGTH:
                await message.reply_text("Caption is too long.")
                return

            join_msg = db.get_join_message()

            db.execute(
                "UPDATE messages SET caption=?,updated_at=? WHERE id=?",
                (text, utc_now(), join_msg["id"]),
                commit=True,
            )

            # Store the original Telegram entities so custom/premium emoji and
            # formatting survive while {Username} is replaced per recipient.
            db.set_setting("join_msg_source_chat", str(message.chat_id))
            db.set_setting("join_msg_source_id", str(message.message_id))
            db.set_setting(
                "join_msg_source_entities",
                serialize_message_entities(message.entities or ()),
            )

            context.user_data.pop("awaiting", None)
            await message.reply_text("✅ Caption saved. Premium/custom emoji and formatting saved.", reply_markup=admin_menu())
            return

        # ── Photo ────────────────────────────────────────────────────────────
        if state == "photo":
            if not message.photo:
                await message.reply_text("Please send a photo.")
                return

            if len(message.caption or "") > 1024:
                await message.reply_text("Photo caption is too long. Telegram allows up to 1024 characters.")
                return

            photo = message.photo[-1]
            join_msg = db.get_join_message()

            db.execute(
                "UPDATE messages SET media_type='photo',file_id=?,updated_at=? WHERE id=?",
                (photo.file_id, utc_now(), join_msg["id"]),
                commit=True,
            )

            # If caption came with the photo, save it and source message too
            if message.caption:
                db.execute(
                    "UPDATE messages SET caption=? WHERE id=?",
                    (message.caption, join_msg["id"]),
                    commit=True,
                )
                db.set_setting("join_msg_source_chat", str(message.chat_id))
                db.set_setting("join_msg_source_id", str(message.message_id))
                db.set_setting(
                    "join_msg_source_entities",
                    serialize_message_entities(message.caption_entities or ()),
                )
            else:
                # Clear stale source entities — this photo has no caption.
                db.set_setting("join_msg_source_chat", "0")
                db.set_setting("join_msg_source_id", "0")
                db.set_setting("join_msg_source_entities", "[]")

            context.user_data.pop("awaiting", None)
            await message.reply_text("✅ Photo saved.", reply_markup=admin_menu())
            return

        # ── Guided button flow: Step 1 — link ────────────────────────────────
        if state == "btn_link":
            raw = (message.text or "").strip()

            # Accept @username, t.me/ links, or full https:// links
            if raw.startswith("@"):
                raw = f"https://t.me/{raw.lstrip('@')}"
            elif raw.startswith("t.me/") or raw.startswith("https://t.me/"):
                if not raw.startswith("http"):
                    raw = "https://" + raw

            if not valid_http_url(raw):
                await message.reply_text(
                    "Invalid link. Send a channel link like:\n"
                    "https://t.me/yourchannel\n"
                    "or @yourchannel\n\n"
                    "Use /cancel to cancel."
                )
                return

            context.user_data["btn_pending_url"] = raw
            context.user_data["awaiting"] = "btn_name"
            await message.reply_text(
                "Step 2/3 — What should the button say?\n"
                "Example: Join Now\n\n"
                "Use /cancel to cancel."
            )
            return

        # ── Guided button flow: Step 2 — button name ─────────────────────────
        if state == "btn_name":
            name = (message.text or "").strip()

            if not name:
                await message.reply_text("Button name cannot be empty. Try again.")
                return

            context.user_data["btn_pending_name"] = name[:64]
            context.user_data["awaiting"] = "btn_style_choice"
            await message.reply_text(
                "Step 3/3 — Choose button color style:",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            _make_callback_button("🔵 Primary", "btn_add_style:primary", "primary"),
                            _make_callback_button("🟢 Success", "btn_add_style:success", "success"),
                            _make_callback_button("🔴 Danger", "btn_add_style:danger", "danger"),
                        ]
                    ]
                ),
            )
            return

        # ── Guided button flow: Step 3 — waiting for inline style tap ────────
        # User typed text instead of pressing the style button — remind them.
        if state == "btn_style_choice":
            await message.reply_text(
                "Please tap one of the color buttons above to finish adding the button.\n\n"
                "Use /cancel to abort.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            _make_callback_button("🔵 Primary", "btn_add_style:primary", "primary"),
                            _make_callback_button("🟢 Success", "btn_add_style:success", "success"),
                            _make_callback_button("🔴 Danger", "btn_add_style:danger", "danger"),
                        ]
                    ]
                ),
            )
            return

        # ── Channel add ───────────────────────────────────────────────────────
        if state == "channel":
            raw = (message.text or "").strip()

            # Accept @username or t.me/ link
            if raw.startswith("@"):
                raw = raw.lstrip("@")
            elif "t.me/" in raw:
                raw = raw.split("t.me/")[-1].rstrip("/")

            # Try as username first, fallback to numeric ID
            try:
                try:
                    channel_id_or_username = int(raw)
                except ValueError:
                    channel_id_or_username = f"@{raw}"

                chat = await context.bot.get_chat(channel_id_or_username)

                if chat.type != "channel":
                    await message.reply_text("Please provide a Telegram channel link or ID.")
                    return

                member = await context.bot.get_chat_member(chat.id, context.bot.id)
                status = str(getattr(member, "status", ""))

                if status not in ("administrator", "creator"):
                    await message.reply_text("Bot is not an administrator in this channel.")
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

                context.user_data.pop("awaiting", None)

                conf = (
                    f"✅ Channel configured.\n\n"
                    f"Title: {chat.title or '-'}\n"
                    f"ID: {chat.id}\n"
                )
                if chat.username:
                    conf += f"Username: @{chat.username}"

                await message.reply_text(conf, reply_markup=admin_menu())

            except TelegramError as exc:
                await message.reply_text(
                    f"Could not access the channel.\n\n{clean_error(exc)[:700]}"
                )
            return

        # ── Broadcast ─────────────────────────────────────────────────────────
        if state == "broadcast":
            await create_broadcast(update, context)
            return

    except Exception as exc:
        logger.exception("Admin input failed: %s", state)
        db.log_error("EXCEPTION", "admin_input", state, repr(exc))
        await message.reply_text(
            f"Operation failed safely:\n{clean_error(exc)[:700]}"
        )


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

    style = data.split(":", 1)[1]
    url = context.user_data.pop("btn_pending_url", None)
    name = context.user_data.pop("btn_pending_name", None)
    context.user_data.pop("awaiting", None)

    await query.answer()

    if not url or not name:
        await query.message.reply_text("Button data lost. Start over with Add Button.")
        return

    join_msg = db.get_join_message()
    existing_buttons = db.get_message_buttons(join_msg["id"])
    next_row = len(existing_buttons)

    db.add_message_button(
        join_msg["id"],
        name,
        url,
        row_number=next_row,
        position=0,
        style=style if style in BUTTON_STYLES else "primary",
    )

    await query.message.reply_text(
        f"✅ Button added: [{style.upper()}] {name} → {url}",
        reply_markup=admin_menu(),
    )


# ============================================================
# BROADCAST
# Premium emoji fix: store source_chat_id + source_message_id so
# run_broadcast uses copy_message instead of re-sending text.
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
    source_chat_id = message.chat_id
    source_message_id = message.message_id

    if message.photo:
        media_type = "photo"
        file_id = message.photo[-1].file_id
        caption = message.caption or ""
    else:
        text = message.text or ""
        caption = text

    if not text and not caption:
        await message.reply_text("Broadcast content cannot be empty.")
        return

    if len(text or caption) > MAX_TEXT_LENGTH:
        await message.reply_text("Broadcast text is too long.")
        return

    cursor = db.execute(
        """
        INSERT INTO broadcasts(
            admin_id,text,media_type,file_id,caption,
            parse_mode,source_chat_id,source_message_id,
            buttons_json,status,created_at
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            user.id,
            text,
            media_type,
            file_id,
            caption,
            db.get_join_message()["parse_mode"] or "HTML",
            source_chat_id,
            source_message_id,
            "[]",
            "pending",
            utc_now(),
        ),
        commit=True,
    )

    broadcast_id = cursor.lastrowid
    context.user_data["pending_broadcast_id"] = broadcast_id

    await message.reply_text(
        f"📢 Broadcast #{broadcast_id} created.\n\n"
        "Premium emoji are preserved automatically.\n\n"
        "Reply /broadcast_confirm to send.\n"
        "Or /cancel"
    )


async def broadcast_confirm(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):
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

    row = db.fetchone(
        "SELECT * FROM broadcasts WHERE id=? AND status='pending'",
        (broadcast_id,),
    )

    if not row:
        await update.message.reply_text("Pending broadcast not found.")
        return

    db.execute(
        "UPDATE broadcasts SET status='running',started_at=? WHERE id=?",
        (utc_now(), broadcast_id),
        commit=True,
    )

    context.user_data.pop("pending_broadcast_id", None)

    await update.message.reply_text(f"📢 Broadcast #{broadcast_id} started.")

    context.application.create_task(
        run_broadcast(context.application, broadcast_id)
    )


async def send_broadcast_to_user(bot, row, user_id: int):
    """
    Send a broadcast message preserving premium emoji via copy_message.
    Falls back to send_photo / send_message when copy fails.
    """
    source_chat = safe_int(row["source_chat_id"] if "source_chat_id" in row.keys() else 0, 0)
    source_msg = safe_int(row["source_message_id"] if "source_message_id" in row.keys() else 0, 0)

    # Prefer copy_message — it preserves all entities including premium emoji
    if source_chat and source_msg:
        try:
            return await bot.copy_message(
                chat_id=user_id,
                from_chat_id=source_chat,
                message_id=source_msg,
            )
        except TelegramError:
            pass  # Fall through to direct send

    buttons = parse_json(row["buttons_json"], [])
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
        text=(row["text"] or row["caption"] or " ")[:MAX_TEXT_LENGTH],
        parse_mode=row["parse_mode"] or None,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )


async def run_broadcast(application: Application, broadcast_id: int):
    try:
        row = db.fetchone(
            "SELECT * FROM broadcasts WHERE id=?",
            (broadcast_id,),
        )

        if not row:
            return

        users = db.fetchall(
            "SELECT user_id FROM users WHERE is_blocked=0 ORDER BY user_id"
        )

        total = len(users)

        db.execute(
            "UPDATE broadcasts SET total=? WHERE id=?",
            (total, broadcast_id),
            commit=True,
        )

        sent = 0
        failed = 0
        blocked = 0

        for item in users:
            user_id = item["user_id"]

            existing = db.fetchone(
                """
                SELECT status FROM broadcast_logs
                WHERE broadcast_id=? AND user_id=?
                """,
                (broadcast_id, user_id),
            )

            if existing and existing["status"] in ("sent", "blocked"):
                continue

            try:
                await send_broadcast_to_user(application.bot, row, user_id)
                sent += 1

                db.execute(
                    """
                    INSERT INTO broadcast_logs(broadcast_id,user_id,status,error,created_at)
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(broadcast_id,user_id) DO UPDATE SET
                        status=excluded.status,error=excluded.error,
                        created_at=excluded.created_at
                    """,
                    (broadcast_id, user_id, "sent", None, utc_now()),
                    commit=True,
                )

            except Forbidden as exc:
                blocked += 1
                db.execute(
                    "UPDATE users SET is_blocked=1 WHERE user_id=?",
                    (user_id,),
                    commit=True,
                )
                db.execute(
                    """
                    INSERT INTO broadcast_logs(broadcast_id,user_id,status,error,created_at)
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(broadcast_id,user_id) DO UPDATE SET
                        status=excluded.status,error=excluded.error,
                        created_at=excluded.created_at
                    """,
                    (broadcast_id, user_id, "blocked", clean_error(exc), utc_now()),
                    commit=True,
                )

            except RetryAfter as exc:
                try:
                    await asyncio.sleep(float(exc.retry_after) + 1)
                    await send_broadcast_to_user(application.bot, row, user_id)
                    sent += 1
                except Forbidden as retry_exc:
                    blocked += 1
                    db.execute(
                        "UPDATE users SET is_blocked=1 WHERE user_id=?",
                        (user_id,),
                        commit=True,
                    )
                    db.log_error("WARNING", "broadcast", "retry_forbidden", repr(retry_exc))
                except Exception as retry_exc:
                    failed += 1
                    db.log_error("WARNING", "broadcast", "retry_failed", repr(retry_exc))

            except (NetworkError, TimedOut) as exc:
                failed += 1
                db.execute(
                    """
                    INSERT INTO broadcast_logs(broadcast_id,user_id,status,error,created_at)
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(broadcast_id,user_id) DO UPDATE SET
                        status=excluded.status,error=excluded.error,
                        created_at=excluded.created_at
                    """,
                    (broadcast_id, user_id, "failed", clean_error(exc), utc_now()),
                    commit=True,
                )

            except Exception as exc:
                failed += 1
                db.execute(
                    """
                    INSERT INTO broadcast_logs(broadcast_id,user_id,status,error,created_at)
                    VALUES(?,?,?,?,?)
                    ON CONFLICT(broadcast_id,user_id) DO UPDATE SET
                        status=excluded.status,error=excluded.error,
                        created_at=excluded.created_at
                    """,
                    (broadcast_id, user_id, "failed", clean_error(exc), utc_now()),
                    commit=True,
                )

            db.execute(
                """
                UPDATE broadcasts
                SET sent=?,failed=?,blocked=?,next_user_id=?
                WHERE id=?
                """,
                (sent, failed, blocked, user_id, broadcast_id),
                commit=True,
            )

            await asyncio.sleep(BROADCAST_DELAY)

        db.execute(
            """
            UPDATE broadcasts
            SET status='completed',sent=?,failed=?,blocked=?,
                next_user_id=NULL,finished_at=?
            WHERE id=?
            """,
            (sent, failed, blocked, utc_now(), broadcast_id),
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
        logger.exception("Broadcast crashed safely")
        db.execute(
            "UPDATE broadcasts SET status='paused' WHERE id=? AND status='running'",
            (broadcast_id,),
            commit=True,
        )
        db.log_error("EXCEPTION", "broadcast", "worker", repr(exc))


# ============================================================
# BACKUP
# ============================================================

async def create_backup(query):
    if not DB_PATH.exists():
        await query.message.reply_text("Database file does not exist.")
        return

    filename = (
        "backup_"
        + datetime.now().strftime("%Y_%m_%d_%H%M%S")
        + ".db"
    )

    destination = BACKUP_DIR / filename
    source = None
    target = None

    try:
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
            (filename, utc_now(), query.from_user.id, size),
            commit=True,
        )

        with destination.open("rb") as file:
            await query.message.reply_document(
                document=InputFile(file, filename=filename),
                caption=(
                    "💾 Backup created successfully.\n"
                    f"Size: {size:,} bytes\n\n"
                    "To restore: send this .db file to me."
                ),
            )

    except Exception as exc:
        logger.exception("Backup creation failed")
        db.log_error("ERROR", "backup", "create", repr(exc))
        await query.message.reply_text(f"Backup failed:\n{clean_error(exc)[:700]}")

    finally:
        if source:
            source.close()
        if target:
            target.close()


async def restore_backup_from_document(message, context):
    """
    Owner sends a .db backup file → validate and restore it.
    The bot closes the current connection, overwrites the DB file,
    and reconnects.
    """
    try:
        await message.reply_text("⏳ Validating backup file...")

        tg_file = await context.bot.get_file(message.document.file_id)
        tmp_path = BACKUP_DIR / f"restore_tmp_{datetime.now().strftime('%Y%m%d%H%M%S')}.db"

        await tg_file.download_to_drive(str(tmp_path))

        # Validate the uploaded file
        try:
            check_conn = sqlite3.connect(str(tmp_path), timeout=10)
            integrity = check_conn.execute("PRAGMA integrity_check").fetchone()[0]
            check_conn.close()
        except Exception as exc:
            tmp_path.unlink(missing_ok=True)
            await message.reply_text(f"❌ Invalid backup file:\n{clean_error(exc)[:500]}")
            return

        if integrity != "ok":
            tmp_path.unlink(missing_ok=True)
            await message.reply_text(f"❌ Backup integrity check failed: {integrity}")
            return

        # Close current DB connection
        db.close()

        # Copy the validated backup over the live DB
        shutil.copy2(str(tmp_path), str(DB_PATH))
        tmp_path.unlink(missing_ok=True)

        # Reconnect
        db.connect()

        await message.reply_text(
            "✅ Database restored successfully.\n\n"
            "The bot is back online with the restored data.",
            reply_markup=admin_menu(),
        )

    except Exception as exc:
        logger.exception("Backup restore failed")
        try:
            db.connect()  # Make sure we're reconnected even on failure
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

        # Only /start in the menu — clean, no extra commands shown to users
        await application.bot.set_my_commands(
            [
                BotCommand("start", "Start"),
            ]
        )

        db.log_event(
            "startup",
            details=f"bot_id={me.id};username={me.username}",
        )

        logger.info("Database initialized: %s", DB_PATH.resolve())

    except Exception:
        logger.exception("Startup validation failed")
        raise


async def post_shutdown(application: Application):
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

    application.add_handler(
        MessageHandler(
            (
                filters.ChatType.PRIVATE
                & (filters.TEXT | filters.PHOTO | filters.Document.ALL)
                & ~filters.COMMAND
            ),
            admin_input,
        )
    )

    application.add_error_handler(global_error_handler)

    return application


def main():
    logger.info("Initializing database at: %s", DB_PATH.resolve())
    db.connect()

    application = build_application()

    logger.info("Starting Telegram polling...")

    # Render / cloud hosting fix: drop close_loop kwarg (removed in PTB 20.x)
    # run_polling handles the event loop internally — don't pass close_loop.
    application.run_polling(
        allowed_updates=[
            "message",
            "callback_query",
            "chat_join_request",
        ],
        drop_pending_updates=False,
    )


if __name__ == "__main__":
    main()
