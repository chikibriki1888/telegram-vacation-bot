# Telegram Vacation Bot (Teams, Roles, No external API)
# python-telegram-bot v20+ (async) + aiosqlite
# -----------------------------------------------------------------------------
# Что внутри:
# - Регистрация новой команды: /start -> введите название команды -> выберите роль (СЕО/Овнер/Тимлид/Технарь)
# - Мастер настроек: создать типы отпусков (пошагово), политика пересечений, запрет дат (диапазон + примечание), добавление сотрудников
# - Командная изоляция: все данные по team_id
# - Роли: ADMIN_ROLES = {CEO, OWNER, TIMLID, TECH}; USER_ROLES = {BAER, DESIGNER, FARMER, MANAGER, BUHGALTER}
# - Подача заявки: быстрые диапазоны или вручную; "Пропустить комментарий" работает, подтверждение кнопками
# - Русские статусы
# - Админка:
#   * Добавить/удалить сотрудника (username -> выбор роли кнопками)
#   * Типы отпусков (добавить/изменить/удалить) + пошаговое добавление
#   * Запрет дат (день/диапазон) + снятие запрета
#   * Лимиты (на год/за один отпуск)
#   * Политика пересечений (всем/запрет всем/запрет одинаковым ролям)
#   * Заявки на рассмотрении (инлайн approve/reject)
#   * Список сотрудников с остатками
#   * Экспорт CSV (текущий год)
#   * Лёгкая аналитика (текущий год)
# - Напоминания: за 3 дня и за 1 день до начала отпуска (JobQueue)
# -----------------------------------------------------------------------------

import os
import io
import csv
import logging
import asyncio
from telegram.error import BadRequest
from datetime import datetime, date, timedelta
from typing import Optional, List, Tuple

import aiosqlite
from telegram import __version__ as ptb_version
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    filters,
)

# ====== CONFIG ======
TELEGRAM_TOKEN = "ТОКЕН"
DB_FILE = os.getenv('VACATION_BOT_DB') or 'vacation_bot.db'

# ====== ROLES / UI ======
# Внутренние ключи ролей (англ), отображаем русские ярлыки в интерфейсе.
ADMIN_ROLES = {'CEO', 'OWNER', 'TIMLID', 'TECH'}
USER_ROLES = {'BAER', 'DESIGNER', 'FARMER', 'MANAGER', 'BUHGALTER'}
ALL_ROLES = ADMIN_ROLES | USER_ROLES

ROLE_RU = {
    'CEO': 'СЕО',
    'OWNER': 'Овнер',
    'TIMLID': 'Тимлид',
    'TECH': 'Технарь',
    'BAER': 'Баер',
    'DESIGNER': 'Дизайнер',
    'FARMER': 'Фармер',
    'MANAGER': 'Менеджер',
    'BUHGALTER': 'Бухгалтер',
}

STATUS_HUMAN = {
    'pending': 'На рассмотрении',
    'approved': 'Подтверждена',
    'rejected': 'Отклонена',
    'cancelled': 'Отменена'
}

E_APPLY = "🗓️"
E_MY    = "📅"
E_ALL   = "👥"
E_TYPES = "🧾"
E_ADMIN = "🛠️"
E_HELP  = "❓"
E_HOME  = "🏠"
E_BACK  = "◀️"

# Политика пересечений внутри команды
# allow_all | deny_all | deny_same_role
DEFAULT_OVERLAP = "allow_all"

# Предзаполненные админы (плейсхолдеры, пока не напишут боту)
PRESET_ADMINS = {
    '1': 'TECH',
    '2': 'TIMLID',
    '3': 'CEO',
}

# ====== LOGGING ======
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ====== STATES ======
# Регистрация
REG_TEAM_NAME = "REG_TEAM_NAME"
REG_ROLE      = "REG_ROLE"
REG_TYPES_Q   = "REG_TYPES_Q"
REG_OVERLAP_Q = "REG_OVERLAP_Q"
REG_FORBIDDEN_Q = "REG_FORBIDDEN_Q"
REG_ADD_USERS_Q = "REG_ADD_USERS_Q"

# Пошаговое добавление типа (во время регистрации И в админке — переиспользуем)
TYPES_ADD_NAME = "TYPES_ADD_NAME"
TYPES_ADD_DAYS = "TYPES_ADD_DAYS"
TYPES_ADD_DESC = "TYPES_ADD_DESC"

# Подача заявки
APPLY_TYPE    = "APPLY_TYPE"
APPLY_START   = "APPLY_START"
APPLY_END     = "APPLY_END"
APPLY_COMMENT = "APPLY_COMMENT"
APPLY_CONFIRM = "APPLY_CONFIRM"

# Bootstrap мастер (быстрый)
BOOT_MAX_YEAR   = "BOOT_MAX_YEAR"
BOOT_MAX_SINGLE = "BOOT_MAX_SINGLE"
BOOT_OVERLAP    = "BOOT_OVERLAP"

# ====== MIGRATION / DB INIT ======
async def _column_exists(db, table: str, column: str) -> bool:
    cur = await db.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in await cur.fetchall()]
    return column in cols

async def _add_col_if_missing(db, table: str, column: str, ddl: str):
    if not await _column_exists(db, table, column):
        await db.execute(ddl)

async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('PRAGMA foreign_keys = ON;')

        # базовые таблицы (с team_id уже в схеме)
        await db.executescript("""
        CREATE TABLE IF NOT EXISTS teams(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER UNIQUE,
            username TEXT,
            full_name TEXT,
            role TEXT,
            team_id INTEGER REFERENCES teams(id),
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS vacation_types (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER REFERENCES teams(id),
            name TEXT,
            days_per_year INTEGER,
            description TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS vacations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER,
            user_id INTEGER,
            type_id INTEGER,
            start_date TEXT,
            end_date TEXT,
            days INTEGER,
            status TEXT DEFAULT 'pending',
            admin_comment TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS forbidden_dates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            team_id INTEGER,
            date TEXT,
            note TEXT
        );

        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS admin_actions (
            admin_tg_id INTEGER PRIMARY KEY,
            vacation_id INTEGER,
            action TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_tg_id INTEGER,
            action TEXT,
            target TEXT,
            data TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """)
        await db.commit()

        # Миграция старых баз (добавить team_id где нет)
        await _add_col_if_missing(db, "users", "team_id",
            "ALTER TABLE users ADD COLUMN team_id INTEGER REFERENCES teams(id)")
        await _add_col_if_missing(db, "vacation_types", "team_id",
            "ALTER TABLE vacation_types ADD COLUMN team_id INTEGER REFERENCES teams(id)")
        await _add_col_if_missing(db, "vacations", "team_id",
            "ALTER TABLE vacations ADD COLUMN team_id INTEGER")
        await _add_col_if_missing(db, "forbidden_dates", "team_id",
            "ALTER TABLE forbidden_dates ADD COLUMN team_id INTEGER")

        # Default Team
        await db.execute("INSERT OR IGNORE INTO teams(name) VALUES('Default Team')")
        cur = await db.execute("SELECT id FROM teams WHERE name='Default Team'")
        default_team_id = (await cur.fetchone())[0]

        # Бэкофилл team_id где NULL
        await db.execute("UPDATE users SET team_id=? WHERE team_id IS NULL", (default_team_id,))
        await db.execute("UPDATE vacation_types SET team_id=? WHERE team_id IS NULL", (default_team_id,))
        await db.execute("""
            UPDATE vacations
            SET team_id = (SELECT u.team_id FROM users u WHERE u.id = vacations.user_id)
            WHERE team_id IS NULL
        """)
        await db.execute("UPDATE forbidden_dates SET team_id=? WHERE team_id IS NULL", (default_team_id,))
        await db.commit()

        # Настройки по умолчанию (per-team, ключи вида key:team_id)
        def_key_year = f"max_year_days:{default_team_id}"
        def_key_single = f"max_single_days:{default_team_id}"
        def_overlap = f"overlap_policy:{default_team_id}"
        await db.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (def_key_year, '28'))
        await db.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (def_key_single, '14'))
        await db.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (def_overlap, DEFAULT_OVERLAP))

        # Типы по умолчанию для Default Team
        cur = await db.execute("SELECT 1 FROM vacation_types WHERE team_id=? LIMIT 1", (default_team_id,))
        if not await cur.fetchone():
            await db.execute(
                "INSERT INTO vacation_types(team_id, name, days_per_year, description) VALUES(?,?,?,?)",
                (default_team_id, 'Оплачиваемый отпуск', 28, 'Календарные дни в году')
            )
            await db.execute(
                "INSERT INTO vacation_types(team_id, name, days_per_year, description) VALUES(?,?,?,?)",
                (default_team_id, 'Неоплачиваемый отпуск', 30, 'Неоплачиваемый (макс. дней в году)')
            )

        # Пресет-админы (плейсхолдеры)
        for uname, role in PRESET_ADMINS.items():
            cur = await db.execute('SELECT id FROM users WHERE username=?', (uname,))
            if not await cur.fetchone():
                await db.execute(
                    "INSERT INTO users(tg_id, username, full_name, role, team_id) VALUES(?,?,?,?,?)",
                    (None, uname, uname, role, default_team_id)
                )

        await db.commit()

# ====== SETTINGS HELPERS ======

# --- inline calendar helpers ---
import calendar
RU_MONTHS = ["", "Январь","Февраль","Март","Апрель","Май","Июнь","Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"]
RU_WEEKDAYS = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]

def _prev_month(y, m):
    return (y-1, 12) if m == 1 else (y, m-1)

def _next_month(y, m):
    return (y+1, 1) if m == 12 else (y, m+1)

def build_invite_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Выйти из команды", callback_data="invite:leave")],
        [InlineKeyboardButton("Отклонить приглашение", callback_data="invite:decline")]
    ])

def build_calendar(year: int, month: int, scope: str, phase: str) -> InlineKeyboardMarkup:
    """
    scope: 'apply' | 'forbid'
    phase: 'start' | 'end'
    """
    cal = calendar.Calendar(firstweekday=0)  # Пн
    weeks = cal.monthdatescalendar(year, month)

    py, pm = _prev_month(year, month)
    ny, nm = _next_month(year, month)

    rows = []
    rows.append([
        InlineKeyboardButton(f"« {RU_MONTHS[pm]} {py}", callback_data=f"calnav:{scope}:{phase}:{py}-{pm:02d}"),
        InlineKeyboardButton(f"{RU_MONTHS[month]} {year}", callback_data="noop"),
        InlineKeyboardButton(f"{RU_MONTHS[nm]} {ny} »", callback_data=f"calnav:{scope}:{phase}:{ny}-{nm:02d}"),
    ])
    rows.append([InlineKeyboardButton(w, callback_data="noop") for w in RU_WEEKDAYS])

    for week in weeks:
        line = []
        for d in week:
            if d.month != month:
                line.append(InlineKeyboardButton(" ", callback_data="noop"))
            else:
                line.append(InlineKeyboardButton(
                    str(d.day),
                    callback_data=f"calpick:{scope}:{phase}:{d.isoformat()}"
                ))
        rows.append(line)

    return InlineKeyboardMarkup(rows)

async def noop_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # глушим клики по заголовку/пустым ячейкам
    await update.callback_query.answer()


async def get_setting(key: str) -> Optional[str]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute('SELECT value FROM settings WHERE key=?', (key,))
        row = await cur.fetchone()
        return row[0] if row else None

async def set_setting(key: str, value: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT OR REPLACE INTO settings(key,value) VALUES(?,?)", (key, value))
        await db.commit()

async def get_team_setting(team_id: int, short_key: str, default: Optional[str] = None) -> str:
    val = await get_setting(f"{short_key}:{team_id}")
    return val if val is not None else (default if default is not None else "")

async def set_team_setting(team_id: int, short_key: str, value: str):
    await set_setting(f"{short_key}:{team_id}", value)

# ====== COMMON HELPERS ======
def is_admin_role(role: str) -> bool:
    return role in ADMIN_ROLES

def role_ru(role: str) -> str:
    return ROLE_RU.get(role, role)

def next_monday_from(d: date) -> date:
    ahead = (7 - d.weekday()) % 7
    if ahead == 0:
        ahead = 7
    return d + timedelta(days=ahead)

def nav_home_back() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(f"{E_BACK} Назад", callback_data="menu:home"),
            InlineKeyboardButton(f"{E_HOME} Домой", callback_data="menu:home"),
        ]
    ])

async def admin_show_menu(q, context, extra_text: str | None = None, extra_markup=None, parse_mode=None):
    desired_text = f"{E_ADMIN} Админка — выберите действие:"
    try:
        await q.edit_message_text(
            desired_text,
            reply_markup=build_admin_menu_markup()
        )
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            raise
    if extra_text or extra_markup:
        await context.bot.send_message(
            chat_id=q.from_user.id,
            text=extra_text or " ",
            reply_markup=extra_markup,
            parse_mode=parse_mode,
        )


async def log_action(actor_tg_id: int, action: str, target: str = '', data: str = ''):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            'INSERT INTO audit_logs(actor_tg_id, action, target, data) VALUES(?,?,?,?)',
            (actor_tg_id, action, target, data)
        )
        await db.commit()

# ====== USERS / TEAMS ======
async def get_team_by_name(name: str) -> Optional[dict]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute('SELECT id, name FROM teams WHERE name=?', (name,))
        row = await cur.fetchone()
        if row:
            return {'id': row[0], 'name': row[1]}
        return None
    
async def get_team_name(team_id: Optional[int]) -> str:
    if not team_id:
        return "-"
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT name FROM teams WHERE id=?", (team_id,))
        row = await cur.fetchone()
    return row[0] if row else "-"

async def create_team(name: str) -> int:
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT INTO teams(name) VALUES(?)", (name,))
        await db.commit()
        cur = await db.execute("SELECT id FROM teams WHERE name=?", (name,))
        return (await cur.fetchone())[0]

async def get_user_by_tg_id(tg_id: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            'SELECT id, tg_id, username, full_name, role, team_id FROM users WHERE tg_id=?',
            (tg_id,)
        )
        row = await cur.fetchone()
        if row:
            return dict(id=row[0], tg_id=row[1], username=row[2], full_name=row[3], role=row[4], team_id=row[5])
        return None

async def get_user_by_username(username: str, team_id: Optional[int] = None) -> Optional[dict]:
    if not username:
        return None
    username = username.lstrip('@')
    async with aiosqlite.connect(DB_FILE) as db:
        if team_id is None:
            cur = await db.execute(
                'SELECT id, tg_id, username, full_name, role, team_id '
                'FROM users WHERE username=? ORDER BY id DESC LIMIT 1',
                (username,)
            )
        else:
            cur = await db.execute(
                'SELECT id, tg_id, username, full_name, role, team_id FROM users WHERE username=? AND team_id=?',
                (username, team_id)
            )
        row = await cur.fetchone()
        if row:
            return dict(id=row[0], tg_id=row[1], username=row[2], full_name=row[3], role=row[4], team_id=row[5])
        return None

async def ensure_user_record(tg_user) -> dict:
    # Создать/обновить запись пользователя
    existing = await get_user_by_tg_id(tg_user.id)
    if existing:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                'UPDATE users SET username=?, full_name=? WHERE id=?',
                (tg_user.username or '', tg_user.full_name or '', existing['id'])
            )
            await db.commit()
        existing['username'] = tg_user.username or ''
        existing['full_name'] = tg_user.full_name or ''
        return existing

    # если есть плейсхолдер по username — привязываем tg_id
    if tg_user.username:
        byname = await get_user_by_username(tg_user.username)
        if byname:
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute(
                    'UPDATE users SET tg_id=?, full_name=? WHERE id=?',
                    (tg_user.id, tg_user.full_name or '', byname['id'])
                )
                await db.commit()
            byname['tg_id'] = tg_user.id
            byname['full_name'] = tg_user.full_name or ''
            return byname

    # иначе создаём запись (на Default Team, роль MANAGER — но сразу начнём регистрацию)
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute("SELECT id FROM teams WHERE name='Default Team'")
        default_team_id = (await cur.fetchone())[0]
        await db.execute(
            'INSERT INTO users(tg_id, username, full_name, role, team_id) VALUES(?,?,?,?,?)',
            (tg_user.id, tg_user.username or '', tg_user.full_name or '', 'MANAGER', default_team_id)
        )
        await db.commit()
        cur = await db.execute('SELECT id, tg_id, username, full_name, role, team_id FROM users WHERE tg_id=?', (tg_user.id,))
        row = await cur.fetchone()
        return dict(id=row[0], tg_id=row[1], username=row[2], full_name=row[3], role=row[4], team_id=row[5])

# ====== TEXT/UI HELPERS ======
async def build_main_menu(role: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(f"{E_APPLY} Подать заявку", callback_data="menu:apply")],
        [
            InlineKeyboardButton(f"{E_MY} Мои отпуска", callback_data="menu:my"),
            InlineKeyboardButton(f"{E_ALL} Отпуска за год", callback_data="menu:all"),
        ],
        [InlineKeyboardButton(f"{E_TYPES} Типы отпусков", callback_data="menu:types")],
        [InlineKeyboardButton(f"{E_HELP} Помощь", callback_data="menu:help")],
    ]
    if is_admin_role(role):
        rows.insert(2, [InlineKeyboardButton(f"{E_ADMIN} Админка", callback_data="menu:admin")])
    return InlineKeyboardMarkup(rows)

async def build_remove_users_view(team_id: int, page: int = 0, per_page: int = 10):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT id, full_name, username, role FROM users WHERE team_id=? ORDER BY full_name COLLATE NOCASE",
            (team_id,)
        )
        rows = await cur.fetchall()

    total = len(rows)
    if total == 0:
        text = "В команде пока нет сотрудников."
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(f"{E_BACK} Назад", callback_data="menu:admin")]])
        return text, kb

    pages = (total + per_page - 1) // per_page
    page = max(0, min(page, pages - 1))
    start, end = page * per_page, page * per_page + per_page
    chunk = rows[start:end]

    lines = [f"Выберите сотрудника для удаления (страница {page+1}/{pages}):"]
    kbl = []
    for uid, fname, uname, role in chunk:
        label = f"🗑 {fname or '(без имени)'} @{uname or ''} — {role_ru(role)}"
        kbl.append([InlineKeyboardButton(label, callback_data=f"admin:remove_pick:{uid}")])

    nav = []
    if pages > 1:
        if page > 0:
            nav.append(InlineKeyboardButton("⟵", callback_data=f"admin:remove_page:{page-1}"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton("⟶", callback_data=f"admin:remove_page:{page+1}"))
    if nav:
        kbl.append(nav)
    kbl.append([InlineKeyboardButton(f"{E_BACK} Назад", callback_data="menu:admin")])

    return "\n".join(lines), InlineKeyboardMarkup(kbl)

async def get_my_vacations_text(user_tg_id: int) -> str:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            'SELECT v.id, t.name, v.start_date, v.end_date, v.days, v.status '
            'FROM vacations v JOIN vacation_types t ON v.type_id=t.id '
            'JOIN users u ON u.id=v.user_id '
            'WHERE u.tg_id=? '
            'ORDER BY v.created_at DESC',
            (user_tg_id,)
        )
        rows = await cur.fetchall()
    if not rows:
        return "У вас нет заявок."
    out = ["Ваши заявки:"]
    for r in rows:
        out.append(f'#{r[0]} {r[1]}: {r[2]} — {r[3]} ({r[4]} дн) — {STATUS_HUMAN.get(r[5], r[5])}')
    return "\n".join(out)

async def get_all_vacations_text(team_id: int, year: int) -> str:
    start = date(year, 1, 1).isoformat()
    end = date(year, 12, 31).isoformat()
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT v.id, u.full_name, t.name, v.start_date, v.end_date, v.status "
            "FROM vacations v "
            "JOIN users u ON v.user_id=u.id "
            "JOIN vacation_types t ON v.type_id=t.id "
            "WHERE v.team_id=? AND v.start_date BETWEEN ? AND ? "
            "ORDER BY v.start_date",
            (team_id, start, end)
        )
        rows = await cur.fetchall()
    if not rows:
        return f'Нет отпусков за {year}.'
    out = [f'Отпуска за {year}:']
    for r in rows:
        out.append(f'#{r[0]} {r[1]} — {r[2]}: {r[3]} — {r[4]} ({STATUS_HUMAN.get(r[5], r[5])})')
    return "\n".join(out)

async def get_types_text_and_list(team_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT id, name, days_per_year, description FROM vacation_types WHERE team_id=? ORDER BY id",
            (team_id,)
        )
        rows = await cur.fetchall()
    if not rows:
        return "Типы не найдены.", []
    out = ['Типы отпусков:']
    for r in rows:
        out.append(f'id:{r[0]} {r[1]} — {r[2]} дней — {r[3]}')
    return "\n".join(out), rows

async def build_year_picker() -> InlineKeyboardMarkup:
    y = datetime.now().year
    rows = [
        [
            InlineKeyboardButton(f"{y-1}", callback_data=f"menu:all:{y-1}"),
            InlineKeyboardButton(f"{y}", callback_data=f"menu:all:{y}"),
            InlineKeyboardButton(f"{y+1}", callback_data=f"menu:all:{y+1}")
        ],
        [InlineKeyboardButton(f"{E_BACK} Назад", callback_data="menu:home")]
    ]
    return InlineKeyboardMarkup(rows)

async def build_apply_type_picker(team_id: int) -> InlineKeyboardMarkup:
    # В ЭТОМ шаге — только список типов. Кнопку "ввести даты вручную" перенесли на следующий шаг.
    _, rows = await get_types_text_and_list(team_id)
    if not rows:
        return InlineKeyboardMarkup([[InlineKeyboardButton(f"{E_BACK} Назад", callback_data="menu:home")]])
    kb = []
    for r in rows:
        tid, name = r[0], r[1]
        kb.append([InlineKeyboardButton(f"{name} (id:{tid})", callback_data=f"menu_apply_type:{tid}")])
    kb.append([InlineKeyboardButton(f"{E_BACK} Назад", callback_data="menu:home")])
    return InlineKeyboardMarkup(kb)

def build_quick_ranges_picker(type_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("7 дней с ближайшего понедельника", callback_data=f"apply_quick:{type_id}:7")],
        [InlineKeyboardButton("14 дней с ближайшего понедельника", callback_data=f"apply_quick:{type_id}:14")],
        [InlineKeyboardButton("✍️ Ввести даты вручную", callback_data="menu:apply_manual")],
        [InlineKeyboardButton(f"{E_BACK} Назад", callback_data="menu:apply")]
    ])

def build_admin_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👤 Добавить сотрудника", callback_data="admin:add_user")],
        [InlineKeyboardButton("🗑 Удалить сотрудника", callback_data="admin:remove_user")],
        [InlineKeyboardButton("🧾 Управление типами отпусков", callback_data="admin:types")],
        [
            InlineKeyboardButton("🚫 Запретить дату/диапазон", callback_data="admin:forbid_date"),
            InlineKeyboardButton("♻️ Разрешить дату", callback_data="admin:unforbid_date")
        ],
        [InlineKeyboardButton("📜 Список запрещённых дат", callback_data="admin:list_forbidden")],
        [
            InlineKeyboardButton("⚙️ Лимит на год", callback_data="admin:set_max_year"),
            InlineKeyboardButton("⚙️ Лимит за раз", callback_data="admin:set_max_single")
        ],
        [InlineKeyboardButton("🔗 Политика пересечений", callback_data="admin:overlap")],
        [InlineKeyboardButton("📬 Заявки на рассмотрении", callback_data="admin:pending")],
        [InlineKeyboardButton("👥 Сотрудники и остатки", callback_data="admin:staff_list")],
        [InlineKeyboardButton("⬇️ Экспорт CSV (год)", callback_data="admin:export_csv")],
        [InlineKeyboardButton("📈 Аналитика (год)", callback_data="admin:analytics")],
        [InlineKeyboardButton(f"{E_BACK} Назад", callback_data="menu:home")]
    ])

def yes_no_kb(prefix: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Да", callback_data=f"{prefix}:yes"),
         InlineKeyboardButton("Нет", callback_data=f"{prefix}:no")]
    ])

def apply_skip_comment_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Пропустить комментарий", callback_data="apply:skip_comment")],
        [InlineKeyboardButton("Отмена", callback_data="apply:cancel")]
    ])

def apply_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Подтвердить", callback_data="apply:confirm"),
            InlineKeyboardButton("Отмена", callback_data="apply:cancel")
        ]
    ])

# ====== BUSINESS RULES ======
def dates_overlap(a_start: date, a_end: date, b_start: date, b_end: date) -> bool:
    return not (a_end < b_start or b_end < a_start)

async def is_range_forbidden(team_id: int, start: date, end: date) -> Optional[str]:
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute('SELECT date FROM forbidden_dates WHERE team_id=?', (team_id,))
        rows = await cur.fetchall()
    forbidden = set()
    for r in rows:
        try:
            forbidden.add(datetime.fromisoformat(r[0]).date())
        except Exception:
            continue
    d = start
    while d <= end:
        if d in forbidden:
            return d.isoformat()
        d += timedelta(days=1)
    return None

async def check_overlap_policy(team_id: int, start: date, end: date, role: str) -> Optional[str]:
    policy = await get_team_setting(team_id, 'overlap_policy', DEFAULT_OVERLAP)
    if policy == 'allow_all':
        return None
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT v.start_date, v.end_date, u.full_name, u.role "
            "FROM vacations v JOIN users u ON v.user_id=u.id "
            "WHERE v.team_id=? AND v.status IN ('pending','approved')",
            (team_id,)
        )
        rows = await cur.fetchall()
    for s, e, fname, r in rows:
        try:
            s_d = datetime.fromisoformat(s).date()
            e_d = datetime.fromisoformat(e).date()
        except Exception:
            continue
        if dates_overlap(start, end, s_d, e_d):
            if policy == 'deny_all':
                return f"Пересечение с отпуском {fname} ({s}—{e}). Политика: запрет для всех."
            if policy == 'deny_same_role' and r == role:
                return f"Пересечение с отпуском {fname} ({s}—{e}) той же роли ({role_ru(role)}). Политика: запрет для одинаковых ролей."
    return None

async def user_year_used_days(user_id: int, year: int) -> int:
    start = date(year,1,1).isoformat()
    end = date(year,12,31).isoformat()
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT COALESCE(SUM(days),0) FROM vacations WHERE user_id=? AND status='approved' AND start_date BETWEEN ? AND ?",
            (user_id, start, end)
        )
        v = await cur.fetchone()
        return int(v[0] or 0)

# ====== REGISTRATION FLOW ======
def reg_role_keyboard() -> InlineKeyboardMarkup:
    # только админские роли на первом шаге
    kb = [[InlineKeyboardButton(role_ru(r), callback_data=f"reg:role:{r}")] for r in ['CEO','OWNER','TIMLID','TECH']]
    return InlineKeyboardMarkup(kb)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = await ensure_user_record(update.effective_user)
    # если нет "настоящей" команды (по умолчанию в Default Team) — запускаем регистрацию
    if user.get('team_id'):
        # Проверим, не остались ли на Default Team
        async with aiosqlite.connect(DB_FILE) as db:
            cur = await db.execute("SELECT name FROM teams WHERE id=?", (user['team_id'],))
            rr = await cur.fetchone()
            team_name = rr[0] if rr else ""
    else:
        team_name = "Default Team"

    if team_name == "Default Team":
        context.user_data['reg_mode'] = True
        await update.message.reply_text("Добро пожаловать! Введите название вашей команды:")
        return REG_TEAM_NAME

    # иначе просто меню
    await update.message.reply_text(
        f"Привет, *{update.effective_user.full_name}*! Ваша роль — *{role_ru(user['role'])}*.\n"
        f"Команда — *{team_name}*.",
        parse_mode='Markdown',
        reply_markup=await build_main_menu(user['role'])
    )

async def reg_team_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    team_name = update.message.text.strip()
    if not team_name:
        await update.message.reply_text("Введите непустое название команды:")
        return REG_TEAM_NAME

    team = await get_team_by_name(team_name)
    if not team:
        team_id = await create_team(team_name)
        # дефолтные настройки для этой команды
        await set_team_setting(team_id, 'max_year_days', '28')
        await set_team_setting(team_id, 'max_single_days', '14')
        await set_team_setting(team_id, 'overlap_policy', DEFAULT_OVERLAP)
        # типы по умолчанию НЕ создаём — дадим пользователю создать вручную
        context.user_data['reg_team_id'] = team_id
        context.user_data['reg_team_name'] = team_name
        await update.message.reply_text(
            f"Команда '{team_name}' создана. Выберите вашу роль:",
            reply_markup=reg_role_keyboard()
        )
        return REG_ROLE
    else:
        # подключаемся к уже созданной команде (тоже через выбор роли)
        context.user_data['reg_team_id'] = team['id']
        context.user_data['reg_team_name'] = team['name']
        await update.message.reply_text(
            f"Команда '{team['name']}' найдена. Выберите вашу роль:",
            reply_markup=reg_role_keyboard()
        )
        return REG_ROLE

async def reg_role_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    if not data.startswith("reg:role:"):
        return
    role = data.split(":")[2]
    team_id = context.user_data.get('reg_team_id')
    if not team_id:
        await q.edit_message_text("Ошибка регистрации. Начните /start заново.")
        return ConversationHandler.END

    # сохранить роль и команду пользователю
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE users SET role=?, team_id=? WHERE tg_id=?", (role, team_id, q.from_user.id))
        await db.commit()

    await q.edit_message_text(
        f"Готово! Ваша роль: {role_ru(role)}. Команда: {context.user_data.get('reg_team_name')}."
    )
    # спросим про типы
    await context.bot.send_message(
        chat_id=q.from_user.id,
        text="Создать типы отпусков сейчас?",
        reply_markup=yes_no_kb("reg_types")
    )
    return REG_TYPES_Q

# --- Регистрация: ветка "Создать типы отпусков?" ---
async def reg_types_q_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    ans = q.data.split(':')[1]
    if ans == 'yes':
        await q.edit_message_text("Название типа отпуска:")
        return TYPES_ADD_NAME
    # пропускаем — сразу к политике
    await q.edit_message_text("Настроить политику пересечений отпусков сейчас?", reply_markup=yes_no_kb("reg_overlap"))
    return REG_OVERLAP_Q

# Пошаговое добавление типа (используем и в регистрации, и в админке)
async def types_add_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = update.message.text.strip()
    if not name:
        await update.message.reply_text("Введите название типа:")
        return TYPES_ADD_NAME
    context.user_data['type_add_name'] = name
    await update.message.reply_text("Дней в году (целое число):")
    return TYPES_ADD_DAYS

async def types_add_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        days = int(update.message.text.strip())
    except Exception:
        await update.message.reply_text("Введите число, например 28:")
        return TYPES_ADD_DAYS
    context.user_data['type_add_days'] = days
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Пропустить описание", callback_data="types:add:skip_desc")]])
    await update.message.reply_text("Описание (опционально):", reply_markup=kb)
    return TYPES_ADD_DESC

async def types_add_desc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    desc = update.message.text.strip()
    return await _types_add_finalize(update, context, desc)

async def types_add_desc_skip_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    return await _types_add_finalize(q, context, "")

from telegram import Update  # убедись, что импорт есть вверху файла

async def _types_add_finalize(update_or_q, context: ContextTypes.DEFAULT_TYPE, desc: str):
    # update_or_q — либо Update (когда описание ввели текстом), либо CallbackQuery (кнопка "Пропустить")
    is_update = isinstance(update_or_q, Update)
    actor = update_or_q.effective_user if is_update else update_or_q.from_user

    user = await ensure_user_record(actor)
    name = context.user_data.get('type_add_name')
    days = context.user_data.get('type_add_days')
    if not name or days is None:
        if is_update:
            await update_or_q.message.reply_text("Сбой шага. Введите название типа заново:")
        else:
            await update_or_q.edit_message_text("Сбой шага. Введите название типа заново:")
        return TYPES_ADD_NAME

    # Сохраняем тип
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT INTO vacation_types(team_id,name,days_per_year,description) VALUES(?,?,?,?)",
            (user['team_id'], name, days, desc)
        )
        await db.commit()

    # Очистим временные поля
    context.user_data.pop('type_add_name', None)
    context.user_data.pop('type_add_days', None)

    msg = f'Тип "{name}" добавлен.'
    if is_update:
        await update_or_q.message.reply_text(msg)
        if context.user_data.get('reg_mode'):
            await update_or_q.message.reply_text(
                "Настроить политику пересечений отпусков сейчас?",
                reply_markup=yes_no_kb("reg_overlap")
            )
            return REG_OVERLAP_Q
        await update_or_q.message.reply_text("Админка:", )
        return ConversationHandler.END
    else:
        await update_or_q.edit_message_text(msg)
        if context.user_data.get('reg_mode'):
            await context.bot.send_message(
                chat_id=actor.id,
                text="Настроить политику пересечений отпусков сейчас?",
                reply_markup=yes_no_kb("reg_overlap")
            )
            return REG_OVERLAP_Q
        await context.bot.send_message(chat_id=actor.id, text="Админка:", )
        return ConversationHandler.END


# --- Регистрация: политика пересечений ---
async def reg_overlap_q_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    ans = q.data.split(':')[1]
    if ans == 'yes':
        user = await ensure_user_record(q.from_user)
        pol = await get_team_setting(user['team_id'], 'overlap_policy', DEFAULT_OVERLAP)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(("✅ " if pol=="allow_all" else "") + "Разрешить всем", callback_data="reg:set_overlap:allow_all")],
            [InlineKeyboardButton(("✅ " if pol=="deny_all" else "") + "Запретить всем", callback_data="reg:set_overlap:deny_all")],
            [InlineKeyboardButton(("✅ " if pol=="deny_same_role" else "") + "Запретить только одинаковым ролям", callback_data="reg:set_overlap:deny_same_role")],
        ])
        await q.edit_message_text("Выберите политику пересечений:", reply_markup=kb)
        return REG_OVERLAP_Q
    # пропустить
    await q.edit_message_text("Настроить запрещённые даты сейчас?", reply_markup=yes_no_kb("reg_forbidden"))
    return REG_FORBIDDEN_Q

async def reg_set_overlap_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    pol = q.data.split(':')[2]
    user = await ensure_user_record(q.from_user)
    await set_team_setting(user['team_id'], 'overlap_policy', pol)
    await q.edit_message_text("Политика пересечений сохранена.")
    await context.bot.send_message(q.from_user.id, "Настроить запрещённые даты сейчас?", reply_markup=yes_no_kb("reg_forbidden"))
    return REG_FORBIDDEN_Q

# --- Регистрация: запрет дат (диапазон + примечание) ---
async def reg_forbidden_q_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    ans = q.data.split(':')[1]
    if ans == 'yes':
        today = date.today()
        await q.edit_message_text(
            "Выберите дату начала запрещённого периода:",
            reply_markup=build_calendar(today.year, today.month, "forbid", "start")
        )
        return REG_FORBIDDEN_Q
    await q.edit_message_text("Добавить сотрудников сейчас?", reply_markup=yes_no_kb("reg_add_users"))
    return REG_ADD_USERS_Q

async def cal_forbid_nav_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, scope, phase, ym = q.data.split(":")
    y, m = map(int, ym.split("-"))
    title = "Выберите дату начала запрещённого периода:" if phase == "start" \
        else "Выберите дату окончания запрещённого периода:"
    await q.edit_message_text(title, reply_markup=build_calendar(y, m, "forbid", phase))
    return REG_FORBIDDEN_Q

async def cal_forbid_pick_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, scope, phase, ds = q.data.split(":")
    d = datetime.fromisoformat(ds).date()

    if phase == "start":
        context.user_data['forbid_start'] = d
        await q.edit_message_text(
            "Выберите дату окончания запрещённого периода:",
            reply_markup=build_calendar(d.year, d.month, "forbid", "end")
        )
        return REG_FORBIDDEN_Q

    start_d = context.user_data.get('forbid_start')
    if not start_d:
        today = date.today()
        await q.edit_message_text(
            "Сначала выберите дату начала:",
            reply_markup=build_calendar(today.year, today.month, "forbid", "start")
        )
        return REG_FORBIDDEN_Q

    end_d = d
    if end_d < start_d:
        start_d, end_d = end_d, start_d

    context.user_data['forbid_range'] = (start_d, end_d)
    context.user_data['admin_wizard'] = 'forbid_note_reg'
    await q.edit_message_text(
        "Примечание (опционально):",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Пропустить", callback_data="admin:forbid_note_skip_reg")]])
    )
    return REG_FORBIDDEN_Q


# --- Регистрация: добавление сотрудников ---
from telegram.ext import ConversationHandler

async def reg_add_users_q_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    ans = q.data.split(':')[1]

    if ans == 'yes':
        # выходим из конверсейшена → дальше текст поймает admin_text_handler
        context.user_data['admin_wizard'] = 'add_user_wait_username'
        # оставляем флаг, чтобы после добавления спрашивать «Добавить ещё?»
        context.user_data['reg_mode'] = True
        await q.edit_message_text("Шаг 1: отправьте @username будущего сотрудника.")
        return ConversationHandler.END

    # «Нет» — просто завершаем мастер и показываем меню
    context.user_data.pop('reg_mode', None)
    role = (await ensure_user_record(q.from_user))['role']
    await q.edit_message_text("Готово! 🎉")
    await context.bot.send_message(
        chat_id=q.from_user.id,
        text="Главное меню:",
        reply_markup=await build_main_menu(role)
    )
    return ConversationHandler.END



# ====== APPLY FLOW ======
async def cmd_apply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = await ensure_user_record(update.effective_user)
    await update.message.reply_text(
        "Выберите тип отпуска:",
        reply_markup=await build_apply_type_picker(user['team_id'])
    )
    return APPLY_TYPE

async def apply_manual_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    today = date.today()
    await q.edit_message_text(
        "Выберите дату начала:",
        reply_markup=build_calendar(today.year, today.month, "apply", "start")
    )
    return APPLY_START

async def cal_apply_nav_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, scope, phase, ym = q.data.split(":")
    y, m = map(int, ym.split("-"))
    title = "Выберите дату начала:" if phase == "start" else "Выберите дату окончания:"
    await q.edit_message_text(title, reply_markup=build_calendar(y, m, scope, phase))
    return APPLY_START if phase == "start" else APPLY_END

async def cal_apply_pick_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    _, scope, phase, ds = q.data.split(":")
    d = datetime.fromisoformat(ds).date()

    if phase == "start":
        context.user_data['apply_start'] = d.isoformat()
        await q.edit_message_text(
            "Выберите дату окончания:",
            reply_markup=build_calendar(d.year, d.month, "apply", "end")
        )
        return APPLY_END
    else:
        context.user_data['apply_end'] = d.isoformat()
        # сразу к комменту
        await context.bot.send_message(
            chat_id=q.from_user.id,
            text='Комментарий (необязательно):',
            reply_markup=apply_skip_comment_keyboard()
        )
        return APPLY_COMMENT

async def apply_quick_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # быстрые диапазоны внутри конверсейшена /apply
    q = update.callback_query
    await q.answer()
    _, type_id_s, days_s = q.data.split(":")
    user = await ensure_user_record(q.from_user)
    type_id = int(type_id_s)
    days = int(days_s)
    start_d = next_monday_from(date.today())
    end_d = start_d + timedelta(days=days-1)
    comment = f"Быстрая заявка: {days} дн с ближайшего понедельника"
    await process_apply_submission(q, context, user, type_id, start_d, end_d, comment)
    await q.edit_message_text(
        f"Заявка отправлена на рассмотрение.\nПериод: {start_d.isoformat()} — {end_d.isoformat()} ({days} дн).",
        reply_markup=nav_home_back()
    )
    return ConversationHandler.END


async def apply_type_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    if not data.startswith('apply_type:'):
        return
    type_id = int(data.split(':', 1)[1])
    context.user_data['apply_type_id'] = type_id
    # следующий шаг — быстрые диапазоны или ручной ввод
    await q.edit_message_text(
        f"Тип выбран: id:{type_id}\nВыберите быстрый диапазон или введите даты вручную:",
        reply_markup=build_quick_ranges_picker(type_id)
    )
    return APPLY_TYPE

async def apply_start_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip()
    try:
        start = datetime.fromisoformat(txt).date()
    except Exception:
        await update.message.reply_text('Неверный формат. Повторите ввод (YYYY-MM-DD).')
        return APPLY_START
    context.user_data['apply_start'] = start.isoformat()
    await update.message.reply_text('Введите дату окончания (YYYY-MM-DD):')
    return APPLY_END

async def apply_end_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip()
    try:
        end = datetime.fromisoformat(txt).date()
    except Exception:
        await update.message.reply_text('Неверный формат. Повторите ввод (YYYY-MM-DD).')
        return APPLY_END
    context.user_data['apply_end'] = end.isoformat()
    await update.message.reply_text('Комментарий (необязательно):', reply_markup=apply_skip_comment_keyboard())
    return APPLY_COMMENT

async def apply_comment_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    comment = update.message.text.strip()
    context.user_data['apply_comment'] = comment
    # показать подтверждение с кнопками
    type_id = context.user_data['apply_type_id']
    start = context.user_data['apply_start']
    end = context.user_data['apply_end']
    await update.message.reply_text(
        f"Подтвердите заявку:\nТип {type_id}, {start} — {end}\nКомментарий: {comment or '—'}",
        reply_markup=apply_confirm_keyboard()
    )
    return APPLY_CONFIRM

async def apply_skip_comment_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data['apply_comment'] = ''
    type_id = context.user_data['apply_type_id']
    start = context.user_data['apply_start']
    end = context.user_data['apply_end']
    await q.edit_message_text(
        f"Подтвердите заявку:\nТип {type_id}, {start} — {end}\nКомментарий: —",
        reply_markup=apply_confirm_keyboard()
    )
    return APPLY_CONFIRM

async def apply_confirm_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    if data == 'apply:cancel':
        await q.edit_message_text("Заявка отменена.", reply_markup=nav_home_back())
        return ConversationHandler.END
    if data != 'apply:confirm':
        return
    user = await ensure_user_record(q.from_user)
    type_id = context.user_data['apply_type_id']
    start = datetime.fromisoformat(context.user_data['apply_start']).date()
    end = datetime.fromisoformat(context.user_data['apply_end']).date()
    comment = context.user_data.get('apply_comment', '')
    await process_apply_submission(q, context, user, type_id, start, end, comment)
    return ConversationHandler.END

async def apply_cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('Заявка отменена.', reply_markup=nav_home_back())
    return ConversationHandler.END

# ====== CORE: CREATE VACATION ======
async def notify_admins_and_log(context, user, vac_id, type_id, start, end, days, comment):
    admins_msg = (
        f"Новая заявка #{vac_id}\n"
        f"Пользователь: {user.get('full_name')} (@{user.get('username')})\n"
        f"Тип: {type_id}\n"
        f"Период: {start.isoformat()} — {end.isoformat()} ({days} дн)\n"
        f"Комментарий: {comment if comment else '—'}"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton('Утвердить ✅', callback_data=f'admin_action:approve:{vac_id}'),
        InlineKeyboardButton('Отклонить ❌', callback_data=f'admin_action:reject:{vac_id}')
    ]])

    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute('SELECT tg_id FROM users WHERE team_id=? AND role IN ("CEO","OWNER","TIMLID","TECH") AND tg_id IS NOT NULL', (user['team_id'],))
        rows = await cur.fetchall()
    for (admin_chat_id,) in rows:
        try:
            await context.bot.send_message(chat_id=admin_chat_id, text=admins_msg, reply_markup=kb)
        except Exception:
            logger.exception('Cannot notify admin %s', admin_chat_id)

    await log_action(user['tg_id'], 'apply_created', str(vac_id), f'{start.isoformat()}_{end.isoformat()}')

async def process_apply_submission(update_or_ctx, context, user, type_id: int, start: date, end: date, comment: str):
    if end < start:
        await context.bot.send_message(chat_id=user['tg_id'], text='Дата окончания раньше даты начала — неверно.', reply_markup=nav_home_back())
        return
    days = (end - start).days + 1
    # запрещённые даты
    forbid = await is_range_forbidden(user['team_id'], start, end)
    if forbid:
        await context.bot.send_message(chat_id=user['tg_id'], text=f'В диапазоне есть запрещённая дата: {forbid}.', reply_markup=nav_home_back())
        return
    # лимит за один отпуск
    max_single = int(await get_team_setting(user['team_id'], 'max_single_days', '14'))
    if days > max_single:
        await context.bot.send_message(chat_id=user['tg_id'], text=f'Превышен максимум дней за раз ({max_single}).', reply_markup=nav_home_back())
        return
    # пересечения
    overlap_msg = await check_overlap_policy(user['team_id'], start, end, user['role'])
    if overlap_msg:
        await context.bot.send_message(chat_id=user['tg_id'], text=overlap_msg, reply_markup=nav_home_back())
        return
    # лимит в году
    used = await user_year_used_days(user['id'], start.year)
    max_year = int(await get_team_setting(user['team_id'], 'max_year_days', '28'))
    if used + days > max_year:
        await context.bot.send_message(chat_id=user['tg_id'], text=f'Превышен лимит дней в {start.year} году: {used}/{max_year}.', reply_markup=nav_home_back())
        return
    # сохранить pending
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            'INSERT INTO vacations(team_id,user_id,type_id,start_date,end_date,days,status,admin_comment) VALUES(?,?,?,?,?,?,?,?)',
            (user['team_id'], user['id'], type_id, start.isoformat(), end.isoformat(), days, 'pending', comment)
        )
        await db.commit()
        cur = await db.execute('SELECT last_insert_rowid()')
        vac_id = (await cur.fetchone())[0]

    await context.bot.send_message(chat_id=user['tg_id'], text=f'Заявка #{vac_id} отправлена на рассмотрение.', reply_markup=nav_home_back())
    await notify_admins_and_log(context, user, vac_id, type_id, start, end, days, comment)

# ====== ADMIN APPROVE/REJECT ======
async def admin_action_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split(':')
    if len(parts) != 3:
        await q.edit_message_text('Неверные данные.', reply_markup=nav_home_back())
        return
    action, vac_id_s = parts[1], parts[2]
    try:
        vac_id = int(vac_id_s)
    except:
        await q.edit_message_text('Неверный id заявки.', reply_markup=nav_home_back())
        return
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR REPLACE INTO admin_actions(admin_tg_id, vacation_id, action) VALUES(?,?,?)",
            (q.from_user.id, vac_id, action)
        )
        await db.commit()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Пропустить комментарий", callback_data="admin:skip_comment")]])
    await q.edit_message_text(
        f'Вы выбрали: { "утвердить" if action=="approve" else "отклонить" } заявку #{vac_id}. Пришлите комментарий (или пропустите):',
        reply_markup=kb
    )

async def finalize_admin_decision(context: ContextTypes.DEFAULT_TYPE, admin_tg: int, vac_id: int, action: str, comment: str):
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT v.id, v.user_id, v.start_date, v.end_date, u.tg_id, u.team_id "
            "FROM vacations v JOIN users u ON v.user_id=u.id WHERE v.id=?",
            (vac_id,)
        )
        rv = await cur.fetchone()
        if not rv:
            return False, "Заявка не найдена."
        v_id, user_id, start_date, end_date, user_tg, team_id = rv
        status = 'approved' if action == 'approve' else 'rejected'
        await db.execute('UPDATE vacations SET status=?, admin_comment=? WHERE id=?', (status, comment, vac_id))
        await db.execute('DELETE FROM admin_actions WHERE admin_tg_id=?', (admin_tg,))
        await db.commit()

    try:
        await context.bot.send_message(
            chat_id=user_tg,
            text=f'Ваша заявка #{vac_id}: {STATUS_HUMAN.get(status,status)}. Комментарий: {comment or "—"}'
        )
    except Exception:
        logger.exception('Cannot notify user %s', user_tg)
    await log_action(admin_tg, f'admin_{action}', str(vac_id), comment)
    return True, status

async def admin_skip_comment_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    admin_tg = q.from_user.id

    # есть ли записанное действие админа?
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute('SELECT vacation_id, action FROM admin_actions WHERE admin_tg_id=?', (admin_tg,))
        row = await cur.fetchone()

    if not row:
        await q.edit_message_text("Нет активного действия для пропуска комментария.", reply_markup=nav_home_back())
        return ConversationHandler.END

    vac_id, action = row
    comment = ""
    # применяем решение (ровно как в admin_text_handler)
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute(
            "SELECT v.id, v.user_id, v.start_date, v.end_date, u.tg_id "
            "FROM vacations v JOIN users u ON v.user_id=u.id WHERE v.id=?",
            (vac_id,)
        )
        rv = await cur.fetchone()
        if not rv:
            await q.edit_message_text("Заявка не найдена.", reply_markup=nav_home_back())
            await db.execute('DELETE FROM admin_actions WHERE admin_tg_id=?', (admin_tg,))
            await db.commit()
            return ConversationHandler.END

        v_id, user_id, start_date, end_date, user_tg = rv
        status = 'approved' if action == 'approve' else 'rejected'
        await db.execute('UPDATE vacations SET status=?, admin_comment=? WHERE id=?', (status, comment, vac_id))
        await db.execute('DELETE FROM admin_actions WHERE admin_tg_id=?', (admin_tg,))
        await db.commit()

    try:
        await context.bot.send_message(
            chat_id=user_tg,
            text=f'Ваша заявка #{vac_id}: {STATUS_HUMAN.get(status,status)}. Комментарий: —'
        )
    except Exception:
        logger.exception('Cannot notify user %s', user_tg)

    await q.edit_message_text(
        f'Заявка #{vac_id} помечена как {STATUS_HUMAN.get(status,status)} и пользователь уведомлён.',
        reply_markup=nav_home_back()
    )
    await log_action(admin_tg, f'admin_{action}', str(vac_id), comment)
    return ConversationHandler.END


async def admin_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    caller = await get_user_by_tg_id(update.effective_user.id)
    wizard = context.user_data.get('admin_wizard')

    # если не админ, разрешаем только нужные шаги регистрационного мастера
    if not caller or not is_admin_role(caller['role']):
        if wizard in ('add_user_wait_username', 'forbid_date_range_reg', 'forbid_note_reg'):
            pass
        else:
            return

    admin_tg = update.effective_user.id
    text = update.message.text.strip()

    if wizard == 'unforbid_date':
        try:
            d = datetime.fromisoformat(text).date()
        except Exception:
            await update.message.reply_text("Формат: YYYY-MM-DD", reply_markup=nav_home_back())
            return
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute('DELETE FROM forbidden_dates WHERE team_id=? AND date=?',
                            (caller['team_id'], d.isoformat()))
            await db.commit()
        context.user_data.pop('admin_wizard', None)
        await update.message.reply_text("Дата разрешена.")
        return

    # 1) комментарий к approve/reject
    async with aiosqlite.connect(DB_FILE) as db:
        cur = await db.execute('SELECT vacation_id, action FROM admin_actions WHERE admin_tg_id=?', (admin_tg,))
        row = await cur.fetchone()
    if row:
        vac_id, action = row
        comment = "" if text == '/skip' else text
        ok, status = await finalize_admin_decision(context, admin_tg, vac_id, action, comment)
        if ok:
            await update.message.reply_text(
                f'Заявка #{vac_id} помечена как {STATUS_HUMAN.get(status,status)} и пользователь уведомлён.',
                reply_markup=nav_home_back()
            )
        return
    

    # 2) мини-визарды админки / регистрации
    wizard = context.user_data.get('admin_wizard')
    if not wizard:
        return

    try:
        if wizard == 'add_user_wait_username':
            username = text.lstrip('@')
            if not username:
                await update.message.reply_text("Отправьте @username", reply_markup=nav_home_back())
                return
            context.user_data['add_user_username'] = username
            kb = [[InlineKeyboardButton(role_ru(r), callback_data=f'admin:add_user_role:{r}')] for r in sorted(ALL_ROLES)]
            await update.message.reply_text(
                f"Выберите роль для @{username}:",
                reply_markup=InlineKeyboardMarkup(kb)
            )
            context.user_data['admin_wizard'] = 'add_user_pick_role'
            return

        if wizard == 'forbid_date':
            # принимаем либо одну дату, либо диапазон "YYYY-MM-DD YYYY-MM-DD"
            parts = text.split()
            if len(parts) == 1:
                try:
                    d1 = datetime.fromisoformat(parts[0]).date()
                except Exception:
                    await update.message.reply_text("Формат: YYYY-MM-DD или YYYY-MM-DD YYYY-MM-DD", reply_markup=nav_home_back())
                    return
                # спросим примечание
                context.user_data['forbid_range'] = (d1, d1)
                await update.message.reply_text("Примечание (опционально):", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Пропустить", callback_data="admin:forbid_note_skip")]]))
                context.user_data['admin_wizard'] = 'forbid_note'
                return
            elif len(parts) == 2:
                try:
                    d1 = datetime.fromisoformat(parts[0]).date()
                    d2 = datetime.fromisoformat(parts[1]).date()
                except Exception:
                    await update.message.reply_text("Формат: YYYY-MM-DD или YYYY-MM-DD YYYY-MM-DD", reply_markup=nav_home_back())
                    return
                context.user_data['forbid_range'] = (d1, d2)
                await update.message.reply_text("Примечание (опционально):", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Пропустить", callback_data="admin:forbid_note_skip")]]))
                context.user_data['admin_wizard'] = 'forbid_note'
                return
            else:
                await update.message.reply_text("Формат: YYYY-MM-DD или YYYY-MM-DD YYYY-MM-DD", reply_markup=nav_home_back())
                return

        if wizard == 'forbid_note':
            note = text
            d1, d2 = context.user_data.get('forbid_range', (None, None))
            if not d1:
                await update.message.reply_text("Сбой: диапазон не найден.", reply_markup=nav_home_back())
                return
            # сохранить диапазон
            async with aiosqlite.connect(DB_FILE) as db:
                d = d1
                while d <= d2:
                    await db.execute('INSERT INTO forbidden_dates(team_id,date,note) VALUES(?,?,?)', (caller['team_id'], d.isoformat(), note))
                    d += timedelta(days=1)
                await db.commit()
            context.user_data.pop('forbid_range', None)
            context.user_data.pop('admin_wizard', None)
            await update.message.reply_text("Диапазон запрещённых дат сохранён.", )
            return

        if wizard == 'forbid_date_range_reg':
            # регистрация: ждём диапазон
            parts = text.split()
            if len(parts) == 1:
                try:
                    d1 = datetime.fromisoformat(parts[0]).date()
                    d2 = d1
                except Exception:
                    await update.message.reply_text("Формат: YYYY-MM-DD или YYYY-MM-DD YYYY-MM-DD")
                    return
            elif len(parts) == 2:
                try:
                    d1 = datetime.fromisoformat(parts[0]).date()
                    d2 = datetime.fromisoformat(parts[1]).date()
                except Exception:
                    await update.message.reply_text("Формат: YYYY-MM-DD или YYYY-MM-DD YYYY-MM-DD")
                    return
            else:
                await update.message.reply_text("Формат: YYYY-MM-DD или YYYY-MM-DD YYYY-MM-DD")
                return
            context.user_data['forbid_range'] = (d1, d2)
            await update.message.reply_text("Примечание (опционально):", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Пропустить", callback_data="admin:forbid_note_skip_reg")]]))
            context.user_data['admin_wizard'] = 'forbid_note_reg'
            return

        if wizard == 'forbid_note_reg':
            note = text
            d1, d2 = context.user_data.get('forbid_range', (None, None))
            if not d1:
                await update.message.reply_text("Сбой: диапазон не найден.")
                return
            async with aiosqlite.connect(DB_FILE) as db:
                d = d1
                while d <= d2:
                    await db.execute('INSERT INTO forbidden_dates(team_id,date,note) VALUES(?,?,?)', (caller['team_id'], d.isoformat(), note))
                    d += timedelta(days=1)
                await db.commit()
            context.user_data.pop('forbid_range', None)
            context.user_data.pop('admin_wizard', None)
            await update.message.reply_text("Диапазон запрещённых дат сохранён.")
            await update.message.reply_text("Добавить сотрудников сейчас?", reply_markup=yes_no_kb("reg_add_users"))
            return

        if wizard == 'set_max_year':
            try:
                v = int(text)
            except:
                await update.message.reply_text("Отправьте целое число, напр.: 28", reply_markup=nav_home_back())
                return
            await set_team_setting(caller['team_id'], 'max_year_days', str(v))
            await update.message.reply_text(f'Лимит на год установлен: {v}', )
            context.user_data.pop('admin_wizard', None)
            return

        if wizard == 'set_max_single':
            try:
                v = int(text)
            except:
                await update.message.reply_text("Отправьте целое число, напр.: 14", reply_markup=nav_home_back())
                return
            await set_team_setting(caller['team_id'], 'max_single_days', str(v))
            await update.message.reply_text(f'Лимит за один отпуск установлен: {v}', )
            context.user_data.pop('admin_wizard', None)
            return

        if wizard == 'types_add':
            # сюда можно не попадать (мы используем пошаговые TYPES_ADD_*), но оставим для совместимости
            parts = [p.strip() for p in text.split('|')]
            if len(parts) < 2:
                await update.message.reply_text("Формат: Название | Дней_в_год | Описание(опционально)", reply_markup=nav_home_back())
                return
            name = parts[0]
            try:
                days = int(parts[1])
            except:
                await update.message.reply_text("Количество дней должно быть числом.", reply_markup=nav_home_back())
                return
            desc = parts[2] if len(parts) > 2 else ''
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute(
                    "INSERT INTO vacation_types(team_id,name,days_per_year,description) VALUES(?,?,?,?)",
                    (caller['team_id'], name, days, desc)
                )
                await db.commit()
            await update.message.reply_text(f'Тип "{name}" добавлен.', )
            context.user_data.pop('admin_wizard', None)
            return

        if wizard and wizard.startswith('types_edit:'):
            type_id = int(wizard.split(':')[1])
            parts = [p.strip() for p in text.split('|')]
            if len(parts) < 2:
                await update.message.reply_text("Формат: Название | Дней_в_год | Описание(опционально)", reply_markup=nav_home_back())
                return
            name = parts[0]
            try:
                days = int(parts[1])
            except:
                await update.message.reply_text("Количество дней должно быть числом.", reply_markup=nav_home_back())
                return
            desc = parts[2] if len(parts) > 2 else ''
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute(
                    "UPDATE vacation_types SET name=?, days_per_year=?, description=? WHERE id=? AND team_id=?",
                    (name, days, desc, type_id, caller['team_id'])
                )
                await db.commit()
            await update.message.reply_text(f'Тип обновлён.', )
            context.user_data.pop('admin_wizard', None)
            return
        
        if wizard == 'types_add_name':
            name = text.strip()
            if not name:
                await update.message.reply_text("Введите название типа:")
                return
            context.user_data['type_add_name'] = name
            context.user_data['admin_wizard'] = 'types_add_days'
            await update.message.reply_text("Дней в году (целое число):")
            return

        if wizard == 'types_add_days':
            try:
                days = int(text.strip())
            except Exception:
                await update.message.reply_text("Введите число, например 28:")
                return
            context.user_data['type_add_days'] = days
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("Пропустить описание", callback_data="admin:type_add_skip_desc")]])
            context.user_data['admin_wizard'] = 'types_add_desc'
            await update.message.reply_text("Описание (опционально):", reply_markup=kb)
            return

        if wizard == 'types_add_desc':
            desc = text.strip()
            name = context.user_data.get('type_add_name')
            days = context.user_data.get('type_add_days')
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute(
                    "INSERT INTO vacation_types(team_id,name,days_per_year,description) VALUES(?,?,?,?)",
                    (caller['team_id'], name, days, desc)
                )
                await db.commit()
            for k in ('type_add_name','type_add_days','admin_wizard'):
                context.user_data.pop(k, None)
            await update.message.reply_text(f'Тип "{name}" добавлен.')
            return

    except Exception as e:
        logger.exception("Admin wizard failed: %s", e)
        await update.message.reply_text("Ошибка обработки. Попробуйте ещё раз.", reply_markup=nav_home_back())

# ====== ADMIN INLINE CALLBACKS (roles, types, overlap, forbid-note-skip) ======
async def admin_menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user = await ensure_user_record(q.from_user)
    if not is_admin_role(user['role']):
        await q.answer("Недостаточно прав", show_alert=True)
        return

    key = q.data

    # Всегда: сперва перерисовываем «шапку» админки, ниже — отдельным сообщением содержимое
    async def show(extra_text: str, kb: Optional[InlineKeyboardMarkup] = None, parse_mode=None):
        await admin_show_menu(q, context, extra_text, kb, parse_mode=parse_mode)

    # ---- Типы отпусков: пропуск описания (кнопка) ----
    if key == "admin:type_add_skip_desc":
        name = context.user_data.get('type_add_name')
        days = context.user_data.get('type_add_days')
        if not name or days is None:
            for k in ('type_add_name', 'type_add_days', 'admin_wizard'):
                context.user_data.pop(k, None)
            await show("Сбой шага. Начните заново: Админка → Типы → Добавить тип.")
            return
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                "INSERT INTO vacation_types(team_id,name,days_per_year,description) VALUES(?,?,?,?)",
                (user['team_id'], name, days, "")
            )
            await db.commit()
        for k in ('type_add_name', 'type_add_days', 'admin_wizard'):
            context.user_data.pop(k, None)
        await show(f'Тип "{name}" добавлен.')
        return

    # ---- Добавление сотрудника ----
    elif key == "admin:add_user":
        context.user_data['admin_wizard'] = 'add_user_wait_username'
        await show("Шаг 1: отправьте @username будущего сотрудника.", nav_home_back())
        return

    elif key.startswith('admin:add_user_role:'):
        role = key.split(':')[2]
        username = context.user_data.get('add_user_username')
        if not username:
            await show("Сначала отправьте @username.", nav_home_back())
            return

        # Ищем пользователя по username в ЛЮБОЙ команде
        existing_any = await get_user_by_username(username)

        # Если уже состоит в другой команде — не добавляем, а уведомляем
        if existing_any and existing_any['team_id'] and existing_any['team_id'] != user['team_id']:
            other_team = await get_team_name(existing_any['team_id'])
            this_team  = await get_team_name(user['team_id'])
            await show(
                f"@{username} уже состоит в другой команде «{other_team}».\n"
                "Попросите его выйти из команды и отправьте приглашение заново."
            )

            # Если знаем tg_id — уведомим человека
            if existing_any.get('tg_id'):
                try:
                    await context.bot.send_message(
                        chat_id=existing_any['tg_id'],
                        text=(
                            f"Вас хотят пригласить в новую команду: «{this_team}».\n\n"
                            f"Для принятия приглашения сначала выйдите из текущей команды «{other_team}», "
                            "после этого вас смогут повторно пригласить."
                        ),
                        reply_markup=build_invite_kb()
                    )
                except Exception:
                    logger.exception("Cannot notify invited user %s", existing_any['tg_id'])
            else:
                # tg_id нет — человек ещё не писал боту
                await context.bot.send_message(
                    chat_id=q.from_user.id,
                    text=(
                        f"Предупреждение: @{username} ещё не писал боту, поэтому уведомление не отправлено. "
                        "Попросите его открыть бота и нажать /start."
                    )
                )

            # чистим визард выбора роли
            context.user_data.pop('add_user_username', None)
            context.user_data.pop('admin_wizard', None)
            return

        # Если запись существует и уже в ЭТОЙ команде — просто меняем роль
        if existing_any and existing_any['team_id'] == user['team_id']:
            async with aiosqlite.connect(DB_FILE) as db:
                await db.execute("UPDATE users SET role=? WHERE id=?", (role, existing_any['id']))
                await db.commit()
            context.user_data.pop('add_user_username', None)
            context.user_data.pop('admin_wizard', None)
            await show(f'Роль @{username} обновлена: {role_ru(role)}.')
            return

        # Иначе — создаём плейсхолдер в нашей команде
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                "INSERT OR REPLACE INTO users(tg_id, username, full_name, role, team_id) VALUES(?,?,?,?,?)",
                (None, username, username, role, user['team_id'])
            )
            await db.commit()
        context.user_data.pop('add_user_username', None)
        context.user_data.pop('admin_wizard', None)

        if context.user_data.get('reg_mode'):
            await show(f'Пользователь @{username} добавлен с ролью {role_ru(role)}.')
            await context.bot.send_message(
                q.from_user.id,
                "Добавить ещё сотрудника?",
                reply_markup=yes_no_kb("reg_add_users")
            )
        else:
            await show(f'Пользователь @{username} добавлен с ролью {role_ru(role)}.')
        return

    # ---- Удаление сотрудника: список ----
    elif key == "admin:remove_user":
        context.user_data.pop('admin_wizard', None)  # чистим визарды по тексту
        text, kb = await build_remove_users_view(user['team_id'], page=0)
        await show(text, kb)
        return

    # Пагинация списка
    elif key.startswith("admin:remove_page:"):
        page = int(key.split(":")[2])
        text, kb = await build_remove_users_view(user['team_id'], page=page)
        await show(text, kb)
        return

    # Выбор сотрудника для удаления → спросить подтверждение
    elif key.startswith("admin:remove_pick:"):
        uid = int(key.split(":")[2])
        async with aiosqlite.connect(DB_FILE) as db:
            cur = await db.execute(
                "SELECT id, full_name, username, role, tg_id FROM users WHERE id=? AND team_id=?",
                (uid, user['team_id'])
            )
            target = await cur.fetchone()

        if not target:
            await show("Пользователь не найден.")
            return

        tid, fname, uname, role, tgid = target

        if tid == user['id']:
            await show("Нельзя удалить самого себя.")
            return

        confirm_text = f"Удалить сотрудника {fname or '(без имени)'} @{uname or ''} — {role_ru(role)}?"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да, удалить", callback_data=f"admin:remove_confirm:{tid}")],
            [InlineKeyboardButton("Отмена", callback_data="admin:remove_user")],
        ])
        await show(confirm_text, kb)
        return

    # Подтверждение удаления
    elif key.startswith("admin:remove_confirm:"):
        uid = int(key.split(":")[2])
        if uid == user['id']:
            await show("Нельзя удалить самого себя.")
            return
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("DELETE FROM vacations WHERE user_id=? AND team_id=?", (uid, user['team_id']))
            await db.execute("DELETE FROM users WHERE id=? AND team_id=?", (uid, user['team_id']))
            await db.commit()
        await show("Сотрудник удалён.")
        # Отправим свежий список отдельным сообщением
        text, kb = await build_remove_users_view(user['team_id'], page=0)
        await context.bot.send_message(chat_id=q.from_user.id, text=text, reply_markup=kb)
        return

    # ---- Остальные пункты админки ----
    elif key == "admin:forbid_date":
        context.user_data['admin_wizard'] = 'forbid_date'
        await show(
            "Запретить дату или диапазон.\nОтправьте: `YYYY-MM-DD` или `YYYY-MM-DD YYYY-MM-DD`",
            nav_home_back(),
            parse_mode='Markdown'
        )
        return

    elif key == "admin:unforbid_date":
        context.user_data['admin_wizard'] = 'unforbid_date'
        await show("Разрешить дату\nОтправьте: `YYYY-MM-DD`", nav_home_back(), parse_mode='Markdown')
        return

    elif key == "admin:list_forbidden":
        async with aiosqlite.connect(DB_FILE) as db:
            cur = await db.execute('SELECT date, note FROM forbidden_dates WHERE team_id=? ORDER BY date', (user['team_id'],))
            rows = await cur.fetchall()
        txt = "Нет запрещённых дат." if not rows else "Запрещённые даты:\n" + "\n".join(f"{r[0]} — {r[1] or ''}" for r in rows)
        await show(txt)
        return

    elif key == "admin:set_max_year":
        context.user_data['admin_wizard'] = 'set_max_year'
        await show("Установить лимит на год: отправьте число, напр. 28", nav_home_back())
        return

    elif key == "admin:set_max_single":
        context.user_data['admin_wizard'] = 'set_max_single'
        await show("Установить лимит за один отпуск: отправьте число, напр. 14", nav_home_back())
        return

    elif key == "admin:types":
        async with aiosqlite.connect(DB_FILE) as db:
            cur = await db.execute("SELECT id,name,days_per_year FROM vacation_types WHERE team_id=? ORDER BY id", (user['team_id'],))
            rows = await cur.fetchall()
        lines = ["Типы отпусков:"]
        kb = []
        for (tid, name, days) in rows:
            lines.append(f"id:{tid} {name} — {days} дн")
            kb.append([
                InlineKeyboardButton(f"✏️ id:{tid}", callback_data=f"admin:type_edit:{tid}"),
                InlineKeyboardButton(f"🗑 id:{tid}", callback_data=f"admin:type_del:{tid}")
            ])
        kb.append([InlineKeyboardButton("➕ Добавить тип (пошагово)", callback_data="admin:type_add")])
        await show("\n".join(lines), InlineKeyboardMarkup(kb))
        return

    elif key == "admin:type_add":
        context.user_data['reg_mode'] = False
        context.user_data['admin_wizard'] = 'types_add_name'
        await show("Название типа отпуска:", nav_home_back())
        return

    elif key.startswith("admin:type_edit:"):
        tid = int(key.split(':')[2])
        context.user_data['admin_wizard'] = f'types_edit:{tid}'
        await show("Изменение типа\nФормат: `Название | Дней_в_год | Описание(опц)`", nav_home_back(), parse_mode='Markdown')
        return

    elif key.startswith("admin:type_del:"):
        tid = int(key.split(':')[2])
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Да, удалить", callback_data=f"admin:type_del_confirm:{tid}")],
            [InlineKeyboardButton("Отмена", callback_data="admin:types")]
        ])
        await show(f"Удалить тип id:{tid}?", kb)
        return

    elif key.startswith("admin:type_del_confirm:"):
        tid = int(key.split(':')[2])
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("DELETE FROM vacation_types WHERE id=? AND team_id=?", (tid, user['team_id']))
            await db.commit()
        await show("Тип удалён.")
        return

    elif key == "admin:overlap":
        pol = await get_team_setting(user['team_id'], 'overlap_policy', DEFAULT_OVERLAP)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(("✅ " if pol=="allow_all" else "") + "Разрешить всем", callback_data="admin:overlap_set:allow_all")],
            [InlineKeyboardButton(("✅ " if pol=="deny_all" else "") + "Запретить всем", callback_data="admin:overlap_set:deny_all")],
            [InlineKeyboardButton(("✅ " if pol=="deny_same_role" else "") + "Запретить только одинаковым ролям", callback_data="admin:overlap_set:deny_same_role")],
        ])
        await show("Политика пересечений отпусков в команде:", kb)
        return

    elif key.startswith("admin:overlap_set:"):
        pol = key.split(':')[2]
        await set_team_setting(user['team_id'], 'overlap_policy', pol)
        await show("Политика пересечений обновлена.")
        return

    elif key == "admin:pending":
        async with aiosqlite.connect(DB_FILE) as db:
            cur = await db.execute(
                "SELECT v.id, u.full_name, t.name, v.start_date, v.end_date "
                "FROM vacations v "
                "JOIN users u ON v.user_id=u.id "
                "JOIN vacation_types t ON v.type_id=t.id "
                "WHERE v.team_id=? AND v.status='pending' ORDER BY v.start_date",
                (user['team_id'],)
            )
            rows = await cur.fetchall()
        if not rows:
            await show("Нет заявок на рассмотрении.")
            return
        lines = ["Заявки на рассмотрении:"]
        kbl = []
        for (vid, fname, tname, s, e) in rows[:25]:
            lines.append(f"#{vid} {fname} — {tname}: {s} — {e}")
            kbl.append([
                InlineKeyboardButton(f"✅ #{vid}", callback_data=f"admin_action:approve:{vid}"),
                InlineKeyboardButton(f"❌ #{vid}", callback_data=f"admin_action:reject:{vid}")
            ])
        await show("\n".join(lines), InlineKeyboardMarkup(kbl))
        return

    elif key == "admin:staff_list":
        year = datetime.now().year
        max_year = int(await get_team_setting(user['team_id'], 'max_year_days', '28'))
        async with aiosqlite.connect(DB_FILE) as db:
            cur = await db.execute("SELECT id, full_name, username, role FROM users WHERE team_id=? ORDER BY full_name", (user['team_id'],))
            users = await cur.fetchall()
        lines = [f"Сотрудники (лимит на {year}: {max_year}):"]
        for (uid, fname, uname, role) in users:
            used = await user_year_used_days(uid, year)
            lines.append(f"{fname} (@{uname or ''}) — {role_ru(role)}, остаток: {max_year - used} (использовано {used})")
        await show("\n".join(lines))
        return

    elif key == "admin:export_csv":
        await show("Готовлю экспорт...")
        year = datetime.now().year
        start, end = date(year,1,1).isoformat(), date(year,12,31).isoformat()
        async with aiosqlite.connect(DB_FILE) as db:
            cur = await db.execute(
                "SELECT v.id, u.full_name, u.username, u.role, t.name, v.start_date, v.end_date, v.days, v.status, v.admin_comment "
                "FROM vacations v JOIN users u ON v.user_id=u.id JOIN vacation_types t ON v.type_id=t.id "
                "WHERE v.team_id=? AND v.start_date BETWEEN ? AND ? ORDER BY v.start_date",
                (user['team_id'], start, end)
            )
            rows = await cur.fetchall()
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["id","full_name","username","role","type","start_date","end_date","days","status","comment"])
        for r in rows:
            writer.writerow(r)
        buf.seek(0)
        data = io.BytesIO(buf.read().encode('utf-8-sig'))
        data.seek(0)
        fname = f"vacations_{year}.csv"
        await context.bot.send_document(chat_id=q.from_user.id, document=InputFile(data, filename=fname), caption=f"Экспорт за {year}")
        return

    elif key == "admin:analytics":
        year = datetime.now().year
        async with aiosqlite.connect(DB_FILE) as db:
            cur = await db.execute(
                "SELECT v.start_date, v.end_date, v.days, u.role "
                "FROM vacations v JOIN users u ON v.user_id=u.id "
                "WHERE v.team_id=? AND v.status='approved' AND substr(v.start_date,1,4)=?",
                (user['team_id'], str(year))
            )
            rows = await cur.fetchall()
        by_month = {m:0 for m in range(1,13)}
        by_role = {}
        for s, e, d, role in rows:
            try:
                s_d = datetime.fromisoformat(s).date()
            except:
                continue
            by_month[s_d.month] += int(d or 0)
            by_role[role] = by_role.get(role, 0) + int(d or 0)
        lines = [f"Аналитика за {year}:"]
        lines.append("По месяцам (дни): " + ", ".join(f"{m}:{by_month[m]}" for m in range(1,13)))
        lines.append("По ролям (дни): " + (", ".join(f"{role_ru(k)}:{v}" for k,v in by_role.items()) if by_role else "—"))
        await show("\n".join(lines))
        return

    elif key == "admin:forbid_note_skip":
        rng = context.user_data.get('forbid_range')
        if not rng:
            await show("Сбой: диапазон не найден.", nav_home_back())
            return
        d1, d2 = rng
        async with aiosqlite.connect(DB_FILE) as db:
            d = d1
            while d <= d2:
                await db.execute('INSERT INTO forbidden_dates(team_id,date,note) VALUES(?,?,?)', (user['team_id'], d.isoformat(), ''))
                d += timedelta(days=1)
            await db.commit()
        context.user_data.pop('forbid_range', None)
        context.user_data.pop('admin_wizard', None)
        await show("Диапазон запрещённых дат сохранён.")
        return

    elif key == "admin:forbid_note_skip_reg":
        rng = context.user_data.get('forbid_range')
        if not rng:
            await show("Сбой: диапазон не найден.")
            return
        d1, d2 = rng
        async with aiosqlite.connect(DB_FILE) as db:
            d = d1
            while d <= d2:
                await db.execute('INSERT INTO forbidden_dates(team_id,date,note) VALUES(?,?,?)', (user['team_id'], d.isoformat(), ''))
                d += timedelta(days=1)
            await db.commit()
        context.user_data.pop('forbid_range', None)
        context.user_data.pop('admin_wizard', None)
        await show("Диапазон запрещённых дат сохранён.")
        await context.bot.send_message(q.from_user.id, "Добавить сотрудников сейчас?", reply_markup=yes_no_kb("reg_add_users"))
        return

    else:
        # На всякий случай — неизвестный ключ
        await show("Неизвестная команда админки.")
        return

# ====== MENU CB (GENERAL) ======
async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = await ensure_user_record(update.effective_user)
    await update.message.reply_text("Главное меню:", reply_markup=await build_main_menu(user["role"]))

async def menu_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    user = await ensure_user_record(q.from_user)

    if data == "menu:home":
        for k in ('admin_wizard','add_user_username','forbid_range','forbid_start','type_add_name','type_add_days'):
            context.user_data.pop(k, None)
        await q.edit_message_text("Главное меню:", reply_markup=await build_main_menu(user["role"]))
        return

    if data == "menu:apply":
        await q.edit_message_text("Выберите тип отпуска:", reply_markup=await build_apply_type_picker(user["team_id"]))
        return

    if data.startswith("menu_apply_type:"):
        type_id = int(data.split(":")[1])
        await q.edit_message_text(
            f"Тип выбран: id:{type_id}\nВыберите быстрый диапазон или введите даты вручную:",
            reply_markup=build_quick_ranges_picker(type_id)
        )
        return

    if data.startswith("apply_quick:"):
        _, type_id_s, days_s = data.split(":")
        type_id = int(type_id_s)
        days = int(days_s)
        start_d = next_monday_from(date.today())
        end_d = start_d + timedelta(days=days-1)
        comment = f"Быстрая заявка: {days} дн с ближайшего понедельника"
        await process_apply_submission(q, context, user, type_id, start_d, end_d, comment)
        await q.edit_message_text(
            f"Заявка отправлена на рассмотрение.\nПериод: {start_d.isoformat()} — {end_d.isoformat()} ({days} дн).",
            reply_markup=nav_home_back()
        )
        return

    if data == "menu:my":
        txt = await get_my_vacations_text(user['tg_id'])
        await q.edit_message_text(txt, reply_markup=nav_home_back())
        return

    if data == "menu:all":
        await q.edit_message_text("Выберите год:", reply_markup=await build_year_picker())
        return

    if data.startswith("menu:all:"):
        year = int(data.split(":")[2])
        txt = await get_all_vacations_text(user['team_id'], year)
        await q.edit_message_text(txt, reply_markup=nav_home_back())
        return

    if data == "menu:types":
        txt, _ = await get_types_text_and_list(user['team_id'])
        await q.edit_message_text(txt, reply_markup=nav_home_back())
        return

    if data == "menu:help":
        await q.edit_message_text(
            "Что можно сделать:\n"
            f"{E_APPLY} Подать заявку — быстро или вручную.\n"
            f"{E_MY} Мои отпуска — статус всех заявок.\n"
            f"{E_ALL} Отпуска за год — кто когда в отпуске в вашей команде.\n"
            f"{E_TYPES} Типы отпусков — доступные типы.\n\n"
            "Админка: сотрудники, роли, запреты дат, лимиты, типы, политика пересечений, pending-заявки, экспорт, аналитика.",
            reply_markup=nav_home_back()
        )
        return

    if data == "menu:admin":
        if not is_admin_role(user["role"]):
            await q.answer("Недостаточно прав", show_alert=True)
            return
        # сброс любых незавершённых визардов
        for k in ('admin_wizard','add_user_username','forbid_range','forbid_start','type_add_name','type_add_days'):
            context.user_data.pop(k, None)
        await admin_show_menu(q, context)
        return


# ====== COMMANDS: whoami / setrole ======
async def cmd_whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = await ensure_user_record(update.effective_user)
    team_name = "-"
    if user.get('team_id'):
        async with aiosqlite.connect(DB_FILE) as db:
            cur = await db.execute("SELECT name FROM teams WHERE id=?", (user['team_id'],))
            rr = await cur.fetchone()
            team_name = rr[0] if rr else "-"
    await update.message.reply_text(
        f"Вы: {user.get('full_name') or ''} (@{user.get('username') or ''})\n"
        f"Роль: {role_ru(user['role'])}\n"
        f"Команда: {team_name}"
    )

async def cmd_setrole(update: Update, context: ContextTypes.DEFAULT_TYPE):
    caller = await ensure_user_record(update.effective_user)
    if not is_admin_role(caller['role']):
        await update.message.reply_text("Только администраторы могут менять роли.")
        return
    if len(context.args) < 2:
        await update.message.reply_text("Использование: /setrole @username ROLE")
        return
    username = context.args[0].lstrip('@')
    role = context.args[1].upper()
    if role not in ALL_ROLES:
        await update.message.reply_text(f"Неизвестная роль. Доступные: {', '.join(role_ru(r) for r in sorted(ALL_ROLES))}")
        return
    user = await get_user_by_username(username, caller['team_id'])
    async with aiosqlite.connect(DB_FILE) as db:
        if not user:
            await db.execute(
                "INSERT INTO users(tg_id,username,full_name,role,team_id) VALUES(?,?,?,?,?)",
                (None, username, username, role, caller['team_id'])
            )
        else:
            await db.execute("UPDATE users SET role=? WHERE id=?", (role, user['id']))
        await db.commit()
    await update.message.reply_text(f"Роль @{username} установлена: {role_ru(role)}", reply_markup=nav_home_back())


# ====== DELETE (self-leave) ======
async def cmd_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Попросить подтверждение на выход из текущей команды (удаление пользователя из БД)."""
    user = await ensure_user_record(update.effective_user)

    # Получим имя команды (только для текста предупреждения)
    team_name = "-"
    if user.get('team_id'):
        async with aiosqlite.connect(DB_FILE) as db:
            cur = await db.execute("SELECT name FROM teams WHERE id=?", (user['team_id'],))
            row = await cur.fetchone()
            team_name = row[0] if row else "-"

    warn = (
        f"Вы точно хотите выйти из текущей команды «{team_name}»?\n\n"
        "Будут удалены все ваши заявки, а вы будете удалены из команды. Это действие необратимо."
    )
    await update.message.reply_text(warn, reply_markup=yes_no_kb("delete"))

async def invite_leave_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Нажали 'Выйти из команды' из приглашения — показываем то же подтверждение, что и /delete."""
    q = update.callback_query
    await q.answer()
    user = await ensure_user_record(q.from_user)
    team_name = await get_team_name(user.get('team_id'))
    warn = (
        f"Вы точно хотите выйти из текущей команды «{team_name}»?\n\n"
        "Будут удалены все ваши заявки, а вы будете удалены из команды. Это действие необратимо."
    )
    try:
        await q.edit_message_text(warn, reply_markup=yes_no_kb("delete"))
    except BadRequest:
        await context.bot.send_message(q.from_user.id, warn, reply_markup=yes_no_kb("delete"))

async def invite_decline_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Нажали 'Отклонить приглашение' — просто закрываем вопрос у пользователя."""
    q = update.callback_query
    await q.answer("Приглашение отклонено.")
    try:
        await q.edit_message_text("Вы отклонили приглашение.")
    except BadRequest:
        pass


async def delete_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка подтверждения/отмены выхода из команды."""
    q = update.callback_query
    await q.answer()
    ans = q.data.split(":")[1]

    if ans == "no":
        # Просто показываем главное меню — без изменений
        user = await ensure_user_record(q.from_user)
        try:
            await q.edit_message_text("Отмена. Вы остались в команде.", reply_markup=await build_main_menu(user["role"]))
        except BadRequest:
            # Если сообщение "не изменилось", пошлём новое меню
            await context.bot.send_message(q.from_user.id, "Главное меню:", reply_markup=await build_main_menu(user["role"]))
        return

    if ans == "yes":
        # Удаляем пользователя и его заявки
        user = await get_user_by_tg_id(q.from_user.id)
        if not user:
            await q.edit_message_text("Вы уже не состоите в команде. Нажмите /start, чтобы создать или присоединиться к команде.")
            return

        async with aiosqlite.connect(DB_FILE) as db:
            # Сначала чистим заявки пользователя
            await db.execute('DELETE FROM vacations WHERE user_id=?', (user['id'],))
            # На всякий случай уберём «зависшие» админ-действия, если он был админом
            await db.execute('DELETE FROM admin_actions WHERE admin_tg_id=?', (q.from_user.id,))
            # Теперь удаляем саму запись пользователя
            await db.execute('DELETE FROM users WHERE id=?', (user['id'],))
            await db.commit()

        # Чистим локальные визарды, если что-то было
        for k in ('admin_wizard','add_user_username','forbid_range','forbid_start',
                  'type_add_name','type_add_days','reg_mode'):
            context.user_data.pop(k, None)

        await q.edit_message_text(
            "Вы успешно вышли из команды.\n\n"
            "Теперь вы можете создать новую команду командой /start "
            "или попросить администратора другой команды добавить вас."
        )
        return

# ====== OTHER CMDS ======
async def cmd_myvacations(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(await get_my_vacations_text(update.effective_user.id))

async def cmd_allvacations(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = await ensure_user_record(update.effective_user)
    if context.args:
        try:
            year = int(context.args[0])
        except:
            await update.message.reply_text("Год указан неверно. Использование: /allvacations 2025")
            return
    else:
        year = datetime.now().year
    await update.message.reply_text(await get_all_vacations_text(user['team_id'], year))

async def cmd_list_types(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = await ensure_user_record(update.effective_user)
    txt, _ = await get_types_text_and_list(user['team_id'])
    await update.message.reply_text(txt)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "/start — регистрация/меню\n"
        "/whoami — моя роль и команда\n"
        "/setrole @user ROLE — смена роли (админ)\n"
        "/apply — подать заявку (интерактивно)\n"
        "/myvacations — мои отпуска\n"
        "/allvacations [YYYY] — отпуска команды за год\n"
        "/list_types — типы отпусков\n"
        "/delete — выйти из текущей команды и удалить учётку",
    )

async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Pong!")

# ====== BOOTSTRAP WIZARD (light) ======
async def cmd_bootstrap(update: Update, context: ContextTypes.DEFAULT_TYPE):
    caller = await ensure_user_record(update.effective_user)
    if not is_admin_role(caller['role']):
        await update.message.reply_text('Только администраторы.')
        return ConversationHandler.END
    await update.message.reply_text('Мастер настройки: отправьте лимит на год (напр. 28):')
    return BOOT_MAX_YEAR

async def boot_max_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
    caller = await ensure_user_record(update.effective_user)
    try:
        v = int(update.message.text.strip())
    except:
        await update.message.reply_text('Введите число, напр. 28')
        return BOOT_MAX_YEAR
    await set_team_setting(caller['team_id'], 'max_year_days', str(v))
    await update.message.reply_text('Лимит за один отпуск (напр. 14):')
    return BOOT_MAX_SINGLE

async def boot_max_single(update: Update, context: ContextTypes.DEFAULT_TYPE):
    caller = await ensure_user_record(update.effective_user)
    try:
        v = int(update.message.text.strip())
    except:
        await update.message.reply_text('Введите число, напр. 14')
        return BOOT_MAX_SINGLE
    await set_team_setting(caller['team_id'], 'max_single_days', str(v))
    pol = await get_team_setting(caller['team_id'], 'overlap_policy', DEFAULT_OVERLAP)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton(("✅ " if pol=="allow_all" else "") + "Разрешить всем", callback_data="boot:overlap:allow_all")],
        [InlineKeyboardButton(("✅ " if pol=="deny_all" else "") + "Запретить всем", callback_data="boot:overlap:deny_all")],
        [InlineKeyboardButton(("✅ " if pol=="deny_same_role" else "") + "Запретить только одинаковым ролям", callback_data="boot:overlap:deny_same_role")],
    ])
    await update.message.reply_text('Политика пересечений:', reply_markup=kb)
    return BOOT_OVERLAP

async def boot_set_overlap_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    pol = q.data.split(':')[2]
    caller = await ensure_user_record(q.from_user)
    await set_team_setting(caller['team_id'], 'overlap_policy', pol)
    await q.edit_message_text('Готово!')
    return ConversationHandler.END

# ====== REMINDERS JOB ======
async def reminders_job(context: ContextTypes.DEFAULT_TYPE):
    today = date.today()
    soon = {today + timedelta(days=3), today + timedelta(days=1)}
    async with aiosqlite.connect(DB_FILE) as db:
        # напоминаем пользователям
        for d in soon:
            cur = await db.execute(
                "SELECT v.id, u.tg_id, u.full_name, v.start_date, v.end_date "
                "FROM vacations v JOIN users u ON v.user_id=u.id "
                "WHERE v.status='approved' AND v.start_date=?",
                (d.isoformat(),)
            )
            rows = await cur.fetchall()
            for vid, tg, fname, s, e in rows:
                try:
                    when = "Через 3 дня" if d == today + timedelta(days=3) else "Завтра"
                    await context.bot.send_message(tg, f"{when} начинается ваш отпуск #{vid}: {s} — {e}")
                except Exception:
                    pass

# ====== ERROR HANDLER ======
async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled error: %s", context.error)

# ====== APP ======
async def main():
    await init_db()
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).concurrent_updates(True).build()

    # errors
    app.add_error_handler(on_error)

    # registration conversation
    reg_conv = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            REG_TEAM_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, reg_team_name)],
            REG_ROLE: [CallbackQueryHandler(reg_role_cb, pattern='^reg:role:')],

            REG_TYPES_Q: [CallbackQueryHandler(reg_types_q_cb, pattern='^reg_types:(yes|no)$')],
            TYPES_ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, types_add_name)],
            TYPES_ADD_DAYS: [MessageHandler(filters.TEXT & ~filters.COMMAND, types_add_days)],
            TYPES_ADD_DESC: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, types_add_desc),
                CallbackQueryHandler(types_add_desc_skip_cb, pattern='^types:add:skip_desc$'),
            ],

            REG_OVERLAP_Q: [
                CallbackQueryHandler(reg_overlap_q_cb, pattern='^reg_overlap:(yes|no)$'),
                CallbackQueryHandler(reg_set_overlap_cb, pattern='^reg:set_overlap:(allow_all|deny_all|deny_same_role)$'),
            ],
            REG_FORBIDDEN_Q: [
                CallbackQueryHandler(reg_forbidden_q_cb, pattern='^reg_forbidden:(yes|no)$'),
                CallbackQueryHandler(cal_forbid_nav_cb,  pattern='^calnav:forbid:(start|end):\d{4}-\d{2}$'),
                CallbackQueryHandler(cal_forbid_pick_cb, pattern='^calpick:forbid:(start|end):\d{4}-\d{2}-\d{2}$'),
            ],
            REG_ADD_USERS_Q: [CallbackQueryHandler(reg_add_users_q_cb, pattern='^reg_add_users:(yes|no)$')],
        },
        fallbacks=[],
        allow_reentry=True,
        per_message=False,
    )
    app.add_handler(reg_conv)

    app.add_handler(CallbackQueryHandler(reg_add_users_q_cb, pattern='^reg_add_users:(yes|no)$'))

    # main menu
    app.add_handler(CommandHandler('menu', cmd_menu))
    app.add_handler(CallbackQueryHandler(menu_cb, pattern='^(menu:(?!apply_manual).+|menu_apply_type:|apply_quick:)'))
    app.add_handler(CallbackQueryHandler(admin_skip_comment_cb, pattern='^admin:skip_comment$'))
    # глушилка для календаря
    app.add_handler(CallbackQueryHandler(noop_cb, pattern='^noop$'))

    # команда
    app.add_handler(CommandHandler('delete', cmd_delete))

    # колбэк подтверждения
    app.add_handler(CallbackQueryHandler(delete_cb, pattern='^delete:(yes|no)$'))

    app.add_handler(CallbackQueryHandler(invite_leave_cb, pattern='^invite:leave$'))
    app.add_handler(CallbackQueryHandler(invite_decline_cb, pattern='^invite:decline$'))

    # apply conversation
    apply_conv = ConversationHandler(
        entry_points=[
            CommandHandler('apply', cmd_apply),
            CallbackQueryHandler(apply_manual_cb, pattern='^menu:apply_manual$'),
        ],
        states={
            # выбор типа, быстрые диапазоны и ручной ввод — всё внутри состояния APPLY_TYPE
            APPLY_TYPE: [
                CallbackQueryHandler(apply_type_menu_cb, pattern='^apply_type:'),                  # если где-то останется старый префикс
                CallbackQueryHandler(apply_type_menu_cb, pattern='^menu_apply_type:\d+$'),         # наш текущий префикс
                CallbackQueryHandler(apply_manual_cb, pattern='^menu:apply_manual$'),
                CallbackQueryHandler(apply_quick_cb, pattern='^apply_quick:\d+:\d+$'),
            ],
            APPLY_START: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, apply_start_msg),
                CallbackQueryHandler(cal_apply_nav_cb,  pattern='^calnav:apply:start:\d{4}-\d{2}$'),
                CallbackQueryHandler(cal_apply_pick_cb, pattern='^calpick:apply:start:\d{4}-\d{2}-\d{2}$'),
            ],
            APPLY_END: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, apply_end_msg),
                CallbackQueryHandler(cal_apply_nav_cb,  pattern='^calnav:apply:end:\d{4}-\d{2}$'),
                CallbackQueryHandler(cal_apply_pick_cb, pattern='^calpick:apply:end:\d{4}-\d{2}-\d{2}$'),
            ],
            APPLY_COMMENT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, apply_comment_msg),
                CallbackQueryHandler(apply_skip_comment_cb, pattern='^apply:skip_comment$'),
                CallbackQueryHandler(apply_confirm_cb,    pattern='^apply:cancel$'),
            ],
            APPLY_CONFIRM: [
                CallbackQueryHandler(apply_confirm_cb, pattern='^apply:(confirm|cancel)$'),
                CommandHandler('cancel', apply_cancel_cmd),
            ],
        },
        fallbacks=[CommandHandler('cancel', apply_cancel_cmd)],
        allow_reentry=True,
        per_message=False
    )
    app.add_handler(apply_conv)

    # admin – order matters
    app.add_handler(CallbackQueryHandler(admin_action_cb, pattern='^admin_action:'))
    app.add_handler(CallbackQueryHandler(admin_menu_cb, pattern='^admin:'))

    # admin text wizard (должен быть ПОСЛЕ конверсейшенов)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, admin_text_handler))

    # commands
    app.add_handler(CommandHandler('whoami', cmd_whoami))
    app.add_handler(CommandHandler('setrole', cmd_setrole))
    app.add_handler(CommandHandler('myvacations', cmd_myvacations))
    app.add_handler(CommandHandler('allvacations', cmd_allvacations))
    app.add_handler(CommandHandler('list_types', cmd_list_types))
    app.add_handler(CommandHandler('help', cmd_help))
    app.add_handler(CommandHandler('ping', cmd_ping))

    # bootstrap wizard (опционально)
    boot_conv = ConversationHandler(
        entry_points=[CommandHandler('bootstrap', cmd_bootstrap)],
        states={
            BOOT_MAX_YEAR: [MessageHandler(filters.TEXT & ~filters.COMMAND, boot_max_year)],
            BOOT_MAX_SINGLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, boot_max_single)],
            BOOT_OVERLAP: [CallbackQueryHandler(boot_set_overlap_cb, pattern='^boot:overlap:(allow_all|deny_all|deny_same_role)$')],
        },
        fallbacks=[],
        allow_reentry=True,
        per_message=False
    )
    app.add_handler(boot_conv)

    # reminders
    if app.job_queue:
        app.job_queue.run_repeating(reminders_job, interval=3600, first=10)
    else:
        logger.warning("JobQueue не инициализирован. Установите extras: pip install 'python-telegram-bot[job-queue]'")

    logger.info('Starting bot (PTB v%s).', ptb_version)
    try:
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        try:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()
        except Exception:
            pass

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
