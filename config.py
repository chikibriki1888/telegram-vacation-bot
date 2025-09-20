import os

# -- CONFIG ------------------------------------------------------------------
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN') or 'ТОКЕН'
VACATION_API_KEY = os.getenv('VACATION_API_KEY') or ''
VACATION_API_BASE = os.getenv('VACATION_API_BASE') or ''
VACATION_POST_ENDPOINT = VACATION_API_BASE.rstrip('/') + ''
DB_FILE = os.getenv('VACATION_BOT_DB') or 'vacation_bot.db'

# Predefined admin usernames (as requested)
PRESET_ADMINS = {
    '1': 'TECH',
    '2': 'TIMLID',
    '3': 'CEO',
}