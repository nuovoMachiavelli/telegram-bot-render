import asyncio
import json
import os
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

import gspread
from google.oauth2.service_account import Credentials
from aiohttp import web

# ================= НАСТРОЙКИ =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID") or 0)
MAIN_SHEET_ID = os.getenv("MAIN_SHEET_ID")
GOOGLE_CREDS = json.loads(os.getenv("GOOGLE_CREDS"))

BASE_WEBHOOK_URL = os.getenv("BASE_WEBHOOK_URL")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBAPP_HOST = "0.0.0.0"
WEBAPP_PORT = int(os.getenv("PORT", 8080))

MANAGER_SHEETS = [ ... ]  # твои 7 ID таблиц (оставь как было)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Глобальный gspread
gc = None

def normalize_phone(raw: str):
    if not raw: return None
    s = ''.join(filter(str.isdigit, str(raw).strip()))
    if len(s) == 11 and s.startswith('8'): s = '7' + s[1:]
    elif len(s) == 10: s = '7' + s
    return s if len(s) == 11 and s.startswith('7') else None

# ================= АСИНХРОННЫЕ ФУНКЦИИ (без изменений) =================
# ... (async_open, async_worksheet и т.д. — оставь как было раньше)

# ================= ПРИВЯЗКА НОМЕРА (без изменений) =================
# ... (process_phone, start, handle_contact, handle_manual_phone — оставь как было)

# ================= АДМИН КОМАНДЫ — ТЕПЕРЬ В САМОМ ВЕРХУ =================
@dp.message(Command("sync"))
async def sync_clients(message: Message):
    print(f"DEBUG: /sync received from user {message.from_user.id} (ADMIN_ID={ADMIN_ID})")
    if message.from_user.id != ADMIN_ID:
        await message.answer("Доступ запрещён.")
        return
    await message.answer("🔄 Запускаю синхронизацию...")
    # ... (весь остальной код синхронизации как был)

@dp.message(Command("stats"))
async def stats(message: Message):
    print(f"DEBUG: /stats received from user {message.from_user.id} (ADMIN_ID={ADMIN_ID})")
    if message.from_user.id != ADMIN_ID:
        await message.answer("Доступ запрещён.")
        return
    # ... (весь код stats как был)

@dp.message(Command("broadcast"))
async def broadcast_cmd(message: Message):
    print(f"DEBUG: /broadcast received from user {message.from_user.id}")
    if message.from_user.id != ADMIN_ID:
        await message.answer("Доступ запрещён.")
        return
    # ... (твой код рассылки)

# ================= ОБЫЧНЫЕ ХЕНДЛЕРЫ (после админ-команд!) =================
@dp.message(Command("start"))
async def start(message: Message):
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📱 Поделиться номером", request_contact=True)]], resize_keyboard=True, one_time_keyboard=True)
    await message.answer("Привет! Нажми кнопку или напиши номер цифрами.", reply_markup=kb)

@dp.message(F.contact)
async def handle_contact(message: Message):
    phone_norm = normalize_phone(message.contact.phone_number)
    if phone_norm:
        await process_phone(phone_norm, message)

@dp.message(F.text & ~F.command)
async def handle_manual_phone(message: Message):
    phone_norm = normalize_phone(message.text)
    if phone_norm:
        await message.answer("🔍 Проверяю номер...")
        await process_phone(phone_norm, message)

# ================= STARTUP И ЗАПУСК (без изменений) =================
async def on_startup(bot: Bot):
    global gc
    creds = Credentials.from_service_account_info(GOOGLE_CREDS, scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
    gc = gspread.authorize(creds)
    await bot.set_webhook(f"{BASE_WEBHOOK_URL.rstrip('/')}{WEBHOOK_PATH}")
    print(f"✅ Webhook установлен: {BASE_WEBHOOK_URL}{WEBHOOK_PATH}")

async def main():
    if not BASE_WEBHOOK_URL:
        print("❌ Укажи BASE_WEBHOOK_URL!")
        return
    dp.startup.register(on_startup)
    print("✅ Бот запущен (Render версия)")

    app = web.Application()
    webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=WEBAPP_HOST, port=WEBAPP_PORT)
    await site.start()
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
