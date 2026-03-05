import asyncio
import json
import os
from datetime import datetime

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram import types as aiogram_types

import gspread
from google.oauth2.service_account import Credentials
from aiohttp import web

# ================= НАСТРОЙКИ =================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID") or 0)
MAIN_SHEET_ID = os.getenv("MAIN_SHEET_ID")
GOOGLE_CREDS = json.loads(os.getenv("GOOGLE_CREDS") or "{}")

BASE_WEBHOOK_URL = os.getenv("BASE_WEBHOOK_URL")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
WEBAPP_HOST = "0.0.0.0"
WEBAPP_PORT = int(os.getenv("PORT", 8080))

MANAGER_SHEETS = [
    "1uURwa7q2o_PSzqkXAvobFk-iy1gS9k4JGFmevF1O7NU",
    "1od2y0ZwNpe7myLZfXqgN_Dpwx4fG2g69ByYTW-eECwU",
    "1qyWlfSRyK_3CPVbm2c1mBAeVyfli90R1s6NqNiHj8SY",
    "1nKqRES6loYGDbuc8f58APnrVPiEMPDdkqKr51QG5WzQ",
    "1wBolD2JNQwUXnuCuuANDjcOr6qIJgaFnyzcD0OkBkv4",
    "1VoKVBad6DdqiS8AzFGXfmI5-b4CQ6kT7n3aQVcR5WHA",
    "1bmpMh-VhB_yj6QM9L6ucOAKdwabuo75Zb8ycc7xqRAQ"
]

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

gc = None

def normalize_phone(raw):
    if not raw: return None
    s = ''.join(filter(str.isdigit, str(raw).strip()))
    if len(s) == 11 and s.startswith('8'):
        s = '7' + s[1:]
    elif len(s) == 10:
        s = '7' + s
    return s if len(s) == 11 and s.startswith('7') else None

# ================= АСИНХРОННЫЕ ОБЁРТКИ =================
async def async_open(spreadsheet_id):
    return await asyncio.to_thread(gc.open_by_key, spreadsheet_id)

async def async_worksheet(spreadsheet, title):
    return await asyncio.to_thread(spreadsheet.worksheet, title)

async def async_get_all_records(worksheet):
    return await asyncio.to_thread(worksheet.get_all_records)

async def async_col_values(worksheet, col):
    return await asyncio.to_thread(worksheet.col_values, col)

async def async_append_row(worksheet, row):
    return await asyncio.to_thread(worksheet.append_row, row)

async def async_update(worksheet, range_name, values):
    return await asyncio.to_thread(worksheet.update, range_name, values)

# ================= ОБРАБОТЧИК ОШИБОК =================
@dp.error()
async def error_handler(event: aiogram_types.ErrorEvent):
    print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {event.exception}")
    try:
        await event.update.message.answer("❌ Произошла ошибка.")
    except:
        pass

# ================= ПРИВЯЗКА НОМЕРА (с новым столбцом username из F) =================
async def process_phone(phone_norm: str, message: Message):
    print(f"DEBUG: Обработка номера {phone_norm}")
    try:
        spreadsheet = await async_open(MAIN_SHEET_ID)
        worksheet = await async_worksheet(spreadsheet, "Clients")
        values = await asyncio.to_thread(worksheet.get_all_values)

        row_index = None
        for i, row in enumerate(values[1:], start=2):
            if row and normalize_phone(row[0]) == phone_norm:
                row_index = i
                break

        found_in = None
        region = ""
        client_name = ""                    # ← НОВОЕ: имя из столбца F
        for idx, sid in enumerate(MANAGER_SHEETS, 1):
            try:
                s = await async_open(sid)
                sheet = await async_worksheet(s, "Общий")
                col_e = await async_col_values(sheet, 5)   # телефон
                col_b = await async_col_values(sheet, 2)   # регион
                col_f = await async_col_values(sheet, 6)   # ← ИМЯ (столбец F)

                for j in range(min(len(col_e), len(col_b), len(col_f))):
                    if normalize_phone(col_e[j]) == phone_norm:
                        found_in = f"Таблица {idx}"
                        region = str(col_b[j]).strip()
                        client_name = str(col_f[j]).strip()   # ← БЕРЁМ ИМЯ ИЗ F
                        break
                if found_in: break
            except Exception as e:
                print(f"Ошибка в таблице менеджера {idx}: {e}")
                continue

        if found_in:
            if row_index:
                # Обновляем всё, включая имя из F
                await async_update(worksheet, f"B{row_index}", [[message.chat.id]])
                await async_update(worksheet, f"C{row_index}", [[client_name]])   # ← username
                await async_update(worksheet, f"F{row_index}", [["привязан"]])
                await async_update(worksheet, f"G{row_index}", [[found_in]])
                await async_update(worksheet, f"H{row_index}", [[region]])
                await message.answer("✅ Вы успешно привязаны! Telegram ID и имя обновлены.")
            else:
                await async_append_row(worksheet, [
                    phone_norm, message.chat.id, client_name,          # ← username из F
                    message.from_user.full_name,
                    datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "привязан", found_in, region
                ])
                await message.answer("✅ Вы успешно привязаны!")
        else:
            await message.answer("❌ К сожалению, ваш номер не найден в базе.")
    except Exception as e:
        print(f"CRITICAL ERROR в process_phone: {e}")
        await message.answer("❌ Ошибка при обработке номера.")

# ================= АДМИН КОМАНДЫ =================
@dp.message(Command("sync"))
async def sync_clients(message: Message):
    print(f"DEBUG: /sync от {message.from_user.id}")
    if message.from_user.id != ADMIN_ID:
        await message.answer("Доступ запрещён.")
        return
    await message.answer("🔄 Запускаю синхронизацию...")
    try:
        spreadsheet = await async_open(MAIN_SHEET_ID)
        clients = await async_worksheet(spreadsheet, "Clients")
        data = await async_get_all_records(clients)
        existing = {normalize_phone(row.get("phone_normalized", "")) for row in data if row.get("phone_normalized")}
        added = 0
        for idx, sid in enumerate(MANAGER_SHEETS, 1):
            await message.answer(f"→ Проверяю таблицу менеджера {idx}/7...")
            try:
                s = await async_open(sid)
                sheet = await async_worksheet(s, "Общий")
                phones = await async_col_values(sheet, 5)
                regions = await async_col_values(sheet, 2)
                names = await async_col_values(sheet, 6)          # ← ИМЯ ИЗ F

                for i in range(1, len(phones)):
                    phone_norm = normalize_phone(phones[i])
                    if not phone_norm or phone_norm in existing: continue
                    region = regions[i] if i < len(regions) else ""
                    client_name = str(names[i]).strip() if i < len(names) else ""   # ← БЕРЁМ ИЗ F
                    await async_append_row(clients, [
                        phone_norm, "", client_name, "",               # ← username из F
                        datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "не привязан", f"Таблица {idx}", region
                    ])
                    existing.add(phone_norm)
                    added += 1
            except Exception as e:
                await message.answer(f"⚠️ Ошибка в таблице {idx}: {str(e)[:100]}")
                continue
        await message.answer(f"✅ СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА! Добавлено: {added}")
    except Exception as e:
        await message.answer(f"❌ Ошибка синхронизации: {str(e)}")

@dp.message(Command("stats"))
async def stats(message: Message):
    print(f"DEBUG: /stats от {message.from_user.id}")
    if message.from_user.id != ADMIN_ID:
        await message.answer("Доступ запрещён.")
        return
    try:
        spreadsheet = await async_open(MAIN_SHEET_ID)
        clients = await async_worksheet(spreadsheet, "Clients")
        data = await async_get_all_records(clients)
        total = len(data)
        bound = sum(1 for row in data if str(row.get("status", "")).lower() == "привязан")
        await message.answer(f"📊 Всего клиентов: {total} | Привязано: {bound}")
    except Exception as e:
        await message.answer(f"❌ Ошибка stats: {str(e)}")

@dp.message(Command("broadcast"))
async def broadcast_cmd(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Доступ запрещён.")
        return
    await message.answer("✅ Рассылка готова (заглушка)")

# ================= ОБЫЧНЫЕ ХЕНДЛЕРЫ =================
@dp.message(Command("start"))
async def start(message: Message):
    kb = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="📱 Поделиться номером", request_contact=True)]], resize_keyboard=True, one_time_keyboard=True)
    await message.answer("Привет! Нажми кнопку или напиши номер цифрами.", reply_markup=kb)

@dp.message(F.contact)
async def handle_contact(message: Message):
    print("DEBUG: Получен контакт")
    phone_norm = normalize_phone(message.contact.phone_number)
    if phone_norm:
        await process_phone(phone_norm, message)

@dp.message(F.text & ~F.command)
async def handle_manual_phone(message: Message):
    phone_norm = normalize_phone(message.text)
    if phone_norm:
        await message.answer("🔍 Проверяю номер...")
        await process_phone(phone_norm, message)

# ================= ЗАПУСК =================
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
    print("✅ Бот запущен на Render (username из столбца F добавлен)")

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
