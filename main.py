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

# ================= ГЛОБАЛЬНЫЙ ОБРАБОТЧИК ОШИБОК (чтобы видеть что сломалось) =================
@dp.error()
async def error_handler(event: aiogram_types.ErrorEvent):
    print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {event.exception}")
    print(f"Update: {event.update}")
    try:
        await event.update.message.answer("❌ Произошла ошибка. Админ уже в курсе.")
    except:
        pass

# ================= ПРИВЯЗКА НОМЕРА =================
async def process_phone(phone_norm: str, message: Message):
    print(f"DEBUG: Обработка номера {phone_norm} от пользователя {message.from_user.id}")
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
        for idx, sid in enumerate(MANAGER_SHEETS, 1):
            try:
                s = await async_open(sid)
                sheet = await async_worksheet(s, "Общий")
                col_e = await async_col_values(sheet, 5)
                col_b = await async_col_values(sheet, 2)
                for j in range(min(len(col_e), len(col_b))):
                    if normalize_phone(col_e[j]) == phone_norm:
                        found_in = f"Таблица {idx}"
                        region = str(col_b[j]).strip()
                        break
                if found_in: break
            except Exception as e:
                print(f"Ошибка в таблице менеджера {idx}: {e}")
                continue

        if found_in:
            if row_index:
                await async_update(worksheet, f"B{row_index}", [[message.chat.id]])
                await async_update(worksheet, f"F{row_index}", [["привязан"]])
                await async_update(worksheet, f"G{row_index}", [[found_in]])
                await async_update(worksheet, f"H{row_index}", [[region]])
                await message.answer("✅ Вы успешно привязаны! Telegram ID обновлён.")
            else:
                await async_append_row(worksheet, [
                    phone_norm, message.chat.id, message.from_user.username or "",
                    message.from_user.full_name, datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "привязан", found_in, region
                ])
                await message.answer("✅ Вы успешно привязаны!")
        else:
            await message.answer("❌ К сожалению, ваш номер не найден в базе.")
    except Exception as e:
        print(f"CRITICAL ERROR в process_phone: {type(e).__name__}: {e}")
        await message.answer("❌ Ошибка при обработке номера. Попробуйте позже.")

# ================= АДМИН КОМАНДЫ =================
@dp.message(Command("sync"))
async def sync_clients(message: Message):
    print(f"DEBUG: /sync от {message.from_user.id} (ADMIN_ID={ADMIN_ID})")
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
                for i in range(1, len(phones)):
                    phone_norm = normalize_phone(phones[i])
                    if not phone_norm or phone_norm in existing: continue
                    region = regions[i]
