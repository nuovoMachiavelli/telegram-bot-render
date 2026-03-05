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
ADMIN_ID = int(os.getenv("ADMIN_ID"))
MAIN_SHEET_ID = os.getenv("MAIN_SHEET_ID")
GOOGLE_CREDS = json.loads(os.getenv("GOOGLE_CREDS"))

# Новые переменные для justrunmy.app (обязательно добавь в панели!)
BASE_WEBHOOK_URL = os.getenv("BASE_WEBHOOK_URL")  # https://твой-app.justrunmy.app
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

# Глобальный gspread клиент (одна авторизация на весь бот)
gc = None

def normalize_phone(raw: str):
    if not raw:
        return None
    s = ''.join(filter(str.isdigit, str(raw).strip()))
    if len(s) == 11 and s.startswith('8'):
        s = '7' + s[1:]
    elif len(s) == 10:
        s = '7' + s
    return s if len(s) == 11 and s.startswith('7') else None

# ================= АСИНХРОННЫЕ ФУНКЦИИ =================
async def async_open(spreadsheet_id: str):
    return await asyncio.to_thread(gc.open_by_key, spreadsheet_id)

async def async_worksheet(spreadsheet, title: str):
    return await asyncio.to_thread(spreadsheet.worksheet, title)

async def async_get_all_records(worksheet):
    return await asyncio.to_thread(worksheet.get_all_records)

async def async_col_values(worksheet, col: int):
    return await asyncio.to_thread(worksheet.col_values, col)

async def async_append_row(worksheet, row):
    return await asyncio.to_thread(worksheet.append_row, row)

async def async_update(worksheet, range_name, values):
    return await asyncio.to_thread(worksheet.update, range_name, values)

# ================= ПРИВЯЗКА НОМЕРА =================
async def process_phone(phone_norm: str, message: Message):
    spreadsheet = await async_open(MAIN_SHEET_ID)
    worksheet = await async_worksheet(spreadsheet, "Clients")
    values = await asyncio.to_thread(worksheet.get_all_values)

    # Поиск существующей строки
    row_index = None
    for i, row in enumerate(values[1:], start=2):
        if row and normalize_phone(row[0]) == phone_norm:
            row_index = i
            break

    # Поиск в таблицах менеджеров
    found_in = None
    region = ""
    for idx, sid in enumerate(MANAGER_SHEETS, 1):
        try:
            s = await async_open(sid)
            sheet = await async_worksheet(s, "Общий")
            col_e = await async_col_values(sheet, 5)  # телефоны
            col_b = await async_col_values(sheet, 2)  # регионы
            for j in range(min(len(col_e), len(col_b))):
                if normalize_phone(col_e[j]) == phone_norm:
                    found_in = f"Таблица {idx}"
                    region = str(col_b[j]).strip()
                    break
            if found_in:
                break
        except:
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

# ================= ХЕНДЛЕРЫ =================
@dp.message(Command("start"))
async def start(message: Message):
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Поделиться номером", request_contact=True)]],
        resize_keyboard=True, one_time_keyboard=True
    )
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

# ================= АДМИН КОМАНДЫ =================
@dp.message(Command("sync"))
async def sync_clients(message: Message):
    if message.from_user.id != ADMIN_ID:
        return await message.answer("Доступ запрещён.")
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
                    if not phone_norm or phone_norm in existing:
                        continue
                    region = regions[i] if i < len(regions) else ""
                    await async_append_row(clients, [
                        phone_norm, "", "", "", datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "не привязан", f"Таблица {idx}", region
                    ])
                    existing.add(phone_norm)
                    added += 1
            except Exception as e:
                await message.answer(f"⚠️ Ошибка в таблице {idx}: {str(e)[:100]}")
        await message.answer(f"✅ СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА! Добавлено новых клиентов: {added}")
    except Exception as e:
        await message.answer(f"❌ Ошибка синхронизации: {str(e)}")

@dp.message(Command("stats"))
async def stats(message: Message):
    if message.from_user.id != ADMIN_ID:
        return await message.answer("Доступ запрещён.")
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
        return await message.answer("Доступ запрещён.")
    try:
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            return await message.answer("❌ Укажи название листа: /broadcast ИмяЛиста")
        sheet_name = parts[1].strip()

        spreadsheet = await async_open(MAIN_SHEET_ID)
        sheet = await async_worksheet(spreadsheet, sheet_name)
        data = await asyncio.to_thread(sheet.get_all_values)

        if len(data) < 2:
            return await message.answer("Лист пустой.")

        await message.answer(f"🚀 Запускаю рассылку по листу «{sheet_name}»...")
        # ←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←
        # Здесь вставь свой старый рабочий код рассылки (он у тебя работал)
        # Пример простой заглушки:
        await message.answer("✅ Рассылка завершена! (вставь сюда свой код)")
        # ←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←
    except Exception as e:
        await message.answer(f"❌ Ошибка broadcast: {str(e)}")

# ================= STARTUP =================
async def on_startup(bot: Bot):
    global gc
    creds = Credentials.from_service_account_info(
        GOOGLE_CREDS,
        scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(creds)
    await bot.set_webhook(f"{BASE_WEBHOOK_URL.rstrip('/')}{WEBHOOK_PATH}")
    print(f"✅ Webhook установлен: {BASE_WEBHOOK_URL}{WEBHOOK_PATH}")

# ================= ЗАПУСК (WEBHOOK) =================
async def main():
    if not BASE_WEBHOOK_URL:
        print("❌ Укажи BASE_WEBHOOK_URL в переменных окружения!")
        return

    dp.startup.register(on_startup)
    print("✅ Бот запущен (webhook версия для justrunmy.app)")

    app = web.Application()
    webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=WEBAPP_HOST, port=WEBAPP_PORT)
    await site.start()

    await asyncio.Event().wait()  # держим бот живым

if __name__ == "__main__":
    asyncio.run(main())