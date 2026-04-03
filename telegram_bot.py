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
    if not raw:
        return None
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

async def async_append_rows(worksheet, rows_list):
    if not rows_list:
        return
    return await asyncio.to_thread(worksheet.append_rows, rows_list, value_input_option="RAW")

# ================= ОБРАБОТЧИК ОШИБОК =================
@dp.error()
async def error_handler(event: aiogram_types.ErrorEvent):
    print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {event.exception}")
    try:
        await event.update.message.answer("❌ Произошла ошибка.")
    except:
        pass

# ================= АВТОПРИВЯЗКА =================
async def process_phone(phone_norm: str, message: Message):
    print(f"\n=== DEBUG ПРИВЯЗКА ===\nНомер: {phone_norm} | Chat ID: {message.chat.id}")
    try:
        spreadsheet = await async_open(MAIN_SHEET_ID)
        clients = await async_worksheet(spreadsheet, "Clients")
        clients_values = await asyncio.to_thread(clients.get_all_values)
        
        # Поиск в таблицах менеджеров
        found_in = None
        region = ""
        client_name = ""
        for idx, sid in enumerate(MANAGER_SHEETS, 1):
            try:
                s = await async_open(sid)
                sheet = await async_worksheet(s, "Общий")
                data = await asyncio.to_thread(sheet.get_all_values)
                for row in data[1:]:
                    if not isinstance(row, (list, tuple)) or len(row) < 6:
                        continue
                    if normalize_phone(row[4] if len(row) > 4 else "") == phone_norm:
                        found_in = f"Таблица {idx}"
                        region = str(row[1]).strip() if len(row) > 1 else ""
                        client_name = str(row[5]).strip() if len(row) > 5 else ""
                        break
                if found_in:
                    break
            except:
                continue
        
        # Поиск в Clients
        row_index = None
        for i, row in enumerate(clients_values[1:], start=2):
            if isinstance(row, (list, tuple)) and len(row) > 0 and normalize_phone(row[0]) == phone_norm:
                row_index = i
                break
        
        if found_in:
            if row_index:
                await asyncio.gather(
                    asyncio.to_thread(clients.update, f"B{row_index}", [[message.chat.id]]),
                    asyncio.to_thread(clients.update, f"C{row_index}", [[client_name]]),
                    asyncio.to_thread(clients.update, f"D{row_index}", [["привязан"]]),
                    asyncio.to_thread(clients.update, f"E{row_index}", [[found_in]]),
                    asyncio.to_thread(clients.update, f"F{row_index}", [[region]])
                )
                await message.answer("✅ Вы успешно привязаны! Данные обновлены.")
            else:
                await asyncio.to_thread(clients.append_row, [
                    phone_norm, message.chat.id, client_name, "привязан", found_in, region
                ])
                await message.answer("✅ Вы успешно привязаны!")
            return
        
        await message.answer("❌ К сожалению, ваш номер не найден в базе.")
    except Exception as e:
        print(f"CRITICAL ERROR в process_phone: {e}")
        await message.answer("❌ Ошибка при обработке номера.")

# ================= СИНХРОНИЗАЦИЯ (полностью как в MAX) =================
@dp.message(Command("sync"))
async def sync_clients(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Доступ запрещён.")
        return

    await message.answer("🔄 Запускаю синхронизацию (batch_update)...")

    try:
        spreadsheet = await async_open(MAIN_SHEET_ID)
        clients = await async_worksheet(spreadsheet, "Clients")
        
        values = await asyncio.to_thread(clients.get_all_values)
        existing = {}
        for i, row in enumerate(values[1:], start=2):
            if isinstance(row, (list, tuple)) and len(row) > 0:
                phone_norm = normalize_phone(row[0])
                if phone_norm:
                    existing[phone_norm] = i

        new_rows = []
        batch_updates = []
        updated = 0
        added = 0

        for idx, sid in enumerate(MANAGER_SHEETS, 1):
            await message.answer(f"→ Проверяю таблицу менеджера {idx}/7...")
            try:
                s = await async_open(sid)
                sheet = await async_worksheet(s, "Общий")
                data = await asyncio.to_thread(sheet.get_all_values)

                for row in data[1:]:
                    if not isinstance(row, (list, tuple)) or len(row) < 6:
                        continue
                    
                    phone_raw = str(row[4]) if len(row) > 4 else ""
                    region = str(row[1]).strip() if len(row) > 1 else ""
                    client_name = str(row[5]).strip() if len(row) > 5 else ""
                    phone_norm = normalize_phone(phone_raw)
                    if not phone_norm:
                        continue

                    if phone_norm in existing:
                        r = existing[phone_norm]
                        batch_updates.append({
                            "range": f"C{r}:F{r}",
                            "values": [[client_name, None, f"Таблица {idx}", region]]
                        })
                        updated += 1
                    else:
                        new_rows.append([phone_norm, "", client_name, "не привязан", f"Таблица {idx}", region])
                        added += 1
            except Exception as e:
                await message.answer(f"⚠️ Ошибка в таблице {idx}: {str(e)[:100]}")
                continue

        if batch_updates:
            await asyncio.to_thread(clients.batch_update, batch_updates, value_input_option="RAW")
        if new_rows:
            await async_append_rows(clients, new_rows)

        await message.answer(f"""✅ СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА!
Добавлено новых: {added}
Обновлено ФИО/регион/источник: {updated}""")

    except Exception as e:
        print(f"CRITICAL SYNC ERROR: {e}")
        await message.answer(f"❌ Ошибка синхронизации: {str(e)}")

# ================= РАССЫЛКА (только из колонки H, статус в J, время в K) =================
@dp.message(Command("broadcast"))
async def broadcast_cmd(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Доступ запрещён.")
        return
    
    await message.answer("🚀 Запускаю рассылку (текст из колонки 'сообщение')...")
    
    try:
        spreadsheet = await async_open(MAIN_SHEET_ID)
        rassylka = await async_worksheet(spreadsheet, "Рассылка")
        clients_sheet = await async_worksheet(spreadsheet, "Clients")
        
        data = await asyncio.to_thread(rassylka.get_all_values)
        clients_data = await asyncio.to_thread(clients_sheet.get_all_values)
        
        # Маппинг телефон → chat_id
        phone_to_tg = {}
        for row in clients_data[1:]:
            if isinstance(row, (list, tuple)) and len(row) > 1:
                phone_norm = normalize_phone(row[0])
                if phone_norm:
                    tg_id = str(row[1]).strip()
                    if tg_id and tg_id != "0":
                        phone_to_tg[phone_norm] = tg_id
        
        status_updates = []   # колонка J (индекс 9)
        time_updates = []     # колонка K (индекс 10)
        
        sent = 0
        skipped_no_text = 0
        skipped_no_id = 0
        errors = 0
        batch_counter = 0
        
        for i, row in enumerate(data[1:], start=2):
            if len(row) < 10:
                continue
            
            # Статус в колонке J
            status = str(row[9]).strip().lower() if len(row) > 9 else ""
            if status not in ("новый", ""):
                continue
            
            # Текст из колонки H
            message_text = str(row[7]).strip() if len(row) > 7 and row[7] else ""
            if not message_text:
                status_updates.append({"range": f"J{i}", "values": [["нет текста"]]})
                skipped_no_text += 1
                batch_counter += 1
                continue
            
            # Телефон в колонке C
            phone_raw = str(row[2]) if len(row) > 2 else ""
            phone_norm = normalize_phone(phone_raw)
            if not phone_norm:
                continue
            
            tg_id = phone_to_tg.get(phone_norm)
            if not tg_id:
                status_updates.append({"range": f"J{i}", "values": [["нет Telegram ID"]]})
                skipped_no_id += 1
                batch_counter += 1
            else:
                try:
                    await bot.send_message(chat_id=int(tg_id), text=message_text)
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    status_updates.append({"range": f"J{i}", "values": [["отправлено"]]})
                    time_updates.append({"range": f"K{i}", "values": [[now]]})
                    sent += 1
                    batch_counter += 2
                    await asyncio.sleep(0.5)
                except Exception as e:
                    err_text = str(e)[:80]
                    status_updates.append({"range": f"J{i}", "values": [[f"ошибка: {err_text}"]]})
                    errors += 1
                    batch_counter += 1
            
            if batch_counter >= 50:
                if status_updates:
                    await asyncio.to_thread(rassylka.batch_update, status_updates, value_input_option="RAW")
                    status_updates = []
                if time_updates:
                    await asyncio.to_thread(rassylka.batch_update, time_updates, value_input_option="RAW")
                    time_updates = []
                batch_counter = 0
                await asyncio.sleep(1)
        
        if status_updates:
            await asyncio.to_thread(rassylka.batch_update, status_updates, value_input_option="RAW")
        if time_updates:
            await asyncio.to_thread(rassylka.batch_update, time_updates, value_input_option="RAW")
        
        await message.answer(f"""🎉 РАССЫЛКА ЗАВЕРШЕНА!
✅ Отправлено: {sent}
⏭ Пропущено (нет текста в H): {skipped_no_text}
⏭ Пропущено (нет Telegram ID): {skipped_no_id}
❌ Ошибок: {errors}""")
        
    except Exception as e:
        print(f"CRITICAL BROADCAST ERROR: {e}")
        await message.answer(f"❌ Критическая ошибка рассылки: {str(e)}")

# ================= ОБРАБОТЧИКИ КОМАНД И НОМЕРОВ =================
@dp.message(Command("start"))
async def start(message: Message):
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Поделиться номером", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await message.answer("Привет! Нажми кнопку или напиши номер цифрами.", reply_markup=kb)

@dp.message(F.contact)
async def handle_contact(message: Message):
    phone_norm = normalize_phone(message.contact.phone_number)
    if phone_norm:
        await process_phone(phone_norm, message)

@dp.message(F.text & ~F.command)
async def handle_manual_phone(message: Message):
    # Игнорируем сообщения, начинающиеся с '/'
    if message.text.startswith('/'):
        return
    phone_norm = normalize_phone(message.text)
    if phone_norm:
        await message.answer("🔍 Проверяю номер...")
        await process_phone(phone_norm, message)
    else:
        await message.answer("Пожалуйста, отправьте номер в правильном формате (только цифры).")

# ================= ЗАПУСК ВЕБХУКА =================
async def on_startup(bot: Bot):
    global gc
    creds = Credentials.from_service_account_info(
        GOOGLE_CREDS,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(creds)
    await bot.set_webhook(f"{BASE_WEBHOOK_URL.rstrip('/')}{WEBHOOK_PATH}")
    print(f"✅ Webhook установлен")

async def main():
    if not BASE_WEBHOOK_URL:
        print("❌ Укажи BASE_WEBHOOK_URL!")
        return
    
    dp.startup.register(on_startup)
    print("✅ Бот запущен | Синхронизация и рассылка работают")
    
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
