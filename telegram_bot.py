import asyncio
import logging
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

from config import TELEGRAM_BOT_TOKEN, ADMIN_ID, MAIN_SHEET_ID, MANAGER_SHEETS, WEBHOOK_URL, WEBHOOK_PATH, PORT, GOOGLE_CREDS_JSON
from google_sheets import init_google_sheets, async_open, async_worksheet, async_get_all_values, async_append_rows, async_batch_update

logging.basicConfig(level=logging.INFO)

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

# ------------------- Функции работы с номерами -------------------
def normalize_phone(raw):
    if not raw:
        return None
    s = ''.join(filter(str.isdigit, str(raw).strip()))
    if len(s) == 11 and s.startswith('8'):
        s = '7' + s[1:]
    elif len(s) == 10:
        s = '7' + s
    return s if len(s) == 11 and s.startswith('7') else None

async def process_phone(phone_norm: str, user_id: int, user_name: str = ""):
    logging.info(f"Привязка: номер {phone_norm}, user_id {user_id}")
    try:
        spreadsheet = await async_open(MAIN_SHEET_ID)
        clients_ws = await async_worksheet(spreadsheet, "Clients")
        clients_values = await async_get_all_values(clients_ws)

        # Поиск в таблицах менеджеров
        found_in = None
        region = ""
        client_name = ""
        for idx, sid in enumerate(MANAGER_SHEETS, 1):
            try:
                s = await async_open(sid)
                sheet = await async_worksheet(s, "Общий")
                data = await async_get_all_values(sheet)
                for row in data[1:]:
                    if len(row) < 6:
                        continue
                    phone_raw = str(row[4]) if len(row) > 4 else ""
                    if normalize_phone(phone_raw) == phone_norm:
                        found_in = f"Таблица {idx}"
                        region = str(row[1]).strip() if len(row) > 1 else ""
                        client_name = str(row[5]).strip() if len(row) > 5 else ""
                        break
                if found_in:
                    break
            except Exception as e:
                logging.error(f"Ошибка в таблице {idx}: {e}")
                continue

        # Поиск в Clients
        row_index = None
        for i, row in enumerate(clients_values[1:], start=2):
            if len(row) > 0 and normalize_phone(row[0]) == phone_norm:
                row_index = i
                break

        if found_in:
            if row_index:
                await async_batch_update(clients_ws, [
                    {"range": f"B{row_index}", "values": [[user_id]]},
                    {"range": f"C{row_index}", "values": [[client_name]]},
                    {"range": f"D{row_index}", "values": [["привязан"]]},
                    {"range": f"E{row_index}", "values": [[found_in]]},
                    {"range": f"F{row_index}", "values": [[region]]}
                ])
                await bot.send_message(user_id, "✅ Вы успешно привязаны! Данные обновлены.")
            else:
                await async_append_rows(clients_ws, [[
                    phone_norm, user_id, client_name, "привязан", found_in, region
                ]])
                await bot.send_message(user_id, "✅ Вы успешно привязаны!")
            return
        else:
            await bot.send_message(user_id, "❌ К сожалению, ваш номер не найден в базе.")
    except Exception as e:
        logging.exception("Ошибка в process_phone")
        await bot.send_message(user_id, "❌ Ошибка при обработке номера.")

# ------------------- Обработчики команд -------------------
@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Поделиться номером", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await message.answer("Привет! Нажмите кнопку, чтобы поделиться номером телефона, или отправьте номер цифрами.", reply_markup=kb)

@dp.message(F.contact)
async def handle_contact(message: types.Message):
    phone_norm = normalize_phone(message.contact.phone_number)
    if phone_norm:
        await process_phone(phone_norm, message.from_user.id, message.from_user.full_name)
    else:
        await message.answer("❌ Не удалось распознать номер.")

@dp.message(F.text & ~F.command)
async def handle_text(message: types.Message):
    phone_norm = normalize_phone(message.text)
    if phone_norm:
        await message.answer("🔍 Проверяю номер...")
        await process_phone(phone_norm, message.from_user.id, message.from_user.full_name)
    else:
        await message.answer("Пожалуйста, отправьте номер в правильном формате (только цифры).")

@dp.message(Command("sync"))
async def sync_cmd(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Доступ запрещён.")
        return
    await message.answer("🔄 Запускаю синхронизацию...")
    try:
        spreadsheet = await async_open(MAIN_SHEET_ID)
        clients_ws = await async_worksheet(spreadsheet, "Clients")
        clients_values = await async_get_all_values(clients_ws)

        existing = {}
        for i, row in enumerate(clients_values[1:], start=2):
            phone_norm = normalize_phone(row[0]) if len(row) > 0 else None
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
                data = await async_get_all_values(sheet)
                for row in data[1:]:
                    if len(row) < 6:
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
            await async_batch_update(clients_ws, batch_updates)
        if new_rows:
            await async_append_rows(clients_ws, new_rows)

        await message.answer(f"✅ СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА!\nДобавлено новых: {added}\nОбновлено ФИО/регион/источник: {updated}")
    except Exception as e:
        logging.exception("Sync error")
        await message.answer(f"❌ Ошибка синхронизации: {str(e)}")

@dp.message(Command("broadcast"))
async def broadcast_cmd(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("Доступ запрещён.")
        return
    await message.answer("🚀 Запускаю рассылку (только из колонки 'сообщение')...")
    try:
        spreadsheet = await async_open(MAIN_SHEET_ID)
        rassylka_ws = await async_worksheet(spreadsheet, "Рассылка")
        clients_ws = await async_worksheet(spreadsheet, "Clients")

        data = await async_get_all_values(rassylka_ws)
        clients_data = await async_get_all_values(clients_ws)

        phone_to_user = {}
        for row in clients_data[1:]:
            if len(row) > 1:
                phone_norm = normalize_phone(row[0])
                if phone_norm:
                    user_id_str = str(row[1]).strip()
                    if user_id_str and user_id_str != "0":
                        try:
                            phone_to_user[phone_norm] = int(user_id_str)
                        except:
                            pass

        status_updates = []
        time_updates = []
        sent = 0
        skipped_no_text = 0
        skipped_no_id = 0
        errors = 0
        batch_counter = 0

        for i, row in enumerate(data[1:], start=2):
            if len(row) < 10:
                continue
            status = str(row[9]).strip().lower() if len(row) > 9 else ""
            if status not in ("новый", ""):
                continue

            message_text = str(row[7]).strip() if len(row) > 7 and row[7] else ""
            if not message_text:
                status_updates.append({"range": f"J{i}", "values": [["нет текста"]]})
                skipped_no_text += 1
                batch_counter += 1
                continue

            phone_raw = str(row[2]) if len(row) > 2 else ""
            phone_norm = normalize_phone(phone_raw)
            if not phone_norm:
                continue

            user_id = phone_to_user.get(phone_norm)
            if not user_id:
                status_updates.append({"range": f"J{i}", "values": [["нет Telegram ID"]]})
                skipped_no_id += 1
                batch_counter += 1
            else:
                try:
                    await bot.send_message(user_id, message_text)
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
                    await async_batch_update(rassylka_ws, status_updates)
                    status_updates = []
                if time_updates:
                    await async_batch_update(rassylka_ws, time_updates)
                    time_updates = []
                batch_counter = 0
                await asyncio.sleep(1)

        if status_updates:
            await async_batch_update(rassylka_ws, status_updates)
        if time_updates:
            await async_batch_update(rassylka_ws, time_updates)

        await message.answer(f"🎉 РАССЫЛКА ЗАВЕРШЕНА!\n✅ Отправлено: {sent}\n⏭ Нет текста в H: {skipped_no_text}\n⏭ Нет Telegram ID: {skipped_no_id}\n❌ Ошибок: {errors}")
    except Exception as e:
        logging.exception("Broadcast error")
        await message.answer(f"❌ Критическая ошибка рассылки: {str(e)}")

# ------------------- Запуск вебхука -------------------
async def on_startup(bot: Bot):
    await bot.set_webhook(f"{WEBHOOK_URL}{WEBHOOK_PATH}")
    logging.info(f"Webhook set to {WEBHOOK_URL}{WEBHOOK_PATH}")

async def main():
    init_google_sheets(GOOGLE_CREDS_JSON)
    logging.info("Google Sheets initialized")
    dp.startup.register(on_startup)

    app = web.Application()
    webhook_handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    webhook_handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()
    logging.info(f"Server started on port {PORT}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
