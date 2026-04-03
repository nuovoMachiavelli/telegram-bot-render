import asyncio
import json
import gspread
from google.oauth2.service_account import Credentials

gc = None

def init_google_sheets(creds_json: str):
    global gc
    creds_dict = json.loads(creds_json)
    creds = Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(creds)

async def async_open(spreadsheet_id):
    return await asyncio.to_thread(gc.open_by_key, spreadsheet_id)

async def async_worksheet(spreadsheet, title):
    return await asyncio.to_thread(spreadsheet.worksheet, title)

async def async_get_all_values(worksheet):
    return await asyncio.to_thread(worksheet.get_all_values)

async def async_append_rows(worksheet, rows_list):
    if rows_list:
        await asyncio.to_thread(worksheet.append_rows, rows_list, value_input_option="RAW")

async def async_batch_update(worksheet, updates):
    if updates:
        await asyncio.to_thread(worksheet.batch_update, updates, value_input_option="RAW")
