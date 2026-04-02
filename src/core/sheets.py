"""
ОБЩИЙ МОДУЛЬ для работы с Google Sheets
Используется и Telegram-ботом, и MAX-ботом
"""

import asyncio
import json
import os
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import gspread
from google.oauth2.service_account import Credentials


class GoogleSheetsManager:
    """Класс для работы с Google Таблицами"""
    
    def __init__(self):
        self.gc = None
        self.main_sheet_id = os.getenv("MAIN_SHEET_ID")
        self.manager_sheets = [
            "1uURwa7q2o_PSzqkXAvobFk-iy1gS9k4JGFmevF1O7NU",
            "1od2y0ZwNpe7myLZfXqgN_Dpwx4fG2g69ByYTW-eECwU",
            "1qyWlfSRyK_3CPVbm2c1mBAeVyfli90R1s6NqNiHj8SY",
            "1nKqRES6loYGDbuc8f58APnrVPiEMPDdkqKr51QG5WzQ",
            "1wBolD2JNQwUXnuCuuANDjcOr6qIJgaFnyzcD0OkBkv4",
            "1VoKVBad6DdqiS8AzFGXfmI5-b4CQ6kT7n3aQVcR5WHA",
            "1bmpMh-VhB_yj6QM9L6ucOAKdwabuo75Zb8ycc7xqRAQ"
        ]
        
    async def connect(self):
        """Подключение к Google Sheets"""
        if self.gc is None:
            google_creds = json.loads(os.getenv("GOOGLE_CREDS") or "{}")
            creds = Credentials.from_service_account_info(
                google_creds,
                scopes=[
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"
                ]
            )
            self.gc = await asyncio.to_thread(gspread.authorize, creds)
        return self.gc
    
    @staticmethod
    def normalize_phone(raw: str) -> Optional[str]:
        """Нормализация номера телефона"""
        if not raw:
            return None
        s = ''.join(filter(str.isdigit, str(raw).strip()))
        if len(s) == 11 and s.startswith('8'):
            s = '7' + s[1:]
        elif len(s) == 10:
            s = '7' + s
        return s if len(s) == 11 and s.startswith('7') else None
    
    async def find_client_by_phone(self, phone_norm: str) -> Optional[Dict]:
        """
        Ищет клиента по номеру во всех таблицах менеджеров
        Возвращает: {region, client_name, source_table} или None
        """
        await self.connect()
        
        for idx, sheet_id in enumerate(self.manager_sheets, 1):
            try:
                spreadsheet = await asyncio.to_thread(self.gc.open_by_key, sheet_id)
                sheet = await asyncio.to_thread(spreadsheet.worksheet, "Общий")
                data = await asyncio.to_thread(sheet.get_all_values)
                
                for row in data[1:]:  # Пропускаем заголовок
                    if not isinstance(row, (list, tuple)) or len(row) < 6:
                        continue
                    if self.normalize_phone(row[4] if len(row) > 4 else "") == phone_norm:
                        return {
                            'region': str(row[1]).strip() if len(row) > 1 else "",
                            'client_name': str(row[5]).strip() if len(row) > 5 else "",
                            'source_table': f"Таблица {idx}"
                        }
            except Exception as e:
                print(f"Ошибка при поиске в таблице {idx}: {e}")
                continue
        
        return None
    
    async def get_or_create_client_in_main(self, phone_norm: str) -> Tuple[int, bool]:
        """
        Находит или создаёт клиента в главной таблице Clients
        Возвращает: (row_index, is_new)
        """
        await self.connect()
        spreadsheet = await asyncio.to_thread(self.gc.open_by_key, self.main_sheet_id)
        clients_sheet = await asyncio.to_thread(spreadsheet.worksheet, "Clients")
        values = await asyncio.to_thread(clients_sheet.get_all_values)
        
        # Ищем существующего
        for i, row in enumerate(values[1:], start=2):
            if isinstance(row, (list, tuple)) and len(row) > 0:
                if self.normalize_phone(row[0]) == phone_norm:
                    return i, False
        
        # Создаём нового
        await asyncio.to_thread(
            clients_sheet.append_row,
            [phone_norm, "", "", "не привязан", "", ""]
        )
        return len(values) + 1, True
    
    async def update_client_binding(self, phone_norm: str, chat_id: str, 
                                   client_name: str, source: str, region: str):
        """Обновляет привязку клиента к мессенджеру"""
        await self.connect()
        spreadsheet = await asyncio.to_thread(self.gc.open_by_key, self.main_sheet_id)
        clients_sheet = await asyncio.to_thread(spreadsheet.worksheet, "Clients")
        values = await asyncio.to_thread(clients_sheet.get_all_values)
        
        # Находим строку
        row_index = None
        for i, row in enumerate(values[1:], start=2):
            if isinstance(row, (list, tuple)) and len(row) > 0:
                if self.normalize_phone(row[0]) == phone_norm:
                    row_index = i
                    break
        
        if row_index:
            # Обновляем существующую строку
            updates = [
                {"range": f"B{row_index}", "values": [[chat_id]]},
                {"range": f"C{row_index}", "values": [[client_name]]},
                {"range": f"D{row_index}", "values": [["привязан"]]},
                {"range": f"E{row_index}", "values": [[source]]},
                {"range": f"F{row_index}", "values": [[region]]}
            ]
            await asyncio.to_thread(clients_sheet.batch_update, updates, value_input_option="RAW")
        else:
            # Создаём новую строку
            await asyncio.to_thread(clients_sheet.append_row, [
                phone_norm, chat_id, client_name, "привязан", source, region
            ])
    
    async def get_broadcast_data(self) -> List[Dict]:
        """Получает данные для рассылки из листа 'Рассылка'"""
        await self.connect()
        spreadsheet = await asyncio.to_thread(self.gc.open_by_key, self.main_sheet_id)
        rassylka = await asyncio.to_thread(spreadsheet.worksheet, "Рассылка")
        data = await asyncio.to_thread(rassylka.get_all_values)
        
        broadcasts = []
        for i, row in enumerate(data[1:], start=2):
            if not isinstance(row, (list, tuple)) or len(row) < 9:
                continue
            
            status = str(row[8]).strip().lower() if len(row) > 8 else ""
            if status not in ("новый", ""):
                continue
            
            broadcasts.append({
                'row_index': i,
                'phone_raw': str(row[2]) if len(row) > 2 else "",
                'shop_number': str(row[4]).strip() if len(row) > 4 else "—",
                'amount': str(row[5]).strip() if len(row) > 5 else "—",
                'link': str(row[6]).strip() if len(row) > 6 else "",
                'period': str(row[7]).strip() if len(row) > 7 else "—"
            })
        
        return broadcasts
    
    async def update_broadcast_status(self, row_index: int, status: str, 
                                     timestamp: Optional[str] = None):
        """Обновляет статус рассылки"""
        await self.connect()
        spreadsheet = await asyncio.to_thread(self.gc.open_by_key, self.main_sheet_id)
        rassylka = await asyncio.to_thread(spreadsheet.worksheet, "Рассылка")
        
        updates = [{"range": f"I{row_index}", "values": [[status]]}]
        if timestamp:
            updates.append({"range": f"J{row_index}", "values": [[timestamp]]})
        
        await asyncio.to_thread(rassylka.batch_update, updates, value_input_option="RAW")
    
    async def get_all_clients(self) -> Dict[str, str]:
        """Возвращает словарь phone_norm -> chat_id для всех привязанных клиентов"""
        await self.connect()
        spreadsheet = await asyncio.to_thread(self.gc.open_by_key, self.main_sheet_id)
        clients_sheet = await asyncio.to_thread(spreadsheet.worksheet, "Clients")
        values = await asyncio.to_thread(clients_sheet.get_all_values)
        
        phone_to_chat = {}
        for row in values[1:]:
            if isinstance(row, (list, tuple)) and len(row) > 1:
                phone_norm = self.normalize_phone(row[0])
                if phone_norm:
                    chat_id = str(row[1]).strip()
                    if chat_id and chat_id != "0":
                        phone_to_chat[phone_norm] = chat_id
        
        return phone_to_chat


# Создаём глобальный экземпляр для использования в ботах
sheets_manager = GoogleSheetsManager()
