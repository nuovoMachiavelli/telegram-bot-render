import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("TELEGRAM_ADMIN_ID", "0"))
MAIN_SHEET_ID = os.getenv("MAIN_SHEET_ID")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS")
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").rstrip("/")
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook")
PORT = int(os.getenv("PORT", "8080"))

# Таблицы менеджеров (те же самые)
MANAGER_SHEETS = [
    "1uURwa7q2o_PSzqkXAvobFk-iy1gS9k4JGFmevF1O7NU",
    "1od2y0ZwNpe7myLZfXqgN_Dpwx4fG2g69ByYTW-eECwU",
    "1qyWlfSRyK_3CPVbm2c1mBAeVyfli90R1s6NqNiHj8SY",
    "1nKqRES6loYGDbuc8f58APnrVPiEMPDdkqKr51QG5WzQ",
    "1wBolD2JNQwUXnuCuuANDjcOr6qIJgaFnyzcD0OkBkv4",
    "1VoKVBad6DdqiS8AzFGXfmI5-b4CQ6kT7n3aQVcR5WHA",
    "1bmpMh-VhB_yj6QM9L6ucOAKdwabuo75Zb8ycc7xqRAQ"
]
