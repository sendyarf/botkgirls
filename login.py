import os
import asyncio
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())
from pyrogram import Client
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
PHONE_NUMBER = os.getenv("PHONE_NUMBER", "")

async def main():
    if not API_ID or not API_HASH:
        print("API_ID atau API_HASH belum diset di .env")
        return
        
    print("Memulai proses login Pyrogram...")
    app = Client("user_session", api_id=API_ID, api_hash=API_HASH, phone_number=PHONE_NUMBER)
    
    await app.start()
    print("\n✅ LOGIN BERHASIL! File user_session.session telah dibuat.")
    await app.stop()

if __name__ == "__main__":
    asyncio.run(main())
