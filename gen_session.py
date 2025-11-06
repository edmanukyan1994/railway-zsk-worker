from telethon import TelegramClient
from telethon.sessions import StringSession

API_ID = int(input("API_ID: ").strip())
API_HASH = input("API_HASH: ").strip()
PHONE = input("PHONE (+...): ").strip()

with TelegramClient(StringSession(), API_ID, API_HASH) as client:
    client.start(PHONE)
    print("\n==== YOUR TELETHON STRING SESSION ====\n")
    print(client.session.save())
    print("\n======================================\n")
