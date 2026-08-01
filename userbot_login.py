"""
One-time interactive login for CLEXER BOT's userbot (CoinTrendzBot chart relay).

Run this LOCALLY on your own PC — NOT on Railway — once, using your SECOND
Telegram account (the one you added to the CoinTrendzBot group). It will ask
for that account's phone number, then the login code Telegram sends it, and
your 2FA password if that account has one set. At the end it prints a long
session string.

Copy that string into Railway as the TG_USER_SESSION_STRING environment
variable (along with TG_USER_API_ID and TG_USER_API_HASH, same values you
enter below). The bot then logs in automatically with it on every restart —
you never need to run this script again unless you log that account out
elsewhere or need a fresh session.

Before running this, get your API ID and API Hash:
  1. Go to https://my.telegram.org
  2. Log in with the SECOND account's phone number (the one in the group)
  3. Go to "API Development Tools"
  4. Create an app — any name/platform is fine, e.g. "CLEXER Relay" / Desktop
  5. Copy the "api_id" and "api_hash" it gives you

Install the one dependency this needs, then run:
    pip install telethon
    python userbot_login.py
"""
import asyncio
from telethon import TelegramClient
from telethon.sessions import StringSession


async def main():
    print("=== CLEXER BOT userbot login ===\n")
    api_id = input("API ID: ").strip()
    api_hash = input("API Hash: ").strip()
    print("\nA login code will be sent to that account now — check Telegram.\n")

    async with TelegramClient(StringSession(), int(api_id), api_hash) as client:
        me = await client.get_me()
        session_string = client.session.save()
        print(f"\nLogged in as: {me.first_name} (@{me.username or 'no username'})\n")
        print("=" * 70)
        print("SAVE THESE THREE VALUES AS RAILWAY ENVIRONMENT VARIABLES:\n")
        print(f"TG_USER_API_ID={api_id}")
        print(f"TG_USER_API_HASH={api_hash}")
        print(f"TG_USER_SESSION_STRING={session_string}")
        print("\n" + "=" * 70)
        print("\nDone. You can close this now — no need to run it again.")


if __name__ == "__main__":
    asyncio.run(main())
