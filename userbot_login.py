"""
One-time interactive login for CLEXER BOT's userbot (CoinTrendzBot chart relay).

Run this LOCALLY on your own PC — NOT on Railway — once, using your SECOND
Telegram account (the one you added to the CoinTrendzBot group). It will ask
for that account's phone number, then the login code Telegram sends it, and
your 2FA password if that account has one set. At the end it prints a long
session string, writes it to a file, and (on Windows) puts it straight on
your clipboard.

Copy that string into Railway as the TG_USER_SESSION_STRING environment
variable (along with TG_USER_API_ID and TG_USER_API_HASH, same values you
enter below). The bot then logs in automatically with it on every restart —
you never need to run this script again unless you log that account out
elsewhere or need a fresh session.

WHEN YOU NEED TO RE-RUN THIS
  Telegram permanently revokes a session the moment it is used from two IP
  addresses at once — the bot reports that as AuthKeyDuplicatedError, and no
  amount of reconnecting brings it back. Before generating a replacement,
  STOP any old or abandoned deploy that still has these env vars set, or the
  new session dies the same way within hours.

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
import os
import subprocess
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

        # Written to a file as well as printed. The session string is ~350
        # characters on a single line, which is genuinely awkward to select out
        # of a terminal without catching a line break — and a session string
        # with a stray newline in it fails later in a way that looks like a bad
        # login rather than a bad paste.
        out = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "userbot_session.txt")
        with open(out, "w", encoding="utf-8") as f:
            f.write(f"TG_USER_API_ID={api_id}\n")
            f.write(f"TG_USER_API_HASH={api_hash}\n")
            f.write(f"TG_USER_SESSION_STRING={session_string}\n")

        print("=" * 70)
        print("SAVE THESE THREE VALUES AS RAILWAY ENVIRONMENT VARIABLES:\n")
        print(f"TG_USER_API_ID={api_id}")
        print(f"TG_USER_API_HASH={api_hash}")
        print(f"TG_USER_SESSION_STRING={session_string}")
        print("=" * 70)
        print(f"\nAlso written to:\n  {out}")
        print("Open that file and copy from there — easier than selecting a")
        print("350-character line in a terminal.\n")

        # Windows: put it straight on the clipboard so there is nothing to select.
        if os.name == "nt":
            try:
                subprocess.run("clip", input=session_string.encode("utf-8"), check=True)
                print("The SESSION STRING is now on your clipboard — paste it "
                      "straight into Railway.\n")
            except Exception as e:
                print(f"(couldn't reach the clipboard: {e} — use the file above)\n")

        print("DELETE userbot_session.txt once it is in Railway. Anyone holding")
        print("that string has full access to this Telegram account.\n")
        print("Done. You can close this now — no need to run it again.")


if __name__ == "__main__":
    asyncio.run(main())
