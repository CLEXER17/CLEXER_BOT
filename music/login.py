"""One-time login for the music assistant account.

Run on your own PC:   python music/login.py
It asks for the assistant's phone number and the code Telegram sends, then
writes MUSIC_SESSION_STRING into music/.env (local, gitignored). Copy the
three MUSIC_* values into the Railway service. Never paste the string in chat.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ENV = os.path.join(HERE, ".env")


def _env():
    out = {}
    if os.path.exists(ENV):
        for ln in open(ENV, encoding="utf-8"):
            ln = ln.strip()
            if ln and not ln.startswith("#") and "=" in ln:
                k, v = ln.split("=", 1)
                out[k.strip()] = v.strip()
    return out


def main():
    try:
        from pyrogram import Client
    except ImportError:
        print("pip install pyrogram tgcrypto   (then run again)")
        sys.exit(1)
    e = _env()
    api_id, api_hash = e.get("MUSIC_API_ID"), e.get("MUSIC_API_HASH")
    if not api_id or not api_hash:
        print("music/.env needs MUSIC_API_ID and MUSIC_API_HASH first")
        sys.exit(1)
    print("Logging in the ASSISTANT account (not your main one).")
    with Client("clexer-music-login", api_id=int(api_id), api_hash=api_hash, in_memory=True) as app:
        me = app.get_me()
        s = app.export_session_string()
    lines = [ln for ln in open(ENV, encoding="utf-8").read().splitlines()
             if not ln.strip().startswith(("MUSIC_SESSION_STRING", "# MUSIC_SESSION_STRING"))]
    lines.append(f"MUSIC_SESSION_STRING={s}")
    with open(ENV, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"\nLogged in as {me.first_name} (@{me.username or '-'}). Session string saved to music/.env.")
    print("Copy MUSIC_API_ID, MUSIC_API_HASH and MUSIC_SESSION_STRING into the Railway music service.")


if __name__ == "__main__":
    main()
