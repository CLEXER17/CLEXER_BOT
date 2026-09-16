# CLEXER Music

Voice-chat music for Telegram groups. `bot.py` owns the commands and buttons;
this service owns the assistant account that joins the voice chat and streams.

Railway: new service from this repo, **Root Directory = `music`** (it builds
from the Dockerfile here). Variables:

| name | value |
|---|---|
| `MUSIC_API_ID` / `MUSIC_API_HASH` | from my.telegram.org (the assistant's app) |
| `MUSIC_SESSION_STRING` | from `python music/login.py` |
| `TELEGRAM_BOT_TOKEN` | the bot's token (used only to send the now-playing card) |
| `MUSIC_SECRET` | any long random string - the same value goes on the bot service |
| `CLEXER_API_URL` / `PUSH_STATE_SECRET` | optional, same values as on the bot service: lets a redeploy resume the music where it was |

Bot service gets two extra variables: `MUSIC_URL` (this service's public URL,
e.g. `https://clexer-music.up.railway.app`) and the same `MUSIC_SECRET`.

Users: `/play <song or YouTube link>` in a group with a running voice chat;
`/skip` `/pause` `/resume` `/stop` `/queue` `/now` `/volume 80`. The assistant
account must be a member of the group (the bot invites it when it has the
invite-users right; otherwise add it by hand).
