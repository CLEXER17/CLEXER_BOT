#!/usr/bin/env python3
"""inlinebot.py - the short-name front bot (e.g. @pagelibot).

It exists so people can type a short @name anywhere on Telegram and get
CLEXER's answers. It runs on its own token, in its own poll loop, and owns
nothing: every card it posts carries a button into the main bot, and every
answer is produced by the main bot's own code (prices, live trades, the coin
engine), passed in by bot.py at start-up.

Three Telegram surfaces, one set of cards:
  * inline mode    - "@pagelibot eth" in the message box, the user picks a card
  * guest mode     - "@pagelibot eth" sent as a message in a chat the bot is
                     not a member of (Bot API 10.0); the bot posts the card
  * its own DM     - /start explains what it is and links to the main bot

The coin analysis is the only expensive answer: the card is posted first
("running…") and edited in place when the engine returns, so the chat is
never left waiting on a spinner that blocks anything.
"""

import base64
import json
import threading
import time
import traceback

import requests

TOKEN = ""
MAIN_USERNAME = ""          # the bot people are sent to
_me = {"username": ""}
_hooks = {}                 # filled by init(): price / live / analysis callables
_busy: dict = {}            # user id -> the moment their last analysis started


def init(token: str, main_username: str, hooks: dict):
    global TOKEN, MAIN_USERNAME
    TOKEN, MAIN_USERNAME = (token or "").strip(), (main_username or "").strip()
    _hooks.clear(); _hooks.update(hooks or {})


def _api(method: str, payload: dict, timeout: int = 15) -> dict:
    try:
        r = requests.post(f"https://api.telegram.org/bot{TOKEN}/{method}", json=payload, timeout=timeout)
        j = r.json()
        if not j.get("ok"):
            print(f"  [PAGE] {method}: {j.get('description')}")
        return j
    except Exception as e:
        print(f"  [PAGE] {method}: {e}")
        return {}


def _open_kb(label="📈 Open CLEXER", extra=None, start=""):
    """`start` is a /start payload: the button opens the main bot AND runs
    that command there (trades, games, music, vip, coin_ETH, chat)."""
    rows = []
    if MAIN_USERNAME:
        url = f"https://t.me/{MAIN_USERNAME}" + (f"?start={start}" if start else "")
        rows.append([{"text": label, "url": url}])
    if extra:
        rows = extra + rows
    return {"inline_keyboard": rows} if rows else None


# Adding a bot through a link always makes Telegram post "/start …" in the
# group - Telegram does that, not us, and other bots in the room may answer
# it. Asking for delete_messages in the same dialog lets CLEXER remove that
# line the instant it lands; pin_messages keeps the music card pinned and
# invite_users lets it bring @CLEXFM in without anyone doing it by hand.
ADD_RIGHTS = "delete_messages+pin_messages+invite_users"


def _addlink(payload: str) -> str:
    return f"https://t.me/{MAIN_USERNAME}?startgroup={payload}&admin={ADD_RIGHTS}"


def _article(id_, title, desc, text, kb=None):
    return {"type": "article", "id": f"{id_}_{int(time.time())}", "title": title, "description": desc,
            "input_message_content": {"message_text": text, "parse_mode": "HTML",
                                      "link_preview_options": {"is_disabled": True}},
            "reply_markup": kb or _open_kb()}


# ── the cards ──────────────────────────────────────────────────────────────
def _coin_cards(sym: str) -> list:
    """Price now, plus the two analysis choices for that coin."""
    out = []
    got = _hooks["price"](sym)
    if not got:
        return out
    s, px, chg = got
    fmt = f"{px:,.6g}" if px < 1 else f"{px:,.2f}"
    # the price card carries the two analysis choices, so whoever reads it in
    # the chat can go straight to a setup without leaving for the bot
    choose = [[{"text": "📈 Market entry", "callback_data": f"an:{s}:market"},
               {"text": "⏳ Pullback entry", "callback_data": f"an:{s}:pullback"}]]
    out.append(_article(f"px_{s}", f"{s}/USDT  {fmt}",
                        f"24h {'+' if chg >= 0 else ''}{chg:.2f}% · price + analysis buttons",
                        f"{'🟢' if chg >= 0 else '🔴'} <b>{s}/USDT</b>  <code>{fmt}</code>\n"
                        f"24h {'+' if chg >= 0 else ''}{chg:.2f}%\n\n"
                        f"<blockquote>🧠 Want a setup? Pick the entry style below and CLEX reads "
                        f"the market right here.</blockquote>",
                        _open_kb("🧠 Analyse in the bot", extra=choose, start=f"coin_{s}")))
    for mode, label, blurb in (("market", "Market entry", "an entry near the current price"),
                               ("pullback", "Pullback entry", "a zone to wait for")):
        out.append(_article(
            f"an_{s}_{mode}", f"{s} · {label} analysis", f"Full CLEX analysis — {blurb}",
            f"🧠 <b>{s}/USDT — {label.lower()} analysis</b>\n\n"
            f"<blockquote>⏳ Reading the market… the card fills in here in a few seconds.</blockquote>",
            _open_kb("🧠 Analyse in the bot", extra=[[{"text": "🔄 Run it", "callback_data": f"an:{s}:{mode}"}]],
                     start=f"coin_{s}")))
    return out


def _live_card():
    txt, n = _hooks["live"]()
    return _article("live", f"Live trades ({n})" if n else "Live trades — none right now",
                    f"{n} running · levels in the bot" if n else "Nothing running right now",
                    txt, _open_kb("📡 See the levels", start="trades"))


# the games people actually ask for by name; the rest stay behind /games
GAMES_QUICK = [("ludo", "🎲 Ludo", "4 players, robots fill the seats"),
               ("chess", "♟ Chess", "classic, against a robot or a friend"),
               ("snl", "🐍 Snake & Ladder", "roll and climb"),
               ("uno", "🃏 Uno-style cards", "first to empty their hand"),
               ("ttt", "❌ Tic-Tac-Toe", "three in a row"),
               ("c4", "🔴 Connect 4", "four in a line"),
               ("trivia", "🧠 Trivia quiz", "questions, fastest answer wins"),
               ("mines", "💣 Minesweeper", "clear the field"),
               ("ships", "🚢 Battleship", "sink the fleet"),
               ("rps", "✋ Rock Paper Scissors", "best of three")]


def _game_card(kind, title, blurb):
    txt = (f"{title}\n\n<blockquote>{blurb}\n\n"
           f"Tap below - the table opens right away, no menu.</blockquote>")
    extra = [[{"text": "▶ Play here (group)", "url": _addlink(f"game_{kind}")}]] if MAIN_USERNAME else None
    return _article(f"g_{kind}", title, blurb, txt, _open_kb("▶ Play now", extra, start=f"game_{kind}"))


def _games_here_card(chat_id):
    """Guest mode gives us the chat: the card lists the games and starts the
    chosen one right there (the main bot posts the board, so it must be in
    the group - the button says so when it is not)."""
    rows, row = [], []
    for kind, title, _ in GAMES_QUICK[:8]:
        row.append({"text": title, "callback_data": f"g:{kind}:{chat_id}"})
        if len(row) == 2:
            rows.append(row); row = []
    if row:
        rows.append(row)
    txt = ("🎮 <b>CLEXER Games</b>\n\n"
           "<blockquote>Pick a game below and the table opens in this chat.\n"
           "Friends tap <b>Join</b> on the board; empty seats can be robots.</blockquote>")
    return _article("games_here", "Games - play here", "Pick a game, the table opens in this chat",
                    txt, {"inline_keyboard": rows})

def _games_card():
    txt = ("🎮 <b>CLEXER Games</b>\n\n"
           "<blockquote>30 games inside Telegram — Ludo, Chess, Snake &amp; Ladder, Uno, Trivia,\n"
           "Battleship, Minesweeper, Mafia and more.\n\n"
           "Play against robots in DM, or with friends in any group.</blockquote>\n\n"
           "Send <code>/games</code> to start a table.")
    extra = [[{"text": "🎮 Play in a group", "url": _addlink("games")}]] if MAIN_USERNAME else None
    return _article("games", "CLEXER Games", "30 games — play in DM or in a group", txt,
                    _open_kb("🎮 Play in DM", extra, start="games"))


def _play_payload(prefix: str, what: str) -> str:
    """A /start payload carrying the song name (payloads allow only
    A-Za-z0-9_- and 64 characters, so the text rides along base64-encoded)."""
    if not what.strip():
        return prefix
    b = base64.urlsafe_b64encode(what.strip().encode()).decode().rstrip("=")
    return f"{prefix}_{b}"[:64]


def _music_card(query: str, video: bool = False):
    """One tap away from music: the button adds CLEXER to the group the
    user picks and starts this very song there (see bot.py, /start play_…)."""
    what = (query or "").strip()
    kind = "video" if video else "song"
    cmd = "/find" if video else "/play"
    head = "🎬 <b>CLEXER — play a video</b>" if video else "🎵 <b>CLEXER — play music</b>"
    if what:
        body = (f"{head}\n\n<b>{what}</b>\n\n"
                f"<blockquote>Tap <b>Play in a group</b> and pick the group.\n"
                f"CLEXER joins it and starts this {kind} - you only need the group's "
                f"voice chat to be running.</blockquote>")
        title = ("Play video · " if video else "Play · ") + what
    else:
        body = (f"{head}\n\n"
                f"<blockquote>Songs, full videos and live streams, in a group's voice chat.\n\n"
                f"Type <code>@{_me[chr(39) + chr(39)] if False else (_me['username'] or 'this bot')} {cmd} name</code> to pick one, or tap below to bring CLEXER into a group.</blockquote>")
        title = "Play a video" if video else "Play music"
    payload = _play_payload("find" if video else "play", what)
    extra = [[{"text": "▶ Play in a group", "url": _addlink(payload)}]] if MAIN_USERNAME else None
    return _article("music", title, "Plays in a group's voice chat · one tap to set up", body,
                    _open_kb("ℹ How it works", extra, start="music"))

def _about_card():
    txt = ("👋 <b>Welcome! Here is what I can do:</b>\n\n"
           "<blockquote>🛰 <b>Automated market scanning</b> — signals with entry, "
           "stop-loss and two targets\n"
           "📈 <b>Copy trading</b> — automatically copy supported trades on BingX\n"
           "📝 <b>Paper trading</b> — practice with virtual trades and monthly reports\n"
           "🎮 <b>30+ games</b> — play right inside Telegram\n"
           "🎵 <b>Music in voice chats</b> — listen together</blockquote>\n\n"
           "One bot. Multiple features. 🚀")
    return _article("about", "About CLEXER", "Signals, copy trading, paper trading, games, music",
                    txt, _open_kb("🚀 Start CLEXER", start="menu"))

def _vip_card():
    txt = ("👑 <b>VIP MEMBERS GET FULL ACCESS</b>\n\n"
           "<blockquote>⚡ Every signal generated by our scanners, delivered the moment it fires.\n"
           "📊 Entry • Stop-Loss • TP1 • TP2 with every trade.\n"
           "🔄 Copy Trading — automatically follow supported signals.\n"
           "📱 Full Mini App — all VIP tools, signals and trading features in one place.\n"
           "🎧 Exclusive Support — dedicated assistance for VIP members.</blockquote>")
    return _article("vip", "CLEXER VIP", "Full access — every signal, copy trading, Mini App, support",
                    txt, _open_kb("⭐ See VIP", start="vip"))

def _results(q: str) -> list:
    q = (q or "").strip()
    low = q.lower().lstrip("/")
    if not q:
        return [_live_card(), _about_card(), _games_card(), _music_card(""), _vip_card()]
    if low.startswith(("play", "music", "song")):
        return [_music_card(q.split(None, 1)[1] if " " in q else "")]
    if low.startswith(("find", "video", "episode", "movie")):
        return [_music_card(q.split(None, 1)[1] if " " in q else "", video=True)]
    if low.startswith(("game", "games", "play a game")):
        rest = q.split(None, 1)[1].strip().lower() if " " in q else ""
        cards = [_game_card(k, t, b) for k, t, b in GAMES_QUICK if not rest or rest in k or rest in t.lower()]
        return (cards or [_game_card(*GAMES_QUICK[0])])[:8] + [_games_card()]
    if low in ("trades", "trade", "running", "live", "signals", "signal", "open"):
        return [_live_card(), _vip_card(), _about_card()]
    if low in ("vip", "subscribe", "price of vip"):
        return [_vip_card(), _about_card()]
    if low in ("about", "help", "info", "clexer", "bot"):
        return [_about_card(), _vip_card()]
    for w in q.replace("?", " ").replace(",", " ").split()[:8]:
        cards = _coin_cards(w)
        if cards:
            return cards + [_live_card()]
    return [_about_card(), _live_card()]


# ── the analysis button on a posted card ───────────────────────────────────
def _edit_inline(inline_message_id: str, text: str, kb=None):
    _api("editMessageText", {"inline_message_id": inline_message_id, "text": text[:4096],
                             "parse_mode": "HTML", "link_preview_options": {"is_disabled": True},
                             **({"reply_markup": kb} if kb else {})})


def _run_analysis(inline_message_id: str, sym: str, mode: str, uid):
    """The engine call, off the poll thread: the card is already on screen."""
    try:
        text = _hooks["analysis"](sym, mode)
    except Exception as e:
        print(f"  [PAGE] analysis {sym} {mode}: {e}")
        traceback.print_exc()
        _edit_inline(inline_message_id,
                     f"🧠 <b>{sym}/USDT — {mode} analysis</b>\n\n"
                     f"<blockquote>⚠️ Couldn't read the market just now. Try again in a minute.</blockquote>",
                     _open_kb("🧠 Try in the bot", start=f"coin_{sym}"))
        return
    body = text if len(text) <= 3600 else text[:3600] + "\n…"
    _edit_inline(inline_message_id, body, _open_kb("📈 Trade this in CLEXER", start=f"coin_{sym}"))
    _busy.pop(str(uid), None)


def _on_callback(cb: dict):
    data = cb.get("data") or ""
    uid = (cb.get("from") or {}).get("id")
    imid = cb.get("inline_message_id")
    if data.startswith("g:"):
        _, kind, chat = data.split(":", 2)
        who = (cb.get("from") or {}).get("first_name") or "Player"
        res = _hooks.get("game", lambda *a: "nohook")(kind, chat, uid, who)
        if res == "ok":
            _api("answerCallbackQuery", {"callback_query_id": cb["id"], "text": "🎮 Table is open below."}, timeout=6)
            if imid:
                _edit_inline(imid, "🎮 <b>CLEXER Games</b>\n\n"
                                   "<blockquote>Table opened in this chat - scroll down and tap <b>Join</b>.</blockquote>",
                             _open_kb("🎮 More games", start="games"))
        elif res == "nobot":
            _api("answerCallbackQuery", {"callback_query_id": cb["id"],
                                         "text": "Add CLEXER to this group first - it posts the board.",
                                         "show_alert": True}, timeout=6)
            if imid:
                _edit_inline(imid, "🎮 <b>CLEXER Games</b>\n\n"
                                   "<blockquote>CLEXER has to be in this group to post the board.</blockquote>",
                             _open_kb("🎮 Play in DM", [[{"text": "➕ Add to this group",
                                                                 "url": _addlink("game_" + kind)}]],
                                      start=f"game_{kind}"))
        else:
            _api("answerCallbackQuery", {"callback_query_id": cb["id"], "text": "Could not start that one - try again."}, timeout=6)
        return
    if not data.startswith("an:") or not imid:
        _api("answerCallbackQuery", {"callback_query_id": cb["id"]}, timeout=6)
        return
    _, sym, mode = data.split(":", 2)
    last = _busy.get(str(uid), 0)
    if time.time() - last < 45:                       # one at a time per person
        _api("answerCallbackQuery", {"callback_query_id": cb["id"],
                                     "text": "⏳ Your last analysis is still running.", "show_alert": True}, timeout=6)
        return
    _busy[str(uid)] = time.time()
    _api("answerCallbackQuery", {"callback_query_id": cb["id"], "text": f"🧠 Reading {sym}…"}, timeout=6)
    _edit_inline(imid, f"🧠 <b>{sym}/USDT — {mode} analysis</b>\n\n"
                       f"<blockquote>⏳ Reading live price, structure and volume…</blockquote>")
    threading.Thread(target=_run_analysis, args=(imid, sym, mode, uid), daemon=True).start()


# ── the poll loop ──────────────────────────────────────────────────────────
def _start_text():
    return ("👋 <b>This is CLEXER's quick lane.</b>\n\n"
            "<blockquote>Type <code>@" + (_me["username"] or "this bot") + " btc</code> in any chat for a live price,\n"
            "<code>eth market</code> for an analysis, <code>trades</code> for what is running,\n"
            "<code>games</code>, <code>/play</code> or <code>/find</code> for the rest.</blockquote>\n\n"
            f"Everything itself lives in @{MAIN_USERNAME or 'the main bot'}.")


def _loop():
    offset = None
    try:
        me = requests.get(f"https://api.telegram.org/bot{TOKEN}/getMe", timeout=10).json()
        _me["username"] = ((me.get("result") or {}).get("username") or "")
        print(f"[PAGE] front bot @{_me['username']} ready -> @{MAIN_USERNAME}")
    except Exception as e:
        print(f"[PAGE] getMe: {e}")
    while True:
        try:
            r = requests.get(f"https://api.telegram.org/bot{TOKEN}/getUpdates",
                             params={"offset": offset, "timeout": 25,
                                     "allowed_updates": json.dumps(["inline_query", "chosen_inline_result",
                                                                    "callback_query", "message", "guest_message"])},
                             timeout=35).json()
            for upd in r.get("result", []):
                offset = upd["update_id"] + 1
                if upd.get("inline_query"):
                    iq = upd["inline_query"]
                    _api("answerInlineQuery", {"inline_query_id": iq["id"],
                                               "results": json.dumps(_results(iq.get("query", ""))),
                                               "cache_time": 15, "is_personal": True,
                                               "button": {"text": "Open CLEXER", "start_parameter": "inline"}})
                elif upd.get("guest_message"):
                    gm = upd["guest_message"]
                    gq = gm.get("guest_query_id")
                    if gq:
                        txt = (gm.get("text") or gm.get("caption") or "")
                        if _me["username"]:
                            i = txt.lower().find("@" + _me["username"].lower())
                            if i >= 0:
                                txt = txt[:i] + txt[i + len(_me["username"]) + 1:]
                        q = txt.strip()
                        low = q.lower().lstrip("/")
                        chat = (gm.get("chat") or {}).get("id")
                        if chat and low.startswith(("game", "games")):
                            card = _games_here_card(chat)          # playable list, right here
                        else:
                            card = _results(q)[0]
                        _api("answerGuestQuery", {"guest_query_id": gq, "result": json.dumps(card)})
                elif upd.get("chosen_inline_result"):
                    # the user picked a card - if it was an analysis card it runs
                    # right away, no second tap. Needs Inline Feedback on in
                    # BotFather; the card keeps a "Run it" button for when it is off.
                    cr = upd["chosen_inline_result"]
                    rid, imid = cr.get("result_id", ""), cr.get("inline_message_id")
                    uid = (cr.get("from") or {}).get("id")
                    if rid.startswith("an_") and imid:
                        bits = rid.split("_")
                        sym, mode = (bits[1], bits[2]) if len(bits) > 2 else ("", "")
                        if sym and mode and time.time() - _busy.get(str(uid), 0) > 5:
                            _busy[str(uid)] = time.time()
                            _edit_inline(imid, f"🧠 <b>{sym}/USDT — {mode} analysis</b>\n\n"
                                               f"<blockquote>⏳ Reading live price, structure and volume…</blockquote>")
                            threading.Thread(target=_run_analysis, args=(imid, sym, mode, uid), daemon=True).start()
                elif upd.get("callback_query"):
                    _on_callback(upd["callback_query"])
                elif upd.get("message"):
                    m = upd["message"]
                    if str(m.get("text", "")).startswith("/start"):
                        _api("sendMessage", {"chat_id": m["chat"]["id"], "text": _start_text(),
                                             "parse_mode": "HTML", "reply_markup": _open_kb("🚀 Open CLEXER", start="menu")})
        except Exception as e:
            print(f"[PAGE] loop: {e}")
            time.sleep(5)


def start():
    if not TOKEN:
        return False
    threading.Thread(target=_loop, daemon=True, name="pagebot").start()
    return True
