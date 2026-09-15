#!/usr/bin/env python3
"""i18n.py - languages for the bot DM and the Mini App.

How it works (no live translation service, no AI, no cost):
  * lang/<code>.json holds, per language, a table  key -> translated line.
    The key is an English line with every number replaced by {} and the
    text case-folded, so "Balance: <b>$20.00</b>" and "balance: <b>$5</b>"
    share one entry.  Translations keep the {} in the same order and the
    same HTML tags.
  * Every Telegram call to a private chat passes through translate_payload()
    (bot.py's requests.post hook).  For a user whose language is not
    English each line of text / caption / button label is: un-styled (the
    admin's unicode font is undone, other alphabets cannot be styled
    anyway), looked up, and filled with the original numbers.  Lines the
    catalog does not know stay as they were.
  * A user's language lives in their copytrade record ("lang"), so the
    Mini App (api.py) sees the same value.
"""

import json
import os
import re

LANGS = [("en", "English", "🇬🇧"), ("ru", "Русский", "🇷🇺"), ("tl", "Filipino", "🇵🇭"),
         ("ur", "اردو", "🇵🇰"), ("id", "Bahasa Indonesia", "🇮🇩"), ("pt", "Português (BR)", "🇧🇷"),
         ("es", "Español", "🇪🇸"), ("ar", "العربية", "🇸🇦"), ("zh", "中文", "🇨🇳"),
         ("tr", "Türkçe", "🇹🇷"), ("vi", "Tiếng Việt", "🇻🇳"), ("fr", "Français", "🇫🇷")]
CODES = [c for c, _, _ in LANGS]
RTL = {"ur", "ar"}
LANG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lang")

_catalog: dict = {}            # lang -> {key: template}
lang_getter = None             # set by bot.py: cid -> "en"/"ru"/...
_TELEGRAM_TO_CODE = {"en": "en", "ru": "ru", "tl": "tl", "fil": "tl", "ur": "ur", "id": "id", "in": "id",
                     "pt": "pt", "pt-br": "pt", "es": "es", "ar": "ar", "zh": "zh", "zh-hans": "zh", "zh-cn": "zh",
                     "zh-hant": "zh", "zh-tw": "zh", "tr": "tr", "vi": "vi", "fr": "fr"}

# ── un-styling (undo the /font alphabets) ──────────────────────────────────
_REV: dict = {}


def _mk(up, lo, dig):
    for c in range(65, 91):
        _REV[chr(up + c - 65)] = chr(c)
    for c in range(97, 123):
        _REV[chr(lo + c - 97)] = chr(c)
    for c in range(48, 58):
        _REV[chr(dig + c - 48)] = chr(c)


for _args in ((0x1D400, 0x1D41A, 0x1D7CE), (0x1D5D4, 0x1D5EE, 0x1D7EC), (0x1D5A0, 0x1D5BA, 0x1D7E2),
              (0x1D670, 0x1D68A, 0x1D7F6), (0xFF21, 0xFF41, 0xFF10)):
    _mk(*_args)
for _a, _b in zip("abcdefghijklmnopqrstuvwxyz", "ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘǫʀꜱᴛᴜᴠᴡxʏᴢ"):
    _REV.setdefault(_b, _a)
_REV_TABLE = str.maketrans(_REV)


def unstyle(text: str) -> str:
    return text.translate(_REV_TABLE) if text else text


# ── keys ───────────────────────────────────────────────────────────────────
NUM = re.compile(r"\u2063[^\u2063]*\u2063|(?<!&#)(?<![A-Za-z_\d])-?\$?\d[\d,]*(?:\.\d+)?%?")
_SEG = " \u00b7 "
_WS = re.compile(r"\s+")

# Dates: "15 Sep 2026  01:45 PM IST", "September 2026". The month is treated
# as a dynamic value (like a number) so one catalog line covers every month,
# and the name itself is swapped for the local one afterwards.
_EN_MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August",
              "September", "October", "November", "December"]
_EN_ABBR = [m[:3] for m in _EN_MONTHS]
_MONTHS = {
    "ru": (["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"],
           ["Янв", "Фев", "Мар", "Апр", "Май", "Июн", "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек"]),
    "es": (["Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"],
           ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]),
    "pt": (["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"],
           ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]),
    "fr": (["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"],
           ["Janv", "Févr", "Mars", "Avr", "Mai", "Juin", "Juil", "Août", "Sept", "Oct", "Nov", "Déc"]),
    "tr": (["Ocak", "Şubat", "Mart", "Nisan", "Mayıs", "Haziran", "Temmuz", "Ağustos", "Eylül", "Ekim", "Kasım", "Aralık"],
           ["Oca", "Şub", "Mar", "Nis", "May", "Haz", "Tem", "Ağu", "Eyl", "Eki", "Kas", "Ara"]),
    "id": (["Januari", "Februari", "Maret", "April", "Mei", "Juni", "Juli", "Agustus", "September", "Oktober", "November", "Desember"],
           ["Jan", "Feb", "Mar", "Apr", "Mei", "Jun", "Jul", "Agu", "Sep", "Okt", "Nov", "Des"]),
    "vi": ([f"Tháng {i}" for i in range(1, 13)], [f"Thg {i}" for i in range(1, 13)]),
    "tl": (["Enero", "Pebrero", "Marso", "Abril", "Mayo", "Hunyo", "Hulyo", "Agosto", "Setyembre", "Oktubre", "Nobyembre", "Disyembre"],
           ["Ene", "Peb", "Mar", "Abr", "May", "Hun", "Hul", "Ago", "Set", "Okt", "Nob", "Dis"]),
    "ar": (["يناير", "فبراير", "مارس", "أبريل", "مايو", "يونيو", "يوليو", "أغسطس", "سبتمبر", "أكتوبر", "نوفمبر", "ديسمبر"], None),
    "ur": (["جنوری", "فروری", "مارچ", "اپریل", "مئی", "جون", "جولائی", "اگست", "ستمبر", "اکتوبر", "نومبر", "دسمبر"], None),
    "zh": ([f"{i}月" for i in range(1, 13)], None),
}
# a player called "Jan" or "May" is already inside markers - leave those alone
_MONTH_RE = re.compile("(?<!\u2063)" + r"\b(" + "|".join(_EN_MONTHS + _EN_ABBR) + r")\b" + "(?!\u2063)")
# months get a zero-width space inside the marker so a player named "May"
# (marked by the game, no ZWSP) is never mistaken for the month
_MARKED_MONTH_RE = re.compile("\u2063\u200b(" + "|".join(_EN_MONTHS + _EN_ABBR) + ")\u2063")


def _mark_months(line: str) -> str:
    return _MONTH_RE.sub(lambda m: "\u2063\u200b" + m.group(1) + "\u2063", line)


def _loc_months(line: str, lang: str) -> str:
    """Swap marked English month names for the language's own."""
    tbl = _MONTHS.get(lang)
    if not tbl or "\u2063" not in line:
        return line
    full, abbr = tbl
    abbr = abbr or full

    def _one(m):
        name = m.group(1)
        if name in _EN_MONTHS:
            return "\u2063" + full[_EN_MONTHS.index(name)] + "\u2063"
        return "\u2063" + abbr[_EN_ABBR.index(name)] + "\u2063"
    return _MARKED_MONTH_RE.sub(_one, line)


_TG = re.compile(r'<tg-emoji emoji-id="\d+">|</tg-emoji>')


def strip_tg(line: str) -> str:
    """Premium-emoji wrappers carry an id number and add nothing to the
    meaning - keys and translations work on the plain emoji."""
    return _TG.sub("", line)


def key_of(line: str) -> str:
    """Catalog key for an (un-styled) English line."""
    return _WS.sub(" ", NUM.sub("{}", strip_tg(line))).strip().casefold()


def _fill(tpl: str, vals: list) -> str:
    out = []
    i = 0
    for part in tpl.split("{}"):
        out.append(part)
        if i < len(vals):
            out.append(vals[i])
            i += 1
    return "".join(out)


# ── loading ────────────────────────────────────────────────────────────────

def load(path: str = None):
    global _catalog
    path = path or LANG_DIR
    cat = {}
    en = []
    try:
        with open(os.path.join(path, "en.json"), encoding="utf-8") as f:
            en = json.load(f)
    except Exception as e:
        print(f"[I18N] en.json: {e}")
    if os.path.isdir(path):
        for fn in os.listdir(path):
            if fn.endswith(".json") and fn[:-5] in CODES and fn[:-5] != "en":
                try:
                    with open(os.path.join(path, fn), encoding="utf-8") as f:
                        raw = json.load(f)
                    if isinstance(raw, list):
                        # aligned with en.json, entry for entry; "" = keep English
                        if len(raw) != len(en):
                            print(f"[I18N] {fn}: {len(raw)} entries, en.json has {len(en)} - loading what lines up")
                        cat[fn[:-5]] = {key_of(k): v for k, v in zip(en, raw) if v and v != k}
                    else:
                        cat[fn[:-5]] = {key_of(k): v for k, v in raw.items() if v}
                except Exception as e:
                    print(f"[I18N] {fn}: {e}")
    # Buttons with a premium icon lose their leading emoji ("🚫 Block BTC"
    # is sent as icon + "Block BTC"), so every emoji-led line also answers
    # to its bare form.
    for lang, m in cat.items():
        for k, v in list(m.items()):
            bare = _bare(k)
            if bare and bare not in m:
                m[bare] = _strip_lead(v)
    _catalog = cat
    return {k: len(v) for k, v in cat.items()}


def _lead_glyph(s: str):
    """Leading emoji token of a line, if it has one ("🚫 Block" -> "🚫")."""
    tok, _, rest = s.partition(" ")
    if rest and tok and len(tok) <= 3 and not any(ch.isalnum() for ch in tok) and tok not in ("-", "•", "·", "|", "+", "(", ")"):
        return tok, rest
    return None, s


def _bare(key: str):
    tok, rest = _lead_glyph(key)
    return rest.strip() if tok else None


def _strip_lead(tpl: str) -> str:
    tok, rest = _lead_glyph(tpl)
    return rest.strip() if tok else tpl


def has(lang: str) -> bool:
    return lang in _catalog


# ── per-user language ──────────────────────────────────────────────────────

def lang_of(cid) -> str:
    try:
        l = lang_getter(cid) if lang_getter else "en"
    except Exception:
        l = "en"
    return l if l in CODES else "en"


def from_telegram(code: str) -> str:
    """Telegram's language_code -> ours, or '' when we don't have it."""
    if not code:
        return ""
    c = code.lower()
    return _TELEGRAM_TO_CODE.get(c) or _TELEGRAM_TO_CODE.get(c.split("-")[0], "")


def name_of(code: str) -> str:
    for c, n, f in LANGS:
        if c == code:
            return f"{f} {n}"
    return code


# ── translating ────────────────────────────────────────────────────────────

def tr_line(line: str, lang: str) -> str:
    if not line or lang == "en" or lang not in _catalog:
        return line
    u = strip_tg(unstyle(line))
    cat = _catalog[lang]
    has_month = bool(_MONTH_RE.search(u))
    if has_month:
        u = _mark_months(u)
    tpl = cat.get(key_of(u))
    if tpl is not None:
        return _loc_months(_fill(tpl, NUM.findall(u)), lang)
    # A line inside a quote block keeps its wrapper tags; the catalog may
    # know it bare ("<blockquote>👋 Welcome back, <b>X</b>!").
    for pre, suf in (("<blockquote>", ""), ("", "</blockquote>"), ("<blockquote>", "</blockquote>")):
        if u.startswith(pre) and u.endswith(suf) and (pre or suf):
            core = u[len(pre):len(u) - len(suf)] if suf else u[len(pre):]
            tpl = cat.get(key_of(core))
            if tpl is not None:
                return pre + _loc_months(_fill(tpl, NUM.findall(core)), lang) + suf
    # Composite lines ("R3 · Ana 🥊 Jab (10) · Bo 🛡 Block (0)") are joined
    # from pieces with " · " - translate the pieces the catalog knows.
    if _SEG in u:
        parts = u.split(_SEG)
        hit = False
        out = []
        for p in parts:
            t = cat.get(key_of(p))
            if t is not None:
                hit = True
                out.append(_fill(t, NUM.findall(p)))
            else:
                out.append(p)
        if hit:
            return _loc_months(_SEG.join(out), lang)
    if has_month:
        return _loc_months(_mark_months(line), lang)
    return line


def tr_text(cid_or_lang, text: str) -> str:
    """Translate a whole message (multi-line HTML) for a user or a language."""
    lang = cid_or_lang if cid_or_lang in CODES else lang_of(cid_or_lang)
    if not text or lang == "en" or lang not in _catalog:
        return text
    return "\n".join(tr_line(ln, lang) for ln in text.split("\n"))


def tr_markup(cid_or_lang, markup):
    lang = cid_or_lang if cid_or_lang in CODES else lang_of(cid_or_lang)
    if not markup or lang == "en" or lang not in _catalog:
        return markup
    was_str = isinstance(markup, str)
    try:
        mk = json.loads(markup) if was_str else markup
    except Exception:
        return markup
    if isinstance(mk, dict) and "inline_keyboard" in mk:
        for row in mk["inline_keyboard"]:
            for b in row:
                if "text" in b:
                    b["text"] = tr_line(b["text"], lang)
    elif isinstance(mk, dict) and "keyboard" in mk:
        for row in mk["keyboard"]:
            for i, b in enumerate(row):
                if isinstance(b, dict) and "text" in b:
                    b["text"] = tr_line(b["text"], lang)
                elif isinstance(b, str):
                    row[i] = tr_line(b, lang)
    return json.dumps(mk, ensure_ascii=False) if was_str else mk


cb_user = None   # user id of the callback being handled - set by the bot's listener

_TEXT_METHODS = {"sendMessage", "editMessageText", "editMessageCaption", "sendPhoto", "editMessageMedia",
                 "sendDocument", "editMessageReplyMarkup", "sendAnimation", "sendVideo"}


def translate_payload(method: str, payload):
    """Rewrite an outgoing Telegram payload (dict for json=, dict for data=)
    for the recipient's language. Private chats only - groups and channels
    are shared, so they stay as written."""
    if not payload or not _catalog:
        return payload
    if method == "answerCallbackQuery":
        # A popup has no chat_id - the listener notes who tapped (cb_user).
        if payload.get("text") and cb_user is not None:
            payload["text"] = tr_text(lang_of(cb_user), payload["text"])
        return payload
    if method not in _TEXT_METHODS:
        return payload
    chat = payload.get("chat_id")
    try:
        if int(chat) <= 0:
            return payload
    except Exception:
        return payload
    lang = lang_of(chat)
    if lang == "en" or lang not in _catalog:
        return payload
    if payload.get("text"):
        payload["text"] = tr_text(lang, payload["text"])
    if payload.get("caption"):
        payload["caption"] = tr_text(lang, payload["caption"])
    if payload.get("media"):
        m = payload["media"]
        try:
            md = json.loads(m) if isinstance(m, str) else m
            if isinstance(md, dict) and md.get("caption"):
                md["caption"] = tr_text(lang, md["caption"])
                payload["media"] = json.dumps(md, ensure_ascii=False) if isinstance(m, str) else md
        except Exception:
            pass
    if payload.get("reply_markup"):
        payload["reply_markup"] = tr_markup(lang, payload["reply_markup"])
    return payload


load()
