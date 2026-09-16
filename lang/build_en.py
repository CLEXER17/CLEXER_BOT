#!/usr/bin/env python3
"""Rebuilds lang/en.json - every English line the bot sends to a DM, one
per entry, numbers replaced by {}. Source: the headless capture run
(scratch en_lines.txt) plus the runtime lines listed in EXTRA below that a
capture cannot reach (payments, expiries, popups, validation errors).

The translation files lang/<code>.json map these exact English lines to
their translation. lang/check.py reports which lines a language misses."""

import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
import i18n  # noqa: E402

SRC = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "..", "..", "en_lines.txt")

DROP = ["Error: No Network", "{}🔴", "{}🟢", "{}🔵", "Language set to 🇧", "Language set to 🇨", "Language set to 🇪",
        "Language set to 🇫", "Language set to 🇬", "Language set to 🇮", "Language set to 🇵", "Language set to 🇷",
        "Language set to 🇸", "Language set to 🇹", "Language set to 🇻", "🇧🇷 Português", "🇪🇸 Español", "🇫🇷 Français",
        "🇬🇧 English", "🇮🇩 Bahasa", "🇵🇭 Filipino", "🇹🇷 Türkçe", "🇻🇳 Tiếng", "Sep {}  {}:{}", "Sep {} {}:{}",
        "Welcome Back, <b>Tester</b>", "<b>SOL</b>", "DOGE", "Block BTC", "Block SOL", "<b>Bhabani wins", "<b>Robo {} wins",
        "September {}", "🏆 <b>Bhabani", "🏆 <b>Robo", "Wave {}: "]

EXTRA = [
    "👋 Welcome back, <b>{}</b>!", "✨ <b>Welcome To CLEXER</b> 👤 User",
    "⚠️ No market data for <b>{}</b>. Check the ticker — e.g. <code>/mtf SOL</code>.",
    "⚠️ No price found for <b>{}</b>. Check the ticker, e.g. <code>/price SOL</code>.",
    "🚫 <b>{} Blocked</b>", "<blockquote>Currently Blocked: {}", "🚫 Block {}", "✅ {} (unblock)",
    "to Unblock: <code>/nocopy clear {}</code></blockquote>", "🏆 <b>{} wins!</b>", "✅ Language set to {}.",
    "🏅 Round {} to {}", "{} win", "{} ✅", "🌐 Use the bot in {}?", "✅ Yes",
    "🌐 Language is a personal setting - open it in a DM with me.", "📊 Virtual trading is personal - open it in a DM with me.",
    "🎉 <b>VIP Activated!</b>", "Paid: {} · {} days", "Paid: ⭐{} Stars · {} days", "Tap ⭐ VIP Channel in /help to get access.",
    "{} days — tap ⭐ VIP Channel in /help to get access.", "💰 <b>Wallet credited</b>: +{}", "💰 <b>Wallet credited</b>: +{} (⭐{} Stars)",
    "New balance: <b>{}</b>", "⏰ <b>Your VIP has expired</b>", "You've been removed from the VIP channel. Contact admin to renew.",
    "✅ <b>Welcome to the VIP channel!</b> Your request was auto-approved.", "💀 <b>Virtual trading stopped</b>",
    "Your virtual balance can no longer fund a trade, so the run is over. Open /virtual and press Reset to start a new one.",
    "📄 Your virtual trading run, before the reset. Everything below is now wiped.", "📄 Your virtual trading report for {}.",
    "📄 Virtual trading report · {}", "📄 Virtual trading report · whole run", "Report sent below.", "Virtual trading started.",
    "Paused - open trades still close normally, no new ones open.", "Running again.", "Run wiped.",
    "Reset wipes the balance and history. Your report is sent to you first. Tap Yes to confirm.", "Already running - Stop or Reset first.",
    "Not running.", "Nothing to resume.", "Capital is gone - Reset to start again.", "Numbers only - try again.", "Capital must be at least $1.",
    "Leverage must be 1-{}.", "Total capital must be at least $1.", "Leverage must be Auto or Manual.", "Leverage must be 1-{}x.",
    "Margin must be in $ or %.", "Margin per trade must be between $0.10 and your capital.", "Margin % must be 1-100.",
    "Risk must be in $ or %.", "Risk per trade must be more than $0.", "Risk % must be 0.1-100.",
    "Risk per trade can't be more than the margin per trade.", "Numbers only.", "PDF limit reached - {} exports this month (VIP plan).",
    "PDF limit reached - {} exports this month (Free plan).", "No trades in that month.", "No trades yet.",
    "⚠️ Virtual trading could not start: {}", "🔢 Send the <b>leverage</b> to use on every trade, {}-{} (for example <code>{}</code>).",
    "The balance can no longer fund a trade, so trading stopped. Reset to start a new run.", "💀 CAPITAL EXHAUSTED", "↺ Reset & set up again",
    "That game isn't ready yet.", "A game is already running in this chat. Finish or cancel it first.", "Friends join by tapping Join on the board.",
    "Pick a number from the buttons.", "{} takes {}-{} players.", "A game is already running in this chat.",
    "That board is old - use the latest one below.", "This game already started.", "You're already in.", "Only the host can add robots.",
    "The game already started - use Quit.", "Only the host can cancel.", "❌ Cancelled by the host.", "You're not in this game.",
    "The game is over - tap Play again.", "Answer the quit question first.", "You're out of this round.", "Not your turn - it's {}'s.", "Not now.",
    "The game is already over.", "Someone is already deciding whether to quit.", "Are you sure you want to leave? Tap Yes to quit.",
    "That question isn't for you.", "Everyone left - game closed. Send /games to play again.", "You left the game.", "The game is still going.",
    "{} left the game", "{} wins - everyone else left", "⌛ Nobody joined - lobby closed.", "⏱ {} idle - moved for them",
    "Something went wrong - try again.", "Yes, quit", "No, keep playing",
    "Anyone here can host. The host picks the number of players, the rest tap Join.", "That square is taken.", "Tap a square.", "Board full.",
    "Pick a column.", "That column is full.", "Pick rock, paper or scissors.", "You already picked - waiting for the others.", "Heads or tails?",
    "Pick a letter that hasn't been used.", "Type a number.", "1 to 100 only.", "Already open.", "Field cleared!", "Tap a card.",
    "That card is already face up.", "Attack, defend or heal.", "You already picked - waiting for the other player.", "Left, centre or right.",
    "Already dug there.", "Tap a piece.", "That piece has no legal move.", "Pick a piece first.", "Not a legal move.", "Stalemate", "Draw",
    "You must finish the jump.", "That piece can't move (jumps are compulsory).", "{} has no moves", "Already fired there.", "Hit or stand?",
    "Tap A, B, C or D.", "You already answered - waiting for the others.", "Pick a colour.", "That card doesn't match.", "Play a card or draw.",
    "Slide up, down, left or right.", "Nothing moves that way.", "Tap Ready first.", "Tap a colour.", "Type a word.", "Pick aim, then power.",
    "Pick aim first.", "Low, medium or high.", "Pick a move.", "Fight, hide or loot.", "Tap a tile.", "That tile isn't next to the gap.",
    "You already chose this wave.", "{} is the last one standing", "Double knockdown!", "🏆 KO! {}", "Tap Roll.", "Roll first.", "Pick a token.",
    "That token can't move.", "Pick a token first.", "{} chooses {}", "{} draws 4 and is skipped", "🔴 Skip", "🟢 Skip", "🔵 Skip", "🟡 Skip",
    "🔴 Reverse", "🟢 Reverse", "🔵 Reverse", "🟡 Reverse", "🔴 +2", "🟢 +2", "🔵 +2", "🟡 +2", "🌈 Wild", "🌈 Wild +4",
    "⚽ {} shoots left, keeper dives left - <b>GOAL!</b>", "⚽ {} shoots centre, keeper dives left - <b>GOAL!</b>",
    "⚽ {} shoots centre, keeper dives centre - <b>GOAL!</b>", "⚽ {} shoots right, keeper dives right - <b>GOAL!</b>",
    "🧤 {} shoots centre - <b>SAVED</b> by {}", "🧤 {} shoots right - <b>SAVED</b> by {}", "🤝 <b>Draw - nobody wins.</b>",
    "📅 <b>{}</b> · page {}/{}", "since {} · run #{}", "Wave {}",
    "📋 Menu", "🔤 Message font", "🌐 Language",
    # /out voting, idle and abandonment
    "You're not in any running game here.",
    "{} players",
    "Who should be voted out?",
    "↩ Cancel",
    "🗳 <b>Vote</b>",
    "Remove {} from the game?",
    "Votes: <b>{}/{}</b>",
    "voted: {}",
    "Only the players in this game can vote. Expires in 5 minutes.",
    "👍 Vote out",
    "👎 Keep",
    "🗳 Vote expired.",
    "This vote has expired.",
    "This vote is closed.",
    "You can't vote on yourself.",
    "That player already left.",
    "A vote on this player is already open.",
    "🗳 {} was voted out ({}/{}).",
    "Done.",
    "Vote counted.",
    "Vote withdrawn.",
    "Vote started.",
    "That game is over.",
    "{} was voted out",
    "{} was removed - inactive for 15 minutes",
    "⌛ Closed - no activity for 25 minutes.",
    "Start a vote to remove a player from a game you are in — the game's players decide",
    "🗳 <code>/out</code> — <b>Vote Out</b>",
    "🗳 Vote Out",
    "🔎 STILL SCANNING...",
    "Don't worry - you haven't missed anything. We are monitoring the market and waiting for the right setup.",
    "📡 MARKET UNDER WATCH...",
    "No need to chase a trade. The scanner is still active, and when the conditions align, you'll know.",
    "⚡ WE ARE ON IT...",
    "The market is being scanned right now. If there's a trade worth taking, we'll be ready. Until then, stay calm - the scan continues.",
    "🕐 CHECK BACK AT {}:{}",
    "🕐 NEXT SCAN AT {}:{}",
    "🕐 COME BACK AT {}:{} TOMORROW",
    "🕐 CHECK BACK AT {}:{} TOMORROW",
    "🕐 NEXT SCAN AT {}:{} TOMORROW",
    "📊 Session: NEW_YORK",
    "📊 Session: ASIA",
    "NEW_YORK Active",
    "ASIA Inactive",
    "🏷 Your Tier: <b>🆓 FREE</b>",
    "🏷 Tier: <b>🆓 FREE</b>",
    "Block {} from being auto-copied?",
    "{} {} {} {}:{} PM IST",
    "{} {} {} {}:{} AM IST",
    "⚠️ Couldn't create the payment link — try again shortly.",
    "⚠️ Couldn't create the Stars invoice — try again shortly.",
    "⚠️ Spin first.",
    "⚠️ Not enough balance ({}) — tap Add Funds first.",
    "<b>🎮 Games</b>",
    "<blockquote>Tap any command to run it instantly 👇</blockquote>",
    "part {}/{}",
    "📩 <b>Request received</b>",
    "Hey {} 👋",
    "Your request to join {} has been received - an admin will look at it shortly.",
    "Meanwhile, CLEXER is right here: live BTC and altcoin signals, copy trading and virtual trading.",
    "🤖 Open Bot",
    "👑 Get VIP",
    "❌ Your request to join {} was declined.",
    "🎵 Music plays in a group's voice chat. Add me to a group, start a voice chat there, and send /play song name.",
    "➕ Add me to a group",
    "📊 Virtual trading",
    "✅ <b>Approved</b> - welcome to {}! Say hi in the group.",
    "🎵 Music",
]


def main():
    lines = open(SRC, encoding="utf-8").read().split("\n")
    keep = []
    seen = set()
    for ln in lines:
        if any(d in ln for d in DROP):
            continue
        ln = i18n.strip_tg(ln)
        ln = ln.replace('<tg-emoji emoji-id="{}">', "")
        ln = re.sub(r"\s{2,}", " ", ln).strip()
        # numbers become {} so "🔴 +2" and "🔴 +4" are one line, and the
        # translations always carry the same placeholders
        ln = i18n.NUM.sub("{}", ln)
        if ln and i18n.key_of(ln) not in seen:
            seen.add(i18n.key_of(ln)); keep.append(ln)
    for e in EXTRA:
        e = i18n.NUM.sub("{}", e)
        if i18n.key_of(e) not in seen:
            seen.add(i18n.key_of(e)); keep.append(e)
    with open(os.path.join(HERE, "en.json"), "w", encoding="utf-8") as f:
        json.dump(keep, f, ensure_ascii=False, indent=0)
    print(len(keep), "english lines")


if __name__ == "__main__":
    main()
