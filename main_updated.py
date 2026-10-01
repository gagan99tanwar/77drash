import asyncio
import json
import os
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from telethon import TelegramClient, events
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

# Create the Telegram client before registering event handlers.

# ============================== CONFIG ======================================

API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
STRING_SESSION = os.environ.get("STRING_SESSION", "")
client = TelegramClient(
    StringSession(STRING_SESSION),
    API_ID,
    API_HASH,
)

INTERVAL_SECONDS = int(os.environ.get("INTERVAL_SECONDS", "300"))  # 5 minutes
START_HOUR = int(os.environ.get("START_HOUR", "7"))
END_HOUR = int(os.environ.get("END_HOUR", "23"))
TIMEZONE = os.environ.get("TIMEZONE", "Asia/Kolkata")
GAP_BETWEEN_GROUPS = float(os.environ.get("GAP_BETWEEN_GROUPS", "2"))



# ============================== DM FLOW =====================================

# DM states:
# 0 -> First user message: send "hiee 🌸" then "Tumhara tinder pe account h??"
# 1 -> Next user message: send "kitna purana hai tumhara tinder Account"
# 2 -> Next user message: if any digit is present, send the buying message;
#      otherwise send "👍 okay"
# 3 -> Next user message: send the channel link, then "join karo channel h🎈"
# 4 -> Flow finished; ignore further DM messages.
DM_STATE_FILE = "dm_state.json"


def load_dm_state():
    try:
        with open(DM_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return {str(k): int(v) for k, v in data.items()}
    except (FileNotFoundError, json.JSONDecodeError, TypeError, ValueError):
        return {}


def save_dm_state(state):
    try:
        with open(DM_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception as e:
        print(f"Could not save {DM_STATE_FILE}: {e}")


dm_state = load_dm_state()

# Prevent two rapid messages from the same user from racing the shared state.
dm_locks = {}
# Serialize outgoing DM sends so a large burst of users cannot race Telegram
# requests against each other.
dm_send_lock = asyncio.Lock()


def get_dm_lock(user_id):
    key = str(user_id)
    lock = dm_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        dm_locks[key] = lock
    return lock


async def send_dm_text(event, text):
    """Send a DM reliably, retrying temporary Telegram/network failures."""
    while True:
        try:
            async with dm_send_lock:
                await client.send_message(event.chat_id, text)
            return
        except FloodWaitError as e:
            wait = max(1, int(getattr(e, "seconds", 1))) + 1
            print(f"DM FloodWait: waiting {wait}s before retrying.")
            await asyncio.sleep(wait)
        except Exception as e:
            print(f"DM send failed: {e}; retrying.")
            await asyncio.sleep(5)


@client.on(events.NewMessage(incoming=True))
async def dm_handler(event):
    if event.out or not event.is_private:
        return

    sender = await event.get_sender()
    if sender is None or getattr(sender, "bot", False):
        return

    user_id = sender.id
    key = str(user_id)
    lock = get_dm_lock(user_id)

    async with lock:
        step = dm_state.get(key, 0)

        # Flow already finished for this user.
        if step >= 4:
            return

        # Any first DM (text, photo, GIF, sticker, etc.).
        if step == 0:
            await send_dm_text(event, "hiee 🌸")
            await send_dm_text(event, "Tumhara tinder pe account h??")
            dm_state[key] = 1
            save_dm_state(dm_state)
            return

        # Any second DM.
        if step == 1:
            await send_dm_text(event, "kitna purana hai tumhara tinder Account")
            dm_state[key] = 2
            save_dm_state(dm_state)
            return

        # Age/account reply: any digit anywhere in the message counts,
        # including values such as 1.5. Media-only messages have no digits.
        if step == 2:
            raw_text = event.raw_text or ""
            if re.search(r"\d", raw_text):
                await send_dm_text(
                    event,
                    "me tinder account kharidti hun tum bhejna chate ho to bech sakte ho.",
                )
            else:
                await send_dm_text(event, "👍 okay")

            dm_state[key] = 3
            save_dm_state(dm_state)
            return

        # After the buying/okay reply, the next user message gets the link
        # followed immediately by the channel-join message. Then the flow ends.
        if step == 3:
            await send_dm_text(event, "https://t.me/buying_oldtinder")
            await send_dm_text(event, "join karo channel h🎈")
            dm_state[key] = 4
            save_dm_state(dm_state)
            return


# ============================== GROUP FLOW ==================================

GROUP_MESSAGES = [
    "Hello koi merii chuttt dekhegaaa free me😘😘",
    "koi marlo meri thukai krdo💦!",
    "koi meri chaddi faaddd do bubu dikhaungi",
    "dildoo ghusaaa rahi hunn dekhne ke liye dmmm kroo",
]
group_message_index = 0


def is_within_active_hours() -> bool:
    now = datetime.now(ZoneInfo(TIMEZONE))
    return START_HOUR <= now.hour < END_HOUR


async def get_authorized_group_ids():
    """
    If GROUP_IDS is configured, keep the original behavior and use those IDs.

    If GROUP_IDS is empty, automatically discover groups that the logged-in
    Telegram account is already a member of. No username or manually entered
    group ID is required.
    """
    raw = os.environ.get("GROUP_IDS", "").strip()

    # Keep the original explicit GROUP_IDS option unchanged.
    if raw:
        group_ids = []
        for value in raw.split(","):
            value = value.strip()
            if not value:
                continue
            try:
                group_ids.append(int(value))
            except ValueError:
                print(f"Invalid GROUP_IDS value ignored: {value}")
        return group_ids

    # New fallback: discover groups from the logged-in account's dialogs.
    groups = []
    async for dialog in client.iter_dialogs():
        entity = dialog.entity
        if getattr(entity, "megagroup", False) or getattr(entity, "gigagroup", False):
            groups.append(dialog.id)

    print(f"Auto-detected {len(groups)} joined group(s).")
    return groups


async def broadcast_once():
    global group_message_index

    groups = await get_authorized_group_ids()

    if not groups:
        print("No joined groups found; skipping group broadcast.")
        return

    message = GROUP_MESSAGES[group_message_index]
    group_message_index = (group_message_index + 1) % len(GROUP_MESSAGES)

    sent = 0

    for chat_id in groups:
        try:
            await client.send_message(chat_id, message)
            sent += 1
        except Exception as e:
            print(f"Failed to send to group {chat_id}: {e}")

        await asyncio.sleep(GAP_BETWEEN_GROUPS)

    print(f"Sent '{message}' to {sent}/{len(groups)} authorized groups.")


async def broadcast_loop():
    while True:
        if is_within_active_hours():
            await broadcast_once()
        else:
            now = datetime.now(ZoneInfo(TIMEZONE))
            print(
                f"Outside active hours "
                f"({now.strftime('%H:%M')} {TIMEZONE}), "
                "skipping this cycle."
            )

        await asyncio.sleep(INTERVAL_SECONDS)


# ============================== RUN =========================================

async def run():
    await client.start()

    me = await client.get_me()

    print(f"Logged in as {me.first_name}")
    print(
        f"Group messages rotate every {INTERVAL_SECONDS}s "
        f"between {START_HOUR}:00 and {END_HOUR}:00 ({TIMEZONE})."
    )
    print("DM flow enabled.")

    await broadcast_loop()


if __name__ == "__main__":
    missing = []

    if not API_ID:
        missing.append("API_ID")

    if not API_HASH:
        missing.append("API_HASH")

    if not STRING_SESSION.strip():
        missing.append("STRING_SESSION")

    if missing:
        print(
            f"Missing environment variables: {', '.join(missing)}"
        )
        print(
            "Railway Variables me API_ID, API_HASH aur STRING_SESSION set karein."
        )
    else:
        print("Userbot starting...")
        client.loop.run_until_complete(run())
