import os, asyncio, json, re, time
from datetime import datetime
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from redis import Redis
import aiohttp

# ── ENV ─────────────────────────────────────────────────────────────
TG_API_ID   = int(os.getenv("TG_API_ID"))
TG_API_HASH = os.getenv("TG_API_HASH")
TG_SESSION  = os.getenv("TELETHON_STRING")
BOT_TOKEN   = os.getenv("BOT_TOKEN")
REDIS_URL   = os.getenv("REDIS_URL")

QUEUE_KEY   = "zsk:queue"
CACHE_KEY   = "zsk:cache"
CACHE_TTL   = 86400

ZSK_BOT     = "zskbenefitsarbot"
RESPONSE_TIMEOUT = 60

redis = Redis.from_url(REDIS_URL, decode_responses=True)
client = TelegramClient(StringSession(TG_SESSION), TG_API_ID, TG_API_HASH)

# ── HELPERS ─────────────────────────────────────────────────────────
async def send_bot_message(chat_id, text):
    if not chat_id:
        return
    async with aiohttp.ClientSession() as s:
        await s.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text}
        )

def cache_get(inn):
    val = redis.get(f"{CACHE_KEY}:{inn}")
    return json.loads(val) if val else None

def cache_set(inn, data):
    redis.setex(f"{CACHE_KEY}:{inn}", CACHE_TTL, json.dumps(data))

def latest_set(inn, data):
    payload = dict(data)
    payload["updated_at"] = int(time.time())
    redis.hset(f"zsk:latest:{inn}", mapping={k: v or "" for k, v in payload.items()})

# ── PARSER ──────────────────────────────────────────────────────────
RISK_MAP = {'высок': 'high', 'средн': 'medium', 'низк': 'low', 'отсут': 'none'}

def parse_answer(raw):
    text = re.sub(r'\s+', ' ', raw or '').strip()

    inn, risk_ru = None, None

    m = re.search(r'\|\s*(\d{10,12})', text)
    if m:
        inn = m.group(1)

    m = re.search(r'уровень\s+риска.*?(Высокий|Средний|Низкий|Отсутствует)', text, re.I)
    if m:
        risk_ru = m.group(1).capitalize()

    risk = 'unknown'
    if risk_ru:
        for k, v in RISK_MAP.items():
            if risk_ru.lower().startswith(k):
                risk = v
                break

    return {
        "inn": inn,
        "risk": risk,
        "risk_ru": risk_ru,
        "raw": text
    }

# ── ZSK BOT ─────────────────────────────────────────────────────────
LAST_START_AT = 0

async def ensure_started():
    global LAST_START_AT
    if time.time() - LAST_START_AT > 20 * 60:
        await client.send_message(ZSK_BOT, "/start")
        LAST_START_AT = time.time()
        await asyncio.sleep(1)

async def ask_zsk(inn):
    await ensure_started()
    await client.send_message(ZSK_BOT, inn)

    collected = []
    start = time.time()

    @client.on(events.NewMessage(from_users=ZSK_BOT))
    async def on_msg(ev):
        collected.append(ev.raw_text or "")

    while time.time() - start < RESPONSE_TIMEOUT:
        await asyncio.sleep(1)
        if collected:
            break

    client.remove_event_handler(on_msg)

    if not collected:
        raise TimeoutError("No response from ZSK bot")

    return "\n".join(collected)

# ── QUEUE ───────────────────────────────────────────────────────────
def pop_job():
    item = redis.blpop(QUEUE_KEY, timeout=5)
    if not item:
        return None
    _, payload = item
    try:
        return json.loads(payload)
    except:
        return {"inn": str(payload)}

# ── MAIN LOOP ───────────────────────────────────────────────────────
async def run():
    await client.start()
    print("Worker started")

    while True:
        job = pop_job()
        if not job:
            continue

        inn = re.sub(r"\D", "", str(job.get("inn", "")))
        if not re.fullmatch(r"\d{10,12}", inn):
            continue

        force = str(job.get("force", "0")) in ("1", "true", "True")
        chat_id = job.get("chat_id")

        if not force:
            cached = cache_get(inn)
            if cached:
                latest_set(inn, cached)
                continue

        try:
            raw = await ask_zsk(inn)
            parsed = parse_answer(raw)
            effective_inn = parsed.get("inn") or inn

            cache_set(effective_inn, parsed)
            latest_set(effective_inn, parsed)

        except FloodWaitError as fw:
            await asyncio.sleep(fw.seconds + 5)
        except Exception as e:
            print(f"Error {inn}: {e}")

if __name__ == "__main__":
    asyncio.run(run())
