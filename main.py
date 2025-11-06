import os, asyncio, json, re, time
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from redis import Redis
import aiohttp

# ── ENV ────────────────────────────────────────────────────────────────────────
TG_API_ID   = int(os.getenv("TG_API_ID"))
TG_API_HASH = os.getenv("TG_API_HASH")
TG_SESSION  = os.getenv("TELETHON_STRING")  # StringSession генерим один раз
BOT_TOKEN   = os.getenv("BOT_TOKEN")        # токен твоего Bot API (Vercel-бота)
REDIS_URL   = os.getenv("REDIS_URL")        # rediss://:pwd@host:port
QUEUE_KEY   = os.getenv("QUEUE_KEY", "zsk:queue")
CACHE_KEY   = os.getenv("CACHE_KEY", "zsk:cache")
CACHE_TTL   = int(os.getenv("CACHE_TTL", "86400"))  # 24h
ZSK_BOT     = os.getenv("ZSK_BOT", "zskbenefitsarbot")
RESPONSE_TIMEOUT = int(os.getenv("RESPONSE_TIMEOUT", "60"))

redis = Redis.from_url(REDIS_URL, decode_responses=True)
client = TelegramClient(StringSession(TG_SESSION), TG_API_ID, TG_API_HASH)

# ── HELPERS ───────────────────────────────────────────────────────────────────
async def send_bot_message(chat_id: int, text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with aiohttp.ClientSession() as s:
        await s.post(url, json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True})

def cache_get(inn: str):
    val = redis.get(f"{CACHE_KEY}:{inn}")
    return json.loads(val) if val else None

def cache_set(inn: str, data: dict):
    redis.setex(f"{CACHE_KEY}:{inn}", CACHE_TTL, json.dumps(data))

def parse_answer(raw: str) -> dict:
    cleaned = re.sub(r'\s+', ' ', raw).strip()
    res = {"raw": cleaned}
    if re.search(r'(высок(ий|ая)|высокого уровня|группа высокого риска)', cleaned, re.I):
        res["risk"] = "high"
    elif re.search(r'(сведения отсутствуют|не найден|не обнаружен|информация отсутствует)', cleaned, re.I):
        res["risk"] = "none"
    else:
        res["risk"] = "unknown"
    m = re.search(r'(разъясн|поясн)[^:]*:\s*(.+?)(?:$|Контакт|Телеграм|Примечание)', cleaned, re.I)
    if m:
        res["explanation"] = m.group(2)[:1200]
    return res

async def ask_zsk(inn: str) -> str:
    """
    Общаемся с @zskbenefitsarbot через диалог.
    Принимаем, что бот выдаёт 1-3 сообщения. Соберём их все за короткое «окно».
    """
    # гарантируем /start хотя бы раз (не критично, но полезно)
    try:
        await client.send_message(ZSK_BOT, "/start")
        await asyncio.sleep(1)
    except:
        pass

    # отправляем ИНН
    await client.send_message(ZSK_BOT, inn)

    # ждём ответы от бота
    collected = []
    started = time.time()
    last_len = 0
    idle_window = 5  # если 5 сек нет новых сообщений — завершаем

    @client.on(events.NewMessage(from_users=ZSK_BOT))
    async def on_msg(ev):
        txt = ev.raw_text or ""
        collected.append(txt)

    # «пассивное» ожидание без блокировки основного цикла:
    while time.time() - started < RESPONSE_TIMEOUT:
        await asyncio.sleep(1)
        if len(collected) != last_len:
            last_len = len(collected)
            idle_start = time.time()
        elif time.time() - idle_start > idle_window and collected:
            break

    try:
        client.remove_event_handler(on_msg)
    except:
        pass

    if not collected:
        raise TimeoutError("Нет ответа от @zskbenefitsarbot")
    return "\n\n".join(collected)

def queue_pop_blocking(timeout=5):
    item = redis.blpop(QUEUE_KEY, timeout=timeout)  # (key, payload) | None
    if not item:
        return None
    _, payload = item
    return json.loads(payload)

# ── WORKER LOOP ────────────────────────────────────────────────────────────────
async def run():
    await client.start()  # StringSession не спросит код, он уже в переменной
    print("Worker connected as user. Listening queue…")

    while True:
        job = queue_pop_blocking(timeout=5)
        if not job:
            continue

        inn = job.get("inn")
        chat_id = job.get("chat_id")
        if not inn or not chat_id:
            continue

        # кэш
        cached = cache_get(inn)
        if cached:
            await send_bot_message(chat_id, f"ИНН: {inn}\nРезультат (кэш 24ч): {cached['risk']}\n\n{cached.get('explanation','') or cached.get('raw','')[:1200]}")
            continue

        try:
            raw = await ask_zsk(inn)
            parsed = parse_answer(raw)
            cache_set(inn, parsed)
            await send_bot_message(chat_id, f"ИНН: {inn}\nРезультат: {parsed['risk']}\n\n{parsed.get('explanation','') or parsed.get('raw','')[:1200]}")
        except FloodWaitError as fw:
            await send_bot_message(chat_id, f"Telegram ограничил частоту. Подождите {fw.seconds} сек…")
            await asyncio.sleep(fw.seconds + 3)
        except Exception as e:
            await send_bot_message(chat_id, f"Ошибка запроса к @{ZSK_BOT}: {e}")

if __name__ == "__main__":
    asyncio.run(run())
