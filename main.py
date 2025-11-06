import os, asyncio, json, re, time
from datetime import datetime
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError
from redis import Redis
import aiohttp

# ── ENV ────────────────────────────────────────────────────────────────────────
TG_API_ID   = int(os.getenv("TG_API_ID"))
TG_API_HASH = os.getenv("TG_API_HASH")
TG_SESSION  = os.getenv("TELETHON_STRING")  # StringSession
BOT_TOKEN   = os.getenv("BOT_TOKEN")        # токен Bot API (Vercel)
REDIS_URL   = os.getenv("REDIS_URL")        # rediss://:pwd@host:port
QUEUE_KEY   = os.getenv("QUEUE_KEY", "zsk:queue")
CACHE_KEY   = os.getenv("CACHE_KEY", "zsk:cache")
CACHE_TTL   = int(os.getenv("CACHE_TTL", "86400"))  # 24 часа
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

# ── ПАРСЕР ────────────────────────────────────────────────────────────────────
RISK_MAP = {
    'высок': 'high',
    'средн': 'medium',
    'низк':  'low',
    'отсут': 'none'
}

def parse_answer(raw: str) -> dict:
    """
    Парсит ответ @zskbenefitsarbot даже если вокруг есть промо-текст.
    Извлекает:
      - subject (ООО/ИП ...), inn
      - risk ('high'|'medium'|'low'|'none'|'unknown) + risk_ru
      - risk_code (напр. '1.02') и risk_reason (краткое описание)
      - added_at (ISO, если найдена дата 'Добавлен: ДД.ММ.ГГГГ')
      - raw (очищенный сплошной текст)
    """
    text = re.sub(r'\s+', ' ', raw or '').strip()

    subj, inn, risk_ru = None, None, None
    # 1) ИНН и субъект
    m = re.search(r'(ООО|АО|ПАО|ИП)\s+([^|]+?)\s*\|\s*(\d{10,12})', text, re.I)
    if m:
        subj = (m.group(1) + ' ' + m.group(2)).strip()
        inn  = m.group(3)

    # 2) Уровень риска
    m = re.search(r'Текущий\s+уровень\s+риска\s+ЗСК:?\s*(?:[^\w]|)+\s*(Высокий|Средний|Низкий|Отсутствует)', text, re.I)
    if not m:
        m = re.search(r'Уровень\s+риска:?\s*(?:[^\w]|)+\s*(Высокий|Средний|Низкий|Отсутствует)', text, re.I)
    if m:
        risk_ru = m.group(1).capitalize()

    risk = 'unknown'
    if risk_ru:
        key = risk_ru.lower()[:5]
        for k, v in RISK_MAP.items():
            if key.startswith(k):
                risk = v
                break

    # 3) Основной риск
    risk_code, risk_reason = None, None
    m = re.search(r'Основной\s+риск:\s*([0-9]{1,2}\.[0-9]{2})\s+(.+?)(?:[\.\!]|$)', text, re.I)
    if m:
        risk_code = m.group(1)
        risk_reason = m.group(2).strip()

    # 4) Дата "Добавлен"
    added_at_iso = None
    m = re.search(r'Добавлен:\s*(\d{2}\.\d{2}\.\d{4})', text)
    if m:
        try:
            added_at_iso = datetime.strptime(m.group(1), '%d.%m.%Y').date().isoformat()
        except Exception:
            pass

    return {
        "risk": risk,
        "risk_ru": risk_ru,
        "risk_code": risk_code,
        "risk_reason": risk_reason,
        "subject": subj,
        "inn": inn,
        "added_at": added_at_iso,
        "raw": text
    }

# ── ВЗАИМОДЕЙСТВИЕ С ZSK-БОТОМ ───────────────────────────────────────────────
async def ask_zsk(inn: str) -> str:
    try:
        await client.send_message(ZSK_BOT, "/start")
        await asyncio.sleep(1)
    except:
        pass

    await client.send_message(ZSK_BOT, inn)
    collected, started, last_len = [], time.time(), 0
    idle_window = 5

    @client.on(events.NewMessage(from_users=ZSK_BOT))
    async def on_msg(ev):
        txt = ev.raw_text or ""
        collected.append(txt)

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
    item = redis.blpop(QUEUE_KEY, timeout=timeout)
    if not item:
        return None
    _, payload = item
    return json.loads(payload)

# ── ГЛАВНЫЙ ЦИКЛ ─────────────────────────────────────────────────────────────
async def run():
    await client.start()
    print("Worker connected. Listening queue…")

    while True:
        job = queue_pop_blocking(timeout=5)
        if not job:
            continue

        inn = job.get("inn")
        chat_id = job.get("chat_id")
        if not inn or not chat_id:
            continue

        cached = cache_get(inn)
        if cached:
            await send_bot_message(chat_id, f"ИНН: {inn}\nРезультат (кэш 24ч): {cached['risk']}\n\n{cached.get('risk_reason','') or cached.get('raw','')[:1200]}")
            continue

        try:
            raw = await ask_zsk(inn)
            parsed = parse_answer(raw)
            cache_set(inn, parsed)
            await send_bot_message(
                chat_id,
                f"ИНН: {inn}\nРезультат: {parsed['risk_ru'] or parsed['risk']}\n"
                f"Код риска: {parsed.get('risk_code') or '-'}\n"
                f"Причина: {parsed.get('risk_reason') or '-'}\n"
                f"Добавлен: {parsed.get('added_at') or '-'}"
            )
        except FloodWaitError as fw:
            await send_bot_message(chat_id, f"Telegram ограничил частоту. Подождите {fw.seconds} сек…")
            await asyncio.sleep(fw.seconds + 3)
        except Exception as e:
            await send_bot_message(chat_id, f"Ошибка запроса к @{ZSK_BOT}: {e}")

if __name__ == "__main__":
    asyncio.run(run())
