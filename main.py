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

# логирование: по умолчанию тихо (чтобы не ловить лимит Railway)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()  # INFO | DEBUG | QUIET

def log_info(msg: str):
    if LOG_LEVEL in ("INFO", "DEBUG"):
        print(msg, flush=True)

def log_debug(msg: str):
    if LOG_LEVEL == "DEBUG":
        print(msg, flush=True)

def log_quiet(msg: str):
    # никогда не печатаем
    pass

redis = Redis.from_url(REDIS_URL, decode_responses=True)
client = TelegramClient(StringSession(TG_SESSION), TG_API_ID, TG_API_HASH)

# ── HELPERS ───────────────────────────────────────────────────────────────────
async def send_bot_message(chat_id: int, text: str):
    if not chat_id:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with aiohttp.ClientSession() as s:
        await s.post(
            url,
            json={"chat_id": chat_id, "text": text, "disable_web_page_preview": True},
            timeout=aiohttp.ClientTimeout(total=20)
        )

def cache_get(inn: str):
    val = redis.get(f"{CACHE_KEY}:{inn}")
    return json.loads(val) if val else None

def cache_set(inn: str, data: dict):
    redis.setex(f"{CACHE_KEY}:{inn}", CACHE_TTL, json.dumps(data, ensure_ascii=False))

def latest_set(inn: str, data: dict):
    payload = dict(data)
    payload["updated_at"] = int(time.time())
    redis.hset(
        f"zsk:latest:{inn}",
        mapping={k: ("" if v is None else str(v)) for k, v in payload.items()}
    )

# ── ПАРСЕР ────────────────────────────────────────────────────────────────────
RISK_MAP = {'высок': 'high', 'средн': 'medium', 'низк': 'low', 'отсут': 'none'}

def parse_answer(raw: str) -> dict:
    text = re.sub(r'\s+', ' ', raw or '').strip()

    subj, inn, risk_ru = None, None, None
    m = re.search(r'(ООО|АО|ПАО|ИП)\s+([^|]+?)\s*\|\s*(\d{10,12})', text, re.I)
    if m:
        subj = (m.group(1) + ' ' + m.group(2)).strip()
        inn  = m.group(3)

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

    risk_code, risk_reason = None, None
    m = re.search(r'Основной\s+риск:\s*([0-9]{1,2}\.[0-9]{2})\s+(.+?)(?:[\.\!]|$)', text, re.I)
    if m:
        risk_code = m.group(1)
        risk_reason = m.group(2).strip()

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
LAST_START_AT = 0  # анти-спам /start (не чаще 1 раза в 20 минут)

async def ensure_started():
    global LAST_START_AT
    now = time.time()
    if now - LAST_START_AT > 20 * 60:
        try:
            await client.send_message(ZSK_BOT, "/start")
            LAST_START_AT = now
            await asyncio.sleep(1)
            log_debug("↪️ sent /start")
        except Exception as e:
            log_info(f"⚠️ /start error: {e}")

async def ask_zsk(inn: str) -> str:
    await ensure_started()
    await client.send_message(ZSK_BOT, inn)

    collected, started = [], time.time()
    idle_window = 5
    idle_start = time.time()

    @client.on(events.NewMessage(from_users=ZSK_BOT))
    async def on_msg(ev):
        nonlocal collected, idle_start
        collected.append(ev.raw_text or "")
        idle_start = time.time()

    while time.time() - started < RESPONSE_TIMEOUT:
        await asyncio.sleep(1)
        if collected and (time.time() - idle_start > idle_window):
            break

    try:
        client.remove_event_handler(on_msg)
    except:
        pass

    if not collected:
        raise TimeoutError("Нет ответа от @zskbenefitsarbot")

    return "\n\n".join(collected)

# ── ОЧЕРЕДЬ ───────────────────────────────────────────────────────────────────
def queue_pop_blocking(timeout=5):
    """
    Возвращает dict минимум с ключом 'inn'.
    Поддерживает:
      - JSON dict: {"inn":"...","chat_id":0,"force":1}
      - plain: "7729..."
      - JSON number: 7729...
    """
    item = redis.blpop(QUEUE_KEY, timeout=timeout)
    if not item:
        return None
    _, payload = item  # str

    # пробуем JSON
    try:
        obj = json.loads(payload)
        if isinstance(obj, dict):
            if "inn" in obj:
                obj["inn"] = str(obj["inn"])
            return obj
        if isinstance(obj, (int, float, str)):
            plain = str(obj).strip()
            return {"inn": plain} if plain else None
    except Exception:
        pass

    # plain string
    plain = str(payload).strip()
    return {"inn": plain} if plain else None

# ── ГЛАВНЫЙ ЦИКЛ ─────────────────────────────────────────────────────────────
async def run():
    await client.start()
    log_info("Worker connected. Listening queue…")

    while True:
        job = queue_pop_blocking(timeout=5)
        if not job:
            continue

        raw_inn = str(job.get("inn") or "")
        inn = re.sub(r"\D", "", raw_inn)
        chat_id = job.get("chat_id")
        force = bool(job.get("force"))  # <-- ВАЖНО

        # валидация ИНН: 10–12 цифр
        if not re.fullmatch(r"\d{10,12}", inn):
            log_info(f"⚠️ skip job без валидного inn: {job}")
            continue

        log_debug(f"▶️ JOB: {inn} (force={int(force)})")

        # кэш используем только если НЕ force
        if not force:
            cached = cache_get(inn)
            if cached:
                latest_set(inn, cached)
                # тихий режим: не шлем сообщение в телеграм, если chat_id=0
                log_debug(f"💾 cache hit: {inn} -> {cached.get('risk')}")
                if chat_id:
                    await send_bot_message(
                        chat_id,
                        f"ИНН: {inn}\nРезультат (кэш 24ч): {cached.get('risk_ru') or cached['risk']}\n"
                        f"Код риска: {cached.get('risk_code') or '-'}\n"
                        f"Причина: {cached.get('risk_reason') or '-'}\n"
                        f"Добавлен: {cached.get('added_at') or '-'}"
                    )
                continue

        # force=1 ИЛИ кэша нет → делаем реальный запрос боту
        try:
            raw = await ask_zsk(inn)
            parsed = parse_answer(raw)
            effective_inn = re.sub(r"\D", "", (parsed.get("inn") or inn).strip()) or inn

            cache_set(effective_inn, parsed)
            latest_set(effective_inn, parsed)

            log_info(f"✅ done: {effective_inn} -> {parsed.get('risk')} ({parsed.get('risk_code') or '-'})")

            if chat_id:
                await send_bot_message(
                    chat_id,
                    f"ИНН: {effective_inn}\nРезультат: {parsed.get('risk_ru') or parsed['risk']}\n"
                    f"Код риска: {parsed.get('risk_code') or '-'}\n"
                    f"Причина: {parsed.get('risk_reason') or '-'}\n"
                    f"Добавлен: {parsed.get('added_at') or '-'}"
                )

        except FloodWaitError as fw:
            log_info(f"⏳ FloodWait {fw.seconds}s on {inn}")
            if chat_id:
                await send_bot_message(chat_id, f"Telegram ограничил частоту. Подождите {fw.seconds} сек…")
            await asyncio.sleep(fw.seconds + 3)

        except Exception as e:
            log_info(f"❌ error on {inn}: {e}")
            if chat_id:
                await send_bot_message(chat_id, f"Ошибка запроса к @{ZSK_BOT}: {e}")

if __name__ == "__main__":
    asyncio.run(run())
