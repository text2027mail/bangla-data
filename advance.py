#!/usr/bin/env python3
"""
Advance Bookings Script – Fetches seat data for the next 6 days (excluding today).
Rewrites the entire JSON for each date independently.
Saves to: /advance/YYYY/MM-DD.json (minified, value-only arrays).

Each daily file includes a top-level "data" object and a "last_updated" timestamp (IST).
"""

import asyncio
import aiohttp
import random
import string
import json
import os
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional, Dict, List, Tuple, Any
import pytz

# ========== CONFIGURATION ==========
LOGIN_URL = "https://cineplex-web-api.cineplexbd.com/api/v1/login"
SHOW_URL = "https://cineplex-web-api.cineplexbd.com/api/v1/movie-show-time"
SEAT_URL = "https://cineplex-ticket-api.cineplexbd.com/api/v1/get-seat"

SEAT_AUTH = "Bearer 175714|CINE-TICKET-1OgNEfYvrMNAQRnQwVUkiGVUhG88hh5dsE9AKbHM30ee001b"
AVG_PRICE = 500

LOCATIONS = [1, 2, 3, 4, 5, 6, 8, 9, 10]

MAX_CONCURRENT = 50
RANDOM_DELAY_RANGE = (0.0, 0.0)
RETRIES = 2

USER_AGENTS = [
    "Mozilla/5.0 (Linux; Android 6.0; Nexus 5)",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
]

# Timezone: Indian Standard Time (UTC+5:30) for last_updated
IST = pytz.timezone("Asia/Kolkata")

# ========== HELPERS ==========
def random_ua() -> str:
    return random.choice(USER_AGENTS)

def generate_device_key(length: int = 64) -> str:
    return ''.join(random.choice('0123456789abcdef') for _ in range(length))

def generate_user_id(length: int = 33) -> str:
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

def ist_now() -> datetime:
    return datetime.now(IST)

def get_ist_timestamp() -> str:
    """Return current IST time as 'YYYY-MM-DD HH:MM IST'."""
    return ist_now().strftime("%Y-%m-%d %H:%M IST")

def get_ist_date_str() -> str:
    return ist_now().strftime("%Y-%m-%d")

def get_future_dates(days_ahead: int = 6) -> List[str]:
    """Return list of date strings for the next `days_ahead` days, excluding today."""
    today = ist_now().date()
    return [(today + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, days_ahead + 1)]

def get_path_for_date(date_str: str) -> str:
    """Return file path for a given date string (YYYY-MM-DD)."""
    year, month, day = date_str.split("-")
    dir_path = os.path.join("advance", year)
    os.makedirs(dir_path, exist_ok=True)
    return os.path.join(dir_path, f"{month}-{day}.json")

# Global device key (fixed per run)
GLOBAL_DEVICE_KEY = generate_device_key()

# ========== LOGIN & HEADERS ==========
async def get_show_auth(session: aiohttp.ClientSession) -> Optional[str]:
    print("🔐 Logging in...")
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "origin": "https://www.cineplexbd.com",
        "referer": "https://www.cineplexbd.com/",
        "user-agent": random_ua()
    }
    payload = {"user_id": generate_user_id()}
    try:
        async with session.post(LOGIN_URL, json=payload, headers=headers, timeout=15) as resp:
            data = await resp.json()
            if data.get("status") == "success":
                print("✅ SHOW_AUTH received")
                return f"Bearer {data['data']}"
            print("❌ Login failed:", data)
            return None
    except Exception as e:
        print("❌ Login error:", e)
        return None

def show_headers(show_auth: str) -> dict:
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": show_auth,
        "origin": "https://www.cineplexbd.com",
        "referer": "https://www.cineplexbd.com/",
        "user-agent": random_ua()
    }

def seat_headers() -> dict:
    return {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": SEAT_AUTH,
        "device-key": GLOBAL_DEVICE_KEY,
        "appsource": "web",
        "origin": "https://ticket.cineplexbd.com",
        "referer": "https://ticket.cineplexbd.com/",
        "user-agent": random_ua()
    }

# ========== API CALLS ==========
async def fetch_shows(session: aiohttp.ClientSession, show_auth: str, loc: int,
                       date_from: str, date_to: str) -> Optional[dict]:
    payload = {
        "location": loc,
        "date_from": date_from,
        "date_to": date_to
    }
    try:
        async with session.post(SHOW_URL, json=payload,
                                headers=show_headers(show_auth), timeout=15) as resp:
            print(f"   📡 LOC {loc} STATUS: {resp.status}")
            data = await resp.json()
            if "data" not in data:
                print(f"   ❌ No data for loc {loc}")
                return None
            print(f"   ✅ Movies: {len(data['data'])}")
            return data
    except Exception as e:
        print(f"   ❌ fetch_shows error loc {loc}: {e}")
        return None

async def fetch_seats(session: aiohttp.ClientSession, loc: int, pid: int,
                      retries: int = RETRIES) -> Optional[Tuple[int, int]]:
    payload = {"location": loc, "programId": pid}
    headers = seat_headers()
    for attempt in range(retries + 1):
        try:
            async with session.post(SEAT_URL, json=payload, headers=headers, timeout=8) as resp:
                if resp.status != 200:
                    raise Exception(f"HTTP {resp.status}")
                data = await resp.json()
                if data.get("status") == "error":
                    return None
                total, sold = 0, 0
                for t in data["data"]["seatTypes"]:
                    for s in t["seatStatus"]:
                        total += 1
                        if s["seatStatus"] == 0:
                            sold += 1
                return (total, sold)
        except Exception:
            if attempt < retries:
                await asyncio.sleep(random.uniform(0.3, 1.0) * (attempt + 1))
            else:
                return None
    return None

# ========== PROCESS SINGLE DATE ==========
async def process_date(session: aiohttp.ClientSession, show_auth: str,
                       date_str: str) -> Dict[str, List[List[Any]]]:
    """Fetch all shows for a given date across all locations and return seat data."""
    # Gather shows from all locations
    show_tasks = [fetch_shows(session, show_auth, loc, date_str, date_str) for loc in LOCATIONS]
    show_results = await asyncio.gather(*show_tasks)

    tasks = []
    for loc, show_data in zip(LOCATIONS, show_results):
        if not show_data:
            continue
        for movie in show_data["data"]:
            title = movie["movie_detail"]["title"]
            for day in movie["show_time"]:
                # day["raw_date"] should equal date_str
                for slot in day["slot"]:
                    pid = slot["schedule_id"]
                    show_time = slot.get("time", "00:00:00")
                    tasks.append((title, loc, pid, show_time))

    print(f"🎯 Total seat requests for {date_str}: {len(tasks)}")
    if not tasks:
        return {}

    random.shuffle(tasks)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async def bounded_fetch(title, loc, pid, show_time):
        async with semaphore:
            if RANDOM_DELAY_RANGE[1] > 0:
                await asyncio.sleep(random.uniform(*RANDOM_DELAY_RANGE))
            seats = await fetch_seats(session, loc, pid)
            if seats is None:
                return None
            total, sold = seats
            return (title, loc, pid, show_time, total, sold)

    coros = [bounded_fetch(t, l, p, st) for (t, l, p, st) in tasks]
    results = await asyncio.gather(*coros, return_exceptions=True)

    movie_data: Dict[str, List[List[Any]]] = defaultdict(list)
    for res in results:
        if isinstance(res, Exception) or res is None:
            continue
        title, loc, pid, show_time, total, sold = res
        movie_data[title].append([pid, loc, show_time, total, sold])

    return dict(movie_data)

# ========== MAIN ==========
async def main():
    future_dates = get_future_dates(6)
    print(f"📅 Processing dates: {future_dates}")

    async with aiohttp.ClientSession() as session:
        show_auth = await get_show_auth(session)
        if not show_auth:
            print("❌ Cannot continue without SHOW_AUTH")
            return

        for date_str in future_dates:
            print(f"\n--- Processing {date_str} ---")
            data = await process_date(session, show_auth, date_str)
            if not data:
                print(f"⚠️ No data for {date_str}, skipping file.")
                continue

            filepath = get_path_for_date(date_str)
            # Build the output structure with timestamp
            output = {
                "data": data,
                "last_updated": get_ist_timestamp()
            }
            # Write fresh (rewrite entire file)
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(output, f, separators=(',', ':'), ensure_ascii=False)
            print(f"💾 Saved {filepath} (last_updated: {output['last_updated']})")

if __name__ == "__main__":
    asyncio.run(main())
