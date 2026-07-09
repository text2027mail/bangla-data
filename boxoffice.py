#!/usr/bin/env python3
"""
Box Office Script – Fetches seat data for the current Bangladesh date only.
Updates existing JSON file by matching programId (append/update).
Saves to: /boxoffice/YYYY/MM-DD.json (minified, value-only arrays).

Also builds/updates a movie‑level database:
- movie/data/{slug}.json – day‑wise aggregated stats per movie
- movie/index.json – master index of all movies with lifetime totals

Each daily file and the index include a "last_updated" timestamp (IST).
"""

import asyncio
import aiohttp
import random
import string
import json
import os
import re
from datetime import datetime
from collections import defaultdict
from typing import Optional, Dict, List, Tuple, Any
import pytz

# ========== CONFIGURATION ==========
LOGIN_URL = "https://cineplex-web-api.cineplexbd.com/api/v1/login"
SHOW_URL = "https://cineplex-web-api.cineplexbd.com/api/v1/movie-show-time"
SEAT_URL = "https://cineplex-ticket-api.cineplexbd.com/api/v1/get-seat"

SEAT_AUTH = "Bearer 175714|CINE-TICKET-1OgNEfYvrMNAQRnQwVUkiGVUhG88hh5dsE9AKbHM30ee001b"
AVG_PRICE = 500  # used for gross calculations

LOCATIONS = [1, 2, 3, 4, 5, 6, 8, 9, 10]

MAX_CONCURRENT = 50
RANDOM_DELAY_RANGE = (0.0, 0.0)
RETRIES = 2

USER_AGENTS = [
    "Mozilla/5.0 (Linux; Android 6.0; Nexus 5)",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
]

# Timezone: Bangladesh Standard Time (UTC+6) for date logic
BST = pytz.timezone("Asia/Dhaka")
# Indian Standard Time (UTC+5:30) for last_updated
IST = pytz.timezone("Asia/Kolkata")

# ========== HELPERS ==========
def random_ua() -> str:
    return random.choice(USER_AGENTS)

def generate_device_key(length: int = 64) -> str:
    return ''.join(random.choice('0123456789abcdef') for _ in range(length))

def generate_user_id(length: int = 33) -> str:
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

def bst_now() -> datetime:
    return datetime.now(BST)

def get_bst_date_str() -> str:
    return bst_now().strftime("%Y-%m-%d")

def get_bst_year_month_day() -> Tuple[str, str, str]:
    now = bst_now()
    return now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")

def ist_now() -> datetime:
    return datetime.now(IST)

def get_ist_timestamp() -> str:
    """Return current IST time as 'YYYY-MM-DD HH:MM IST'."""
    return ist_now().strftime("%Y-%m-%d %H:%M IST")

def slugify(title: str) -> str:
    """Generate a URL‑friendly slug from a movie title."""
    slug = re.sub(r'[^a-zA-Z0-9\s]', '', title).strip().lower()
    slug = re.sub(r'\s+', '-', slug)
    return slug

# Global device key (fixed per run)
GLOBAL_DEVICE_KEY = generate_device_key()

# ========== LOGIN (async) ==========
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

# ========== HEADERS ==========
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
    """Fetch show data for a location and date range."""
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
    """Fetch seat availability for a given programId. Returns (total, sold)."""
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

# ========== PROCESS DATE ==========
async def process_date(session: aiohttp.ClientSession, show_auth: str,
                       date_str: str) -> Dict[str, List[List[Any]]]:
    """
    Fetch all shows for the given date across all locations,
    then fetch seat data for each slot.
    Returns dict: {movie_title: [[programId, location, showTime, total, sold], ...]}
    """
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

# ========== DAILY FILE I/O ==========
def get_boxoffice_filepath() -> str:
    year, month, day = get_bst_year_month_day()
    dir_path = os.path.join("boxoffice", year)
    os.makedirs(dir_path, exist_ok=True)
    return os.path.join(dir_path, f"{month}-{day}.json")

def load_existing_data(filepath: str) -> Dict[str, List[List[Any]]]:
    """Load the data portion from a daily JSON file.
       Handles both new (with 'data' key) and old (direct dict) formats."""
    if not os.path.exists(filepath):
        return {}
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = json.load(f)
        # If it's a dict with a "data" key, extract it
        if isinstance(content, dict) and "data" in content:
            return content["data"]
        # Otherwise assume it's the old format (direct movie->entries)
        return content
    except:
        return {}

def merge_and_save(filepath: str, new_data: Dict[str, List[List[Any]]]):
    """Merge new_data into existing file (update by programId),
       then save with a top-level "data" and "last_updated" (IST)."""
    existing = load_existing_data(filepath)

    # Merge new_data into existing
    for movie, entries in new_data.items():
        if movie not in existing:
            existing[movie] = []
        existing_map = {entry[0]: entry for entry in existing[movie]}
        for entry in entries:
            pid = entry[0]
            existing_map[pid] = entry
        existing[movie] = list(existing_map.values())

    # Build the final structure with timestamp
    output = {
        "data": existing,
        "last_updated": get_ist_timestamp()
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(output, f, separators=(',', ':'), ensure_ascii=False)
    print(f"💾 Updated {filepath} (last_updated: {output['last_updated']})")

# ========== MOVIE DATABASE BUILDER ==========
def update_movie_database():
    """
    Scan all daily JSON files under boxoffice/, aggregate per movie per date,
    and write per‑movie summary files + an index.
    The index now includes a "last_updated" field.
    """
    print("\n📊 Building movie database...")
    base_dir = "boxoffice"
    if not os.path.exists(base_dir):
        print("⚠️ No boxoffice data found.")
        return

    # Collect all daily files: boxoffice/YYYY/MM-DD.json
    daily_files = []
    for year_dir in os.listdir(base_dir):
        year_path = os.path.join(base_dir, year_dir)
        if not os.path.isdir(year_path):
            continue
        for file in os.listdir(year_path):
            if file.endswith(".json") and "-" in file:
                month_day = file.replace(".json", "")
                month, day = month_day.split("-")
                date_str = f"{year_dir}{month}{day}"  # YYYYMMDD
                daily_files.append((date_str, os.path.join(year_path, file)))

    if not daily_files:
        print("⚠️ No daily files found.")
        return

    # Aggregate: movie -> date -> stats
    movie_agg: Dict[str, Dict[str, Dict[str, int]]] = defaultdict(lambda: defaultdict(lambda: {
        "shows": 0,
        "seats": 0,
        "sold": 0,
        "venues": set()   # will convert to count later
    }))

    for date_str, filepath in daily_files:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                content = json.load(f)
            # Extract data: new format has "data" key; old is direct
            if isinstance(content, dict) and "data" in content:
                data = content["data"]
            else:
                data = content
        except:
            continue
        for movie, entries in data.items():
            shows = len(entries)
            seats = sum(e[3] for e in entries)
            sold = sum(e[4] for e in entries)
            venues = {e[1] for e in entries}

            agg = movie_agg[movie][date_str]
            agg["shows"] += shows
            agg["seats"] += seats
            agg["sold"] += sold
            agg["venues"].update(venues)

    # Now write per‑movie files and build index
    os.makedirs("movie/data", exist_ok=True)
    index = []

    for movie, dates in movie_agg.items():
        slug = slugify(movie)
        day_rows = []
        total_gross = 0
        total_tickets = 0

        for date_str, stats in sorted(dates.items()):
            gross = stats["sold"] * AVG_PRICE
            total_gross += gross
            total_tickets += stats["sold"]
            venues_count = len(stats["venues"])
            day_rows.append([
                int(date_str),
                gross,
                stats["shows"],
                stats["seats"],
                venues_count
            ])

        # Write per‑movie file
        movie_file = os.path.join("movie/data", f"{slug}.json")
        with open(movie_file, "w", encoding="utf-8") as f:
            json.dump(day_rows, f, separators=(',', ':'), ensure_ascii=False)
        print(f"   📄 {movie_file}")

        index.append({
            "name": movie,
            "slug": slug,
            "totalGross": total_gross,
            "totalTickets": total_tickets
        })

    # Write index file with last_updated
    index_file = os.path.join("movie", "index.json")
    output_index = {
        "movies": index,
        "last_updated": get_ist_timestamp()
    }
    with open(index_file, "w", encoding="utf-8") as f:
        json.dump(output_index, f, separators=(',', ':'), ensure_ascii=False)
    print(f"💾 {index_file} (last_updated: {output_index['last_updated']})")
    print("✅ Movie database updated.\n")

# ========== MAIN ==========
async def main():
    date_str = get_bst_date_str()
    print(f"📅 Processing date: {date_str} (Bangladesh Time)")

    async with aiohttp.ClientSession() as session:
        show_auth = await get_show_auth(session)
        if not show_auth:
            print("❌ Cannot continue without SHOW_AUTH")
            return

        new_data = await process_date(session, show_auth, date_str)

        if not new_data:
            print("⚠️ No data fetched.")
            return

        filepath = get_boxoffice_filepath()
        merge_and_save(filepath, new_data)

    # After updating daily file, rebuild the movie database
    update_movie_database()

if __name__ == "__main__":
    asyncio.run(main())
