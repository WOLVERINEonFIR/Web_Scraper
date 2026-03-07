import requests
from bs4 import BeautifulSoup
import random
import time
import json
import re
from pathlib import Path

# =====================================================
# CONFIG
# =====================================================

INPUT_FILE = "output/samsung_urls.json"
OUTPUT_FILE = "output/samsung_specs_test.json"
PROGRESS_FILE = "output/samsung_progress.json"

MAX_PHONES = 20   # SAFE TEST LIMIT

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 Firefox/121.0",
]

Path("output").mkdir(exist_ok=True)

# =====================================================
# HELPERS
# =====================================================

def headers():
    return {"User-Agent": random.choice(USER_AGENTS)}

def human_delay():
    delay = random.uniform(15, 25)  # VERY SAFE DELAY
    print(f"Sleeping {round(delay,1)} seconds...")
    time.sleep(delay)

def extract_number(text):
    if not text:
        return None
    match = re.search(r"\d+(\.\d+)?", text)
    return float(match.group()) if match else None

# =====================================================
# PROGRESS SAVE / LOAD
# =====================================================

def load_progress():
    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return []

def save_progress(data):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# =====================================================
# EXTRACT PHONE DATA
# =====================================================

def get_phone_data(url):

    print("\nFetching:", url)

    try:
        r = requests.get(url, headers=headers(), timeout=15)

        if r.status_code == 429:
            print("⚠️ Rate limited — wait before running again")
            return None

        r.raise_for_status()

    except Exception as e:
        print("Request failed:", e)
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    # ---------- NAME ----------
    title = soup.select_one(".specs-phone-name-title")
    if not title:
        return None

    name = title.text.strip()

    # ---------- SPECS ----------
    specs_container = soup.select_one("#specs-list")

    if not specs_container:
        print("Specs container missing")
        return None

    specs = {}

    # ⭐ CORRECT ROW PARSING (based on your screenshot)
    rows = specs_container.select("tr")

    for row in rows:
        ttl = row.select_one("td.ttl")
        nfo = row.select_one("td.nfo")

        if ttl and nfo:
            key = ttl.get_text(strip=True)
            value = nfo.get_text(" ", strip=True)
            specs[key] = value

    # ---------- BUILD OBJECT ----------
    phone_data = {
        "name": name,
        "status": specs.get("Status"),
        "announced": specs.get("Announced"),
        "chipset": specs.get("Chipset"),
        "cpu": specs.get("CPU"),
        "gpu": specs.get("GPU"),
        "display_inches": extract_number(specs.get("Size")),
        "battery_mah": extract_number(
            specs.get("Battery") or
            specs.get("Type") or
            specs.get("Battery Type")
        ),
        "weight_g": extract_number(
            specs.get("Weight") or
            specs.get("Dimensions")
        ),
        "url": url
    }

    return phone_data

# =====================================================
# MAIN
# =====================================================

def main():

    urls = json.load(open(INPUT_FILE))

    phones = load_progress()
    completed = {p["url"] for p in phones}

    print("Resuming scraper...")
    print("Already collected:", len(phones))

    for url in urls:

        if len(phones) >= MAX_PHONES:
            print("\n✅ Test limit reached")
            break

        if url in completed:
            continue

        print(f"\nProcessing #{len(phones)+1}")

        data = get_phone_data(url)

        if data:
            phones.append(data)
            save_progress(phones)
            print("✅ Saved:", data["name"])
        else:
            print("⏭ Skipped")

        human_delay()

    # FINAL SAVE
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(phones, f, indent=2, ensure_ascii=False)

    print("\n✅ DONE")
    print("Phones collected:", len(phones))


if __name__ == "__main__":
    main()