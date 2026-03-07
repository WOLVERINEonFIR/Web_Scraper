import requests
from bs4 import BeautifulSoup
import random
import time
import json
from urllib.parse import urljoin
from pathlib import Path

# =====================================================
# CONFIG
# =====================================================

BASE_URL = "https://www.gsmarena.com/"
OUTPUT = "output/google.json"

BRANDS = [
    "motorola-phones-f-4-0-p2.php",
    "oneplus-phones-f-95-0-p2.php",
    "oppo-phones-f-82-0-p2.php",
    "realme-phones-f-118-0-p2.php",
    "samsung-phones-f-9-0-p2.php",
    "tecno-phones-f-120-0-p2.php",
    "vivo-phones-f-98-0-p2.php",
    "xiaomi-phones-f-80-0-p2.php"
]

# tablets to ignore
TABLET_KEYWORDS = ["tab", "pad", "tablet", "ipad"]
WATCH_KEYWORDS = ["watch","band","gear","fit","wear"]

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

def delay():
    time.sleep(random.uniform(8, 12))  # safe delay

def is_phone(name):
    name = name.lower()

    # remove tablets
    if any(k in name for k in TABLET_KEYWORDS):
        return False

    # remove watches
    if any(k in name for k in WATCH_KEYWORDS):
        return False

    return True

# =====================================================
# COLLECT BRAND URLS
# =====================================================

def collect_brand(brand_slug):

    print(f"\n🔍 Collecting {brand_slug}")
    links = []
    page = 1

    while True:

        if page == 1:
            url = BASE_URL + brand_slug
        else:
            url = BASE_URL + brand_slug.replace(".php", f"-p{page}.php")

        print("Reading:", url)

        try:
            r = requests.get(url, headers=headers(), timeout=15)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                break  # normal end of pages
            else:
                print("Request failed:", e)
                break

        soup = BeautifulSoup(r.text, "html.parser")

        phones = soup.select(".makers ul li a")

        if not phones:
            break

        for phone in phones:
            name = phone.get_text(strip=True)

            if not is_phone(name):
                continue

            links.append(urljoin(BASE_URL, phone["href"]))

        print("Collected so far:", len(links))

        page += 1
        delay()

    return links

# =====================================================
# MAIN
# =====================================================

def main():

    # resume support
    try:
        with open(OUTPUT, "r") as f:
            all_links = set(json.load(f))
            print("Resuming with", len(all_links), "existing URLs")
    except:
        all_links = set()

    for brand in BRANDS:

        brand_links = collect_brand(brand)
        all_links.update(brand_links)

        # ⭐ autosave after each brand
        with open(OUTPUT, "w") as f:
            json.dump(sorted(all_links), f, indent=2)

        print("✅ Progress saved:", len(all_links))

        print("Cooling down between brands...")
        time.sleep(random.uniform(15, 25))

    print("\n🚀 DONE")
    print("Total URLs collected:", len(all_links))


if __name__ == "__main__":
    main()