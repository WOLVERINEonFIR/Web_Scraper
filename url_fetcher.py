import requests
from bs4 import BeautifulSoup
import random
import time
import json
import re
from urllib.parse import urljoin
from pathlib import Path

# =====================================================
# CONFIG
# =====================================================

BASE_URL = "https://www.gsmarena.com/"
OUTPUT = "output/tabs_urls.json"

BRANDS = []

TABLET_RESULTS = [
    "https://www.gsmarena.com/results.php3?mode=tablet&nYearMin=2025&sMakers=48,46,107,121,119,94,73,4,128,95,82,118,9,7,120,98,80",
    "https://www.gsmarena.com/results.php3?mode=tablet&nYearMin=2024&nYearMax=2025&sMakers=48,46,107,121,119,94,73,4,128,95,82,118,9,7,120,98,80",
    "https://www.gsmarena.com/results.php3?mode=tablet&nYearMin=2023&nYearMax=2024&sMakers=48,46,107,121,119,94,73,4,128,95,82,118,9,7,120,98,80"
]

# remove watches only
WATCH_KEYWORDS = ["watch", "band", "gear", "fit", "wear"]

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
    time.sleep(random.uniform(8, 12))

def is_valid_device(name):
    name = name.lower()

    # remove watches
    if any(k in name for k in WATCH_KEYWORDS):
        return False

    return True

def normalize_model_name(name):
    if not name:
        return name

    name_lower = name.lower()

    # keep India models
    if "(india)" in name_lower:
        return name.strip()

    # remove other region tags
    name = re.sub(r"\((china|global|europe|usa|japan|international)\)", "", name, flags=re.I)

    return name.strip()

def _split_brand_model(name, url=None):
    raw = (name or "").strip()
    if raw:
        parts = raw.split(maxsplit=1)
        brand = parts[0].strip()
        model = parts[1].strip() if len(parts) > 1 else ""
        return brand, model

    slug = ""
    if url:
        m = re.search(r"/([^/]+)-\d+\.php$", url)
        if m:
            slug = m.group(1).replace("_", " ").strip()
    if slug:
        parts = slug.split(maxsplit=1)
        brand = parts[0].strip()
        model = parts[1].strip() if len(parts) > 1 else ""
        return brand, model
    return "", ""

def _is_india_variant(name, url=None):
    text = f"{name or ''} {url or ''}".lower()
    return "(india)" in text or re.search(r"(^|[_\s-])india($|[_\s-])", text) is not None

def upsert_device_url(seen_devices, name, url):
    brand, model_name = _split_brand_model(name, url)
    normalized_model = normalize_model_name(model_name or "")
    key = f"{brand.lower()}|{normalized_model.lower()}"
    is_india = _is_india_variant(name, url)

    if key not in seen_devices:
        seen_devices[key] = {"url": url, "is_india": is_india}
        return

    if is_india and not seen_devices[key]["is_india"]:
        seen_devices[key] = {"url": url, "is_india": True}

# =====================================================
# COLLECT PHONE BRAND PAGES
# =====================================================

def collect_brand(brand_slug):

    print(f"\n🔍 Collecting brand: {brand_slug}")
    devices_found = []
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
                break
            else:
                print("Request failed:", e)
                break

        soup = BeautifulSoup(r.text, "html.parser")

        devices = soup.select(".makers ul li a")

        if not devices:
            break

        for device in devices:

            name = device.get_text(strip=True)

            if not is_valid_device(name):
                continue

            devices_found.append((name, urljoin(BASE_URL, device["href"])))

        print("Collected so far:", len(devices_found))

        page += 1
        delay()

    return devices_found

# =====================================================
# COLLECT TABLETS
# =====================================================

def collect_tablets():

    print("\n📱 Collecting tablets")
    links = []

    for url in TABLET_RESULTS:

        print("Reading:", url)

        try:
            r = requests.get(url, headers=headers(), timeout=15)
            r.raise_for_status()
        except Exception as e:
            print("Failed:", e)
            continue

        soup = BeautifulSoup(r.text, "html.parser")

        devices = soup.select(".makers ul li a")

        for device in devices:

            name = device.get_text(strip=True)

            if not is_valid_device(name):
                continue

            links.append(urljoin(BASE_URL, device["href"]))

        print("Tablet links collected:", len(links))

        delay()

    return links

# =====================================================
# MAIN
# =====================================================

def main():

    # resume support
    try:
        with open(OUTPUT, "r") as f:
            existing_links = json.load(f)
            seen_devices = {}
            for existing_url in existing_links:
                upsert_device_url(seen_devices, "", existing_url)
            print("Resuming with", len(seen_devices), "existing URLs")
    except:
        seen_devices = {}

    # collect phones
    for brand in BRANDS:

        brand_devices = collect_brand(brand)
        for name, link in brand_devices:
            upsert_device_url(seen_devices, name, link)

        all_links = sorted(v["url"] for v in seen_devices.values())

        with open(OUTPUT, "w") as f:
            json.dump(all_links, f, indent=2)

        print("✅ Progress saved:", len(all_links))

        print("Cooling down between brands...")
        time.sleep(random.uniform(15, 25))

    # collect tablets
    for url in TABLET_RESULTS:

        print("Reading:", url)

        try:
            r = requests.get(url, headers=headers(), timeout=15)
            r.raise_for_status()
        except Exception as e:
            print("Failed:", e)
            continue

        soup = BeautifulSoup(r.text, "html.parser")

        devices = soup.select(".makers ul li a")

        for device in devices:
            name = device.get_text(strip=True)
            if not is_valid_device(name):
                continue
            full_url = urljoin(BASE_URL, device["href"])
            upsert_device_url(seen_devices, name, full_url)

        print("Tablet links collected:", len(seen_devices))
        delay()

    all_links = sorted(v["url"] for v in seen_devices.values())
    with open(OUTPUT, "w") as f:
        json.dump(all_links, f, indent=2)

    print("\n🚀 DONE")
    print("Total device URLs collected:", len(all_links))


if __name__ == "__main__":
    main()
