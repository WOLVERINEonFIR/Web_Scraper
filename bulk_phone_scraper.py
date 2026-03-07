import requests
from bs4 import BeautifulSoup
import random
import time
import json
import re
import os
import hashlib
from pathlib import Path
from datetime import datetime, timezone

# =====================================================
# CONFIG
# =====================================================

INPUT_FILE = "output/demo.json"
OUTPUT_FILE = "output/phones_dataset1.json"
TMP_OUTPUT_FILE = "output/phones_dataset1.tmp.json"
PROGRESS_FILE = "output/progress1.json"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

Path("output").mkdir(exist_ok=True)

# =====================================================
# HELPERS
# =====================================================

def headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "DNT": "1",
        "Upgrade-Insecure-Requests": "1",
    }

def delay():
    time.sleep(random.uniform(20, 35))

RETRY_STATUS_CODES = {403, 408, 425, 429, 500, 502, 503, 504}
BLOCK_TEXT_MARKERS = [
    "too many requests",
    "you have been blocked",
    "unusual traffic",
    "access denied",
    "captcha",
    "temporarily unavailable",
]

def _is_block_page(text):
    if not text:
        return False
    low = text.lower()
    return any(marker in low for marker in BLOCK_TEXT_MARKERS)

def fetch_with_backoff(url, max_attempts=6):
    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            r = requests.get(url, headers=headers(), timeout=25)

            # Block pages can still return 200; treat as retryable throttle event.
            if r.status_code in RETRY_STATUS_CODES or _is_block_page(r.text):
                wait = random.uniform(35, 60) * attempt
                print(f"Throttle/block detected for {url} (attempt {attempt}/{max_attempts}). Sleeping {wait:.0f}s...")
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r

        except requests.HTTPError as e:
            last_error = e
            code = e.response.status_code if e.response is not None else None
            if code not in RETRY_STATUS_CODES or attempt == max_attempts:
                raise
            wait = random.uniform(25, 45) * attempt
            print(f"HTTP {code} for {url} (attempt {attempt}/{max_attempts}). Retrying in {wait:.0f}s...")
            time.sleep(wait)

        except requests.RequestException as e:
            last_error = e
            if attempt == max_attempts:
                raise
            wait = random.uniform(15, 30) * attempt
            print(f"Request error for {url} (attempt {attempt}/{max_attempts}): {e}. Retrying in {wait:.0f}s...")
            time.sleep(wait)

    if last_error:
        raise last_error
    raise RuntimeError(f"Failed to fetch URL: {url}")

def extract_number(text):
    if not text:
        return None
    m = re.search(r"\d+\.?\d*", text)
    return float(m.group()) if m else None

def extract_resolution(text):
    if not text:
        return None, None
    m = re.search(r"(\d+)\s*x\s*(\d+)", text)
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)

def extract_thickness(dimensions):
    if not dimensions:
        return None
    m = re.search(r"x\s*\d+\.?\d*\s*x\s*(\d+\.?\d*)", dimensions)
    return float(m.group(1)) if m else None

def extract_ip_rating(text):
    if not text:
        return None
    m = re.search(r"(IP\d{2})", text, re.I)
    return m.group(1) if m else None

def clean_sim_type(text):
    if not text:
        return None

    text = re.sub(r'^[+·•\s]+', '', text)
    text = re.sub(r'\s+', ' ', text).strip()

    parts = [p.strip() for p in text.split('+')]
    unique = []
    for p in parts:
        if p and p not in unique:
            unique.append(p)

    return " + ".join(unique) if unique else None

def save_dataset_atomic(dataset):
    with open(TMP_OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(dataset, f, indent=2, ensure_ascii=False)
    os.replace(TMP_OUTPUT_FILE, OUTPUT_FILE)

def load_progress():
    if not Path(PROGRESS_FILE).exists():
        return {"last_index": -1, "input_signature": None}
    try:
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if isinstance(obj, dict):
            return {
                "last_index": int(obj.get("last_index", -1)),
                "input_signature": obj.get("input_signature"),
            }
    except Exception:
        return {"last_index": -1, "input_signature": None}
    return {"last_index": -1, "input_signature": None}

def save_progress(last_index, input_signature):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(
            {
                "last_index": int(last_index),
                "input_signature": input_signature,
            },
            f,
            ensure_ascii=False
        )

def _norm_token(text):
    text = (text or "").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def build_unique_key(phone):
    brand = _norm_token(phone.get("brand"))
    model = _norm_token(phone.get("model_name"))
    if not brand or not model:
        return None
    return (brand, model)

def key_from_url(url):
    # Example: https://www.gsmarena.com/google_pixel_10_pro_fold-14014.php
    # slug -> google_pixel_10_pro_fold
    m = re.search(r"/([^/]+)-\d+\.php$", url)
    slug = m.group(1) if m else Path(url).stem
    model_guess = slug.replace("_", " ").strip()
    brand_guess = model_guess.split(" ")[0] if model_guess else ""
    if not brand_guess or not model_guess:
        return None, slug
    return (_norm_token(brand_guess), _norm_token(model_guess)), model_guess

# =====================================================
# SECTION PARSER
# =====================================================

def get_section_data(soup, section_name):
    section = soup.find("th", string=lambda s: s and s.strip().lower() == section_name.lower())
    if not section:
        return {}

    table = section.find_parent("table")
    data = {}

    for row in table.find_all("tr"):
        ttl = row.find("td", class_="ttl")
        nfo = row.find("td", class_="nfo")
        if ttl and nfo:
            data[ttl.get_text(strip=True)] = nfo.get_text(" ", strip=True)

    return data

# =====================================================
# PARSERS
# =====================================================

def parse_launch_status(status_text):
    """
    Extract device lifecycle status and actual release date.
    """

    status = None
    release_date = None

    if not status_text:
        return status, release_date

    text = status_text.lower()

    # -----------------------
    # Status detection
    # -----------------------
    if "coming soon" in text:
        status = "coming_soon"
    elif "discontinued" in text:
        status = "discontinued"
    elif "available" in text or "released" in text:
        status = "available"

    # -----------------------
    # Release date extraction
    # -----------------------
    # Example:
    # "Coming soon. Exp. release 2026, March 05"
    match = re.search(
        r"(\d{4},\s*[a-zA-Z]+\s*\d{1,2})",
        status_text
    )

    if match:
        release_date = match.group(1).strip()

    return status, release_date

def parse_variants(internal, soup):
    variants = []
    ufs_speed = None

    if internal:
        matches = re.findall(r"(\d+(?:TB|GB))\s+(\d+)\s*GB\s*RAM", internal, re.I)
        for storage, ram in matches:
            if "TB" in storage.upper():
                storage_gb = int(storage.replace("TB","")) * 1024
            else:
                storage_gb = int(storage.replace("GB",""))
            variants.append({
                "storage_gb": storage_gb,
                "ram_gb": int(ram),
                "price_inr": None
            })

    ufs_tag = soup.select_one('[data-spec="memoryother"]')
    if ufs_tag:
        ufs_speed = ufs_tag.get_text(strip=True)

    return variants, ufs_speed


def parse_charging(text):
    wired = wireless = None
    reverse_watt = None
    reverse_supported = False

    if not text:
        return wired, wireless, reverse_watt, reverse_supported

    # Wired charging (main)
    wired_values = re.findall(r"(\d+)\s*W\s*wired", text, re.I)
    if wired_values:
        wired = max(int(w) for w in wired_values)

    # Wireless charging (main)
    wireless_values = re.findall(r"(\d+)\s*W\s*wireless", text, re.I)
    if wireless_values:
        wireless = max(int(w) for w in wireless_values)

    # Reverse charging: only extract watts tied to reverse-specific segments
    reverse_values = []
    normalized = re.sub(r"(?i)<br\s*/?>|<hr\s*/?>", "\n", text)
    segments = [s.strip() for s in re.split(r"[\r\n]+", normalized) if s.strip()]
    if not segments:
        segments = [normalized.strip()]

    for seg in segments:
        if "reverse" not in seg.lower():
            continue

        seg_values = [
            float(v)
            for v in re.findall(
                r"\breverse\b(?:\s+[a-zA-Z-]+){0,4}\s+(\d+\.?\d*)\s*W",
                seg,
                re.I
            )
        ]
        seg_values.extend(
            float(v)
            for v in re.findall(
                r"(\d+\.?\d*)\s*W(?:\s+[a-zA-Z-]+){0,4}\s+\breverse\b",
                seg,
                re.I
            )
        )

        if not seg_values:
            all_watts = re.findall(r"(\d+\.?\d*)\s*W", seg, re.I)
            if len(all_watts) == 1:
                seg_values = [float(all_watts[0])]

        reverse_values.extend(seg_values)

    if reverse_values:
        reverse_supported = True
        reverse_watt = max(reverse_values)
    elif "reverse" in text.lower():
        reverse_supported = True

    return wired, wireless, reverse_watt, reverse_supported

def parse_fingerprint(sensor_text):
    fingerprint_location = None
    is_ultrasonic = False

    if not sensor_text:
        return fingerprint_location, is_ultrasonic

    # Extract fingerprint section
    match = re.search(r"Fingerprint\s*\((.*?)\)", sensor_text, re.I)
    if match:
        content = match.group(1).lower()

        # Location (before comma)
        parts = content.split(",")
        fingerprint_location = parts[0].strip()

        # Ultrasonic detection
        if "ultrasonic" in content:
            is_ultrasonic = True

    return fingerprint_location, is_ultrasonic

def parse_display(type_text):
    panel = refresh = peak = None

    if type_text:
        panel = type_text.split(",")[0].strip()

        hz = re.search(r"(\d+)Hz", type_text)
        if hz:
            refresh = int(hz.group(1))

        peak_match = re.search(r"(\d+)\s*nits", type_text, re.I)
        if peak_match:
            peak = int(peak_match.group(1))

    return panel, refresh, peak

def parse_build(build):
    front = back = frame = None
    if not build:
        return front, back, frame

    parts = build.split(",")
    for p in parts:
        low = p.lower()
        inside = re.search(r"\((.*?)\)", p)
        value = inside.group(1) if inside else p.strip()

        if "front" in low:
            front = value
        elif "back" in low:
            back = value
        elif "frame" in low:
            frame = value.replace("frame", "").strip()

    return front, back, frame


def parse_android(os_text):
    version = updates = None
    if not os_text:
        return version, updates

    v = re.search(r"(Android\s*\d+)", os_text)
    if v:
        version = v.group(1)

    up = re.search(r"(\d+)\s*major", os_text)
    if up:
        updates = int(up.group(1))

    return version, updates


def parse_video_features(video_text, features_text=None):
    max_video_resolution = None
    max_fps_4k = None
    has_hdr_video = False

    if not video_text and not features_text:
        return max_video_resolution, max_fps_4k, has_hdr_video

    video_upper = (video_text or "").upper()
    features_upper = (features_text or "").upper()

    # Resolution detection
    if "8K" in video_upper:
        max_video_resolution = "8K"
    elif "4K" in video_upper:
        max_video_resolution = "4K"
    elif "1080" in video_upper:
        max_video_resolution = "1080P"

    # 4K FPS extraction
    fps_match = re.search(r"4K@([\d/]+)FPS", video_upper)
    if fps_match:
        fps_values = fps_match.group(1).split("/")
        max_fps_4k = max(int(f) for f in fps_values)

    # HDR detection
    if "HDR" in video_upper or "HDR" in features_upper:
        has_hdr_video = True

    return max_video_resolution, max_fps_4k, has_hdr_video


def parse_benchmarks(soup):
    benchmarks = {
        "antutu": None,
        "geekbench": None,
        "3dmark": None,
        "gfxbench": None
    }

    bench_tag = soup.select_one('[data-spec="tbench"]')
    if not bench_tag:
        return benchmarks

    text = bench_tag.get_text(" ", strip=True)

    antutu = re.search(r"AnTuTu:\s*([\d,]+)", text, re.I)
    geek = re.search(r"GeekBench.*?:\s*([\d,]+)", text, re.I)
    dmark = re.search(r"3DMark:\s*([\d,]+)", text, re.I)
    gfx = re.search(r"GFXBench:\s*([\d,]+)", text, re.I)

    if antutu:
        benchmarks["antutu"] = int(antutu.group(1).replace(",", ""))
    if geek:
        benchmarks["geekbench"] = int(geek.group(1).replace(",", ""))
    if dmark:
        benchmarks["3dmark"] = int(dmark.group(1).replace(",", ""))
    if gfx:
        benchmarks["gfxbench"] = int(gfx.group(1).replace(",", ""))

    return benchmarks


def parse_rear_camera(soup):
    cam_tag = soup.select_one('[data-spec="cam1modules"]')
    video_tag = soup.select_one('[data-spec="cam1video"]')

    lenses = []
    ois = False
    pdaf = False

    if cam_tag:
        lines = cam_tag.get_text("\n").split("\n")
        for line in lines:
            mp_match = re.search(r"(\d+\.?\d*)\s*MP", line)
            if not mp_match:
                continue

            mp = float(mp_match.group(1))
            lower = line.lower()

            if (
                "ultra" in lower
                or "ultrawide" in lower
                or "ultra-wide" in lower
                or "°" in line
                or "˚" in line
            ):
                lens_type = "ultrawide"
            elif "periscope" in lower:
                lens_type = "periscope_telephoto"
            elif "telephoto" in lower:
                lens_type = "telephoto"
            elif "wide" in lower:
                lens_type = "wide"
            else:
                lens_type = "unknown"

            if "OIS" in line.upper():
                ois = True
            if "PDAF" in line.upper():
                pdaf = True

            lenses.append({"type": lens_type, "mp": mp})

    video = video_tag.get_text(strip=True) if video_tag else None
    return lenses, video, ois, pdaf

def parse_wifi(wifi_text):
    if not wifi_text:
        return None, None

    # Extract base WiFi standard
    standard_match = re.search(r"(802\.11)", wifi_text)
    wifi_standard = standard_match.group(1) if standard_match else None

    text = wifi_text.lower()

    generation = None
    detected_generations = []

    # -------------------------------------------------
    # 1️⃣ Detect numeric generations (WiFi 6 / 7 style)
    # -------------------------------------------------
    numeric_matches = re.findall(r"(?:wifi\s*)?([4-9])(?=[,/ ])", text)
    for num in numeric_matches:
        detected_generations.append(int(num))

    # -------------------------------------------------
    # 2️⃣ Detect IEEE standards
    # -------------------------------------------------
    if re.search(r"802\.11\s*be|\bbe\b(?=[,/ ])", text):
        detected_generations.append(7)

    if (
        "6e" in text
        or "6 ghz" in text
        or "6ghz" in text
        or "wifi 6e" in text
    ):
        generation = "WiFi 6E"

    if re.search(r"802\.11\s*ax|\bax\b", text):
        detected_generations.append(6)

    if re.search(r"\bac\b", text):
        detected_generations.append(5)

    if re.search(r"\bn\b", text):
        detected_generations.append(4)

    # -------------------------------------------------
    # 3️⃣ Choose highest detected generation
    # -------------------------------------------------
    if generation != "WiFi 6E" and detected_generations:
        max_gen = max(detected_generations)
        generation = f"WiFi {max_gen}"

    return wifi_standard, generation

def parse_bluetooth(bt_text):
    if not bt_text:
        return None

    # Extract version number (e.g., 5.4)
    match = re.search(r"(\d+(?:\.\d+)?)", bt_text)
    return float(match.group(1)) if match else None

def parse_release_parts(release_date_text):
    if not release_date_text:
        return None, None, None

    for fmt in ("%Y, %B %d", "%Y, %b %d"):
        try:
            dt = datetime.strptime(release_date_text.strip(), fmt).replace(tzinfo=timezone.utc)
            return dt.year, dt.month, int(dt.timestamp())
        except ValueError:
            continue

    # Fallback: year-only extraction if date parsing fails
    m = re.search(r"\b(\d{4})\b", release_date_text)
    if m:
        year = int(m.group(1))
        return year, None, None
    return None, None, None

def parse_sim_features(sim_text):
    if not sim_text:
        return False, False, None

    text = sim_text.lower()
    has_esim = "esim" in text

    sim_count_max = None

    max_at_time = re.findall(r"max\s+(\d+)\s+at\s+a\s+time", text)
    if max_at_time:
        sim_count_max = max(int(x) for x in max_at_time)

    nano_count = len(re.findall(r"nano-sim", text))
    if nano_count > 0:
        sim_count_max = max(sim_count_max or 0, nano_count)

    if "dual" in text:
        sim_count_max = max(sim_count_max or 0, 2)

    if sim_count_max is None and ("sim" in text or "esim" in text):
        sim_count_max = 1

    has_dual_sim = bool(sim_count_max and sim_count_max >= 2)
    return has_esim, has_dual_sim, sim_count_max

def compute_display_ppi(width, height, size_inches):
    if not width or not height or not size_inches:
        return None
    if size_inches <= 0:
        return None
    ppi = ((width ** 2 + height ** 2) ** 0.5) / size_inches
    return round(ppi, 1)

def parse_water_resistance_level(ip_rating):
    if not ip_rating:
        return None
    m = re.search(r"IP(\d{2})", ip_rating, re.I)
    if m:
        return int(m.group(1))
    m = re.search(r"IPX(\d)", ip_rating, re.I)
    if m:
        return int(m.group(1))
    return None

def compute_scores_and_tags(phone):
    performance = phone.get("performance", {})
    display = phone.get("display", {})
    battery = phone.get("battery", {})
    camera = phone.get("camera", {})
    rear = camera.get("rear", {})
    variants = phone.get("variants", [])

    antutu = (performance.get("benchmarks") or {}).get("antutu")
    geekbench = (performance.get("benchmarks") or {}).get("geekbench")
    dmark = (performance.get("benchmarks") or {}).get("3dmark")
    refresh = display.get("refresh_rate_hz") or 0
    panel_type = (display.get("panel_type") or "").lower()
    brightness = display.get("peak_brightness_nits")
    capacity = battery.get("capacity_mah")
    wired = battery.get("charging_watt")
    main_mp = None
    lenses = rear.get("lenses") or []
    if lenses:
        wide = [l.get("mp") for l in lenses if isinstance(l, dict) and l.get("type") == "wide" and l.get("mp") is not None]
        if wide:
            main_mp = max(wide)
        else:
            any_mp = [l.get("mp") for l in lenses if isinstance(l, dict) and l.get("mp") is not None]
            main_mp = max(any_mp) if any_mp else None

    max_ram = max((v.get("ram_gb") or 0) for v in variants) if variants else 0
    ppi = phone.get("filter", {}).get("display_ppi") or 0

    gaming_score = 0
    if antutu is not None:
        gaming_score += min(60, int(antutu / 35000))
    elif geekbench is not None:
        gaming_score += min(45, int(geekbench / 180))
    elif dmark is not None:
        gaming_score += min(40, int(dmark / 100))
    if refresh >= 120:
        gaming_score += 15
    elif refresh >= 90:
        gaming_score += 8
    if max_ram >= 12:
        gaming_score += 15
    elif max_ram >= 8:
        gaming_score += 10
    gaming_score = min(100, gaming_score)

    camera_score = 0
    if main_mp is not None:
        camera_score += min(30, int(main_mp / 2))
    lens_count = len([l for l in lenses if isinstance(l, dict)])
    if lens_count >= 3:
        camera_score += 20
    elif lens_count == 2:
        camera_score += 12
    if camera.get("ois"):
        camera_score += 20
    if rear.get("max_video_resolution") == "8K":
        camera_score += 20
    elif rear.get("max_video_resolution") == "4K":
        camera_score += 12
    if rear.get("has_hdr_video"):
        camera_score += 10
    camera_score = min(100, camera_score)

    display_score = 0
    if "oled" in panel_type or "amoled" in panel_type:
        display_score += 30
    if refresh >= 120:
        display_score += 25
    elif refresh >= 90:
        display_score += 15
    if brightness is not None:
        if brightness >= 2000:
            display_score += 25
        elif brightness >= 1200:
            display_score += 18
        elif brightness >= 800:
            display_score += 10
    if ppi >= 500:
        display_score += 20
    elif ppi >= 390:
        display_score += 12
    display_score = min(100, display_score)

    battery_score = 0
    if capacity is not None:
        if capacity >= 5500:
            battery_score += 45
        elif capacity >= 5000:
            battery_score += 35
        elif capacity >= 4500:
            battery_score += 25
        elif capacity >= 4000:
            battery_score += 15
    if wired is not None:
        if wired >= 100:
            battery_score += 30
        elif wired >= 67:
            battery_score += 24
        elif wired >= 45:
            battery_score += 18
        elif wired >= 25:
            battery_score += 10
    if battery.get("wireless_charging_watt"):
        battery_score += 10
    if battery.get("reverse_charging_supported"):
        battery_score += 5
    battery_score = min(100, battery_score)

    scores = {
        "gaming": gaming_score,
        "camera": camera_score,
        "display": display_score,
        "battery": battery_score,
        "scoring_version": 1
    }
    tags = {
        "can_game": gaming_score >= 65,
        "good_camera": camera_score >= 65,
        "good_display": display_score >= 65,
        "good_battery": battery_score >= 65
    }
    return scores, tags

def collect_missing_fields(phone):
    checks = [
        ("release_date", phone.get("release_date")),
        ("performance.chipset", phone.get("performance", {}).get("chipset")),
        ("display.peak_brightness_nits", phone.get("display", {}).get("peak_brightness_nits")),
        ("memory.storage_type", phone.get("memory", {}).get("storage_type")),
        ("physical.ip_rating", phone.get("physical", {}).get("ip_rating")),
        ("camera.rear.max_fps_4k", phone.get("camera", {}).get("rear", {}).get("max_fps_4k")),
    ]
    missing = [name for name, value in checks if value is None or value == ""]
    return missing

def parse_selfie_camera(soup):
    cam_tag = soup.select_one('[data-spec="cam2modules"]')
    video_tag = soup.select_one('[data-spec="cam2video"]')

    mp = extract_number(cam_tag.get_text()) if cam_tag else None
    video = video_tag.get_text(strip=True) if video_tag else None

    return mp, video

# =====================================================
# SCRAPER
# =====================================================

def scrape_prices(price_url):
    """
    Scrapes variant prices from GSMArena price page.
    Returns dict like:
    {
        (128, 4): 11999,
        (128, 6): 12999
    }
    """

    price_map = {}
    price_hash = None

    try:
        # Small gap before secondary request to reduce burst patterns.
        time.sleep(random.uniform(2.0, 5.0))
        r = fetch_with_backoff(price_url)
        price_html = r.text
        price_hash = hashlib.md5(price_html.encode("utf-8")).hexdigest()
        soup = BeautifulSoup(price_html, "html.parser")

        tables = soup.select('table[class*="pricing"]')
        if not tables:
            return price_map, price_hash

        india_table = None
        for table in tables:
            caption = table.find("caption")
            caption_text = caption.get_text(" ", strip=True).lower() if caption else ""
            if "india" in caption_text:
                india_table = table
                break
        if not india_table:
            return price_map, price_hash

        thead = india_table.find("thead")
        tbody = india_table.find("tbody")
        if not thead or not tbody:
            return price_map, price_hash

        headers_row = thead.find_all("th")[1:]
        variant_labels = [h.get_text(" ", strip=True) for h in headers_row]

        for row in tbody.find_all("tr"):
            price_cells = row.find_all("td")
            for label, cell in zip(variant_labels, price_cells):
                price_text = cell.get_text(" ", strip=True)
                if not price_text:
                    continue

                price_digits = re.sub(r"[^\d]", "", price_text)
                if not price_digits:
                    continue
                price_value = int(price_digits)

                # Extract storage + RAM from labels like "256GB 16GB RAM", "1TB 16GB RAM"
                match = re.search(r"(\d+)\s*(TB|GB)\s*(\d+)\s*GB\s*RAM", label, re.I)
                if not match:
                    continue

                storage = int(match.group(1))
                if match.group(2).upper() == "TB":
                    storage *= 1024
                ram = int(match.group(3))
                key = (storage, ram)

                # Multiple store rows may exist; keep the lowest observed price.
                if key in price_map:
                    price_map[key] = min(price_map[key], price_value)
                else:
                    price_map[key] = price_value

    except Exception as e:
        print("Price scrape error:", e)

    return price_map, price_hash

def scrape_phone(url):

    print("Fetching:", url)
    r = fetch_with_backoff(url)
    time.sleep(random.uniform(1.5, 3.5))  # human read pause
    spec_html = r.text
    spec_hash = hashlib.md5(spec_html.encode("utf-8")).hexdigest()
    soup = BeautifulSoup(spec_html, "html.parser")
    now_iso = datetime.now(timezone.utc).isoformat()

    name = soup.select_one(".specs-phone-name-title").text.strip()
    brand = name.split()[0]

    img_tag = soup.select_one(".specs-photo-main img")
    image_url = img_tag["src"] if img_tag else None

    launch = get_section_data(soup, "Launch")
    display_sec = get_section_data(soup, "Display")
    memory_sec = get_section_data(soup, "Memory")
    battery_sec = get_section_data(soup, "Battery")
    body_sec = get_section_data(soup, "Body")
    platform_sec = get_section_data(soup, "Platform")
    sound_sec = get_section_data(soup, "Sound")
    comms_sec = get_section_data(soup, "Comms")
    network_sec = get_section_data(soup, "Network")
    features_sec = get_section_data(soup, "Features")
     
    status_text = launch.get("Status")
    device_status, release_date = parse_launch_status(status_text)

    fingerprint_location, is_ultrasonic = parse_fingerprint(
        features_sec.get("Sensors")
    )

    wifi_standard, wifi_generation = parse_wifi(comms_sec.get("WLAN"))
    bluetooth = parse_bluetooth(comms_sec.get("Bluetooth"))
    has_5g = "5G" in network_sec.get("Technology", "").upper()

    nfc_value = comms_sec.get("NFC")
    has_nfc = None
    if nfc_value:
        has_nfc = "yes" in nfc_value.lower()
    ir_value = comms_sec.get("Infrared port")
    has_ir = False
    if ir_value:
        has_ir = "yes" in ir_value.lower()
    
    size = extract_number(display_sec.get("Size"))
    res_w, res_h = extract_resolution(display_sec.get("Resolution"))
    panel, refresh, peak = parse_display(display_sec.get("Type"))
    if refresh is None:
        refresh = 60

    variants, ufs = parse_variants(memory_sec.get("Internal"), soup)
    price_url = re.sub(r"-(\d+)\.php$", r"-price-\1.php", url)

    price_map, price_hash = scrape_prices(price_url)

    for v in variants:
        v["price_last_checked_at"] = now_iso

    # Map prices to variants
    for v in variants:
        key = (v["storage_gb"], v["ram_gb"])
        if key in price_map:
            v["price_inr"] = price_map[key]
    battery_type = battery_sec.get("Type")
    capacity = None
    if battery_type:
        m = re.search(r"(\d{3,5})\s*mAh", battery_type)
        if m:
            capacity = int(m.group(1))

    wired, wireless, reverse, reverse_supported = parse_charging(battery_sec.get("Charging"))
    if wired is None and brand.lower() == "apple":
        wired = 20

    front, back, frame = parse_build(body_sec.get("Build"))

    ip_tag = soup.select_one('[data-spec="bodyother"]')
    ip_rating = extract_ip_rating(ip_tag.get_text()) if ip_tag else None

    rear_lenses, rear_video, ois, pdaf = parse_rear_camera(soup)
    rear_features_tag = soup.select_one('[data-spec="cam1features"]')
    rear_features_text = rear_features_tag.get_text(" ", strip=True) if rear_features_tag else None
    max_res, max_fps_4k, has_hdr_video = parse_video_features(rear_video, rear_features_text)

    selfie_mp, selfie_video = parse_selfie_camera(soup)
    selfie_features_tag = soup.select_one('[data-spec="cam2features"]')
    selfie_features_text = selfie_features_tag.get_text(" ", strip=True) if selfie_features_tag else None
    s_res, s_fps, s_hdr = parse_video_features(selfie_video, selfie_features_text)

    android_version, updates = parse_android(platform_sec.get("OS"))
    if updates is None:
        updates = 5

    reliable_upto = None
    if release_date and updates:
        m_rel = re.match(r"^\s*(\d{4})(.*)$", release_date)
        if m_rel:
            release_year = int(m_rel.group(1))
            suffix = m_rel.group(2) or ""
            reliable_year = release_year + updates - 1
            reliable_upto = f"{reliable_year}{suffix}"

    benchmarks = parse_benchmarks(soup)

    has_3_5mm_jack = False
    loudspeaker_type = None

    if sound_sec:
        jack_value = sound_sec.get("3.5mm jack")
        if jack_value and "Yes" in jack_value:
            has_3_5mm_jack = True

        loudspeaker_value = sound_sec.get("Loudspeaker")
        if loudspeaker_value:
            if "stereo" in loudspeaker_value.lower():
                loudspeaker_type = "stereo"
            elif "dolby" in loudspeaker_value.lower():
                loudspeaker_type = "dolby"
            else:
                loudspeaker_type = "mono"

    phone = {
        "device_type": "phone",
        "brand": brand,
        "model_name": name,
        "status": device_status,
        "release_date": release_date,
        "image": image_url,

        "performance": {
            "chipset": platform_sec.get("Chipset"),
            "cpu": platform_sec.get("CPU"),
            "gpu": platform_sec.get("GPU"),
            "benchmarks": benchmarks
        },

        "memory": {
            "storage_type": ufs,
            "expandable": "microSD" in memory_sec.get("Card slot", "")
        },

        "variants": variants,

        "display": {
            "size_inches": size,
            "resolution_width": res_w,
            "resolution_height": res_h,
            "refresh_rate_hz": refresh,
            "panel_type": panel,
            "peak_brightness_nits": peak
        },

        "battery": {
            "capacity_mah": capacity,
            "charging_watt": wired,
            "wireless_charging_watt": wireless,
            "reverse_charging_watt": reverse,
            "reverse_charging_supported": reverse_supported
        },

        "physical": {
            "weight_g": extract_number(body_sec.get("Weight")),
            "thickness_mm": extract_thickness(body_sec.get("Dimensions")),
            "front_material": front,
            "back_material": back,
            "frame_material": frame,
            "sim_type": clean_sim_type(body_sec.get("SIM")),
            "ip_rating": ip_rating,
            "has_3_5mm_jack": has_3_5mm_jack,
            "loudspeaker_type": loudspeaker_type,
            "fingerprint_location": fingerprint_location,
            "is_ultrasonic_fingerprint": is_ultrasonic,
        },
        "connectivity": {
            "wifi_standard": wifi_standard,
            "wifi_generation": wifi_generation,
            "bluetooth": bluetooth,
            "has_nfc": has_nfc,
            "has_ir": has_ir,
            "has_5g": has_5g
        },

        "camera": {
            "rear": {
                "lenses": rear_lenses,
                "max_video_resolution": max_res,
                "max_fps_4k": max_fps_4k,
                "has_hdr_video": has_hdr_video
            },
            "selfie": {
                "mp": selfie_mp,
                "max_video_resolution": s_res,
                "max_fps_4k": s_fps,
                "has_hdr_video": s_hdr
            },
            "ois": ois,
            "pdaf": pdaf
        },

        "software": {
            "launch_android_version": android_version,
            "major_android_updates_years": updates,
            "reliable_upto": reliable_upto
        }
    }

    release_year, release_month, release_ts = parse_release_parts(release_date)
    priced_variants = [v for v in variants if isinstance(v, dict) and v.get("price_inr") is not None]
    price_values = [v.get("price_inr") for v in priced_variants]
    ram_options = sorted({v.get("ram_gb") for v in variants if isinstance(v, dict) and v.get("ram_gb") is not None})
    storage_options = sorted({v.get("storage_gb") for v in variants if isinstance(v, dict) and v.get("storage_gb") is not None})
    sim_text = phone.get("physical", {}).get("sim_type")
    has_esim, has_dual_sim, sim_count_max = parse_sim_features(sim_text)
    display_ppi = compute_display_ppi(res_w, res_h, size)
    panel_text = (panel or "").lower()
    is_ltpo = "ltpo" in panel_text
    model_text = name.lower()
    is_foldable = ("fold" in model_text) or ("flip" in model_text) or ("foldable" in model_text)
    water_level = parse_water_resistance_level(ip_rating)

    phone["filter"] = {
        "release_year": release_year,
        "release_month": release_month,
        "release_ts": release_ts,
        "price_min_inr": min(price_values) if price_values else None,
        "price_max_inr": max(price_values) if price_values else None,
        "price_available_variant_count": len(price_values),
        "has_price_data": len(price_values) > 0,
        "ram_options_gb": ram_options,
        "storage_options_gb": storage_options,
        "sim": {
            "has_esim": has_esim,
            "has_dual_sim": has_dual_sim,
            "sim_count_max": sim_count_max
        },
        "display_ppi": display_ppi,
        "is_ltpo": is_ltpo,
        "is_foldable": is_foldable,
        "water_resistance_level": water_level
    }

    phone["source"] = {
        "spec_url": url,
        "price_url": price_url
    }

    scores, tags = compute_scores_and_tags(phone)
    phone["scores"] = scores
    phone["tags"] = tags

    missing_fields = collect_missing_fields(phone)
    phone["parse_quality"] = {
        "missing_fields": missing_fields,
        "missing_fields_count": len(missing_fields)
    }

    phone["schema_version"] = 2
    phone["scraped_at"] = now_iso
    phone["spec_last_verified_at"] = now_iso
    phone["spec_hash"] = spec_hash
    phone["price_hash"] = price_hash

    phone.setdefault("scraped_at", now_iso)
    phone.setdefault("spec_last_verified_at", now_iso)
    phone.setdefault("spec_hash", None)
    phone.setdefault("price_hash", None)

    return phone


def main():
    urls = json.load(open(INPUT_FILE))
    input_signature = hashlib.md5("\n".join(urls).encode("utf-8")).hexdigest()
    dataset = []
    if Path(OUTPUT_FILE).exists():
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                existing = json.load(f)
            if isinstance(existing, list):
                dataset = existing
            else:
                print("Warning: existing output is not a list, starting fresh dataset")
        except Exception as e:
            print("Warning: could not read existing output file, starting fresh dataset:", e)

    scraped_keys = set()
    for p in dataset:
        key = build_unique_key(p) if isinstance(p, dict) else None
        if key:
            scraped_keys.add(key)

    progress = load_progress()
    if progress.get("input_signature") == input_signature:
        start_index = max(0, progress.get("last_index", -1) + 1)
    else:
        start_index = 0
        if progress.get("last_index", -1) >= 0:
            print("Progress file input differs from current URL list. Starting from index 0.")
    total = len(urls)
    required_root_keys = ("device_type", "brand", "model_name", "schema_version")

    if start_index > 0:
        print(f"Resuming from index {start_index} of {total}")

    for idx, url in enumerate(urls):
        if idx < start_index:
            continue

        guessed_key, guessed_model = key_from_url(url)
        if guessed_key and guessed_key in scraped_keys:
            print(f"[SKIP] Already scraped: {guessed_model}")
            save_progress(idx, input_signature)
            delay()
            continue

        try:
            print(f"[{idx + 1}/{total}] Scraping: {guessed_model}")
            phone = scrape_phone(url)

            missing = [k for k in required_root_keys if k not in phone]
            if missing or not (phone.get("brand") and phone.get("model_name") and phone.get("device_type")):
                print(f"Warning: skipping phone due to missing keys {missing}")
                save_progress(idx, input_signature)
                delay()
                continue

            dataset.append(phone)
            key = build_unique_key(phone)
            if key:
                scraped_keys.add(key)

            save_progress(idx, input_signature)

            if len(dataset) % 20 == 0:
                save_dataset_atomic(dataset)
                print("[AUTOSAVE] Dataset saved safely")
                cooldown = random.uniform(60, 120)
                print(f"Cooldown after autosave: sleeping {cooldown:.0f}s")
                time.sleep(cooldown)
        except Exception as e:
            print("Error:", e)
        delay()

    save_dataset_atomic(dataset)

    print("DONE")


if __name__ == "__main__":
    main()
