import requests
from bs4 import BeautifulSoup
import random
import time
import json
import math
import re
import os
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urljoin

# =====================================================
# CONFIG
# =====================================================

INPUT_FILE = "output/demo.json"
OUTPUT_FILE = "output/phones_data.json"
TMP_OUTPUT_FILE = "output/phones_data.tmp.json"
PROGRESS_FILE = "output/progress1.json"
BASE_URL = "https://www.gsmarena.com/"
DEBUG = False

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
    patterns = [
        r"(\d+(?:\.\d+)?)",
        r"(\d+)\s*",
        r"(\d+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text)
        if not m:
            continue
        value = m.group(1)
        try:
            return float(value) if "." in value else int(value)
        except ValueError:
            continue
    return None

def extract_resolution(text):
    if not text:
        return None, None
    patterns = [
        r"(\d+)\s*x\s*(\d+)",
        r"(\d+)x(\d+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.I)
        if not m:
            continue
        try:
            return int(m.group(1)), int(m.group(2))
        except ValueError:
            continue
    return None, None

def extract_thickness(dimensions):
    if not dimensions:
        return None

    text = str(dimensions)
    unfolded_match = re.search(r"(\d+(?:\.\d+)?)\s*mm\s*\(\s*unfolded\s*\)", text, re.I)
    folded_match = re.search(r"(\d+(?:\.\d+)?)\s*mm\s*\(\s*folded\s*\)", text, re.I)

    if unfolded_match and folded_match:
        try:
            return {
                "unfolded": float(unfolded_match.group(1)),
                "folded": float(folded_match.group(1)),
            }
        except ValueError:
            return None

    # Keep current dimensions parsing first, then fall back to generic mm extraction.
    patterns = [
        r"x\s*\d+\.?\d*\s*x\s*(\d+\.?\d*)",
        r"(\d+(?:\.\d+)?)\s*mm",
        r"(\d+(?:\.\d+)?)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.I)
        if not m:
            continue
        try:
            return float(m.group(1))
        except ValueError:
            continue
    return None

def extract_ip_rating(text):
    if not text:
        return None
    m = re.search(r"\b(IP(?:\d{2}|X\d))\b", str(text), re.I)
    return m.group(1).upper() if m else None

def derive_ip_rating(text, device_type=None):

    if not text:
        return None

    t = str(text).lower()

    m = re.search(r"ip(\d{2})", t)
    if m:
        return f"IP{m.group(1)}"

    if "ip69" in t:
        return "IP69"

    if "water repellent" in t or "nano coating" in t:
        return "IP52"

    if "water resistant" in t or "splash resistant" in t:
        if device_type == "tablet":
            return "IPX4"
        return "IP54"

    return None

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

def parse_variants(internal, soup, brand, chipset_tier):
    variants = []
    ufs_speed = None
    seen = set()

    if internal:
        matches = re.findall(
            r"(\d+(?:TB|GB))\s*,?\s*(\d+)\s*GB\s*RAM",
            internal,
            re.I
        )

        for storage, ram in matches:
            num = int(re.search(r"\d+", storage).group())

            if "TB" in storage.upper():
                storage_gb = num * 1024
            else:
                storage_gb = num

            key = (storage_gb, int(ram))
            if key in seen:
                continue
            seen.add(key)

            variants.append({
                "storage_gb": storage_gb,
                "ram_gb": int(ram),
                "price_inr": None,
                "price_last_checked_at": None,
                "price_source": None,
                "price_url": None,
            })

    def _extract_storage_type(text):

        raw = str(text or "")
        if not raw:
            return None

        # Detect UFS / NVMe first
        primary = re.search(r"\b(UFS\s*[-]?\s*\d+(?:\.\d+)?)\b|NVMe", raw, re.I)

        if primary:
            value = primary.group(0)

            if "ufs" in value.lower():
                num = re.search(r"\d+(?:\.\d+)?", value)
                if num:
                    return f"UFS {num.group(0)}"
                return "UFS"

            return "NVMe"

        # fallback detection
        fallback = re.search(r"\beMMC\s*\d+(?:\.\d+)?\b|\bSSD\b", raw, re.I)

        if fallback:
            value = fallback.group(0)

            if "emmc" in value.lower():
                num = re.search(r"\d+(?:\.\d+)?", value)
                return f"eMMC {num.group(0)}" if num else "eMMC"

            return "SSD"

        return None

    ufs_tag = soup.select_one('[data-spec="memoryother"]')
    if ufs_tag:
        ufs_speed = _extract_storage_type(ufs_tag.get_text(" ", strip=True))

    if not ufs_speed:
        ufs_speed = _extract_storage_type(internal)

    if brand and brand.lower() == "apple" and not ufs_speed:
        ufs_speed = "NVMe"

    # Chipset tier fallback
    if not ufs_speed and chipset_tier:

        if chipset_tier == "ultimate":
            ufs_speed = "UFS 4.1"

        elif chipset_tier == "flagship":
            ufs_speed = "UFS 4.0"

        elif chipset_tier == "upper_mid":
            ufs_speed = "UFS 3.1"

        elif chipset_tier == "midrange":
            ufs_speed = "UFS 2.2"

    return variants, ufs_speed

def parse_charging(text):
    wired = wireless = None
    reverse_watt = None
    reverse_supported = False

    if not text:
        return wired, wireless, reverse_watt, reverse_supported

    # Wired charging
    wired_values = re.findall(r"(\d+)\s*W\s*(?:wired|charging|fast)", text, re.I)
    if wired_values:
        wired = max(int(w) for w in wired_values)

    # Wireless charging
    wireless_values = re.findall(
        r"(\d+)\s*W\s*wireless(?:\s*charging)?",
        text,
        re.I
    )
    if wireless_values:
        wireless = max(int(w) for w in wireless_values)

    # Reverse charging
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
    panel = refresh = None
    typical_nits = None
    hbm_nits = None
    peak_nits = None
    supports_hdr10 = False
    supports_hdr10_plus = False
    supports_dolby_vision = False

    if type_text:
        panel = type_text.split(",")[0].strip()
        display_text = type_text.lower()

        hz_values = [int(v) for v in re.findall(r"(\d+)\s*Hz", type_text, re.I)]

        valid_rates = [v for v in hz_values if 30 <= v <= 240]

        if valid_rates:
            refresh = max(valid_rates)

        # fallback
        if refresh is None:
            panel_text = panel.lower() if panel else ""

            if "ltpo" in panel_text:
                refresh = 120
            elif "amoled" in panel_text or "oled" in panel_text:
                refresh = 120
            elif "ips" in panel_text:
                refresh = 60
            elif "lcd" in panel_text:
                refresh = 60
                
        typ_match = re.search(r"(\d{3,4})\s*nits\s*\(typ\)", type_text, re.I)
        if typ_match:
            typical_nits = int(typ_match.group(1))

        hbm_match = re.search(r"(\d{3,4})\s*nits\s*\(HBM\)", type_text, re.I)
        if hbm_match:
            hbm_nits = int(hbm_match.group(1))

        peak_match = re.search(
            r"(\d{3,4})\s*nits(?:\s*\(peak\)|\s*peak)",
            type_text,
            re.I
        )
        if peak_match:
            peak_nits = int(peak_match.group(1))

        untagged_matches = re.findall(
            r"(\d{3,4})\s*nits(?!\s*\((?:typ|hbm|peak)\))",
            type_text,
            re.I
        )

        if typical_nits is None and untagged_matches:
            typical_nits = max(int(v) for v in untagged_matches)

        if peak_nits is None:
            if hbm_nits is not None:
                peak_nits = hbm_nits
            elif typical_nits is not None:
                peak_nits = typical_nits

        if "hdr10+" in display_text:
            supports_hdr10_plus = True
        if "hdr10" in display_text:
            supports_hdr10 = True
        if "dolby vision" in display_text:
            supports_dolby_vision = True

        if typical_nits is None and hbm_nits is None and peak_nits is None:

            panel_text = panel.lower() if panel else ""

            if "ltpo" in panel_text and ("amoled" in panel_text or "oled" in panel_text):
                peak_nits = 1500
            elif "amoled" in panel_text or "oled" in panel_text:
                peak_nits = 1000
            elif "ips lcd" in panel_text:
                peak_nits = 500
            elif "lcd" in panel_text:
                peak_nits = 400
    return (
        panel,
        refresh,
        typical_nits,
        hbm_nits,
        peak_nits,
        supports_hdr10,
        supports_hdr10_plus,
        supports_dolby_vision,
    )

import re

def parse_build(build, is_foldable=False):
    front = back = frame = None

    if not build:
        return front, back, frame

    build_text = str(build)

    # -------- Detect foldable inner display --------
    if re.search(r"\bplastic\s+front\s+when\s+unfolded\b", build_text, re.I):
        front = "plastic"

    # Remove fold-state wording to prevent parsing errors
    build_text = re.sub(r"(?i)\bwhen\s+folded\b", "", build_text)
    build_text = re.sub(r"(?i)\bwhen\s+unfolded\b", "", build_text)

    # Normalize spacing
    build_text = re.sub(r"\s+,", ",", build_text)
    build_text = re.sub(r"\s{2,}", " ", build_text).strip()

    parts = re.split(r",(?![^()]*\))", build_text)

    for p in parts:
        low = p.lower()
        inside = re.search(r"\((.*?)\)", p)

        # Parse front only if not already forced by foldable detection
        if "front" in low and front is None:
            if inside:
                front = inside.group(1).strip()
            else:
                front = re.sub(r"(?i)\bfront\b", "", p).strip(" ,-")

        elif "back" in low:
            if inside:
                back = inside.group(1).strip()
            else:
                back = re.sub(r"(?i)\bback\b", "", p).strip(" ,-")

        elif "frame" in low:
            outside = re.sub(r"\(.*?\)", "", p)
            outside = re.sub(r"(?i)\bframe\b", "", outside).strip(" ,-")
            inside_val = inside.group(1).strip() if inside else ""

            combined = " ".join(part for part in [outside, inside_val] if part)
            combined = re.sub(r"\s+", " ", combined).strip()

            frame = combined if combined else None

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

    # -------------------------
    # Resolution detection
    # -------------------------

    if "8K" in video_upper or "4320" in video_upper:
        max_video_resolution = "8K"

    elif "4K" in video_upper or "2160" in video_upper:
        max_video_resolution = "4K"

    elif "1080" in video_upper:
        max_video_resolution = "1080P"

    # -------------------------
    # 4K FPS detection
    # -------------------------

    fps_match = re.search(r"4K@([\d/,]+)", video_upper)

    if fps_match:
        fps_values = re.findall(r"\d+", fps_match.group(1))
        if fps_values:
            max_fps_4k = max(int(f) for f in fps_values)

    # -------------------------
    # HDR detection
    # -------------------------

    if "HDR" in video_upper or "HDR" in features_upper:
        has_hdr_video = True

    return max_video_resolution, max_fps_4k, has_hdr_video

def extract_process_node_nm(chipset):
    if not chipset:
        return None

    m = re.search(r"(\d+)\s*nm", str(chipset).lower())
    if m:
        return int(m.group(1))

    return None

def derive_process_node_nm(chipset_name):
    if not chipset_name:
        return None

    text = str(chipset_name).lower()

    match = re.search(r"(\d+)\s*nm", text)
    if match:
        return float(match.group(1))

    if any(x in text for x in [
        "8 elite", "gen 4", "gen 5",
        "a18", "a19",
        "dimensity 9400"
    ]):
        return 3.0

    if any(x in text for x in [
        "8 gen 3",
        "8s gen 3",
        "dimensity 9300",
        "a17"
    ]):
        return 4.0

    return None


def detect_device_type(url, page_html):
    url_text = (url or "").lower()
    if "ipad" in url_text or "tab" in url_text or "tablet" in url_text or "pad" in url_text:
        return "tablet"
    return "phone"


def validate_device(device):

    required_root_keys = ("device_type", "brand", "model_name", "schema_version")

    if not isinstance(device, dict):
        return None

    for key in required_root_keys:
        value = device.get(key)
        if value is None or value == "":
            return None

    display = device.get("display", {})
    battery = device.get("battery", {})

    size_inches = display.get("size_inches")

    if isinstance(size_inches, (int, float)) and not (3 <= size_inches <= 20):
        display["size_inches"] = None

    refresh = display.get("refresh_rate_hz")

    if isinstance(refresh, (int, float)) and not (30 <= refresh <= 240):
        display["refresh_rate_hz"] = None

    capacity = battery.get("capacity_mah")

    if isinstance(capacity, (int, float)) and not (500 <= capacity <= 30000):
        battery["capacity_mah"] = None

    device["display"] = display
    device["battery"] = battery

    missing_fields = collect_missing_fields(device)

    device["parse_quality"] = {
        "missing_fields": missing_fields,
        "missing_fields_count": len(missing_fields)
    }

    return device

def parse_release_date_iso(release_date_text):

    if not release_date_text:
        return None

    text = release_date_text.strip()

    formats = [
        "%Y, %B %d",
        "%Y, %b %d",
        "%Y, %B",
        "%Y, %b",
        "%Y"
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt)

            # fill missing components
            if fmt == "%Y":
                dt = dt.replace(month=1, day=1)
            elif fmt in ("%Y, %B", "%Y, %b"):
                dt = dt.replace(day=1)

            return dt.strftime("%Y-%m-%d")

        except ValueError:
            continue

    return None

def parse_benchmarks(soup):

    benchmarks = {
        "antutu": None,
        "geekbench": None,
        "gfxbench": None
    }

    bench_tag = soup.select_one('[data-spec="tbench"]')

    if not bench_tag:
        return benchmarks

    text = bench_tag.get_text(" ", strip=True)

    antutu = re.search(r"AnTuTu:\s*([\d,]+)", text, re.I)
    geek = re.search(r"GeekBench.*?:\s*([\d,]+)", text, re.I)
    gfx = re.search(r"GFXBench.*?:\s*([\d,.]+)", text, re.I)

    if antutu:
        benchmarks["antutu"] = int(antutu.group(1).replace(",", ""))

    if geek:
        benchmarks["geekbench"] = int(geek.group(1).replace(",", ""))

    if gfx:
        benchmarks["gfxbench"] = int(float(gfx.group(1).replace(",", "")))

    return benchmarks

def parse_battery_test_metrics(soup):
    active_use_hours = None
    endurance_hours = None

    if not soup:
        return active_use_hours, endurance_hours

    text = soup.get_text(" ", strip=True)

    active_match = re.search(r"Active use score\s*(\d{1,2}):(\d{2})h", text, re.I)
    if active_match:
        try:
            hours = int(active_match.group(1))
            minutes = int(active_match.group(2))
            active_use_hours = round(hours + (minutes / 60.0), 2)
        except ValueError:
            active_use_hours = None

    endurance_match = re.search(r"Endurance rating\s*(\d+)\s*h", text, re.I)
    if endurance_match:
        try:
            endurance_hours = int(endurance_match.group(1))
        except ValueError:
            endurance_hours = None

    return active_use_hours, endurance_hours

def parse_rear_camera(soup):

    cam_tag = soup.select_one('[data-spec="cam1modules"]')
    video_tag = soup.select_one('[data-spec="cam1video"]')

    lenses = []
    ois = False
    pdaf = False

    if cam_tag:
        lines = [ln.strip() for ln in cam_tag.get_text("\n").split("\n") if ln.strip()]
        current_lens = None

        for line in lines:
            upper = line.upper()
            if "OIS" in upper:
                ois = True
            if "PDAF" in upper:
                pdaf = True

            mp_match = re.search(r"(\d+(?:\.\d+)?)\s*MP", line, re.I)
            if mp_match:
                mp_val = float(mp_match.group(1))
                mp = int(mp_val) if mp_val.is_integer() else mp_val
                lower = line.lower()

                if "ultra" in lower or "ultrawide" in lower or "°" in line or "˚" in line:
                    lens_type = "ultrawide"
                elif "periscope" in lower:
                    lens_type = "periscope_telephoto"
                elif "telephoto" in lower:
                    lens_type = "telephoto"
                elif "macro" in lower:
                    lens_type = "macro"
                elif "depth" in lower:
                    lens_type = "depth"
                elif "mono" in lower:
                    lens_type = "monochrome"
                elif "wide" in lower:
                    lens_type = "wide"
                else:
                    lens_type = "unknown"

                aperture_match = re.search(r"f/\d+(?:\.\d+)?", line, re.I)
                sensor_size_match = re.search(
                    r"1/\d+(?:\.\d+)?\s*\"?|\d+(?:\.\d+)?\s*\"(?:-type)?|\d+(?:\.\d+)?-inch",
                    line,
                    re.I
                )
                focal_length_match = re.search(r"(\d+)\s*mm", line, re.I)
                pixel_size_match = re.search(r"(\d+(?:\.\d+)?)\s*(?:µ|μ)m", line, re.I)

                sensor_size = None
                if sensor_size_match:
                    raw_sensor = sensor_size_match.group(0).strip().lower()
                    raw_sensor = raw_sensor.replace('"', "")
                    raw_sensor = raw_sensor.replace("-type", "")
                    raw_sensor = raw_sensor.replace("-inch", "")
                    raw_sensor = raw_sensor.strip()
                    if re.fullmatch(r"\d+\.0+", raw_sensor):
                        raw_sensor = str(int(float(raw_sensor)))
                    sensor_size = raw_sensor or None
                aperture = aperture_match.group(0).lower() if aperture_match else None
                focal_length_mm = int(focal_length_match.group(1)) if focal_length_match else None
                pixel_size_um = float(pixel_size_match.group(1)) if pixel_size_match else None

                current_lens = {
                    "type": lens_type,
                    "mp": mp,
                    "sensor_size": sensor_size,
                    "aperture": aperture,
                    "focal_length_mm": focal_length_mm,
                    "pixel_size_um": pixel_size_um,
                }
                lenses.append(current_lens)
                continue

            if not current_lens:
                continue

            if current_lens.get("sensor_size") is None:
                sensor_size_match = re.search(
                    r"1/\d+(?:\.\d+)?\s*\"?|\d+(?:\.\d+)?\s*\"(?:-type)?|\d+(?:\.\d+)?-inch",
                    line,
                    re.I
                )
                if sensor_size_match:
                    raw_sensor = sensor_size_match.group(0).strip().lower()
                    raw_sensor = raw_sensor.replace('"', "")
                    raw_sensor = raw_sensor.replace("-type", "")
                    raw_sensor = raw_sensor.replace("-inch", "")
                    raw_sensor = raw_sensor.strip()
                    if re.fullmatch(r"\d+\.0+", raw_sensor):
                        raw_sensor = str(int(float(raw_sensor)))
                    current_lens["sensor_size"] = raw_sensor or None

            if current_lens.get("pixel_size_um") is None:
                pixel_size_match = re.search(r"(\d+(?:\.\d+)?)\s*(?:µ|μ)m", line, re.I)
                if pixel_size_match:
                    current_lens["pixel_size_um"] = float(pixel_size_match.group(1))

            if current_lens.get("focal_length_mm") is None:
                focal_length_match = re.search(r"(\d+)\s*mm", line, re.I)
                if focal_length_match:
                    current_lens["focal_length_mm"] = int(focal_length_match.group(1))

            if current_lens.get("aperture") is None:
                aperture_match = re.search(r"f/\d+(?:\.\d+)?", line, re.I)
                if aperture_match:
                    current_lens["aperture"] = aperture_match.group(0).lower()

    video = video_tag.get_text(strip=True) if video_tag else None

    return lenses, video, ois, pdaf

def parse_wifi(wifi_text):

    if not wifi_text:
        return None, None, []

    text = wifi_text.lower()

    match = re.search(r"(802\.11\s*[a-z0-9/]+)", text)

    wifi_standard = match.group(1).replace(" ", "") if match else None
    if wifi_standard:
        wifi_standard = wifi_standard.replace("802.11", "802.11 ").strip()

    generation = None

    if "be" in text or "/7" in text:
        generation = "WiFi 7"
    elif "6e" in text:
        generation = "WiFi 6E"
    elif "ax" in text or "/6" in text:
        generation = "WiFi 6"
    elif "ac" in text:
        generation = "WiFi 5"
    elif "n" in text:
        generation = "WiFi 4"

    features = []

    if "dual-band" in text:
        features.append("dual-band")

    if "tri-band" in text:
        features.append("tri-band")

    if "hotspot" in text:
        features.append("hotspot")

    if "wi-fi direct" in text or "wifi direct" in text:
        features.append("wifi-direct")

    if "tri-band" in features:
        features = [f for f in features if f != "dual-band"]

    return wifi_standard, generation, features

import re

def parse_bluetooth(bt_text):

    if not bt_text:
        return None, []

    text = bt_text.strip()

    # -------------------------
    # Version detection
    # -------------------------

    version = None

    # Preferred match (with word Bluetooth)
    match = re.search(r"Bluetooth\s*(\d+(?:\.\d+)?)", text, re.I)

    if match:
        version = match.group(1)

    else:
        # Fallback if GSMArena omits the word Bluetooth
        fallback = re.search(r"^\s*(\d+(?:\.\d+)?)", text)

        if fallback:
            version = fallback.group(1)

    # Safety check to avoid WiFi confusion
    if version and float(version) < 4:
        version = None

    # -------------------------
    # Feature detection
    # -------------------------

    features = []

    feature_keywords = [
        "A2DP",
        "LE",
        "aptX",
        "aptX HD",
        "aptX Adaptive",
        "aptX Lossless",
        "LDAC",
        "LHDC",
        "LC3"
    ]

    text_lower = text.lower()

    for feature in feature_keywords:
        if feature.lower() in text_lower:
            features.append(feature)

    return version, features

def parse_release_parts(release_date_text):

    if not release_date_text:
        return None, None, None

    text = release_date_text.strip()

    formats = [
        "%Y, %B %d",
        "%Y, %b %d",
        "%Y, %B",
        "%Y, %b",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)

            # If day missing, default to 1
            if "%d" not in fmt:
                dt = dt.replace(day=1)

            return dt.year, dt.month, int(dt.timestamp())

        except ValueError:
            continue

    # Quarter format (Q1–Q4)
    q = re.search(r"(\d{4}),?\s*q([1-4])", text, re.I)
    if q:
        year = int(q.group(1))
        quarter = int(q.group(2))
        month = (quarter - 1) * 3 + 1
        dt = datetime(year, month, 1, tzinfo=timezone.utc)
        return year, month, int(dt.timestamp())

    # Year only
    y = re.search(r"\b(\d{4})\b", text)
    if y:
        year = int(y.group(1))
        return year, None, None

    return None, None, None

def parse_sim_features(sim_text):
    if not sim_text:
        return False, False, None

    text = sim_text.lower()
    has_esim = "esim" in text
    has_nano = "nano-sim" in text or "nano sim" in text

    if (
        "dual" in text
        or re.search(r"max\s*2", text, re.I)
        or (has_nano and has_esim)
        or text.count("esim") >= 2
    ):
        sim_count_max = 2
    else:
        sim_count_max = 1

    has_dual_sim = bool(sim_count_max and sim_count_max >= 2)

    return has_esim, has_dual_sim, sim_count_max

def compute_display_ppi(width, height, size_inches):

    try:
        width = float(width)
        height = float(height)
        size_inches = float(size_inches)
    except (TypeError, ValueError):
        return None

    if width <= 0 or height <= 0 or size_inches <= 0:
        return None

    ppi = ((width ** 2 + height ** 2) ** 0.5) / size_inches

    return round(ppi, 1)

def parse_water_resistance_level(ip_rating):

    if not ip_rating:
        return None

    matches = re.findall(r"IP(\d{2})", ip_rating, re.I)

    if matches:
        return max(int(m) for m in matches)

    m = re.search(r"IPX(\d)", ip_rating, re.I)

    if m:
        return int(m.group(1))

    return None


def clamp_score(value, low=0, high=100):

    if value is None:
        return low

    try:
        value = float(value)
    except (TypeError, ValueError):
        return low

    if math.isnan(value):
        return low

    return max(low, min(high, int(round(value))))

def derive_chipset_tier(chipset_name, current_tier=None):
    # Preserve already assigned values
    if current_tier is not None:
        return current_tier

    text = (chipset_name or "").lower()
    if not text:
        return None

    # -------------------------
    # Google Tensor
    # -------------------------

    if "tensor g" in text:
        return "flagship"

    # -------------------------
    # Apple Silicon
    # -------------------------

    # iPad chips
    if re.search(r"\bapple\s*m\d\b", text):
        return "ultimate"

    # iPhone chips
    if re.search(r"\bapple\s*a(18|19|20)\b", text):
        return "ultimate"

    if re.search(r"\bapple\s*a(16|17)\b", text):
        return "flagship"

    if re.search(r"\bapple\s*a(14|15)\b", text):
        return "upper_mid"

    if re.search(r"\bapple\s*a(12|13)\b", text):
        return "midrange"

    # -------------------------
    # Snapdragon
    # -------------------------

    if "snapdragon" in text:

        if re.search(r"8\s*elite", text):
            return "ultimate"

        if re.search(r"snapdragon\s*8", text):
            return "flagship"

        if re.search(r"snapdragon\s*8s", text):
            return "upper_mid"

        if re.search(r"snapdragon\s*7\+", text):
            return "upper_mid"

        if re.search(r"snapdragon\s*7", text):
            return "midrange"

        if re.search(r"snapdragon\s*6", text):
            return "budget_mid"

        if re.search(r"snapdragon\s*4", text):
            return "budget"

        # legacy Snapdragon chips
        if re.search(r"(870|865|860|855)", text):
            return "flagship"

        if re.search(r"(778|780|782)", text):
            return "upper_mid"

        if re.search(r"(695|690)", text):
            return "midrange"

    # -------------------------
    # MediaTek Dimensity
    # -------------------------

    if "dimensity" in text:

        if re.search(r"9[4-9]00", text):
            return "ultimate"

        if re.search(r"9[0-3]00", text):
            return "flagship"

        if re.search(r"8[2-3]00", text):
            return "upper_mid"

        if re.search(r"8[0-1]00", text):
            return "upper_mid"

        if re.search(r"7[2-3]00", text):
            return "midrange"

        if re.search(r"7050", text):
            return "midrange"

        if re.search(r"6[0-1]00", text):
            return "budget_mid"

        if re.search(r"6000", text):
            return "budget"

    # -------------------------
    # Exynos
    # -------------------------

    if "exynos" in text:

        if re.search(r"(2400|2300|2200|2100)", text):
            return "flagship"

        if re.search(r"1480", text):
            return "upper_mid"

        if re.search(r"(1380|1280)", text):
            return "midrange"

        if re.search(r"(9611|9610)", text):
            return "midrange"

        if re.search(r"850", text):
            return "budget"

    return None

def derive_ltpo(panel, refresh, chipset_tier, display_sec=None):

    panel_text = (panel or "").lower()

    # direct LTPO mention
    if "ltpo" in panel_text:
        return True

    display_blob = ""

    if display_sec:
        display_blob = " ".join(str(v) for v in display_sec.values()).lower()

    combined = panel_text + " " + display_blob

    # adaptive refresh detection
    if re.search(r"\b1[-–]?\s*120\s*hz\b", combined):
        return True

    if re.search(r"\b10[-–]?\s*120\s*hz\b", combined):
        return True

    if re.search(r"\b1[-–]?\s*144\s*hz\b", combined):
        return True

    # Samsung panels
    if "dynamic amoled 2x" in combined and chipset_tier in ["flagship", "ultimate"]:
        return True

    return False

def derive_lens_type(focal_length):

    if not focal_length:
        return None

    if 20 <= focal_length <= 30:
        return "wide"

    if 13 <= focal_length <= 19:
        return "ultrawide"

    if 70 <= focal_length <= 150:
        return "telephoto"

    return None

def estimate_active_use_score(
    capacity,
    refresh,
    node_nm,
    is_ltpo=False,
    display_size=None,
    device_type=None
):

    if not capacity:
        return None

    # -------------------------
    # Base model
    # -------------------------

    if device_type == "tablet":
        base_hours = capacity / 520
    else:
        base_hours = capacity / 370

    # -------------------------
    # Refresh rate penalty
    # -------------------------

    if refresh and refresh >= 120:
        penalty = 0.4 if is_ltpo else 1.2
        base_hours -= penalty

    # -------------------------
    # Process node efficiency
    # -------------------------

    if node_nm and node_nm <= 3:
        base_hours += 1.5
    elif node_nm and node_nm >= 6:
        base_hours -= 2.0

    # -------------------------
    # Display size penalty
    # -------------------------

    if display_size:

        if device_type == "tablet":
            if display_size >= 12:
                base_hours -= 2.0
            elif display_size >= 10:
                base_hours -= 1.5
            elif display_size >= 8:
                base_hours -= 1.0

        else:
            if display_size >= 6.8:
                base_hours -= 0.5

    return round(base_hours, 2)

def estimate_charging_watt(advertised_text):

    if not advertised_text:
        return None

    m = re.search(r"50%\s*in\s*(\d+)\s*min", advertised_text)

    if not m:
        return None

    minutes = int(m.group(1))

    if minutes <= 15:
        return 120

    elif minutes <= 20:
        return 100

    elif minutes <= 30:
        return 80

    elif minutes <= 40:
        return 65

    elif minutes <= 60:
        return 45

    return None

GPU_TIER_MAP = {

    # ULTIMATE
    "Adreno 840": "ultimate",
    "Adreno 830": "ultimate",
    "Immortalis-G925": "ultimate",
    "Xclipse 950": "ultimate",

    # FLAGSHIP
    "Adreno 750": "flagship",
    "Adreno 740": "flagship",
    "Immortalis-G720": "flagship",   # Dimensity 9300
    "Mali-G715": "flagship",
    "Mali-G710": "flagship",
    "Xclipse 940": "flagship",
    "Apple GPU": "ultimate",

    # UPPER MID
    "Adreno 829": "upper_mid",
    "Adreno 825": "upper_mid",
    "Adreno 735": "upper_mid",
    "Adreno 732": "upper_mid",
    "Adreno 730": "upper_mid",
    "Mali-G720": "upper_mid",        # Dimensity 8300
    "Mali-G615": "upper_mid",
    "Xclipse 920": "upper_mid",

    # MIDRANGE
    "Adreno 722": "midrange",
    "Adreno 720": "midrange",
    "Adreno 710": "midrange",
    "Adreno 650": "midrange",
    "Adreno 660": "midrange",
    "Mali-G610": "midrange",
    "Mali-G68": "midrange",
    "Mali-G57": "midrange"
}

def derive_gpu_tier(gpu_name, fallback=None):

    if not gpu_name:
        return fallback or "midrange"

    gpu_lower = str(gpu_name).lower()

    # Remove core suffix (MC7 / MC10 etc)
    gpu_lower = re.sub(r"\bmc\d+\b", "", gpu_lower).strip()

    # Apple GPUs perform at top mobile tier
    if "apple gpu" in gpu_lower:

        if "6-core" in gpu_lower or "5-core" in gpu_lower:
            return "ultimate"

        return fallback or "flagship"

    for key, tier in GPU_TIER_MAP.items():
        if key.lower() in gpu_lower:
            return tier

    return fallback or "midrange"

def _max_numeric(values):
    nums = []
    for v in values or []:
        if isinstance(v, (int, float)):
            nums.append(float(v))
    return max(nums) if nums else None

def _to_float(value):
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        m = re.search(r"(\d+(?:\.\d+)?)", value.replace(",", ""))
        if m:
            try:
                return float(m.group(1))
            except ValueError:
                return None
    return None

def _sensor_fraction(sensor_size):
    text = str(sensor_size or "").strip()
    m = re.search(r"(\d+(?:\.\d+)?)\s*/\s*(\d+(?:\.\d+)?)", text)
    if not m:
        return None
    num = _to_float(m.group(1))
    den = _to_float(m.group(2))
    if not num or not den:
        return None
    return num / den

def _score_software(major_updates_years):
    years = _to_float(major_updates_years)
    if years is None:
        years = 2
    if years >= 7:
        return 100
    if years >= 5:
        return 85
    if years >= 4:
        return 75
    if years >= 3:
        return 65
    if years >= 2:
        return 55
    return 0

def _score_thermal_efficiency(process_node_nm):
    node = _to_float(process_node_nm) or 0
    if node <= 0:
        return 0
    if node <= 3:
        return 15
    if node <= 4:
        return 12
    if node <= 5:
        return 10
    if node <= 6:
        return 8
    if node <= 7:
        return 6
    if node <= 10:
        return 4
    return 2

def _score_phone_gaming(performance, display, filter_data, memory):
    score = 0

    performance = performance or {}
    display = display or {}
    filter_data = filter_data or {}
    memory = memory or {}

    # thermal efficiency bonus
    process_node_nm = _to_float(performance.get("process_node_nm"))

    if process_node_nm is None:
        process_node_nm = extract_process_node_nm(performance.get("chipset"))
        if process_node_nm is None:
            process_node_nm = derive_process_node_nm(performance.get("chipset"))
        if process_node_nm is None:
            process_node_nm = 6
        performance["process_node_nm"] = process_node_nm

    score += _score_thermal_efficiency(process_node_nm)

    # chipset tier scoring
    chipset_tier = performance.get("chipset_tier")

    score += {
        "ultimate": 45,
        "flagship": 40,
        "upper_mid": 32,
        "midrange": 25,
        "budget_mid": 18,
        "budget": 10,
    }.get(chipset_tier, 0)

    # GPU tier scoring
    gpu_tier = performance.get("gpu_tier")

    score += {
        "ultimate": 25,
        "flagship": 20,
        "upper_mid": 15,
        "midrange": 10,
        "budget": 5,
    }.get(gpu_tier, 0)

    # RAM scoring
    max_ram = _max_numeric(filter_data.get("ram_options_gb") or [])
    if max_ram is None:
        max_ram = 6

    if max_ram >= 16:
        score += 15
    elif max_ram >= 12:
        score += 12
    elif max_ram >= 8:
        score += 8
    elif max_ram >= 6:
        score += 5

    # refresh rate scoring
    refresh = _to_float(display.get("refresh_rate_hz")) or 0

    if refresh >= 144:
        score += 10
    elif refresh >= 120:
        score += 8
    elif refresh >= 90:
        score += 5

    # storage type scoring
    storage_type = str(memory.get("storage_type") or "").lower()
    if "nvme" in storage_type:
        score += 7
    elif "ufs 4" in storage_type:
        score += 6
    elif "ufs 3" in storage_type:
        score += 4
    elif "ufs" in storage_type:
        score += 2
    else:
        score += 2

    return clamp_score(score)

def _score_camera(camera):
    score = 0
    camera = camera or {}
    rear = camera.get("rear") or {}
    lenses = rear.get("lenses") or []

    main_mp = 0
    best_sensor_fraction = 0
    has_ultrawide = False
    has_tele = False
    has_periscope = False
    lens_count = 0

    for lens in lenses:
        if not isinstance(lens, dict):
            continue
        lens_count += 1
        lens_mp = _to_float(lens.get("mp")) or 0
        if lens_mp > main_mp:
            main_mp = lens_mp

        sensor_fraction = _sensor_fraction(lens.get("sensor_size"))
        if sensor_fraction is None:
            sensor_fraction = (1 / 3)
        if sensor_fraction > best_sensor_fraction:
            best_sensor_fraction = sensor_fraction

        lens_type = str(lens.get("type") or "wide").lower()
        if "ultrawide" in lens_type:
            has_ultrawide = True
        if "telephoto" in lens_type or "periscope" in lens_type:
            has_tele = True
        if "periscope" in lens_type:
            has_periscope = True

    if main_mp >= 200:
        score += 25
    elif main_mp >= 108:
        score += 22
    elif main_mp >= 64:
        score += 18
    elif main_mp >= 50:
        score += 16
    elif main_mp >= 48:
        score += 14
    elif main_mp >= 12:
        score += 8

    if best_sensor_fraction >= (1 / 1.3):
        score += 20
    elif best_sensor_fraction >= (1 / 1.5):
        score += 16
    elif best_sensor_fraction >= (1 / 2):
        score += 12

    if lens_count >= 4:
        score += 20
    elif lens_count == 3:
        score += 12
    elif lens_count == 2:
        score += 6

    if camera.get("ois"):
        score += 10
    if has_ultrawide:
        score += 8
    if has_tele:
        score += 10
    if has_periscope:
        score += 12

    max_video_resolution = str(rear.get("max_video_resolution") or "").upper()
    max_fps_4k = _to_float(rear.get("max_fps_4k")) or 0
    has_4k = ("4K" in max_video_resolution) or ("8K" in max_video_resolution) or (max_fps_4k > 0)
    if has_4k:
        score += 6
    if rear.get("has_hdr_video"):
        score += 6

    return clamp_score(score)

def _score_display(display, filter_data):
    score = 0
    display = display or {}
    filter_data = filter_data or {}

    ppi = _to_float(filter_data.get("display_ppi")) or 0
    if ppi >= 500:
        score += 20
    elif ppi >= 450:
        score += 18
    elif ppi >= 400:
        score += 16
    elif ppi >= 350:
        score += 12

    refresh = _to_float(display.get("refresh_rate_hz")) or 0
    if refresh >= 144:
        score += 18
    elif refresh >= 120:
        score += 15
    elif refresh >= 90:
        score += 8

    panel_text = str(display.get("panel_type") or "").lower()
    if "ltpo" in panel_text:
        score += 20
    elif "amoled" in panel_text:
        score += 15
    elif "oled" in panel_text:
        score += 12
    elif "ips" in panel_text:
        score += 6

    brightness = _to_float(display.get("peak_nits"))
    if brightness is None:
        brightness = _to_float(display.get("hbm_nits"))
    if brightness is None:
        brightness = _to_float(display.get("typical_nits"))
    if brightness is None:
        brightness = _to_float(display.get("peak_brightness_nits"))
    if brightness is None and "fold" in panel_text:
        brightness = 1000
    if brightness is None:
        brightness = 800
    brightness = brightness or 0
    if brightness >= 2500:
        score += 20
    elif brightness >= 1500:
        score += 15
    elif brightness >= 1000:
        score += 10

    if display.get("supports_hdr10_plus"):
        score += 10
    if display.get("supports_hdr10"):
        score += 5
    if display.get("supports_dolby_vision"):
        score += 5

    return clamp_score(score)

def _score_tablet_display(display, filter_data):
    score = 0
    display = display or {}
    filter_data = filter_data or {}

    size = _to_float(display.get("size_inches")) or 0
    if size >= 12:
        score += 8
    elif size >= 10:
        score += 6
    elif size >= 8:
        score += 4

    # Tablet panels naturally run lower PPI at larger sizes.
    ppi = _to_float(filter_data.get("display_ppi")) or 0
    if ppi >= 350:
        score += 20
    elif ppi >= 300:
        score += 16
    elif ppi >= 250:
        score += 12
    elif ppi >= 200:
        score += 8

    refresh = _to_float(display.get("refresh_rate_hz")) or 0
    if refresh >= 144:
        score += 18
    elif refresh >= 120:
        score += 15
    elif refresh >= 90:
        score += 8

    panel_text = str(display.get("panel_type") or "").lower()
    if "ltpo" in panel_text:
        score += 20
    elif "amoled" in panel_text:
        score += 15
    elif "oled" in panel_text:
        score += 12
    elif "ips" in panel_text:
        score += 6

    brightness = _to_float(display.get("peak_nits"))
    if brightness is None:
        brightness = _to_float(display.get("hbm_nits"))
    if brightness is None:
        brightness = _to_float(display.get("typical_nits"))
    if brightness is None:
        brightness = _to_float(display.get("peak_brightness_nits"))
    if brightness is None:
        brightness = 800
    brightness = brightness or 0
    if brightness >= 800:
        score += 10
    elif brightness >= 500:
        score += 8

    if display.get("supports_hdr10_plus"):
        score += 10
    if display.get("supports_hdr10"):
        score += 5
    if display.get("supports_dolby_vision"):
        score += 5

    return clamp_score(score)

def _score_battery(battery):
    score = 0
    battery = battery or {}

    active_use = _to_float(battery.get("active_use_hours"))
    endurance = _to_float(battery.get("endurance_hours"))
    capacity = _to_float(battery.get("capacity_mah")) or 0

    if active_use is not None:
        if active_use >= 16:
            score += 30
        elif active_use >= 13:
            score += 26
        elif active_use >= 11:
            score += 22
        elif active_use >= 9:
            score += 18
        elif active_use >= 7:
            score += 14
    elif endurance is not None:
        if endurance >= 120:
            score += 30
        elif endurance >= 100:
            score += 26
        elif endurance >= 80:
            score += 22
        elif endurance >= 60:
            score += 18
    else:
        if capacity >= 6000:
            score += 22
        elif capacity >= 5000:
            score += 18
        elif capacity >= 4500:
            score += 14
        elif capacity >= 4000:
            score += 10

    charging = _to_float(battery.get("charging_watt"))
    if charging is None:
        charging = _to_float((battery.get("charging") or {}).get("estimated_watt"))
    charging = charging or 0
    if charging >= 120:
        score += 25
    elif charging >= 80:
        score += 20
    elif charging >= 65:
        score += 15
    elif charging >= 45:
        score += 10
    elif charging >= 25:
        score += 6

    wireless = _to_float(battery.get("wireless_charging_watt")) or 0
    if wireless > 0:
        score += 10

    if battery.get("reverse_charging_supported"):
        score += 5

    return clamp_score(score)

def _score_connectivity(connectivity):
    score = 0
    connectivity = connectivity or {}
    wifi = connectivity.get("wifi") or {}
    bluetooth = connectivity.get("bluetooth") or {}

    wifi_generation = str(wifi.get("generation") or "WiFi 5").strip()
    wifi_key = wifi_generation.lower().replace(" ", "")
    if wifi_key == "wifi7":
        score += 25
    elif wifi_key == "wifi6e":
        score += 22
    elif wifi_key == "wifi6":
        score += 18
    elif wifi_key == "wifi5":
        score += 12
    else:
        score += 8

    bt_version = _to_float(bluetooth.get("version"))
    if bt_version is None:
        bt_version = 5.0
    if bt_version >= 6.0:
        score += 20
    elif bt_version >= 5.4:
        score += 18
    elif bt_version >= 5.3:
        score += 16
    elif bt_version >= 5.2:
        score += 14
    else:
        score += 10

    if connectivity.get("has_nfc"):
        score += 12
    if connectivity.get("has_ir"):
        score += 6
    if connectivity.get("has_5g"):
        score += 15

    return clamp_score(score)

def _score_tablet_performance(performance, filter_data):
    score = 0
    chipset_tier = performance.get("chipset_tier")
    score += {
        "ultimate": 50,
        "flagship": 42,
        "upper_mid": 32,
        "midrange": 25,
        "budget_mid": 18,
        "budget": 10,
    }.get(chipset_tier, 0)

    gpu_tier = performance.get("gpu_tier")
    score += {
        "ultimate": 30,
        "flagship": 22,
        "upper_mid": 15,
        "midrange": 10,
        "budget": 5,
    }.get(gpu_tier, 0)

    max_ram = _max_numeric(filter_data.get("ram_options_gb") or [])
    if max_ram is not None:
        if max_ram >= 16:
            score += 15
        elif max_ram >= 12:
            score += 12
        elif max_ram >= 8:
            score += 8
        elif max_ram >= 6:
            score += 5

    return clamp_score(score)

def _score_tablet_media(display, physical):
    score = 0
    size = _to_float((display or {}).get("size_inches")) or 0
    if size >= 13:
        score += 35
    elif size >= 11:
        score += 28
    elif size >= 10:
        score += 22
    elif size >= 8:
        score += 14

    has_hdr = (
        (display or {}).get("supports_hdr10")
        or (display or {}).get("supports_hdr10_plus")
        or (display or {}).get("supports_dolby_vision")
    )
    if has_hdr:
        score += 35

    loudspeaker_type = str((physical or {}).get("loudspeaker_type") or "").lower()
    if loudspeaker_type in {"stereo", "dolby"}:
        score += 30

    return clamp_score(score)

def compute_phone_scores(device):
    performance = device.get("performance") or {}
    display = device.get("display") or {}
    battery = device.get("battery") or {}
    camera = device.get("camera") or {}
    filter_data = device.get("filter") or {}
    memory = device.get("memory") or {}
    connectivity = device.get("connectivity") or {}

    chipset_tier = derive_chipset_tier(performance.get("chipset"), performance.get("chipset_tier")) or "midrange"
    gpu_tier = derive_gpu_tier(performance.get("gpu"), performance.get("gpu_tier") or chipset_tier)
    process_node_nm = _to_float(performance.get("process_node_nm"))
    if process_node_nm is None:
        process_node_nm = extract_process_node_nm(performance.get("chipset"))
        if process_node_nm is None:
            process_node_nm = derive_process_node_nm(performance.get("chipset"))
    performance["chipset_tier"] = chipset_tier
    performance["gpu_tier"] = gpu_tier
    performance["process_node_nm"] = process_node_nm
    device["performance"] = performance

    gaming_score = _score_phone_gaming(performance, display, filter_data, memory)
    camera_score = _score_camera(camera)
    display_score = _score_display(display, filter_data)
    battery_score = _score_battery(battery)
    connectivity_score = _score_connectivity(connectivity)
    software_score = _score_software((device.get("software") or {}).get("major_android_updates_years"))
    # Internal overall weighting intentionally omitted from output schema.

    return {
        "gaming": gaming_score,
        "camera": camera_score,
        "display": display_score,
        "battery": battery_score,
        "scoring_version": 5,
    }

def compute_tablet_scores(device):
    performance = device.get("performance") or {}
    display = device.get("display") or {}
    battery = device.get("battery") or {}
    camera = device.get("camera") or {}
    filter_data = device.get("filter") or {}
    physical = device.get("physical") or {}
    connectivity = device.get("connectivity") or {}

    chipset_tier = derive_chipset_tier(performance.get("chipset"), performance.get("chipset_tier")) or "midrange"
    gpu_tier = derive_gpu_tier(performance.get("gpu"), performance.get("gpu_tier") or chipset_tier)
    process_node_nm = _to_float(performance.get("process_node_nm"))
    if process_node_nm is None:
        process_node_nm = extract_process_node_nm(performance.get("chipset"))
        if process_node_nm is None:
            process_node_nm = derive_process_node_nm(performance.get("chipset"))
    performance["chipset_tier"] = chipset_tier
    performance["gpu_tier"] = gpu_tier
    performance["process_node_nm"] = process_node_nm
    device["performance"] = performance

    performance_score = _score_tablet_performance(performance, filter_data)
    media_score = _score_tablet_media(display, physical)
    camera_score = _score_camera(camera)
    display_score = _score_tablet_display(display, filter_data)
    battery_score = _score_battery(battery)
    connectivity_score = _score_connectivity(connectivity)
    software_score = _score_software((device.get("software") or {}).get("major_android_updates_years"))
    # Keep schema stable by exposing the performance/media blend as scores.gaming.
    gaming_score = clamp_score((performance_score * 0.8) + (media_score * 0.2))

    # Internal overall weighting intentionally omitted from output schema.

    return {
        "gaming": gaming_score,
        "camera": camera_score,
        "display": display_score,
        "battery": battery_score,
        "scoring_version": 5,
    }

def compute_scores_and_tags(device):
    device_type = str(device.get("device_type") or "").lower()
    if device_type == "tablet":
        scores = compute_tablet_scores(device)
    else:
        scores = compute_phone_scores(device)

    tags = {
        "can_game": (scores.get("gaming") or 0) >= 70,
        "good_camera": (scores.get("camera") or 0) >= 65,
        "good_display": (scores.get("display") or 0) >= 70,
        "good_battery": (scores.get("battery") or 0) >= 45,
    }

    device["scores"] = scores
    device["tags"] = tags
    return device
def collect_missing_fields(phone):
    checks = [
        ("release_date", phone.get("release_date")),
        ("performance.chipset", phone.get("performance", {}).get("chipset")),
        ("display.peak_nits", phone.get("display", {}).get("peak_nits")),
        ("memory.storage_type", phone.get("memory", {}).get("storage_type")),
        ("physical.ip_rating", phone.get("physical", {}).get("ip_rating")),
        ("camera.rear.max_fps_4k", phone.get("camera", {}).get("rear", {}).get("max_fps_4k")),
    ]
    missing = [name for name, value in checks if value is None or value == ""]
    return missing

def parse_selfie_camera(soup):
    cam_tag = soup.select_one('[data-spec="cam2modules"]')
    video_tag = soup.select_one('[data-spec="cam2video"]')

    cam_text = cam_tag.get_text(" ", strip=True) if cam_tag else None
    mp_match = re.search(r"(\d+)\s*MP", cam_text or "", re.I)
    mp = int(mp_match.group(1)) if mp_match else (extract_number(cam_text) if cam_text else None)

    aperture_match = re.search(r"f/\d+(?:\.\d+)?", cam_text or "", re.I)
    focal_length_match = re.search(r"(\d+)\s*mm", cam_text or "", re.I)
    lower = (cam_text or "").lower()

    lens_type = None
    if "ultrawide" in lower or "ultra-wide" in lower or "ultra wide" in lower:
        lens_type = "ultrawide"
    elif "wide" in lower:
        lens_type = "wide"
    elif "telephoto" in lower:
        lens_type = "telephoto"

    has_pdaf = ("pdaf" in lower) if cam_text else None
    has_3d_sensor = (
        any(token in lower for token in ("3d", "depth", "biometrics")) if cam_text else None
    )
    video = video_tag.get_text(strip=True) if video_tag else None

    return {
        "mp": mp,
        "aperture": aperture_match.group(0).lower() if aperture_match else None,
        "focal_length_mm": int(focal_length_match.group(1)) if focal_length_match else None,
        "lens_type": lens_type,
        "has_pdaf": has_pdaf,
        "has_3d_sensor": has_3d_sensor,
        "video": video,
    }

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
    store_url = None
    store_source = None

    try:
        # Small gap before secondary request to reduce burst patterns.
        time.sleep(random.uniform(2.0, 5.0))
        r = fetch_with_backoff(price_url)
        price_html = r.text
        price_hash = hashlib.md5(price_html.encode("utf-8")).hexdigest()
        soup = BeautifulSoup(price_html, "html.parser")
        if DEBUG:
            print("===== PRICE SCRAPE START =====")
            print("Price page URL:", price_url)
            print("HTML snippet:", soup.prettify()[:300])

        tables = soup.select("table.pricing")
        if DEBUG:
            print("Tables found:", len(tables))
            for table in tables:
                caption = table.find("caption")
                print("Table caption:", caption.text if caption else None)
        if not tables:
            return price_map, price_hash, store_url, store_source

        india_table = None
        for table in tables:
            caption = table.find("caption")
            if caption and "india" in caption.get_text(" ", strip=True).lower():
                india_table = table
                break
        if DEBUG:
            print("India table detected:", india_table is not None)
        if not india_table:
            return price_map, price_hash, store_url, store_source

        thead = india_table.find("thead")
        tbody = india_table.find("tbody")
        if not thead or not tbody:
            return price_map, price_hash, store_url, store_source

        headers_row = thead.find_all("th")[1:]
        variant_labels = [h.get_text(" ", strip=True) for h in headers_row]

        modal_fetched = False
        for row in tbody.find_all("tr"):
            price_cells = row.find_all("td")
            for label, cell in zip(variant_labels, price_cells):
                if not modal_fetched:
                    price_link = cell.select_one("a[data-url]")
                    if DEBUG:
                        print("price_link found:", price_link is not None)
                        if price_link:
                            print("price_link attributes:", price_link.attrs)
                    if price_link and price_link.get("data-url"):
                        modal_path = price_link.get("data-url")
                        if DEBUG:
                            print("modal_path:", modal_path)
                        modal_url = urljoin(BASE_URL, modal_path)
                        if DEBUG:
                            print("Modal URL:", modal_url)
                        try:
                            modal_response = requests.get(modal_url, headers=headers(), timeout=15)
                            if DEBUG:
                                print("Modal status:", modal_response.status_code)
                                print("Modal HTML snippet:", modal_response.text[:300])
                            modal_response.raise_for_status()
                            store_url = None
                            store_source = None

                            match = re.search(r'https:\\/\\/www\.amazon\.in\\/[^"]+', modal_response.text)

                            if match:
                                store_url = match.group(0)

                                # unescape slashes
                                store_url = store_url.replace('\\/', '/')

                                # remove tracking parameters
                                store_url = store_url.split("?")[0]

                                store_source = "Amazon"
                        except Exception as e:
                            print("Modal store scrape error:", e)
                        modal_fetched = True

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
        if DEBUG:
            print("Resolved store_url:", store_url)
            print("Resolved store_source:", store_source)

    except Exception as e:
        print("Price scrape error:", e)

    return price_map, price_hash, store_url, store_source

def scrape_device(url, device_type=None):

    print("Fetching:", url)
    r = fetch_with_backoff(url)
    time.sleep(random.uniform(1.5, 3.5))  # human read pause
    spec_html = r.text
    detected_device_type = device_type or detect_device_type(url, spec_html)
    spec_hash = hashlib.md5(spec_html.encode("utf-8")).hexdigest()
    soup = BeautifulSoup(spec_html, "html.parser")
    now_iso = datetime.now(timezone.utc).isoformat()

    name = soup.select_one(".specs-phone-name-title").text.strip()
    brand = name.split()[0]

    img_tag = soup.select_one(".specs-photo-main img")
    image_url = img_tag["src"] if img_tag else None
    if image_url:
        image_url = image_url.replace("-.jpg", ".jpg")

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
    release_date_iso = parse_release_date_iso(release_date)

    fingerprint_location, is_ultrasonic = parse_fingerprint(
        features_sec.get("Sensors")
    )

    wifi_standard, wifi_generation, wifi_features = parse_wifi(comms_sec.get("WLAN"))
    bluetooth_version, bluetooth_features = parse_bluetooth(comms_sec.get("Bluetooth"))
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
    (
        panel,
        refresh,
        typical_nits,
        hbm_nits,
        peak_nits,
        supports_hdr10,
        supports_hdr10_plus,
        supports_dolby_vision,
    ) = parse_display(display_sec.get("Type"))
    if refresh is None:
        refresh = 60

    # Brightness fallback only when all brightness fields are missing.
    if typical_nits is None and hbm_nits is None and peak_nits is None:
        panel_text = (panel or "").lower()
        if "ltpo" in panel_text and ("amoled" in panel_text or "oled" in panel_text):
            peak_nits = 1500
        elif "amoled" in panel_text or "oled" in panel_text:
            peak_nits = 1000
        elif "ips lcd" in panel_text:
            peak_nits = 500
        elif "lcd" in panel_text:
            peak_nits = 400

    protection_text = display_sec.get("Protection")
    if protection_text:
        protection_text = re.sub(r",?\s*mohs level\s*\d+", "", protection_text, flags=re.I).strip()

    device_type = detect_device_type(url, spec_html)
    chipset = platform_sec.get("Chipset") or ""
    chipset_tier = derive_chipset_tier(chipset)
    is_ltpo = derive_ltpo(panel, refresh, chipset_tier, display_sec)

    variants, ufs = parse_variants(memory_sec.get("Internal"), soup, brand, chipset_tier)

    if not ufs:

        if brand and brand.lower() == "apple":
            ufs = "NVMe"

        elif chipset_tier in ["ultimate", "flagship"]:
            ufs = "UFS 4.0"

        elif chipset_tier in ["upper_mid", "upper_midrange"]:
            ufs = "UFS 3.1"

        elif chipset_tier == "midrange":
            ufs = "UFS 2.2"

        else:
            ufs = "UFS"
    price_url = re.sub(r"-(\d+)\.php$", r"-price-\1.php", url)

    price_map, price_hash, store_url, store_source = scrape_prices(price_url)

    # Map prices to variants
    for v in variants:
        key = (v["storage_gb"], v["ram_gb"])
        if key in price_map:
            v["price_inr"] = price_map[key]
            v["price_last_checked_at"] = now_iso
            v["price_source"] = store_source
            v["price_url"] = store_url
    battery_type = battery_sec.get("Type")
    capacity = None
    if battery_type:
        m = re.search(r"(\d{3,5})\s*mAh", battery_type)
        if m:
            capacity = int(m.group(1))
        else:
            wh_match = re.search(r"(\d+(?:\.\d+)?)\s*Wh", battery_type, re.I)
            if wh_match:
                wh = float(wh_match.group(1))
                capacity = int(round((wh * 1000) / 3.85))

    wired, wireless, reverse, reverse_supported = parse_charging(battery_sec.get("Charging"))
    reverse_watt = _to_float(reverse)
    charging_text = battery_sec.get("Charging") or ""
    if reverse_watt and reverse_watt > 0:
        reverse = float(reverse_watt)
        reverse_supported = True
    else:
        reverse = None
        reverse_supported = "reverse" in charging_text.lower()
    if brand.lower() == "apple":
        reverse = None
        reverse_supported = False
    reverse_watt = _to_float(reverse)

    model_text = name.lower()
    is_foldable = ("fold" in model_text) or ("flip" in model_text) or ("foldable" in model_text)
    front, back, frame = parse_build(body_sec.get("Build"), is_foldable=is_foldable)

    ip_tag = soup.select_one('[data-spec="bodyother"]')
    ip_rating = extract_ip_rating(ip_tag.get_text()) if ip_tag else None
    if ip_rating is None:
        ip_fallback_text = ip_tag.get_text(" ", strip=True) if ip_tag else ""
        if not ip_fallback_text:
            ip_fallback_text = " ".join(
                str(v) for v in [
                    body_sec.get("Build"),
                    body_sec.get("SIM"),
                ] if v
            )
        ip_rating = derive_ip_rating(ip_fallback_text, device_type=device_type)

    rear_lenses, rear_video, ois, pdaf = parse_rear_camera(soup)
    for lens in rear_lenses:
        if not isinstance(lens, dict):
            continue
        if lens.get("type") in {None, "", "unknown"}:
            derived_type = derive_lens_type(lens.get("focal_length_mm"))
            if derived_type is not None:
                lens["type"] = derived_type
    rear_features_tag = soup.select_one('[data-spec="cam1features"]')
    rear_features_text = rear_features_tag.get_text(" ", strip=True) if rear_features_tag else None
    max_res, max_fps_4k, has_hdr_video = parse_video_features(rear_video, rear_features_text)

    selfie_data = parse_selfie_camera(soup)
    if selfie_data.get("lens_type") is None:
        derived_selfie_lens_type = derive_lens_type(selfie_data.get("focal_length_mm"))
        if derived_selfie_lens_type is not None:
            selfie_data["lens_type"] = derived_selfie_lens_type
    selfie_mp = selfie_data.get("mp")
    selfie_video = selfie_data.get("video")
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
    active_use_hours, endurance_hours = parse_battery_test_metrics(soup)
    process_node_nm = extract_process_node_nm(chipset)
    if process_node_nm is None:
        process_node_nm = derive_process_node_nm(chipset)
    if active_use_hours is None:

        active_use_hours = estimate_active_use_score(
            capacity,
            refresh,
            process_node_nm,
            is_ltpo,
            size,
            device_type
        )

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

    sim_type = clean_sim_type(body_sec.get("SIM"))
    if device_type == "tablet":
        sim_lower = (sim_type or "").strip().lower()
        if not sim_type or sim_lower in {"no", "none"}:
            sim_type = "WiFi"
        elif "esim" in sim_lower:
            sim_type = "eSIM"
    has_esim, has_dual_sim, sim_count_max = parse_sim_features(sim_type)
    if device_type == "tablet" and sim_type == "WiFi":
        has_esim = False
        has_dual_sim = False
        sim_count_max = None
    sim_struct = {
        "nano": "nano-sim" in (sim_type or "").lower(),
        "esim": has_esim,
        "max_active": sim_count_max,
    }

    charging_type = None
    charging_advertised = None
    if charging_text:
        charging_type_match = re.search(r"(PD\d\.\d|QC\d(?:\.\d)?|VOOC|SuperVOOC|Dart|Turbo|Pump Express)", charging_text, re.I)
        if charging_type_match:
            charging_type = charging_type_match.group(1)
        advertised_parts = re.findall(r"\d+%\s*in\s*\d+\s*min", charging_text, re.I)
        if advertised_parts:
            charging_advertised = "; ".join(advertised_parts)
    if wired is None:
        wired = estimate_charging_watt(charging_advertised)

    phone = {
        "device_type": device_type,
        "brand": brand,
        "model_name": name,
        "status": device_status,
        "release_date": release_date,
        "release_date_iso": release_date_iso,
        "image": image_url,

        "performance": {
            "chipset": platform_sec.get("Chipset"),
            "cpu": platform_sec.get("CPU"),
            "gpu": platform_sec.get("GPU"),
            "process_node_nm": process_node_nm,
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
            "typical_nits": typical_nits,
            "hbm_nits": hbm_nits,
            "peak_nits": peak_nits,
            "supports_hdr10": supports_hdr10,
            "supports_hdr10_plus": supports_hdr10_plus,
            "supports_dolby_vision": supports_dolby_vision,
            "protection": protection_text,
        },

        "battery": {
            "capacity_mah": capacity,
            "active_use_hours": active_use_hours,
            "endurance_hours": endurance_hours,
            "charging": {
                "type": charging_type,
                "advertised": charging_advertised,
                "estimated_watt": wired,
            },
            "wireless_charging_watt": wireless,
            "reverse_charging_watt": reverse_watt,
            "reverse_charging_supported": reverse_supported,
        },

        "physical": {
            "weight_g": extract_number(body_sec.get("Weight")),
            "thickness_mm": extract_thickness(body_sec.get("Dimensions")),
            "materials": {
                "front": front,
                "back": back,
                "frame": frame,
            },
            "sim": sim_struct,
            "ip_rating": ip_rating,
            "has_3_5mm_jack": has_3_5mm_jack,
            "loudspeaker_type": loudspeaker_type,
            "fingerprint_location": fingerprint_location,
            "is_ultrasonic_fingerprint": is_ultrasonic,
        },
        "connectivity": {
            "wifi": {
                "standard": wifi_standard,
                "generation": wifi_generation,
                "features": wifi_features,
            },
            "bluetooth": {
                "version": bluetooth_version,
                "features": bluetooth_features,
            },
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
                "aperture": selfie_data.get("aperture"),
                "focal_length_mm": selfie_data.get("focal_length_mm"),
                "lens_type": selfie_data.get("lens_type"),
                "has_pdaf": selfie_data.get("has_pdaf"),
                "has_3d_sensor": selfie_data.get("has_3d_sensor"),
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
    if any(value is not None for value in benchmarks.values()):
        phone["performance"]["benchmarks"] = benchmarks

    release_year, release_month, release_ts = parse_release_parts(release_date)
    priced_variants = [v for v in variants if isinstance(v, dict) and v.get("price_inr") is not None]
    price_values = [v.get("price_inr") for v in priced_variants]
    ram_options = sorted({v.get("ram_gb") for v in variants if isinstance(v, dict) and v.get("ram_gb") is not None})
    storage_options = sorted({v.get("storage_gb") for v in variants if isinstance(v, dict) and v.get("storage_gb") is not None})
    display_ppi = compute_display_ppi(res_w, res_h, size)
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
        "water_resistance": {
            "level": water_level,
            "derived": True,
        },
        "water_resistance_level": water_level
    }

    phone["source"] = {
        "spec_url": url,
        "price_url": price_url
    }

    phone = compute_scores_and_tags(phone)

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

    return validate_device(phone)


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
            phone = scrape_device(url)
            if phone is None:
                print("Warning: skipping invalid device record after validation")
                save_progress(idx, input_signature)
                delay()
                continue

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
