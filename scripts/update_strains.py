#!/usr/bin/env python3
"""
Bloomy's Bud Log — Strain Data Updater
=======================================
Since MedBud.wiki renders strain listings with JavaScript (invisible to simple
HTTP scrapers), this script takes a different approach:

1. Fetches each producer's page and extracts links to individual strain pages
2. Scrapes each individual strain page (which DOES have data in the HTML)
3. Enriches with terpene/effect/flavour data from Weedstrain.com
4. Merges with existing strains.json (never deletes, only adds/updates)

Designed to run as a GitHub Action on a weekly schedule.

Usage:
    python scripts/update_strains.py

Requirements:
    pip install requests beautifulsoup4
"""

import json
import re
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MEDBUD_BASE = "https://medbud.wiki"
WEEDSTRAIN_BASE = "https://weedstrain.com/uk/weed-strains"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# All known producer slugs on MedBud (medbud.wiki/strains/<slug>/)
PRODUCERS = {
    "4c-labs": "4C Labs",
    "aurora-pedanios": "Aurora (Pedanios)",
    "bedrocan": "Bedrocan",
    "big-narstie-medical": "Big Narstie Medical",
    "all-nations": "All Nations",
    "grow-lab-organics": "Grow Lab Organics",
    "growlab-organics": "Grow Lab Organics",
    "tilray": "Tilray Medical",
    "somai": "Somaí Pharmaceuticals",
    "northern-leaf": "Northern Leaf",
    "antg": "ANTG",
    "little-green-pharma": "Little Green Pharma",
    "argent-biopharma": "Argent BioPharma",
    "linneo-health": "Linneo Health",
    "linneo": "Linneo Health",
    "upstate": "Upstate",
    "noidecs": "Noidecs",
    "releaf": "Releaf",
    "grow-pharma": "Grow Pharma",
    "grow": "Grow Pharma",
    "cantourage": "Cantourage",
    "curaleaf": "Curaleaf",
    "mamedica": "Mamedica",
    "lot420": "Lot420",
    "doja": "Doja",
    "adven": "Adven",
    "khiron": "Khiron",
    "lyphe": "Lyphe",
    "peace-naturals": "Peace Naturals",
    "medcan": "MedCan",
    "dalgety": "Dalgety",
    "glass-pharms": "Glass Pharms",
    "natural-history": "Natural History",
    "medicus": "Medicus",
    "craft-botanics": "Craft Botanics",
    "sundaze": "Sundaze",
    "circle": "Circle",
    "phant": "Phant",
    "dank-of-england-medical": "DOE Medical",
    "breathing-green": "Breathing Green",
    "wellford": "Wellford",
    "montu": "Montu",
    "specials-pharma": "Specials Pharma",
    "lumir": "Lumir",
    "columbian": "Columbian",
    "aaaa": "AAAA",
    "fotmer": "Fotmer",
    "althea": "Althea",
    "beacon": "Beacon",
    "cellen": "Cellen",
    "bpi": "BPI",
}

KNOWN_TERPENES = [
    'Myrcene', 'Caryophyllene', 'Limonene', 'Linalool', 'Pinene',
    'Humulene', 'Ocimene', 'Terpinolene', 'Bisabolol', 'Valencene',
    'Geraniol', 'Terpineol', 'Camphene', 'Nerolidol',
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def fetch(url, retries=3, delay=2):
    """Fetch a URL with retries and polite delays."""
    for attempt in range(retries):
        try:
            time.sleep(delay if attempt > 0 else 0.5)
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt == retries - 1:
                return None
    return None


def slugify(name):
    """Convert a strain name to a URL slug for Weedstrain.com."""
    s = name.lower().strip()
    s = re.sub(r'\s*\(.*?\)\s*', '', s)
    s = re.sub(r'[^a-z0-9\s-]', '', s)
    s = re.sub(r'\s+', '-', s.strip())
    return s


def make_code(name, existing_codes):
    """Generate a short unique code from a strain name."""
    words = re.sub(r'[^a-zA-Z0-9\s]', '', name).split()
    if len(words) >= 2:
        code = ''.join(w[0].upper() for w in words[:3])
    else:
        code = name[:3].upper()
    base = code
    counter = 1
    while code in existing_codes:
        code = f"{base}{counter}"
        counter += 1
    existing_codes.add(code)
    return code


# ---------------------------------------------------------------------------
# MedBud: Discover strain page URLs from a producer page
# ---------------------------------------------------------------------------
def discover_strain_urls(producer_slug):
    """
    Fetch the producer index page and find links to individual strain pages.
    MedBud strain URLs follow: /strains/<producer>/<strain-slug>/
    """
    url = f"{MEDBUD_BASE}/strains/{producer_slug}/"
    resp = fetch(url, retries=2, delay=1)
    if not resp:
        return []

    soup = BeautifulSoup(resp.text, 'html.parser')

    # Match links like /strains/upstate/wo-t23-white-out/
    pattern = re.compile(
        rf'^(?:https?://medbud\.wiki)?/strains/{re.escape(producer_slug)}/([^/]+)/?$'
    )
    strain_urls = set()

    for link in soup.find_all('a', href=True):
        href = link['href']
        if pattern.match(href):
            if href.startswith('/'):
                full = MEDBUD_BASE + href.rstrip('/') + '/'
            else:
                full = href.rstrip('/') + '/'
            strain_urls.add(full)

    return sorted(strain_urls)


# ---------------------------------------------------------------------------
# MedBud: Scrape an individual strain page
# ---------------------------------------------------------------------------
def scrape_strain_page(url, producer_name):
    """Scrape a single MedBud strain page for structured data."""
    resp = fetch(url, retries=2, delay=1)
    if not resp:
        return None

    text = resp.text
    page_text = BeautifulSoup(text, 'html.parser').get_text(' ', strip=True)

    # Extract "Cultivar/Strain" field — the clean strain name
    strain_name = None
    m = re.search(r'Cultivar/Strain\s*·?\s*([A-Za-z][A-Za-z0-9\s\'\-\&\.]+)', page_text)
    if m:
        strain_name = m.group(1).strip()

    if not strain_name or len(strain_name) < 2 or len(strain_name) > 60:
        return None

    # Extract THC — look for the registered percentage
    thc = 0
    # Pattern like "THC Potential Range (±10%) 20.7-25.3%" — take the high end
    thc_range = re.search(r'THC.*?(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*%', page_text)
    if thc_range:
        thc = round(float(thc_range.group(2)))  # Use high end
    else:
        thc_simple = re.search(r'(\d+(?:\.\d+)?)\s*%\s*THC', page_text)
        if thc_simple:
            thc = round(float(thc_simple.group(1)))

    # Extract CBD
    cbd = 0
    cbd_range = re.search(r'CBD.*?(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*%', page_text)
    if cbd_range:
        val = float(cbd_range.group(2))
        if val >= 1:
            cbd = round(val)
    else:
        cbd_simple = re.search(r'(\d+(?:\.\d+)?)\s*%\s*CBD', page_text)
        if cbd_simple:
            val = float(cbd_simple.group(1))
            if val >= 1:
                cbd = round(val)

    # Extract type/classification
    strain_type = "Hybrid"
    type_m = re.search(r'Classification\s*·?\s*(Indica|Sativa|Hybrid|Indica Hybrid|Sativa Hybrid)', page_text, re.IGNORECASE)
    if type_m:
        t = type_m.group(1).lower()
        if 'indica' in t:
            strain_type = "Indica"
        elif 'sativa' in t:
            strain_type = "Sativa"

    # Extract designation code
    code = ""
    code_m = re.search(r'Designation\s*·?\s*([A-Z0-9][A-Z0-9\-\s]*(?:T\d+)?)', page_text)
    if code_m:
        code = code_m.group(1).strip()

    return {
        "name": strain_name,
        "producer": producer_name,
        "thc": thc,
        "cbd": cbd,
        "type": strain_type,
        "code": code,
        "tier": "Core",
    }


# ---------------------------------------------------------------------------
# Weedstrain Enrichment
# ---------------------------------------------------------------------------
def scrape_weedstrain(strain_name):
    """Fetch terpene/effect/flavour data from Weedstrain.com."""
    slug = slugify(strain_name)
    if not slug or len(slug) < 2:
        return None

    url = f"{WEEDSTRAIN_BASE}/{slug}-strain"
    resp = fetch(url, retries=2, delay=1)
    if not resp or resp.status_code != 200:
        return None

    page_text = BeautifulSoup(resp.text, 'html.parser').get_text()
    data = {"terpenes": [], "effects": [], "flavours": [], "helpsWith": [], "negatives": []}

    for terp in KNOWN_TERPENES:
        if re.search(rf'\b{terp}\b', page_text, re.IGNORECASE) and len(data["terpenes"]) < 3:
            data["terpenes"].append(terp)

    for eff in ['Relaxed', 'Euphoric', 'Happy', 'Sleepy', 'Hungry', 'Uplifted',
                'Energetic', 'Creative', 'Focused', 'Calmed', 'Body', 'Giggly']:
        if re.search(rf'\b{eff}\b', page_text, re.IGNORECASE) and len(data["effects"]) < 3:
            data["effects"].append(eff)

    for flav in ['Earthy', 'Sweet', 'Citrus', 'Berry', 'Pine', 'Spicy', 'Floral',
                 'Diesel', 'Herbal', 'Woody', 'Tropical', 'Fruity', 'Lemon', 'Vanilla',
                 'Grape', 'Mint', 'Sour', 'Mango', 'Cheese', 'Creamy']:
        if re.search(rf'\b{flav}\b', page_text, re.IGNORECASE) and len(data["flavours"]) < 3:
            data["flavours"].append(flav)

    for med in ['Pain', 'Stress', 'Anxiety', 'Depression', 'Insomnia', 'Fatigue',
                'Spasticity', 'ADHD', 'PTSD', 'Inflammation']:
        if re.search(rf'\b{med}\b', page_text, re.IGNORECASE) and len(data["helpsWith"]) < 3:
            data["helpsWith"].append(med)

    for neg in ['Dry mouth', 'Dry eyes', 'Dizzy', 'Paranoid', 'Anxious', 'Couch-lock']:
        if neg.lower() in page_text.lower():
            data["negatives"].append(neg)

    return data if data["terpenes"] else None


# ---------------------------------------------------------------------------
# HTML Updater
# ---------------------------------------------------------------------------
def update_html(html_path, strains):
    with open(html_path) as f:
        html = f.read()
    js_json = json.dumps(strains, ensure_ascii=True, separators=(',', ':'))
    js_json = js_json.replace("'", "\\u0027")
    marker = 'const STRAINS_JSON = '
    start = html.find(marker)
    if start < 0:
        print("  ✗ Could not find STRAINS_JSON in index.html")
        return False
    end = html.find(';', start) + 1
    html = html[:start] + f'const STRAINS_JSON = {js_json};' + html[end:]
    with open(html_path, 'w') as f:
        f.write(html)
    print(f"  ✓ Updated index.html with {len(strains)} strains")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    repo_root = Path(__file__).parent.parent
    strains_path = repo_root / "strains.json"
    html_path = repo_root / "index.html"

    print("=" * 60)
    print("Bloomy's Bud Log — Strain Data Updater")
    print("=" * 60)

    # 1. Load existing
    print("\n📂 Loading existing strains...")
    existing = load_existing(strains_path)
    existing_by_key = {}
    existing_codes = set()
    for s in existing:
        key = (s["name"].lower(), s["producer"].lower())
        existing_by_key[key] = s
        existing_codes.add(s.get("code", s.get("id", "")))
    print(f"  Loaded {len(existing)} existing strains")

    # 2. Discover & scrape MedBud
    print("\n🌐 Scraping MedBud.wiki strain pages...")
    new_strains = []
    total_pages = 0

    for slug, producer in PRODUCERS.items():
        print(f"\n  📦 {producer} (/{slug}/)")
        urls = discover_strain_urls(slug)
        if not urls:
            print(f"    ✗ No strain pages found")
            continue

        print(f"    Found {len(urls)} strain pages")
        total_pages += len(urls)

        for surl in urls:
            data = scrape_strain_page(surl, producer)
            if not data or not data["name"]:
                continue

            key = (data["name"].lower(), producer.lower())
            if key in existing_by_key:
                # Update THC/CBD if we have better data
                ex = existing_by_key[key]
                if data["thc"] > 0 and ex.get("thc", 0) == 0:
                    ex["thc"] = data["thc"]
                if data["cbd"] > 0 and ex.get("cbd", 0) == 0:
                    ex["cbd"] = data["cbd"]
            else:
                new_strains.append(data)
                print(f"    ✚ {data['name']} (THC {data['thc']}%, {data['type']})")

        time.sleep(1)

    print(f"\n  Total strain pages checked: {total_pages}")
    print(f"  New strains found: {len(new_strains)}")

    # 3. Enrich new strains via Weedstrain
    if new_strains:
        print(f"\n🔬 Enriching {len(new_strains)} new strains via Weedstrain.com...")
        for s in new_strains:
            ws = scrape_weedstrain(s["name"])
            if ws:
                s.update(ws)
                print(f"  ✓ {s['name']}: {', '.join(ws['terpenes'])}")
            else:
                s.setdefault("terpenes", [])
                s.setdefault("effects", [])
                s.setdefault("flavours", [])
                s.setdefault("helpsWith", [])
                s.setdefault("negatives", [])
                print(f"  ✗ {s['name']}: not on Weedstrain")
            time.sleep(0.5)

    # 4. Merge
    result = list(existing)
    for s in new_strains:
        code = s.get("code") or make_code(s["name"], existing_codes)
        if code not in existing_codes:
            existing_codes.add(code)
        result.append({
            "name": s["name"], "producer": s["producer"], "code": code,
            "tier": s.get("tier", "Core"), "thc": s.get("thc", 0),
            "cbd": s.get("cbd", 0), "type": s.get("type", "Hybrid"),
            "terpenes": s.get("terpenes", []), "effects": s.get("effects", []),
            "flavours": s.get("flavours", []), "helpsWith": s.get("helpsWith", []),
            "negatives": s.get("negatives", []), "id": code,
        })

    # 5. Save
    print(f"\n💾 Saving {len(result)} strains...")
    with open(strains_path, 'w') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    if html_path.exists():
        update_html(str(html_path), result)

    added = len(result) - len(existing)
    print(f"\n{'='*60}")
    print(f"✅ Done! {len(result)} strains ({'+' if added >= 0 else ''}{added} new)")
    print(f"{'='*60}")
    return 0


def load_existing(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


if __name__ == "__main__":
    sys.exit(main())
