#!/usr/bin/env python3
"""
Bloomy's Bud Log — Strain Data Updater
=======================================
Scrapes strain info from MedBud.wiki (names, producers, THC/CBD, tiers)
and enriches with terpene/effect/flavour data from Weedstrain.com.

Designed to run as a GitHub Action on a weekly schedule.
Outputs: strains.json (data file) and updates index.html (embedded data).

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
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MEDBUD_BASE = "https://medbud.wiki"
MEDBUD_STRAINS_URL = f"{MEDBUD_BASE}/strains/"
WEEDSTRAIN_BASE = "https://weedstrain.com/uk/weed-strains"

HEADERS = {
    "User-Agent": "BloomysBudLog/1.0 (strain-data-updater; +https://github.com)"
}

# Producers we track — add new ones here as needed.
# Maps the MedBud URL slug to the display name used in the app.
PRODUCERS = {
    "4c-labs": "4C Labs",
    "aurora": "Aurora (Pedanios)",
    "bedrocan": "Bedrocan",
    "big-narstie-medical": "Big Narstie Medical",
    "all-nations": "All Nations",
    "grow-lab-organics": "Grow Lab Organics",
    "tilray": "Tilray Medical",
    "somai": "Somaí Pharmaceuticals",
    "northern-leaf": "Northern Leaf",
    "antg": "ANTG",
    "little-green-pharma": "Little Green Pharma",
    "argent-biopharma": "Argent BioPharma",
    "linneo-health": "Linneo Health",
    "upstate": "Upstate",
    "noidecs": "Noidecs",
    "releaf": "Releaf",
    "grow-pharma": "Grow Pharma",
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
}

# Tier keywords to look for in MedBud product descriptions/labels
TIER_KEYWORDS = ["Value", "Core", "Craft Organic", "Craft Select", "Craft", "Premium"]

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
            print(f"  ⚠ Attempt {attempt+1}/{retries} failed for {url}: {e}")
            if attempt == retries - 1:
                return None
    return None


def slugify(name):
    """Convert a strain name to a URL slug for Weedstrain.com."""
    s = name.lower().strip()
    # Remove parenthetical suffixes like "(Tilray)"
    s = re.sub(r'\s*\(.*?\)\s*', '', s)
    s = re.sub(r'[^a-z0-9\s-]', '', s)
    s = re.sub(r'\s+', '-', s.strip())
    return s


def make_code(name, existing_codes):
    """Generate a short unique code from a strain name."""
    # Try first letters of each word
    words = re.sub(r'[^a-zA-Z0-9\s]', '', name).split()
    if len(words) >= 2:
        code = ''.join(w[0].upper() for w in words[:3])
    else:
        code = name[:3].upper()
    
    # Ensure uniqueness
    base = code
    counter = 1
    while code in existing_codes:
        code = f"{base}{counter}"
        counter += 1
    existing_codes.add(code)
    return code


# ---------------------------------------------------------------------------
# MedBud Scraper
# ---------------------------------------------------------------------------
def scrape_medbud_producer(slug, display_name):
    """Scrape all strains for a given producer from MedBud.wiki."""
    url = f"{MEDBUD_BASE}/strains/{slug}/"
    print(f"  Fetching MedBud: {display_name} ({url})")
    resp = fetch(url)
    if not resp:
        print(f"  ✗ Could not fetch {display_name}")
        return []
    
    soup = BeautifulSoup(resp.text, 'html.parser')
    strains = []
    
    # MedBud lists strains in cards/links — look for strain entries
    # The structure varies but typically strain names are in links or headings
    # within product cards
    strain_links = soup.find_all('a', href=re.compile(r'/strains/[^/]+/[^/]+'))
    
    seen = set()
    for link in strain_links:
        href = link.get('href', '')
        if href in seen:
            continue
        seen.add(href)
        
        # Extract strain info from the card
        strain_data = parse_medbud_strain_card(link, display_name)
        if strain_data:
            strains.append(strain_data)
    
    # If card parsing didn't work well, try fetching individual strain pages
    if not strains:
        print(f"    Trying alternative parsing for {display_name}...")
        strains = scrape_medbud_producer_alt(soup, slug, display_name)
    
    print(f"    Found {len(strains)} strains for {display_name}")
    return strains


def parse_medbud_strain_card(element, producer):
    """Parse a strain card element from MedBud."""
    # Try to extract name from the link text or nearby elements
    name = element.get_text(strip=True)
    if not name or len(name) > 80 or len(name) < 2:
        return None
    
    # Skip navigation/non-strain links
    skip_words = ['home', 'login', 'register', 'contact', 'guide', 'clinic',
                  'forum', 'blog', 'about', 'menu', 'cart', 'view all', 'back']
    if any(w in name.lower() for w in skip_words):
        return None
    
    # Look for THC/CBD info in surrounding text
    parent = element.parent
    if parent:
        text = parent.get_text()
        thc = extract_percentage(text, 'thc')
        cbd = extract_percentage(text, 'cbd')
    else:
        thc, cbd = 0, 0
    
    # Look for tier info
    tier = "Core"  # default
    if parent:
        parent_text = parent.get_text().lower()
        for t in TIER_KEYWORDS:
            if t.lower() in parent_text:
                tier = t
                break
    
    # Try to determine type (Indica/Sativa/Hybrid)
    strain_type = "Hybrid"  # default
    if parent:
        pt = parent.get_text().lower()
        if 'indica' in pt:
            strain_type = "Indica"
        elif 'sativa' in pt:
            strain_type = "Sativa"
        elif 'hybrid' in pt:
            strain_type = "Hybrid"
    
    return {
        "name": name,
        "producer": producer,
        "thc": thc,
        "cbd": cbd,
        "tier": tier,
        "type": strain_type,
    }


def scrape_medbud_producer_alt(soup, slug, display_name):
    """Alternative MedBud parsing — look for structured data or tables."""
    strains = []
    
    # Look for any JSON-LD or structured data
    scripts = soup.find_all('script', type='application/ld+json')
    for script in scripts:
        try:
            data = json.loads(script.string)
            if isinstance(data, list):
                for item in data:
                    if 'name' in item:
                        strains.append({
                            "name": item.get('name', ''),
                            "producer": display_name,
                            "thc": 0,
                            "cbd": 0,
                            "tier": "Core",
                            "type": "Hybrid",
                        })
        except (json.JSONDecodeError, TypeError):
            pass
    
    return strains


def extract_percentage(text, compound):
    """Extract THC or CBD percentage from text."""
    patterns = [
        rf'{compound}\s*[:\-]?\s*(\d+(?:\.\d+)?)\s*%',
        rf'(\d+(?:\.\d+)?)\s*%\s*{compound}',
        rf'{compound.upper()}\s*(\d+(?:\.\d+)?)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return round(float(match.group(1)))
    return 0


# ---------------------------------------------------------------------------
# Weedstrain Scraper
# ---------------------------------------------------------------------------
def scrape_weedstrain(strain_name):
    """Fetch terpene, effect, flavour, and medical data from Weedstrain.com."""
    slug = slugify(strain_name)
    url = f"{WEEDSTRAIN_BASE}/{slug}-strain"
    
    resp = fetch(url, retries=2, delay=1)
    if not resp or resp.status_code != 200:
        return None
    
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    data = {
        "terpenes": [],
        "effects": [],
        "flavours": [],
        "helpsWith": [],
        "negatives": [],
    }
    
    # Extract terpenes — typically in a terpene section
    terpene_els = soup.find_all(string=re.compile(r'(Myrcene|Caryophyllene|Limonene|Linalool|Pinene|Humulene|Ocimene|Terpinolene|Bisabolol|Valencene|Geraniol|Terpineol|Camphene|Nerolidol)', re.IGNORECASE))
    seen_terps = []
    for el in terpene_els:
        for terp in ['Myrcene', 'Caryophyllene', 'Limonene', 'Linalool', 'Pinene', 
                      'Humulene', 'Ocimene', 'Terpinolene', 'Bisabolol', 'Valencene',
                      'Geraniol', 'Terpineol', 'Camphene', 'Nerolidol']:
            if terp.lower() in el.lower() and terp not in seen_terps:
                seen_terps.append(terp)
    data["terpenes"] = seen_terps[:3]  # Top 3
    
    # Extract effects — look for effect-related sections
    page_text = soup.get_text()
    effect_words = ['Relaxed', 'Euphoric', 'Happy', 'Sleepy', 'Hungry', 'Uplifted',
                    'Energetic', 'Creative', 'Focused', 'Calmed', 'Body', 'Giggly']
    for eff in effect_words:
        if re.search(rf'\b{eff}\b', page_text, re.IGNORECASE):
            if eff not in data["effects"] and len(data["effects"]) < 3:
                data["effects"].append(eff)
    
    # Extract flavours
    flavour_words = ['Earthy', 'Sweet', 'Citrus', 'Berry', 'Pine', 'Spicy', 'Floral',
                     'Diesel', 'Herbal', 'Woody', 'Tropical', 'Fruity', 'Lemon', 'Vanilla',
                     'Grape', 'Mint', 'Sour', 'Coffee', 'Mango', 'Cheese', 'Creamy',
                     'Nutty', 'Hazy', 'Pepper', 'Cookie', 'Candy', 'Butter']
    for flav in flavour_words:
        if re.search(rf'\b{flav}\b', page_text, re.IGNORECASE):
            if flav not in data["flavours"] and len(data["flavours"]) < 3:
                data["flavours"].append(flav)
    
    # Extract medical uses
    medical_words = ['Pain', 'Stress', 'Anxiety', 'Depression', 'Insomnia', 'Fatigue',
                     'Spasticity', 'ADHD', 'PTSD', 'Inflammation', 'Nausea']
    for med in medical_words:
        if re.search(rf'\b{med}\b', page_text, re.IGNORECASE):
            if med not in data["helpsWith"] and len(data["helpsWith"]) < 3:
                data["helpsWith"].append(med)
    
    # Extract negatives
    negative_words = ['Dry mouth', 'Dry eyes', 'Dizzy', 'Paranoid', 'Anxious', 'Couch-lock']
    for neg in negative_words:
        if neg.lower() in page_text.lower():
            if neg not in data["negatives"]:
                data["negatives"].append(neg)
    
    return data if data["terpenes"] else None


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------
def load_existing_strains(path):
    """Load the current strains.json as a fallback/merge base."""
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def merge_strains(existing, scraped_medbud, weedstrain_data):
    """Merge MedBud data with Weedstrain enrichment and existing data."""
    # Build lookup from existing data
    existing_by_name = {}
    for s in existing:
        key = (s["name"].lower(), s["producer"].lower())
        existing_by_name[key] = s
    
    result = []
    existing_codes = set()
    
    # Start with existing strains (preserve all data)
    for s in existing:
        existing_codes.add(s.get("code", s.get("id", "")))
    
    # Process MedBud scraped strains
    for ms in scraped_medbud:
        key = (ms["name"].lower(), ms["producer"].lower())
        
        if key in existing_by_name:
            # Existing strain — keep existing data but update THC/CBD if MedBud has newer
            strain = existing_by_name[key].copy()
            if ms["thc"] > 0:
                strain["thc"] = ms["thc"]
            if ms["cbd"] > 0:
                strain["cbd"] = ms["cbd"]
        else:
            # New strain — create entry
            code = make_code(ms["name"], existing_codes)
            strain = {
                "name": ms["name"],
                "producer": ms["producer"],
                "code": code,
                "tier": ms.get("tier", "Core"),
                "thc": ms.get("thc", 0),
                "cbd": ms.get("cbd", 0),
                "type": ms.get("type", "Hybrid"),
                "terpenes": [],
                "effects": [],
                "flavours": [],
                "helpsWith": [],
                "negatives": [],
                "id": code,
            }
            
            # Try to enrich from Weedstrain
            ws_key = ms["name"]
            if ws_key in weedstrain_data and weedstrain_data[ws_key]:
                ws = weedstrain_data[ws_key]
                strain["terpenes"] = ws.get("terpenes", [])
                strain["effects"] = ws.get("effects", [])
                strain["flavours"] = ws.get("flavours", [])
                strain["helpsWith"] = ws.get("helpsWith", [])
                strain["negatives"] = ws.get("negatives", [])
            
            print(f"  ✚ New strain: {ms['name']} ({ms['producer']})")
        
        result.append(strain)
        existing_by_name.pop(key, None)
    
    # Keep any existing strains not found in MedBud scrape
    # (they may still be valid, just not scraped this run)
    for strain in existing_by_name.values():
        result.append(strain)
    
    return result


def update_html(html_path, strains):
    """Update the embedded STRAINS_JSON in index.html."""
    with open(html_path) as f:
        html = f.read()
    
    # Generate safe JSON string
    js_json = json.dumps(strains, ensure_ascii=True, separators=(',', ':'))
    # Escape any single quotes for JS safety
    js_json = js_json.replace("'", "\\u0027")
    
    # Replace the STRAINS_JSON constant
    marker = 'const STRAINS_JSON = '
    start = html.find(marker)
    if start < 0:
        print("  ✗ Could not find STRAINS_JSON in index.html")
        return False
    
    end = html.find(';', start) + 1
    replacement = f'const STRAINS_JSON = {js_json};'
    html = html[:start] + replacement + html[end:]
    
    with open(html_path, 'w') as f:
        f.write(html)
    
    print(f"  ✓ Updated index.html with {len(strains)} strains")
    return True


def main():
    repo_root = Path(__file__).parent.parent
    strains_path = repo_root / "strains.json"
    html_path = repo_root / "index.html"
    
    print("=" * 60)
    print("Bloomy's Bud Log — Strain Data Updater")
    print("=" * 60)
    
    # 1. Load existing data
    print("\n📂 Loading existing strains...")
    existing = load_existing_strains(strains_path)
    print(f"  Loaded {len(existing)} existing strains")
    
    # 2. Scrape MedBud for each producer
    print("\n🌐 Scraping MedBud.wiki...")
    all_medbud = []
    for slug, name in PRODUCERS.items():
        strains = scrape_medbud_producer(slug, name)
        all_medbud.extend(strains)
        time.sleep(1)  # Be polite
    print(f"\n  Total from MedBud: {len(all_medbud)} strains")
    
    # 3. Enrich new strains with Weedstrain data
    print("\n🔬 Enriching with Weedstrain.com data...")
    existing_names = {s["name"].lower() for s in existing}
    new_names = [s["name"] for s in all_medbud if s["name"].lower() not in existing_names]
    
    weedstrain_data = {}
    for name in new_names:
        print(f"  Looking up: {name}")
        ws = scrape_weedstrain(name)
        weedstrain_data[name] = ws
        if ws:
            print(f"    ✓ Found terpenes: {', '.join(ws['terpenes'])}")
        else:
            print(f"    ✗ Not found on Weedstrain")
        time.sleep(0.5)
    
    # 4. Merge data
    print("\n🔄 Merging data...")
    if all_medbud:
        merged = merge_strains(existing, all_medbud, weedstrain_data)
    else:
        print("  ⚠ No MedBud data scraped — keeping existing data unchanged")
        merged = existing
    
    # 5. Save strains.json
    print(f"\n💾 Saving {len(merged)} strains to strains.json...")
    with open(strains_path, 'w') as f:
        json.dump(merged, f, indent=2, ensure_ascii=False)
    
    # 6. Update index.html
    print("\n📝 Updating index.html...")
    if html_path.exists():
        update_html(str(html_path), merged)
    else:
        print("  ⚠ index.html not found — skipping HTML update")
    
    # 7. Summary
    new_count = len(merged) - len(existing)
    print(f"\n{'=' * 60}")
    print(f"✅ Done! {len(merged)} total strains ({'+' if new_count >= 0 else ''}{new_count} vs previous)")
    print(f"{'=' * 60}")
    
    return 0 if merged else 1


if __name__ == "__main__":
    sys.exit(main())
