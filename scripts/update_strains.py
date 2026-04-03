#!/usr/bin/env python3
"""
Bloomy's Bud Log — Strain Data Updater (Playwright Edition)
=============================================================
Uses a headless browser (Playwright) to scrape MedBud.wiki, which renders
strain data with JavaScript. This allows extraction of terpene profiles,
THC/CBD, type, and other data that simple HTTP scrapers cannot see.

Falls back to Weedstrain.com for any missing terpene/effect/flavour data.

Designed to run as a GitHub Action on a weekly schedule.

Usage:
    python scripts/update_strains.py

Requirements:
    pip install playwright beautifulsoup4
    playwright install chromium
"""

import asyncio
import json
import re
import sys
import time
from pathlib import Path

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

# We still use requests for Weedstrain (simple static pages)
try:
    import requests
except ImportError:
    requests = None

MEDBUD_BASE = "https://medbud.wiki"
WEEDSTRAIN_BASE = "https://weedstrain.com/uk/weed-strains"

PRODUCERS = {
    "4c-labs": "4C Labs",
    "aurora-pedanios": "Aurora (Pedanios)",
    "bedrocan": "Bedrocan",
    "big-narstie-medical": "Big Narstie Medical",
    "all-nations": "All Nations",
    "tilray": "Tilray Medical",
    "somai": "Somaí Pharmaceuticals",
    "little-green-pharma": "Little Green Pharma",
    "upstate": "Upstate",
    "noidecs": "Noidecs",
    "releaf": "Releaf",
    "grow-pharma": "Grow Pharma",
    "curaleaf": "Curaleaf",
    "mamedica": "Mamedica",
    "lot420": "Lot420",
    "doja": "Doja",
    "adven": "Adven",
    "khiron": "Khiron",
    "peace-naturals": "Peace Naturals",
    "medcan": "MedCan",
    "dalgety": "Dalgety",
    "glass-pharms": "Glass Pharms",
    "medicus": "Medicus",
    "craft-botanics": "Craft Botanics",
    "sundaze": "Sundaze",
    "phant": "Phant",
    "dank-of-england-medical": "DOE Medical",
    "breathing-green": "Breathing Green",
    "wellford": "Wellford",
    "lumir": "Lumir",
    "althea": "Althea",
    "cellen": "Cellen",
}

KNOWN_TERPENES = [
    'Myrcene', 'Caryophyllene', 'Limonene', 'Linalool', 'Pinene',
    'Humulene', 'Ocimene', 'Terpinolene', 'Bisabolol', 'Valencene',
    'Geraniol', 'Terpineol', 'Camphene', 'Nerolidol',
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def clean_strain_name(raw):
    """Strip MedBud metadata junk from extracted strain names."""
    if not raw:
        return None
    for stop in ['Classification', 'Chemotype', 'Type I', 'Type II', 'Type III',
                 'Flower Provided', 'THC Potential', 'CBD Potential', 'Trimmed']:
        idx = raw.find(stop)
        if idx > 0:
            raw = raw[:idx]
    return raw.strip(' ·\t\n')


def slugify(name):
    s = name.lower().strip()
    s = re.sub(r'\s*\(.*?\)\s*', '', s)
    s = re.sub(r'[^a-z0-9\s-]', '', s)
    s = re.sub(r'\s+', '-', s.strip())
    return s


def make_code(name, existing_codes):
    words = re.sub(r'[^a-zA-Z0-9\s]', '', name).split()
    code = ''.join(w[0].upper() for w in words[:3]) if len(words) >= 2 else name[:3].upper()
    base = code
    counter = 1
    while code in existing_codes:
        code = f"{base}{counter}"
        counter += 1
    existing_codes.add(code)
    return code


# ---------------------------------------------------------------------------
# MedBud: Playwright-based scraper
# ---------------------------------------------------------------------------
async def discover_strain_urls_pw(page, producer_slug):
    """Navigate to a producer page with Playwright and extract strain subpage links."""
    url = f"{MEDBUD_BASE}/strains/{producer_slug}/"
    try:
        await page.goto(url, wait_until='networkidle', timeout=30000)
        await page.wait_for_timeout(3000)  # Let JS render
    except Exception as e:
        print(f"    ✗ Could not load {url}: {e}")
        return []

    # Extract all links matching /strains/<producer>/<something>/
    links = await page.eval_on_selector_all(
        'a[href]',
        f"""(els) => els
            .map(el => el.getAttribute('href'))
            .filter(h => h && h.match(/^\\/strains\\/{producer_slug}\\/[^/]+\\/?$/))
        """
    )
    urls = set()
    for href in links:
        full = MEDBUD_BASE + href.rstrip('/') + '/'
        urls.add(full)
    return sorted(urls)


async def scrape_strain_page_pw(page, url, producer_name):
    """Scrape a single MedBud strain page using Playwright (JS-rendered)."""
    try:
        await page.goto(url, wait_until='networkidle', timeout=30000)
        await page.wait_for_timeout(2000)
    except Exception:
        return None

    # Get the full rendered page text
    page_text = await page.inner_text('body')

    # --- Extract strain name ---
    strain_name = None
    m = re.search(r'Cultivar/Strain\s*·?\s*([A-Za-z][\w\s\'\-\&\.\,éèü]+?)(?:\s*Classification|\s*Chemotype|\s*Flower|\s*THC)', page_text)
    if m:
        strain_name = clean_strain_name(m.group(1))
    if not strain_name or len(strain_name) < 2 or len(strain_name) > 50:
        return None

    # --- THC ---
    thc = 0
    thc_m = re.search(r'THC.*?(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*%', page_text)
    if thc_m:
        thc = round(float(thc_m.group(2)))
    else:
        thc_m2 = re.search(r'(\d+(?:\.\d+)?)\s*%\s*THC', page_text)
        if thc_m2:
            thc = round(float(thc_m2.group(1)))

    # --- CBD ---
    cbd = 0
    cbd_m = re.search(r'CBD.*?(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*%', page_text)
    if cbd_m:
        val = float(cbd_m.group(2))
        if val >= 1:
            cbd = round(val)

    # --- Type ---
    strain_type = "Hybrid"
    type_m = re.search(r'Classification\s*·?\s*(Indica|Sativa|Hybrid|Indica Hybrid|Sativa Hybrid)', page_text, re.IGNORECASE)
    if type_m:
        t = type_m.group(1).lower()
        if 'indica' in t:
            strain_type = "Indica"
        elif 'sativa' in t:
            strain_type = "Sativa"

    # --- Terpenes (the big win from using Playwright!) ---
    terpenes = []
    for terp in KNOWN_TERPENES:
        # MedBud shows terpenes in a dedicated section with percentages
        if re.search(rf'\b{terp}\b', page_text) and terp not in terpenes:
            terpenes.append(terp)
    # Limit to top terpenes (MedBud usually lists them in order)
    terpenes = terpenes[:5]

    # --- Effects ---
    effects = []
    for eff in ['Relaxed', 'Euphoric', 'Happy', 'Sleepy', 'Hungry', 'Uplifted',
                'Energetic', 'Creative', 'Focused', 'Calmed', 'Body', 'Giggly']:
        if re.search(rf'\b{eff}\b', page_text, re.IGNORECASE) and eff not in effects and len(effects) < 3:
            effects.append(eff)

    # --- Flavours ---
    flavours = []
    for flav in ['Earthy', 'Sweet', 'Citrus', 'Berry', 'Pine', 'Spicy', 'Floral',
                 'Diesel', 'Herbal', 'Woody', 'Tropical', 'Fruity', 'Lemon', 'Vanilla',
                 'Grape', 'Mint', 'Sour', 'Mango', 'Cheese', 'Creamy', 'Pepper',
                 'Nutty', 'Coffee', 'Hazy', 'Candy', 'Cookie', 'Butter', 'Garlic']:
        if re.search(rf'\b{flav}\b', page_text, re.IGNORECASE) and flav not in flavours and len(flavours) < 3:
            flavours.append(flav)

    # --- Medical uses ---
    helps = []
    for med in ['Pain', 'Stress', 'Anxiety', 'Depression', 'Insomnia', 'Fatigue',
                'Spasticity', 'ADHD', 'PTSD', 'Inflammation', 'Nausea']:
        if re.search(rf'\b{med}\b', page_text, re.IGNORECASE) and med not in helps and len(helps) < 3:
            helps.append(med)

    # --- Negatives ---
    negatives = []
    for neg in ['Dry mouth', 'Dry eyes', 'Dizzy', 'Paranoid', 'Anxious', 'Couch-lock']:
        if neg.lower() in page_text.lower() and neg not in negatives:
            negatives.append(neg)

    # --- Code/designation ---
    code = ""
    code_m = re.search(r'Designation\s*·?\s*([A-Z0-9][A-Z0-9\-\s]*)', page_text)
    if code_m:
        code = code_m.group(1).strip()

    return {
        "name": strain_name,
        "producer": producer_name,
        "thc": thc, "cbd": cbd,
        "type": strain_type,
        "code": code,
        "tier": "Core",
        "terpenes": terpenes,
        "effects": effects,
        "flavours": flavours,
        "helpsWith": helps,
        "negatives": negatives,
    }


# ---------------------------------------------------------------------------
# Weedstrain fallback (for strains missing terpene data)
# ---------------------------------------------------------------------------
def scrape_weedstrain(strain_name):
    if not requests:
        return None
    slug = slugify(strain_name)
    if not slug or len(slug) < 2:
        return None
    url = f"{WEEDSTRAIN_BASE}/{slug}-strain"
    try:
        resp = requests.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0"
        })
        if resp.status_code != 200:
            return None
    except Exception:
        return None

    page_text = BeautifulSoup(resp.text, 'html.parser').get_text()
    data = {"terpenes": [], "effects": [], "flavours": [], "helpsWith": [], "negatives": []}

    for terp in KNOWN_TERPENES:
        if re.search(rf'\b{terp}\b', page_text, re.IGNORECASE) and len(data["terpenes"]) < 3:
            data["terpenes"].append(terp)
    for eff in ['Relaxed','Euphoric','Happy','Sleepy','Hungry','Uplifted','Energetic','Creative','Focused','Calmed','Body']:
        if re.search(rf'\b{eff}\b', page_text, re.IGNORECASE) and len(data["effects"]) < 3:
            data["effects"].append(eff)
    for flav in ['Earthy','Sweet','Citrus','Berry','Pine','Spicy','Floral','Diesel','Herbal','Woody','Tropical','Fruity','Lemon']:
        if re.search(rf'\b{flav}\b', page_text, re.IGNORECASE) and len(data["flavours"]) < 3:
            data["flavours"].append(flav)
    for med in ['Pain','Stress','Anxiety','Depression','Insomnia','Fatigue','Spasticity','ADHD']:
        if re.search(rf'\b{med}\b', page_text, re.IGNORECASE) and len(data["helpsWith"]) < 3:
            data["helpsWith"].append(med)
    for neg in ['Dry mouth','Dry eyes','Dizzy','Paranoid','Anxious','Couch-lock']:
        if neg.lower() in page_text.lower():
            data["negatives"].append(neg)

    return data if data["terpenes"] else None


# ---------------------------------------------------------------------------
# HTML updater
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


def load_existing(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    repo_root = Path(__file__).parent.parent
    strains_path = repo_root / "strains.json"
    html_path = repo_root / "index.html"

    print("=" * 60)
    print("Bloomy's Bud Log — Strain Data Updater (Playwright)")
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

    # 2. Launch headless browser
    print("\n🌐 Launching headless browser...")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        new_strains = []
        seen_new = set()
        total_pages = 0

        for slug, producer in PRODUCERS.items():
            print(f"\n  📦 {producer} (/{slug}/)")

            # Discover strain page URLs
            urls = await discover_strain_urls_pw(page, slug)
            if not urls:
                print(f"    ✗ No strain pages found")
                continue

            print(f"    Found {len(urls)} strain pages")
            total_pages += len(urls)

            for surl in urls:
                data = await scrape_strain_page_pw(page, surl, producer)
                if not data or not data["name"]:
                    continue

                key = (data["name"].lower(), producer.lower())

                # Update existing if we have better data
                if key in existing_by_key:
                    ex = existing_by_key[key]
                    if data["thc"] > 0 and ex.get("thc", 0) == 0:
                        ex["thc"] = data["thc"]
                    if data["cbd"] > 0 and ex.get("cbd", 0) == 0:
                        ex["cbd"] = data["cbd"]
                    # Update terpenes if MedBud now has them
                    if data["terpenes"] and not ex.get("terpenes"):
                        ex["terpenes"] = data["terpenes"]
                    continue

                # Deduplicate within this run
                if key in seen_new:
                    continue
                seen_new.add(key)

                new_strains.append(data)
                terp_str = ', '.join(data['terpenes'][:3]) if data['terpenes'] else 'no terpenes'
                print(f"    ✚ {data['name']} (THC {data['thc']}%, {data['type']}, {terp_str})")

            await page.wait_for_timeout(1000)  # Be polite

        await browser.close()

    print(f"\n  Pages checked: {total_pages}")
    print(f"  New unique strains: {len(new_strains)}")

    # 3. Weedstrain fallback for strains still missing terpenes
    missing_terps = [s for s in new_strains if not s.get("terpenes")]
    if missing_terps and requests:
        print(f"\n🔬 Weedstrain fallback for {len(missing_terps)} strains missing terpenes...")
        for s in missing_terps:
            ws = scrape_weedstrain(s["name"])
            if ws:
                if not s["terpenes"]:
                    s["terpenes"] = ws["terpenes"]
                if not s["effects"]:
                    s["effects"] = ws["effects"]
                if not s["flavours"]:
                    s["flavours"] = ws["flavours"]
                if not s["helpsWith"]:
                    s["helpsWith"] = ws["helpsWith"]
                if not s["negatives"]:
                    s["negatives"] = ws["negatives"]
                print(f"  ✓ {s['name']}: {', '.join(ws['terpenes'])}")
            else:
                print(f"  ✗ {s['name']}: not on Weedstrain either")
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


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
