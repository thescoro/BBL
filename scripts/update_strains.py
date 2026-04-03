#!/usr/bin/env python3
"""
Bloomy's Bud Log — Strain Data Updater (Hybrid Edition)
=========================================================
- Step 1: Uses simple HTTP requests to discover strain page URLs from each
  producer's index page (these links ARE in the static HTML).
- Step 2: Uses Playwright headless browser to visit each individual strain
  page and extract the JS-rendered data (terpenes, THC/CBD, type, etc.).
- Step 3: Falls back to Weedstrain.com for any strains still missing terpenes.
- Step 4: Merges with existing strains.json (never deletes, only adds/updates).

Usage:  python scripts/update_strains.py
Requires: pip install playwright requests beautifulsoup4
          playwright install chromium
"""

import asyncio
import json
import re
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

MEDBUD_BASE = "https://medbud.wiki"
WEEDSTRAIN_BASE = "https://weedstrain.com/uk/weed-strains"
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

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
# Step 1: Discover strain URLs via simple HTTP (links are in static HTML)
# ---------------------------------------------------------------------------
def discover_strain_urls(producer_slug):
    url = f"{MEDBUD_BASE}/strains/{producer_slug}/"
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception:
        return []

    soup = BeautifulSoup(resp.text, 'html.parser')
    pattern = re.compile(
        rf'^(?:https?://medbud\.wiki)?/strains/{re.escape(producer_slug)}/([^/]+)/?$'
    )
    urls = set()
    for link in soup.find_all('a', href=True):
        href = link['href']
        if pattern.match(href):
            full = (MEDBUD_BASE + href if href.startswith('/') else href).rstrip('/') + '/'
            urls.add(full)
    return sorted(urls)


# ---------------------------------------------------------------------------
# Step 2: Scrape individual strain page with Playwright (JS-rendered data)
# ---------------------------------------------------------------------------
async def scrape_strain_page_pw(page, url, producer_name):
    try:
        await page.goto(url, timeout=30000)
        # Wait for content to render — look for the Cultivar/Strain text
        try:
            await page.wait_for_selector('text=Cultivar/Strain', timeout=8000)
        except Exception:
            pass  # Some pages may not have this exact text
        await page.wait_for_timeout(2000)  # Extra buffer for terpene data
    except Exception:
        return None

    page_text = await page.inner_text('body')

    # --- Strain name ---
    strain_name = None
    m = re.search(
        r'Cultivar/Strain\s*·?\s*([A-Za-z][\w\s\'\-\&\.\,éèü]+?)'
        r'(?:\s*Classification|\s*Chemotype|\s*Flower|\s*THC)',
        page_text
    )
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
    type_m = re.search(r'Classification\s*·?\s*(Indica|Sativa|Hybrid|Indica Hybrid|Sativa Hybrid)',
                       page_text, re.IGNORECASE)
    if type_m:
        t = type_m.group(1).lower()
        if 'indica' in t:
            strain_type = "Indica"
        elif 'sativa' in t:
            strain_type = "Sativa"

    # --- Terpenes (the main reason for Playwright!) ---
    terpenes = []
    for terp in KNOWN_TERPENES:
        if re.search(rf'\b{terp}\b', page_text) and terp not in terpenes:
            terpenes.append(terp)
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

    # --- Medical ---
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

    # --- Code ---
    code = ""
    code_m = re.search(r'Designation\s*·?\s*([A-Z0-9][A-Z0-9\-\s]*)', page_text)
    if code_m:
        code = code_m.group(1).strip()

    # --- YouTube Reviews ---
    youtube_reviews = []
    try:
        # Click the YouTube Reviews tab if it exists
        yt_tab = page.locator('text=YouTube Reviews').first
        if await yt_tab.count() > 0:
            await yt_tab.click()
            await page.wait_for_timeout(1500)

            # Find all YouTube links on the page
            yt_links = await page.eval_on_selector_all(
                'a[href*="youtube.com"], a[href*="youtu.be"]',
                """els => els.map(el => ({
                    url: el.href,
                    title: (el.closest('[class]')?.querySelector('span, p, div')?.textContent
                           || el.textContent || '').trim().substring(0, 120)
                }))"""
            )

            # Also check for iframes with YouTube embeds
            yt_iframes = await page.eval_on_selector_all(
                'iframe[src*="youtube.com"], iframe[src*="youtu.be"]',
                """els => els.map(el => {
                    let src = el.src || '';
                    let m = src.match(/embed\\/([\\w-]+)/);
                    return m ? { url: 'https://www.youtube.com/watch?v=' + m[1], title: '' } : null;
                }).filter(Boolean)"""
            )

            seen_urls = set()
            for item in yt_links + yt_iframes:
                url = item.get('url', '')
                # Normalise: extract video ID and rebuild clean URL
                vid_m = re.search(r'(?:v=|youtu\.be/|embed/)([\w-]{11})', url)
                if not vid_m:
                    continue
                vid_id = vid_m.group(1)
                clean_url = f"https://www.youtube.com/watch?v={vid_id}"
                if clean_url in seen_urls:
                    continue
                seen_urls.add(clean_url)
                title = item.get('title', '').strip()
                # Skip "View Channel" type links with no real title
                if len(title) < 5:
                    title = ""
                youtube_reviews.append({
                    "url": clean_url,
                    "title": title,
                    "videoId": vid_id,
                })
            youtube_reviews = youtube_reviews[:6]  # Cap at 6 reviews per strain
    except Exception:
        pass  # YouTube reviews are a bonus — never block on failure

    return {
        "name": strain_name, "producer": producer_name,
        "thc": thc, "cbd": cbd, "type": strain_type, "code": code, "tier": "Core",
        "terpenes": terpenes, "effects": effects, "flavours": flavours,
        "helpsWith": helps, "negatives": negatives,
        "youtubeReviews": youtube_reviews,
    }


# ---------------------------------------------------------------------------
# Step 3: Weedstrain fallback
# ---------------------------------------------------------------------------
def scrape_weedstrain(strain_name):
    slug = slugify(strain_name)
    if not slug or len(slug) < 2:
        return None
    url = f"{WEEDSTRAIN_BASE}/{slug}-strain"
    try:
        resp = requests.get(url, timeout=15, headers=HTTP_HEADERS)
        if resp.status_code != 200:
            return None
    except Exception:
        return None

    page_text = BeautifulSoup(resp.text, 'html.parser').get_text()
    data = {"terpenes": [], "effects": [], "flavours": [], "helpsWith": [], "negatives": []}

    for terp in KNOWN_TERPENES:
        if re.search(rf'\b{terp}\b', page_text, re.IGNORECASE) and len(data["terpenes"]) < 3:
            data["terpenes"].append(terp)
    for eff in ['Relaxed','Euphoric','Happy','Sleepy','Uplifted','Energetic','Creative','Focused','Calmed']:
        if re.search(rf'\b{eff}\b', page_text, re.IGNORECASE) and len(data["effects"]) < 3:
            data["effects"].append(eff)
    for flav in ['Earthy','Sweet','Citrus','Berry','Pine','Spicy','Floral','Diesel','Herbal','Woody','Fruity','Lemon']:
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

    # 2. Discover all strain page URLs via simple HTTP
    print("\n🔍 Discovering strain pages (HTTP)...")
    all_urls = {}  # url -> producer_name
    for slug, producer in PRODUCERS.items():
        urls = discover_strain_urls(slug)
        if urls:
            print(f"  📦 {producer}: {len(urls)} pages")
            for u in urls:
                all_urls[u] = producer
        else:
            print(f"  📦 {producer}: ✗ not found")
        time.sleep(0.5)

    print(f"\n  Total strain pages to scrape: {len(all_urls)}")

    if not all_urls:
        print("  ⚠ No pages discovered — keeping existing data")
        return 0

    # 3. Scrape each strain page with Playwright
    print("\n🌐 Scraping strain pages with headless browser...")
    new_strains = []
    seen_new = set()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=HTTP_HEADERS["User-Agent"])
        page = await context.new_page()

        count = 0
        for surl, producer in all_urls.items():
            count += 1
            if count % 50 == 0:
                print(f"    ... processed {count}/{len(all_urls)} pages")

            data = await scrape_strain_page_pw(page, surl, producer)
            if not data or not data["name"]:
                continue

            key = (data["name"].lower(), producer.lower())

            # Update existing
            if key in existing_by_key:
                ex = existing_by_key[key]
                if data["thc"] > 0 and ex.get("thc", 0) == 0:
                    ex["thc"] = data["thc"]
                if data["cbd"] > 0 and ex.get("cbd", 0) == 0:
                    ex["cbd"] = data["cbd"]
                if data["terpenes"] and not ex.get("terpenes"):
                    ex["terpenes"] = data["terpenes"]
                # Always refresh YouTube reviews (they change over time)
                if data.get("youtubeReviews"):
                    ex["youtubeReviews"] = data["youtubeReviews"]
                continue

            if key in seen_new:
                continue
            seen_new.add(key)

            new_strains.append(data)
            terp_str = ', '.join(data['terpenes'][:3]) if data['terpenes'] else 'no terpenes yet'
            print(f"    ✚ {data['name']} ({producer}, THC {data['thc']}%, {terp_str})")

        await browser.close()

    print(f"\n  New unique strains: {len(new_strains)}")

    # 4. Weedstrain fallback for missing terpenes
    missing = [s for s in new_strains if not s.get("terpenes")]
    if missing:
        print(f"\n🔬 Weedstrain fallback for {len(missing)} strains...")
        for s in missing:
            ws = scrape_weedstrain(s["name"])
            if ws:
                for k in ["terpenes", "effects", "flavours", "helpsWith", "negatives"]:
                    if not s.get(k):
                        s[k] = ws[k]
                print(f"  ✓ {s['name']}: {', '.join(ws['terpenes'])}")
            else:
                print(f"  ✗ {s['name']}")
            time.sleep(0.5)

    # 5. Merge
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
            "negatives": s.get("negatives", []), "youtubeReviews": s.get("youtubeReviews", []),
            "id": code,
        })

    # 6. Save
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
