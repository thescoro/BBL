#!/usr/bin/env python3
"""
Bloomy's Bud Log — Strain Data Updater (Hybrid Edition)
=========================================================
- Step 1: Uses simple HTTP requests to discover strain page URLs from each
  producer's index page (these links ARE in the static HTML).
- Step 2: Uses Playwright headless browser to visit each individual strain
  page and extract the JS-rendered data (terpenes, THC/CBD, type, etc.).
- Step 3: Falls back to Weedstrain.com for any strains still missing terpenes.
- Step 4: Scrapes YouTube reviews from MedBud's central reviews page.
- Step 5: Merges with existing strains.json (never deletes, only adds/updates).

Usage:  python scripts/update_strains.py
Debug:  python scripts/update_strains.py --debug <medbud-strain-url>
Reenrich: python scripts/update_strains.py --reenrich
          (Re-scrapes existing strains with schema_version < 2 to backfill
          terpeneDetails and other enrichment fields. Skips discovery,
          YouTube reviews, and HTML update for speed.)
AllBud:   python scripts/update_strains.py --allbud
          (Runs AllBud enrichment on all flowers with < 3 helpsWith tags.
          No Playwright needed. Use after --reenrich to backfill helpsWith.)
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
try:
    from playwright.async_api import async_playwright
except ImportError:
    async_playwright = None

MEDBUD_BASE = "https://medbud.wiki"
CART_BASE_URL = f"{MEDBUD_BASE}/vape-cartridges"
WEEDSTRAIN_BASE = "https://weedstrain.com/uk/weed-strains"
ALLBUD_BASE = "https://www.allbud.com/marijuana-strains"
YOUTUBE_REVIEWS_URL = "https://medbud.wiki/reviews/youtube/"
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
    "somai": "Somai Pharmaceuticals",
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
    "grow-lab-organics": "Grow Lab Organics",
    "northern-leaf": "Northern Leaf",
    "antg": "ANTG",
    "argent-biopharma": "Argent BioPharma",
    "linneo-health": "Linneo Health",
    "scoops": "Scoops",
    "kasa-verde": "Kasa Verde",
    "humble-bud": "Humble Bud",
    "kiseki": "Kiseki",
}

# Cartridge producers on MedBud — same URL structure as flower but under /vape-cartridges/
# Discovery will silently skip any slug that 404s, so over-listing is safe.
CART_PRODUCERS = {
    "curaleaf": "Curaleaf",
    "clearleaf": "Clearleaf",
    "noidecs": "Noidecs",
    "4c-labs": "4C Labs",
    "curo": "Curo",
    "aurora-pedanios": "Aurora (Pedanios)",
    "adven": "Adven",
    "grow-pharma": "Grow Pharma",
    "khiron": "Khiron",
    "mamedica": "Mamedica",
    "releaf": "Releaf",
    "tilray": "Tilray Medical",
    "lumir": "Lumir",
    "althea": "Althea",
    "lot420": "Lot420",
    "medicus": "Medicus",
    "somai": "Somai Pharmaceuticals",
    "big-narstie-medical": "Big Narstie Medical",
}

KNOWN_TERPENES = [
    'Myrcene', 'Caryophyllene', 'Limonene', 'Linalool', 'Pinene',
    'Humulene', 'Ocimene', 'Terpinolene', 'Bisabolol', 'Valencene',
    'Geraniol', 'Terpineol', 'Camphene', 'Nerolidol', 'Guaiol',
    'Eucalyptol', 'Borneol', 'Sabinene', 'Phellandrene', 'Phytol',
    'Carene', 'Fenchol', 'Farnesene', 'Isopulegol', 'Pulegone',
    'Cedrene', 'Cymene',
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def parse_terpene_table(page):
    """
    Parse MedBud's terpene table structurally via Playwright DOM selectors.
    Returns a list of {"name": str, "designation": str} dicts where
    designation is "Major" or "Minor" (or "" if not determinable).
    Falls back to empty list if the table isn't found or parsing fails.
    """
    try:
        details = await page.evaluate("""() => {
            const results = [];
            // MedBud renders terpenes in a table within the "Terpene Profile" section.
            // Walk all table rows looking for terpene names + Major/Minor labels.
            const rows = document.querySelectorAll('table tr, tr');
            const knownTerpenes = new Set([
                'Myrcene', 'Caryophyllene', 'Limonene', 'Linalool', 'Pinene',
                'Humulene', 'Ocimene', 'Terpinolene', 'Bisabolol', 'Valencene',
                'Geraniol', 'Terpineol', 'Camphene', 'Nerolidol', 'Guaiol',
                'Eucalyptol', 'Borneol', 'Sabinene', 'Phellandrene', 'Phytol',
                'Carene', 'Fenchol', 'Farnesene', 'Isopulegol', 'Pulegone',
                'Cedrene', 'Cymene'
            ]);
            const seen = new Set();
            for (const row of rows) {
                const text = row.innerText || '';
                // Check if this row contains a known terpene name
                for (const terp of knownTerpenes) {
                    if (text.includes(terp) && !seen.has(terp)) {
                        seen.add(terp);
                        // Determine designation from row text
                        let designation = '';
                        const lower = text.toLowerCase();
                        if (lower.includes('major')) {
                            designation = 'Major';
                        } else if (lower.includes('minor')) {
                            designation = 'Minor';
                        }
                        results.push({name: terp, designation: designation});
                    }
                }
            }
            return results;
        }""")
        return details if details else []
    except Exception:
        return []


def clean_strain_name(raw):
    if not raw:
        return None
    for stop in ['Classification', 'Chemotype', 'Type I', 'Type II', 'Type III',
                 'Flower Provided', 'THC Potential', 'CBD Potential', 'Trimmed',
                 'Indica', 'Sativa', 'Hybrid', 'Medication Overview',
                 'Ratings', 'YouTube', 'Community']:
        idx = raw.find(stop)
        if idx > 0:
            raw = raw[:idx]
    raw = re.sub(r'\s+', ' ', raw).strip(' \u00b7\t\n')
    return raw if 2 <= len(raw) <= 50 else None


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


def is_valid_code(code):
    """A valid strain code: 2-10 alphanumeric+dash chars, no whitespace."""
    return bool(code) and 2 <= len(code) <= 10 and bool(re.match(r'^[A-Za-z0-9\-]+$', code))


def parse_cart_designation(designation):
    """
    Parse a MedBud cartridge designation string to extract (name, code).

    MedBud cart designations follow patterns like:
      "QMID WPT T840 Wedding Pop Triangle"   -> ("Wedding Pop Triangle", "WPT")
      "Rosin T750C50 GMO"                    -> ("GMO", "")
      "Resin T765 Sourdough"                 -> ("Sourdough", "")
      "Distillate T800 Blue Dream"           -> ("Blue Dream", "")

    Strategy: strip recognised category-code prefixes (QMID/QMIE/QMIF),
    extract-type words (Rosin/Resin/Distillate/Live), and T###/C###/T###C###
    tokens from the front. If a short uppercase acronym then precedes real
    words, treat it as the product code. What remains is the product name.
    """
    parts = designation.split()
    prefixes_to_strip = {
        'QMID', 'QMIE', 'QMIF', 'QMIG',
        'Rosin', 'Resin', 'Distillate', 'Live',
        'Full', 'Broad', 'Spectrum',
    }
    code = ""

    # Keep stripping leading tokens while we match known patterns.
    # Use a multi-pass loop so token order doesn't matter.
    changed = True
    while changed and parts:
        changed = False
        # Strip category/extract prefix
        if parts[0] in prefixes_to_strip:
            parts.pop(0)
            changed = True
            continue
        # Strip T\d+ / C\d+ / T\d+C\d+ / T\d+:C\d+ potency tokens.
        # The colon form (e.g. "T600:C200") is MedBud's canonical format
        # for balanced THC/CBD cartridges.
        if re.match(r'^[TC]\d+(?::?[TC]\d+)?$', parts[0]):
            parts.pop(0)
            changed = True
            continue
        # Capture leading acronym as product code (3-5 uppercase letters).
        # 3+ chars only, to protect 2-letter name fragments like "OG" in
        # "OG Kush" or "MK" in "MK Ultra". Only capture if title-case words
        # follow (rest contains lowercase).
        if not code and len(parts) >= 2 and re.match(r'^[A-Z]{3,5}$', parts[0]):
            rest = ' '.join(parts[1:])
            if re.search(r'[a-z]', rest):
                code = parts.pop(0)
                changed = True
                continue

    # Strip trailing T/C potency tokens that sometimes land after the name
    # (including the colon format "T600:C200").
    while parts and re.match(r'^[TC]\d+(?::?[TC]\d+)?$', parts[-1]):
        parts.pop()

    name = ' '.join(parts).strip()

    # If what remains is a bare 3-5 char uppercase acronym, it's a product
    # code masquerading as a name (e.g. "WPT", "JHR"). On cart pages a real
    # strain name contains lowercase letters or multiple words. Treat the
    # acronym as the code and return an empty name so fallback extraction
    # approaches in scrape_cart_page_pw can try to find something better.
    if name and not code and re.match(r'^[A-Z]{2,5}$', name):
        code = name
        name = ""

    return name, code


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


def discover_cart_urls(producer_slug):
    """Discover cartridge page URLs from MedBud's /vape-cartridges/{producer}/ index."""
    url = f"{CART_BASE_URL}/{producer_slug}/"
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception:
        return []

    soup = BeautifulSoup(resp.text, 'html.parser')
    pattern = re.compile(
        rf'^(?:https?://medbud\.wiki)?/vape-cartridges/{re.escape(producer_slug)}/([^/]+)/?$'
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
        try:
            await page.wait_for_selector('text=Cultivar/Strain', timeout=8000)
        except Exception:
            pass
        await page.wait_for_timeout(1500)
    except Exception:
        return None

    page_text = await page.inner_text('body')

    # --- 404 guard ---
    # MedBud returns HTTP 200 on missing pages but with "404: Not Found" in
    # the title/body. Catch it before extracting anything.
    if ("404: Not Found" in page_text
            or "The resource you\u2019re trying to access" in page_text
            or "The resource you're trying to access" in page_text):
        print(f"    [flower-skip] 404 page for {url}")
        return None

    # --- Tier (from URL slug, then page text) ---
    tier = "Core"
    url_lower = url.lower()
    if '/value-' in url_lower or '-value-' in url_lower:
        tier = "Value"
    elif '/premium-' in url_lower or '-premium-' in url_lower:
        tier = "Premium"
    elif '/craft-organic-' in url_lower or '-craft-organic-' in url_lower:
        tier = "Craft Organic"
    elif '/craft-select-' in url_lower or '-craft-select-' in url_lower:
        tier = "Craft Select"
    elif '/craft-' in url_lower or '-craft-' in url_lower:
        tier = "Craft"
    if tier == "Core":
        tier_m = re.search(r'\b(Value|Premium|Craft Organic|Craft Select|Craft)\b', page_text)
        if tier_m:
            tier = tier_m.group(1)

    # --- Strain name (multiple approaches) ---
    strain_name = None

    # Approach 1: Regex — improved with more stop words
    m = re.search(
        r'Cultivar/Strain\s*\u00b7?\s*'
        r'([A-Za-z][\w\s\'\-\&\.\,\u00e9\u00e8\u00fc#]+?)'
        r'\s*(?:Classification|Chemotype|Flower|THC|Indica|Sativa|Hybrid|Medication)',
        page_text
    )
    if m:
        strain_name = clean_strain_name(m.group(1))

    # Approach 2: Try without "Cultivar/Strain" prefix — some pages may format differently
    if not strain_name:
        # Look for a name followed by Classification near the start of the page
        m2 = re.search(
            r'^(.{0,500}?)'  # Within first 500 chars
            r'([A-Z][a-zA-Z\s\'\-\&\.]{2,40}?)'
            r'\s*Classification',
            page_text, re.DOTALL
        )
        if m2:
            strain_name = clean_strain_name(m2.group(2))

    # DISABLED: URL slug fallback creates bad names like "Hb T24"
    # If we can't extract a proper name, skip this page entirely

    if not strain_name or len(strain_name) < 2 or len(strain_name) > 50:
        # Log diagnostic for first few failures to help debug
        if not hasattr(scrape_strain_page_pw, '_diag_count'):
            scrape_strain_page_pw._diag_count = 0
        if scrape_strain_page_pw._diag_count < 5:
            scrape_strain_page_pw._diag_count += 1
            preview = page_text[:300].replace('\n', '\\n')
            print(f"    [diag] Name extraction failed for {url}")
            print(f"    [diag] Page text starts with: {preview}")
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

    # --- CBD (targeted — avoid grabbing THC values) ---
    cbd = 0
    # Look specifically for "CBD" followed closely by numbers (within ~30 chars)
    cbd_m = re.search(r'CBD\s*(?:Potential\s*)?(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*%', page_text)
    if cbd_m:
        val = float(cbd_m.group(2))
        if val >= 1:
            cbd = round(val)
    else:
        cbd_m2 = re.search(r'CBD\s*(?:Potential\s*)?[:\s]*(\d+(?:\.\d+)?)\s*%', page_text)
        if cbd_m2:
            val = float(cbd_m2.group(1))
            if val >= 1:
                cbd = round(val)
    # Sanity check: CBD shouldn't equal THC (scraping artefact)
    if cbd > 0 and cbd == thc:
        cbd = 0

    # --- Type ---
    strain_type = "Hybrid"
    type_m = re.search(r'Classification\s*\u00b7?\s*(Indica|Sativa|Hybrid|Indica Hybrid|Sativa Hybrid)',
                       page_text, re.IGNORECASE)
    if type_m:
        t = type_m.group(1).lower()
        if 'indica' in t:
            strain_type = "Indica"
        elif 'sativa' in t:
            strain_type = "Sativa"

    # --- Terpenes (targeted to terpene section of page) ---
    terpenes = []
    terp_section = page_text
    terp_idx = page_text.find("Terpene Profile")
    if terp_idx < 0:
        terp_idx = page_text.lower().find("terpene")
    if terp_idx >= 0:
        terp_section = page_text[terp_idx:terp_idx+800]

    for terp in KNOWN_TERPENES:
        if re.search(rf'\b{terp}\b', terp_section) and terp not in terpenes:
            terpenes.append(terp)
    terpenes = terpenes[:5]

    # --- Terpene details (structural parse — Major/Minor designation) ---
    terpene_details = await parse_terpene_table(page)
    # If structural parse found terpenes the regex missed, backfill the flat list
    for td in terpene_details:
        if td["name"] not in terpenes and len(terpenes) < 5:
            terpenes.append(td["name"])

    # --- Effects (targeted to effects section) ---
    effects = []
    eff_section = page_text
    eff_idx = page_text.lower().find("effect")
    if eff_idx >= 0:
        eff_section = page_text[max(0, eff_idx-50):eff_idx+300]
    for eff in ['Relaxed', 'Euphoric', 'Happy', 'Sleepy', 'Hungry', 'Uplifted',
                'Energetic', 'Creative', 'Focused', 'Calmed', 'Body', 'Giggly']:
        if re.search(rf'\b{eff}\b', eff_section, re.IGNORECASE) and eff not in effects and len(effects) < 3:
            effects.append(eff)

    # --- Flavours (targeted to flavour section) ---
    flavours = []
    flav_section = page_text
    flav_idx = page_text.lower().find("flavo")
    if flav_idx < 0:
        flav_idx = page_text.lower().find("taste")
    if flav_idx >= 0:
        flav_section = page_text[max(0, flav_idx-50):flav_idx+300]
    for flav in ['Earthy', 'Sweet', 'Citrus', 'Berry', 'Pine', 'Spicy', 'Floral',
                 'Diesel', 'Herbal', 'Woody', 'Tropical', 'Fruity', 'Lemon', 'Vanilla',
                 'Grape', 'Mint', 'Sour', 'Mango', 'Cheese', 'Creamy', 'Pepper',
                 'Nutty', 'Coffee', 'Hazy', 'Candy', 'Cookie', 'Butter', 'Garlic']:
        if re.search(rf'\b{flav}\b', flav_section, re.IGNORECASE) and flav not in flavours and len(flavours) < 3:
            flavours.append(flav)

    # --- Medical ---
    helps = []
    med_section = page_text
    med_idx = page_text.lower().find("help")
    if med_idx < 0:
        med_idx = page_text.lower().find("medical")
    if med_idx >= 0:
        med_section = page_text[max(0, med_idx-50):med_idx+300]
    for med in ['Pain', 'Stress', 'Anxiety', 'Depression', 'Insomnia', 'Fatigue',
                'Spasticity', 'ADHD', 'PTSD', 'Inflammation', 'Nausea']:
        if re.search(rf'\b{med}\b', med_section, re.IGNORECASE) and med not in helps and len(helps) < 5:
            helps.append(med)
    if helps:
        print(f"    [medbud-medical] {strain_name}: {helps}")

    # --- Negatives ---
    negatives = []
    neg_section = page_text
    neg_idx = page_text.lower().find("negative")
    if neg_idx < 0:
        neg_idx = page_text.lower().find("side effect")
    if neg_idx >= 0:
        neg_section = page_text[max(0, neg_idx-50):neg_idx+300]
    for neg in ['Dry mouth', 'Dry eyes', 'Dizzy', 'Paranoid', 'Anxious', 'Couch-lock']:
        if neg.lower() in neg_section.lower() and neg not in negatives:
            negatives.append(neg)

    # --- Code ---
    code = ""
    code_m = re.search(r'Designation\s*\u00b7?\s*([A-Z0-9][A-Z0-9\-]{1,9})', page_text)
    if code_m:
        candidate = code_m.group(1).strip()
        # Validate: 2-10 chars, no newlines/spaces, alphanumeric+dash only
        if 2 <= len(candidate) <= 10 and re.match(r'^[A-Za-z0-9\-]+$', candidate):
            code = candidate

    # --- Genetics / Parent Strains ---
    genetics = ""

    # MedBud format examples:
    #   Parents: 🇦🇺 Cake Crasher (Sativa Hybrid) x 🇦🇺 Strawberries & Cream (Sativa Hybrid)
    #   Parents: (Blueberry x Hash) x Sour Diesel
    # Strategy: extract the raw Parents line, strip emoji and type annotations, keep groupings

    gen_section = ""
    for keyword in ["Parents:", "Parents", "Parent Strains:", "Parent Strain:",
                     "Genetics:", "Lineage:", "Parentage:"]:
        gen_idx = page_text.find(keyword)
        if gen_idx < 0:
            gen_idx = page_text.lower().find(keyword.lower())
        if gen_idx >= 0:
            start = gen_idx + len(keyword)
            gen_section = page_text[start:start+400]
            break

    if gen_section:
        # Take only the first meaningful line (stop at newline or next section header)
        lines = gen_section.split('\n')
        raw_line = ""
        for line in lines:
            stripped = line.strip()
            if stripped and len(stripped) > 3:
                raw_line = stripped
                break

        if raw_line:
            # Strip emoji/non-ASCII (flags like 🇦🇺 before strain names)
            clean = re.sub(r'[^\x00-\x7F]+', ' ', raw_line)
            # Strip TYPE annotations like (Sativa Hybrid), (Indica), (Hybrid), (Sativa)
            # but keep GROUPING parens like (Blueberry x Hash)
            clean = re.sub(
                r'\(\s*(?:Sativa|Indica|Hybrid|Sativa Hybrid|Indica Hybrid|'
                r'Sativa Dominant|Indica Dominant|50/50|Balanced)\s*\)',
                '', clean, flags=re.IGNORECASE
            )
            # Clean up leading colons, dots, dashes
            clean = re.sub(r'^[\s:·\-]+', '', clean)
            # Collapse whitespace
            clean = re.sub(r'\s{2,}', ' ', clean).strip()
            # Normalise lowercase 'x' between strain names to ×
            clean = re.sub(r'\s+[xX×]\s+', ' × ', clean)
            # Truncate at next-section keywords that leak in
            for stop in ['Strain Type', 'Classification', 'Chemotype', 'THC', 'CBD',
                         'Terpene', 'Effect', 'Flavour', 'Flavor', 'Medical',
                         'Medication', 'Negative', 'Side Effect', 'Please note']:
                stop_idx = clean.find(stop)
                if stop_idx > 0:
                    clean = clean[:stop_idx]
            clean = clean.strip().rstrip('·').rstrip(',').rstrip('.').strip()
            if len(clean) >= 3 and '×' in clean:
                genetics = clean
            elif len(clean) >= 3:
                # No × found — might be a single parent or unusual format, store anyway
                genetics = clean

    # Approach 2: "cross of X and Y" anywhere near top of page
    if not genetics:
        cross_section = re.sub(r'[^\x00-\x7F]+', ' ', page_text[:1500])
        cross_m = re.search(
            r"(?:cross|hybrid)\s+(?:of|between)\s+"
            r"([A-Z][A-Za-z '\-\&]{2,30}?)\s+(?:and|&)\s+"
            r"([A-Z][A-Za-z '\-\&]{2,30}?)[\.\,\n]",
            cross_section, re.IGNORECASE
        )
        if cross_m:
            genetics = f"{cross_m.group(1).strip()} × {cross_m.group(2).strip()}"

    # Final cleanup
    if genetics:
        genetics = re.sub(r'  +', ' ', genetics).strip()
        # Reject MedBud UI text that leaked through
        reject_phrases = ['Login', 'Pharmacy', 'Pricing', 'Availability',
                          'Please note', 'prescription', 'disclaimer',
                          'consult your doctor', 'localStorage', 'MedBud']
        if any(rp.lower() in genetics.lower() for rp in reject_phrases):
            genetics = ""
        # Fix double separators: "x ×" or "× x"
        genetics = re.sub(r'\s*x\s*×\s*', ' × ', genetics)
        genetics = re.sub(r'\s*×\s*x\s*', ' × ', genetics)
        # Remove trailing leaked descriptive words
        genetics = re.sub(
            r'\s+(?:strain|strains|if|is|are|the|a|an|this|which|that|with|from)s?\s*\.?$',
            '', genetics, flags=re.IGNORECASE
        ).strip()
        if len(genetics) > 80:
            genetics = genetics[:80].rsplit(' ', 1)[0]
        # Must be at least 3 chars to be meaningful
        if len(genetics) < 3:
            genetics = ""

    return {
        "name": strain_name, "producer": producer_name, "form": "Flower",
        "thc": thc, "cbd": cbd, "type": strain_type, "code": code, "tier": tier,
        "terpenes": terpenes, "terpeneDetails": terpene_details,
        "effects": effects, "flavours": flavours,
        "helpsWith": helps, "negatives": negatives, "genetics": genetics,
        "schema_version": 2,
    }


# ---------------------------------------------------------------------------
# Step 2b: Scrape individual cartridge page with Playwright
# ---------------------------------------------------------------------------
# Cartridges live at medbud.wiki/vape-cartridges/{producer}/{slug}/ and share
# most of the page structure with flower (name, type, terpenes, genetics),
# but THC/CBD are expressed in milligrams and there are cart-specific fields:
# volume, extract type, terpene source, fitment.
async def scrape_cart_page_pw(page, url, producer_name):
    try:
        await page.goto(url, timeout=30000)
        try:
            await page.wait_for_selector('text=Cultivar/Strain', timeout=8000)
        except Exception:
            pass
        await page.wait_for_timeout(1500)
    except Exception:
        return None

    page_text = await page.inner_text('body')

    # Also grab page title and first h1 — cart pages often put the product
    # name there rather than in a "Cultivar/Strain · ..." inline label.
    page_title = ""
    page_h1 = ""
    try:
        page_title = (await page.title()) or ""
    except Exception:
        pass
    try:
        h1_el = await page.query_selector('h1')
        if h1_el:
            page_h1 = (await h1_el.inner_text()) or ""
    except Exception:
        pass

    # --- 404 guard ---
    # MedBud returns HTTP 200 on missing pages but with a "404: Not Found"
    # title/h1 and a fixed "resource you're trying to access" body message.
    # Without this, we'd happily persist a "404: Not Found" record with
    # mg values parsed from the URL slug.
    if (page_title.startswith('404:') or page_h1.startswith('404:')
            or "The resource you\u2019re trying to access" in page_text
            or "The resource you're trying to access" in page_text):
        print(f"    [cart-skip] 404 page for {url}")
        return None

    # --- Strain name (layered fallbacks for cart pages) ---
    strain_name = None
    designation_code = ""  # filled by parse_cart_designation below, reused as code

    # Approach 1: From "Designation" field in the Medication Details table.
    # Format: "QMID WPT T840 Wedding Pop Triangle" — parse_cart_designation
    # peels off category prefixes, T/C potency codes, and 3+ char product-code
    # acronyms, leaving the human-readable name (and the acronym if any).
    des_m = re.search(r'Designation\s+([A-Z][^\n]+)', page_text)
    if des_m:
        parsed_name, parsed_code = parse_cart_designation(des_m.group(1).strip())
        if parsed_code:
            designation_code = parsed_code
        if parsed_name:
            cleaned = clean_strain_name(parsed_name)
            if cleaned and 2 <= len(cleaned) <= 50:
                strain_name = cleaned

    # Approach 2: Cultivar/Strain regex (same as flower — some cart pages use it)
    if not strain_name:
        m = re.search(
            r'Cultivar/Strain\s*\u00b7?\s*'
            r'([A-Za-z][\w\s\'\-\&\.\,\u00e9\u00e8\u00fc#]+?)'
            r'\s*(?:Classification|Chemotype|Flower|THC|Indica|Sativa|Hybrid|'
            r'Medication|Distillate|Rosin|Resin|Cartridge|Format)',
            page_text
        )
        if m:
            strain_name = clean_strain_name(m.group(1))

    # Approach 3: name before "Classification" (loose regex)
    if not strain_name:
        m2 = re.search(
            r'^(.{0,500}?)'
            r'([A-Z][a-zA-Z\s\'\-\&\.]{2,40}?)'
            r'\s*Classification',
            page_text, re.DOTALL
        )
        if m2:
            strain_name = clean_strain_name(m2.group(2))

    # Approach 4: top product line —
    # "{Producer}® QMID WPT T840 Wedding Pop Triangle Medical Cannabis Cartridge"
    if not strain_name:
        producer_clean = producer_name.split("(")[0].strip().rstrip('®').strip()
        top_m = re.search(
            rf'{re.escape(producer_clean)}\s*[®\*]*\s*(.+?)\s+'
            r'(?:Medical Cannabis Cartridge|Medical Cannabis|Cartridge)',
            page_text, re.IGNORECASE
        )
        if top_m:
            raw = top_m.group(1).strip()
            parsed_name, parsed_code = parse_cart_designation(raw)
            if parsed_code and not designation_code:
                designation_code = parsed_code
            if parsed_name:
                cleaned = clean_strain_name(parsed_name)
                if cleaned and 2 <= len(cleaned) <= 50:
                    strain_name = cleaned

    # Approach 5: first <h1> — cart pages sometimes put product name here
    if not strain_name and page_h1:
        raw = re.sub(r'[^\x00-\x7F]+', ' ', page_h1).strip()
        raw = re.sub(r'\s*[|\-–—]\s*(MedBud|medbud\.wiki|'
                     + re.escape(producer_name) + r').*$', '', raw, flags=re.IGNORECASE)
        strain_name = clean_strain_name(raw)

    # Approach 6: page <title> — last-ditch
    if not strain_name and page_title:
        raw = re.sub(r'[^\x00-\x7F]+', ' ', page_title).strip()
        raw = re.sub(r'\s*[|\-–—]\s*.*$', '', raw)
        strain_name = clean_strain_name(raw)

    if not strain_name or len(strain_name) < 2 or len(strain_name) > 50:
        if not hasattr(scrape_cart_page_pw, '_diag_count'):
            scrape_cart_page_pw._diag_count = 0
        if scrape_cart_page_pw._diag_count < 5:
            scrape_cart_page_pw._diag_count += 1
            print(f"    [cart-diag] Name extraction failed for {url}")
            print(f"    [cart-diag] <title>: {page_title[:120]!r}")
            print(f"    [cart-diag] <h1>:    {page_h1[:120]!r}")
            print(f"    [cart-diag] text preview: {page_text[:200].replace(chr(10), chr(92) + 'n')!r}")
        return None

    # --- THC in milligrams ---
    # MedBud shows THC as e.g. "THC 840mg" or "840mg THC" (100-1200 is typical)
    thc_mg = 0
    cbd_mg = 0

    # PRIORITY: "<THC>mg / <CBD>mg" pattern.
    # Balanced THC/CBD carts are listed as "600mg / 200mg" right by the title.
    # This pattern is authoritative — check it before any "NNNmg THC" fallback,
    # which can accidentally match review/stat text elsewhere on the page.
    # Also handles trace markers: "800mg / <10mg" → cbd treated as trace (0).
    ratio_m = re.search(
        r'\b(\d{2,4}(?:\.\d+)?)\s*mg\s*/\s*(<|\u2264)?\s*(\d{1,4}(?:\.\d+)?)\s*mg\b',
        page_text
    )
    if ratio_m:
        thc_val = float(ratio_m.group(1))
        cbd_is_trace = bool(ratio_m.group(2))
        cbd_val = float(ratio_m.group(3))
        if 50 <= thc_val <= 1500:
            thc_mg = int(round(thc_val))
        if not cbd_is_trace and cbd_val >= 1 and int(round(cbd_val)) <= 500:
            cbd_mg = int(round(cbd_val))

    if thc_mg == 0:
        thc_m = re.search(r'THC[^\d]{0,30}(\d{2,4})\s*mg', page_text, re.IGNORECASE)
        if not thc_m:
            thc_m = re.search(r'(\d{2,4})\s*mg[^\n\r\d]{0,20}THC', page_text, re.IGNORECASE)
        if thc_m:
            val = int(thc_m.group(1))
            if 50 <= val <= 1500:
                thc_mg = val
    # Fallback: parse from URL slug e.g. ".../t840-wpt/", ".../rosin-t750c50-gmo/",
    # or ".../jhr-t600-c200-jack-herer/" (dash-separated balanced format).
    if thc_mg == 0:
        url_thc = re.search(r'[/-][tT](\d{2,4})(?:[-]?[cC]\d+)?[/-]', url)
        if url_thc:
            val = int(url_thc.group(1))
            if 50 <= val <= 1500:
                thc_mg = val

    # --- CBD in milligrams ---
    # Accept decimal values (some carts list "0.8mg CBD"). Round to nearest
    # integer, but treat any sub-1mg value as 0 — that's either a trace
    # amount or an artefact (0.8mg of CBD in a cart has no therapeutic effect
    # and is effectively equivalent to "<1mg").
    # NOTE: cbd_mg may already be set by the priority "NNNmg / MMMmg" regex
    # above. Only run the fallback extractors if it's still 0.
    if cbd_mg == 0:
        cbd_m = re.search(r'CBD[^\d]{0,30}(\d{1,4}(?:\.\d+)?)\s*mg', page_text, re.IGNORECASE)
        if not cbd_m:
            # Reverse order: "50mg CBD" or "<1mg CBD" or "0.8mg CBD"
            cbd_m = re.search(r'(\d{1,4}(?:\.\d+)?)\s*mg[^\n\r\d]{0,20}CBD', page_text, re.IGNORECASE)
        if cbd_m:
            # Check for trace-amount marker immediately before the number ("<1mg", "≤1mg").
            start = cbd_m.start(1)
            preceding = page_text[max(0, start - 5):start]
            is_trace = '<' in preceding or '\u2264' in preceding
            val = float(cbd_m.group(1))
            rounded = int(round(val))
            # Trace (marked with '<'), sub-1mg, equal-to-THC (scraping artefact), or
            # out-of-range values are all rejected.
            if not is_trace and val >= 1.0 and rounded != thc_mg and rounded <= 500:
                cbd_mg = rounded
    if cbd_mg == 0:
        # URL fallback handles both "t600c200" and "t600-c200" slug formats.
        url_cbd = re.search(r'[/-][tT]\d{2,4}[-]?[cC](\d{1,4})[/-]', url)
        if url_cbd:
            val = int(url_cbd.group(1))
            if 0 < val <= 500:
                cbd_mg = val

    # --- Volume (0.5ml, 1ml, 1.2ml etc) ---
    volume = ""
    vol_m = re.search(r'\b(\d+(?:\.\d+)?)\s*ml\b', page_text, re.IGNORECASE)
    if vol_m:
        v = float(vol_m.group(1))
        if 0.1 <= v <= 5:
            # Preserve user-friendly format
            volume = f"{v:g}ml"

    # --- Extract type ---
    extract_type = ""
    extract_patterns = [
        ('Live Rosin', r'\bLive\s+Rosin\b'),
        ('Hash Rosin', r'\bHash\s+Rosin\b'),
        ('Live Resin', r'\bLive\s+Resin\b'),
        ('Full Spectrum', r'\bFull\s+Spectrum\b'),
        ('Broad Spectrum', r'\bBroad\s+Spectrum\b'),
        ('CO2 Extract', r'\bCO2\s+Extract\b'),
        ('Solventless', r'\bSolventless\b'),
        ('Distillate', r'\bDistillate\b'),
    ]
    for label, pat in extract_patterns:
        if re.search(pat, page_text, re.IGNORECASE):
            extract_type = label
            break
    # Fallback from URL slug: ".../rosin-t750c50-gmo/" or ".../resin-t765-sourdough/"
    if not extract_type:
        if re.search(r'/rosin-', url, re.IGNORECASE):
            extract_type = "Rosin"
        elif re.search(r'/resin-', url, re.IGNORECASE):
            extract_type = "Live Resin"
        elif re.search(r'/distillate-', url, re.IGNORECASE):
            extract_type = "Distillate"

    # --- Terpene source ---
    terpene_source = ""
    if re.search(r'\bBotanical\s+Terpenes?\b', page_text, re.IGNORECASE):
        terpene_source = "Botanical"
    elif re.search(r'\bCannabis[\s-]Derived\s+Terpenes?\b|\bStrain[\s-]Specific\s+Terpenes?\b',
                   page_text, re.IGNORECASE):
        terpene_source = "Cannabis-Derived"
    elif re.search(r'\bNo\s+Additives?\b', page_text, re.IGNORECASE):
        terpene_source = "No Additives"

    # --- Fitment ---
    # Primary source: "Vape Cartridge (Proprietary)" parenthetical near the
    # top of the page — this is the authoritative product-level fitment.
    fitment = ""
    fit_primary = re.search(
        r'Vape\s+Cartridge\s*\(\s*(510|Kanabo|Pax[\s-]?Era|Proprietary)\s*\)',
        page_text, re.IGNORECASE
    )
    if fit_primary:
        label = fit_primary.group(1)
        # Normalise capitalisation for known values
        for canonical in ['510', 'Kanabo', 'Pax Era', 'Proprietary']:
            if label.lower().replace('-', '').replace(' ', '') == canonical.lower().replace(' ', ''):
                fitment = canonical
                break
        if not fitment:
            fitment = label

    # Fallback: look for explicit "<type> Fitment/Thread/Threaded" phrasings.
    # Check Proprietary / Kanabo / Pax Era BEFORE 510, because some pages
    # mention "510" in body copy (e.g. comparing cart types) without the
    # product itself being 510.
    if not fitment:
        if re.search(r'\bProprietary\s+(?:Fitment|Thread|Threaded)\b', page_text, re.IGNORECASE):
            fitment = "Proprietary"
        elif re.search(r'\bKanabo\s+(?:Fitment|Thread|Threaded)?\b', page_text, re.IGNORECASE):
            fitment = "Kanabo"
        elif re.search(r'\bPax[\s-]?Era\b', page_text, re.IGNORECASE):
            fitment = "Pax Era"
        elif re.search(r'\b510\s+(?:Fitment|Thread|Threaded)\b', page_text, re.IGNORECASE):
            fitment = "510"
        # Last resort: bare mentions, still in priority order
        elif re.search(r'\bProprietary\b', page_text, re.IGNORECASE):
            fitment = "Proprietary"
        elif re.search(r'\bKanabo\b', page_text, re.IGNORECASE):
            fitment = "Kanabo"
        elif re.search(r'\b510\b', page_text):
            fitment = "510"

    # --- Type (flower has "Classification", carts have "Hybrid • 840mg THC") ---
    # IMPORTANT: the multi-word variants must come first in the alternation,
    # otherwise "Hybrid" matches before "Sativa Hybrid" ever gets a chance.
    strain_type = "Hybrid"
    type_m = re.search(
        r'Classification\s*\u00b7?\s*(Sativa Hybrid|Indica Hybrid|Indica|Sativa|Hybrid)',
        page_text, re.IGNORECASE
    )
    if not type_m:
        # Cart layout: "Sativa Hybrid  • 807.5mg THC" or "Hybrid • 840mg THC"
        # Allow decimal mg values (4C uses 807.5mg etc) and flexible whitespace.
        type_m = re.search(
            r'\b(Sativa Hybrid|Indica Hybrid|Indica|Sativa|Hybrid)\s+[\u2022\u00b7]\s*'
            r'(?:<?\d+(?:\.\d+)?\s*mg|\d+\s*%)',
            page_text, re.IGNORECASE
        )
    if type_m:
        t = type_m.group(1).lower()
        # "Sativa Hybrid" and "Indica Hybrid" are themselves hybrids but
        # with a clear lean — classify by the first word for consistency
        # with existing records.
        if 'indica' in t:
            strain_type = "Indica"
        elif 'sativa' in t:
            strain_type = "Sativa"

    # --- Terpenes (same logic as flower) ---
    terpenes = []
    terp_section = page_text
    terp_idx = page_text.find("Terpene Profile")
    if terp_idx < 0:
        terp_idx = page_text.lower().find("terpene")
    if terp_idx >= 0:
        terp_section = page_text[terp_idx:terp_idx+800]
    for terp in KNOWN_TERPENES:
        if re.search(rf'\b{terp}\b', terp_section) and terp not in terpenes:
            terpenes.append(terp)
    terpenes = terpenes[:5]

    # --- Terpene details (structural parse — Major/Minor designation) ---
    terpene_details = await parse_terpene_table(page)
    for td in terpene_details:
        if td["name"] not in terpenes and len(terpenes) < 5:
            terpenes.append(td["name"])

    # --- Effects / Flavours / Helps / Negatives (may be absent on cart pages) ---
    effects = []
    eff_idx = page_text.lower().find("effect")
    if eff_idx >= 0:
        eff_section = page_text[max(0, eff_idx-50):eff_idx+300]
        for eff in ['Relaxed', 'Euphoric', 'Happy', 'Sleepy', 'Hungry', 'Uplifted',
                    'Energetic', 'Creative', 'Focused', 'Calmed', 'Body', 'Giggly']:
            if re.search(rf'\b{eff}\b', eff_section, re.IGNORECASE) and eff not in effects and len(effects) < 3:
                effects.append(eff)

    flavours = []
    flav_idx = page_text.lower().find("flavo")
    if flav_idx < 0:
        flav_idx = page_text.lower().find("taste")
    if flav_idx >= 0:
        flav_section = page_text[max(0, flav_idx-50):flav_idx+300]
        for flav in ['Earthy', 'Sweet', 'Citrus', 'Berry', 'Pine', 'Spicy', 'Floral',
                     'Diesel', 'Herbal', 'Woody', 'Tropical', 'Fruity', 'Lemon', 'Vanilla',
                     'Grape', 'Mint', 'Sour', 'Mango', 'Cheese', 'Creamy', 'Pepper']:
            if re.search(rf'\b{flav}\b', flav_section, re.IGNORECASE) and flav not in flavours and len(flavours) < 3:
                flavours.append(flav)

    helps = []
    med_section = page_text
    med_idx = page_text.lower().find("help")
    if med_idx < 0:
        med_idx = page_text.lower().find("medical")
    if med_idx >= 0:
        med_section = page_text[max(0, med_idx-50):med_idx+300]
    for med in ['Pain', 'Stress', 'Anxiety', 'Depression', 'Insomnia', 'Fatigue',
                'Spasticity', 'ADHD', 'PTSD', 'Inflammation', 'Nausea']:
        if re.search(rf'\b{med}\b', med_section, re.IGNORECASE) and med not in helps and len(helps) < 5:
            helps.append(med)
    if helps:
        print(f"    [medbud-medical] Cart {strain_name}: {helps}")

    negatives = []
    neg_section = page_text
    neg_idx = page_text.lower().find("negative")
    if neg_idx < 0:
        neg_idx = page_text.lower().find("side effect")
    if neg_idx >= 0:
        neg_section = page_text[max(0, neg_idx-50):neg_idx+300]
    for neg in ['Dry mouth', 'Dry eyes', 'Dizzy', 'Paranoid', 'Anxious', 'Couch-lock']:
        if neg.lower() in neg_section.lower() and neg not in negatives:
            negatives.append(neg)

    # --- Code ---
    # If parse_cart_designation captured a product-code acronym (e.g. "WPT"
    # from "QMID WPT T840 Wedding Pop Triangle"), use it. Otherwise leave
    # empty and let make_code() generate a short code from the strain name.
    code = designation_code if is_valid_code(designation_code) else ""

    # --- Genetics (same logic as flower — parents/lineage extraction) ---
    genetics = ""
    gen_section = ""
    for keyword in ["Parents:", "Parents", "Parent Strains:", "Parent Strain:",
                     "Genetics:", "Lineage:", "Parentage:"]:
        gen_idx = page_text.find(keyword)
        if gen_idx < 0:
            gen_idx = page_text.lower().find(keyword.lower())
        if gen_idx >= 0:
            start = gen_idx + len(keyword)
            gen_section = page_text[start:start+400]
            break

    if gen_section:
        lines = gen_section.split('\n')
        raw_line = ""
        for line in lines:
            stripped = line.strip()
            if stripped and len(stripped) > 3:
                raw_line = stripped
                break
        if raw_line:
            clean = re.sub(r'[^\x00-\x7F]+', ' ', raw_line)
            clean = re.sub(
                r'\(\s*(?:Sativa|Indica|Hybrid|Sativa Hybrid|Indica Hybrid|'
                r'Sativa Dominant|Indica Dominant|50/50|Balanced)\s*\)',
                '', clean, flags=re.IGNORECASE
            )
            clean = re.sub(r'^[\s:·\-]+', '', clean)
            clean = re.sub(r'\s{2,}', ' ', clean).strip()
            clean = re.sub(r'\s+[xX×]\s+', ' × ', clean)
            for stop in ['Strain Type', 'Classification', 'Chemotype', 'THC', 'CBD',
                         'Terpene', 'Effect', 'Flavour', 'Flavor', 'Medical',
                         'Medication', 'Negative', 'Side Effect', 'Please note',
                         'Volume', 'Extract', 'Fitment', 'Cartridge']:
                stop_idx = clean.find(stop)
                if stop_idx > 0:
                    clean = clean[:stop_idx]
            clean = clean.strip().rstrip('·').rstrip(',').rstrip('.').strip()
            if len(clean) >= 3:
                genetics = clean

    if genetics:
        genetics = re.sub(r'  +', ' ', genetics).strip()
        reject_phrases = ['Login', 'Pharmacy', 'Pricing', 'Availability',
                          'Please note', 'prescription', 'disclaimer',
                          'consult your doctor', 'localStorage', 'MedBud']
        if any(rp.lower() in genetics.lower() for rp in reject_phrases):
            genetics = ""
        genetics = re.sub(r'\s*x\s*×\s*', ' × ', genetics)
        genetics = re.sub(r'\s*×\s*x\s*', ' × ', genetics)
        genetics = re.sub(
            r'\s+(?:strain|strains|if|is|are|the|a|an|this|which|that|with|from)s?\s*\.?$',
            '', genetics, flags=re.IGNORECASE
        ).strip()
        if len(genetics) > 80:
            genetics = genetics[:80].rsplit(' ', 1)[0]
        if len(genetics) < 3:
            genetics = ""

    return {
        "name": strain_name, "producer": producer_name, "form": "Cartridge",
        "thc": 0, "cbd": 0,
        "thcMg": thc_mg, "cbdMg": cbd_mg,
        "volume": volume, "extractType": extract_type,
        "terpeneSource": terpene_source, "fitment": fitment,
        "type": strain_type, "code": code, "tier": "Core",
        "terpenes": terpenes, "terpeneDetails": terpene_details,
        "effects": effects, "flavours": flavours,
        "helpsWith": helps, "negatives": negatives, "genetics": genetics,
        "schema_version": 2,
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
    data = {"terpenes": [], "effects": [], "flavours": [], "helpsWith": [], "negatives": [], "genetics": ""}

    # Genetics / Parents — strip parenthetical annotations first
    clean_text = re.sub(r'\([^)]{0,40}\)', '', page_text)
    gen_m = re.search(
        r'(?:parents?|lineage|genetics?|cross)\s*[s:\-]?\s*'
        r'([A-Z][A-Za-z0-9\s\'\-\&]{2,35}?)\s*(?:×|x|X|and|&)\s*'
        r'([A-Z][A-Za-z0-9\s\'\-\&]{2,35}?)(?:\s*$|\s*[\.\,\n]|\s+(?:strain|is|are|the|this|a))',
        clean_text, re.IGNORECASE
    )
    if gen_m:
        p1 = gen_m.group(1).strip()
        p2 = gen_m.group(2).strip()
        if len(p1) >= 2 and len(p2) >= 2:
            data["genetics"] = f"{p1} × {p2}"

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
    return data if (data["terpenes"] or data.get("genetics")) else None


# ---------------------------------------------------------------------------
# Step 3b: AllBud enrichment (primary helpsWith source)
# ---------------------------------------------------------------------------
# AllBud type categories — ordered by frequency in UK medical cannabis catalogue.
# We try the strain's known type first, then fall through the rest.
ALLBUD_TYPE_SLUGS = [
    'indica-dominant-hybrid', 'hybrid', 'sativa-dominant-hybrid',
    'indica', 'sativa',
]

# Map AllBud "May Relieve" tags → our target vocabulary (HANDOVER-SCRAPER.md).
# Tags not in this map are kept as-is if they look sensible (logged for review).
ALLBUD_TAG_MAP = {
    'chronic pain':      'Chronic Pain',
    'pain':              'Pain',
    'nerve pain':        'Nerve Pain',
    'neuropathy':        'Neuropathy',
    'muscle spasms':     'Muscle Spasms',
    'cramps':            'Cramps',
    'stiffness':         'Stiffness',
    'muscle tension':    'Muscle Tension',
    'fatigue':           'Fatigue',
    'chronic fatigue':   'Fatigue',
    'insomnia':          'Insomnia',
    'depression':        'Depression',
    'anxiety':           'Anxiety',
    'stress':            'Stress',
    'chronic stress':    'Stress',
    'panic':             'Panic',
    'ptsd':              'PTSD',
    'add/adhd':          'ADHD',
    'adhd':              'ADHD',
    'headaches':         'Headache',
    'migraines':         'Migraine',
    'nausea':            'Nausea',
    'appetite loss':     'Loss of Appetite',
    'loss of appetite':  'Loss of Appetite',
    'inflammation':      'Inflammation',
    'tremors':           'Tremor',
    'seizures':          'Tremor',
    'irritability':      'Irritability',
    'mood swings':       'Depression',
    'bipolar disorder':  'Depression',
    'pms':               'Cramps',
    'eye pressure':      'Eye Pressure',
    'asthma':            'Asthma',
    'fibromyalgia':      'Fibromyalgia',
    'lack of appetite':  'Loss of Appetite',
    'tinnitus':          'Tinnitus',
    'phantom limb pain': 'Nerve Pain',
    'hypertension':      'Hypertension',
    'gastrointestinal disorder': 'Inflammation',
    'multiple sclerosis': 'Spasticity',
    'spinal cord injury': 'Nerve Pain',
}

# Tags we never want to store — AllBud junk / off-topic
ALLBUD_REJECT_TAGS = {
    'alzheimer\'s', 'glaucoma', 'hiv/aids', 'cachexia',
}


def allbud_slugify(name):
    """Convert a strain name to AllBud's URL slug format."""
    s = name.lower().strip()
    # Strip parenthetical annotations like "(Sativa Hybrid)"
    s = re.sub(r'\s*\(.*?\)\s*', '', s)
    # Remove apostrophes
    s = s.replace("'", "").replace("\u2019", "")
    # Remove non-alphanumeric except spaces and hyphens
    s = re.sub(r'[^a-z0-9\s-]', '', s)
    # Collapse whitespace to hyphens
    s = re.sub(r'\s+', '-', s.strip())
    # Remove double hyphens
    s = re.sub(r'-+', '-', s)
    return s


def scrape_allbud(strain_name, thc_percent, strain_type="Hybrid"):
    """
    Scrape AllBud for helpsWith, effects, flavours, negatives, and genetics.

    Uses plain requests + BeautifulSoup (AllBud doesn't need JS rendering).
    Tries multiple AllBud type categories to find the right page.

    Args:
        strain_name: display name of the strain
        thc_percent: our stored THC % (for sanity check)
        strain_type: "Indica", "Sativa", or "Hybrid" from MedBud

    Returns:
        dict with helpsWith, effects, flavours, negatives, genetics, or None
    """
    slug = allbud_slugify(strain_name)
    if not slug or len(slug) < 2:
        return None

    # Order type attempts: try the most likely category first
    type_order = list(ALLBUD_TYPE_SLUGS)
    if strain_type == "Indica":
        # Bump indica categories to the front
        type_order = ['indica', 'indica-dominant-hybrid'] + [
            t for t in type_order if t not in ('indica', 'indica-dominant-hybrid')]
    elif strain_type == "Sativa":
        type_order = ['sativa', 'sativa-dominant-hybrid'] + [
            t for t in type_order if t not in ('sativa', 'sativa-dominant-hybrid')]
    # Hybrid stays in default order (indica-dominant-hybrid is most common)

    resp = None
    matched_url = None
    for type_slug in type_order:
        url = f"{ALLBUD_BASE}/{type_slug}/{slug}"
        try:
            r = requests.get(url, timeout=15, headers=HTTP_HEADERS)
            if r.status_code == 200 and 'Marijuana Strain' in r.text[:5000]:
                resp = r
                matched_url = url
                break
        except Exception:
            pass
        time.sleep(0.3)  # Brief pause between type probes

    if not resp:
        return None

    soup = BeautifulSoup(resp.text, 'html.parser')

    # --- THC sanity check ---
    thc_text = soup.get_text()
    allbud_thc = 0
    thc_m = re.search(r'THC:\s*(\d+)\s*%?\s*-\s*(\d+)\s*%', thc_text)
    if thc_m:
        allbud_thc = int(thc_m.group(2))  # Use upper end
    else:
        thc_m2 = re.search(r'THC:\s*(\d+)\s*%', thc_text)
        if thc_m2:
            allbud_thc = int(thc_m2.group(1))

    if thc_percent > 0 and allbud_thc > 0 and abs(thc_percent - allbud_thc) > 15:
        print(f"    [allbud-skip] {strain_name}: THC mismatch "
              f"(ours={thc_percent}%, AllBud={allbud_thc}%) — skipping")
        return None

    # --- Extract structured data via link URL patterns ---
    data = {
        "helpsWith": [], "effects": [], "flavours": [],
        "negatives": [], "genetics": "",
    }

    # helpsWith — links to /marijuana-strains/symptom/{slug}
    seen_helps = set()
    for link in soup.find_all('a', href=re.compile(r'/marijuana-strains/symptom/')):
        raw_tag = link.get_text(strip=True)
        if not raw_tag:
            continue
        normalised = ALLBUD_TAG_MAP.get(raw_tag.lower())
        if raw_tag.lower() in ALLBUD_REJECT_TAGS:
            continue
        if normalised is None:
            # Unknown tag — keep it Title Case and log
            normalised = raw_tag.title()
            if not hasattr(scrape_allbud, '_unknown_tags'):
                scrape_allbud._unknown_tags = set()
            if raw_tag.lower() not in scrape_allbud._unknown_tags:
                scrape_allbud._unknown_tags.add(raw_tag.lower())
                print(f"    [allbud-unknown-tag] '{raw_tag}' — keeping as '{normalised}'")
        if normalised not in seen_helps:
            seen_helps.add(normalised)
            data["helpsWith"].append(normalised)

    # effects — links to /marijuana-strains/effect/{slug}
    seen_effects = set()
    for link in soup.find_all('a', href=re.compile(r'/marijuana-strains/effect/')):
        eff = link.get_text(strip=True)
        if eff and eff not in seen_effects:
            seen_effects.add(eff)
            data["effects"].append(eff)

    # flavours — links to /marijuana-strains/taste/{slug}
    seen_flavours = set()
    for link in soup.find_all('a', href=re.compile(r'/marijuana-strains/taste/')):
        flav = link.get_text(strip=True)
        if flav and flav not in seen_flavours:
            seen_flavours.add(flav)
            data["flavours"].append(flav)

    # negatives — not structured on AllBud, skip for now
    # (could parse the description text but low value)

    # genetics — extract from description text
    # Pattern: "crossing the [adjective] X X Y strains"
    desc_text = ""
    desc_el = soup.find('div', class_=re.compile(r'strain.*description|description', re.I))
    if not desc_el:
        # Fallback: grab the first big paragraph
        for p in soup.find_all('p'):
            if len(p.get_text(strip=True)) > 100:
                desc_text = p.get_text(strip=True)
                break
    else:
        desc_text = desc_el.get_text(strip=True)

    if not desc_text:
        desc_text = soup.get_text()[:2000]

    # Try to extract parent strain names from hyperlinks in the description
    if not data["genetics"]:
        # Look for strain links in the first content section
        content_area = soup.find('div', class_=re.compile(r'strain'))
        if content_area:
            parent_links = []
            for link in content_area.find_all('a', href=re.compile(r'/marijuana-strains/\w+/')):
                href = link.get('href', '')
                # Skip non-strain links (symptom, effect, taste, aroma, search pages)
                if any(x in href for x in ['/symptom/', '/effect/', '/taste/',
                                            '/aroma/', '/search', '/variety/']):
                    continue
                parent_name = link.get_text(strip=True)
                if parent_name and len(parent_name) >= 2 and parent_name not in parent_links:
                    parent_links.append(parent_name)
            if len(parent_links) >= 2:
                data["genetics"] = f"{parent_links[0]} × {parent_links[1]}"

    # Regex fallback for genetics from description text
    if not data["genetics"]:
        gen_m = re.search(
            r'cross(?:ing)?(?:\s+(?:of|the|the\s+powerful|the\s+infamous|the\s+potent|the\s+delicious))?\s+'
            r'([A-Z][A-Za-z0-9\s\'\-\&#]+?)\s*(?:×|X|x|and|&)\s*'
            r'([A-Z][A-Za-z0-9\s\'\-\&#]+?)\s*(?:strains?|$|\.)',
            desc_text
        )
        if gen_m:
            p1 = gen_m.group(1).strip().rstrip(',')
            p2 = gen_m.group(2).strip().rstrip(',')
            if len(p1) >= 2 and len(p2) >= 2:
                data["genetics"] = f"{p1} × {p2}"

    has_content = (data["helpsWith"] or data["effects"] or
                   data["flavours"] or data["genetics"])
    return data if has_content else None


# ---------------------------------------------------------------------------
# Step 4: YouTube reviews from central MedBud page
# ---------------------------------------------------------------------------
async def scrape_youtube_reviews(browser):
    """Scrape medbud.wiki/reviews/youtube/ — returns a flat list of review objects."""
    print("\n\u25b6 Scraping YouTube reviews from central page...")
    reviews = []

    try:
        context = await browser.new_context(user_agent=HTTP_HEADERS["User-Agent"])
        page = await context.new_page()
        await page.goto(YOUTUBE_REVIEWS_URL, timeout=30000)
        await page.wait_for_timeout(3000)

        # Scroll to load all rows
        for _ in range(10):
            await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
            await page.wait_for_timeout(800)

        # Extract table rows — use innerText to preserve line breaks
        data = await page.evaluate("""() => {
            const results = [];
            const rows = document.querySelectorAll('table tbody tr, table tr');
            rows.forEach(row => {
                const cells = row.querySelectorAll('td');
                if (cells.length < 4) return;
                const ytLink = row.querySelector('a[href*="youtube.com"], a[href*="youtu.be"]');
                if (!ytLink) return;

                // Get all cell texts
                const texts = Array.from(cells).map(c => c.innerText.trim());

                let medText = '';
                let videoTitle = '';
                let channelName = texts[0] || '';
                let published = '';

                // Video title is usually the cell containing the YouTube link
                const linkCell = ytLink.closest('td');
                if (linkCell) {
                    videoTitle = linkCell.innerText.trim();
                }

                // Medication is the cell with CODE T## pattern
                for (let i = 0; i < cells.length; i++) {
                    const t = texts[i] || '';
                    if (t.match(/[A-Z]{2,5}\\s+(Smalls?\\s+)?T\\d+/) || t.match(/T\\d+\\s+[A-Z]/)) {
                        medText = t;
                        break;
                    }
                }
                // Fallback: use cells[3] if it has content
                if (!medText && texts[3]) {
                    medText = texts[3];
                }

                if (!videoTitle) videoTitle = texts[2] || '';

                // Published date — usually last cell
                const lastText = texts[texts.length - 1] || '';
                if (lastText.match(/today|yesterday|ago|\\d{1,2}[\\s/.-]/i) || lastText.match(/^\\d{4}/)) {
                    published = lastText;
                }

                results.push({
                    url: ytLink.href,
                    title: videoTitle.substring(0, 120),
                    channel: channelName.substring(0, 60),
                    medication: medText,
                    published: published,
                });
            });
            return results;
        }""")

        print(f"  Found {len(data)} YouTube review entries")

        # Debug: print first 3 medication fields
        for i, item in enumerate(data[:3]):
            print(f"  [debug] Row {i} medication: {repr(item.get('medication', ''))}")

        seen_ids = set()
        for item in data:
            url = item.get('url', '')
            vid_m = re.search(r'(?:v=|youtu\.be/|embed/)([\w-]{11})', url)
            if not vid_m:
                continue
            vid_id = vid_m.group(1)
            if vid_id in seen_ids:
                continue
            seen_ids.add(vid_id)
            clean_url = f"https://www.youtube.com/watch?v={vid_id}"

            med = item.get('medication', '')
            # Parse producer from first line
            lines = [l.strip() for l in med.split('\n') if l.strip()]
            producer = lines[0] if len(lines) >= 2 else ''
            detail = lines[1] if len(lines) >= 2 else med

            # Extract readable medication info (code + name)
            medication_display = detail.strip()

            reviews.append({
                "url": clean_url,
                "videoId": vid_id,
                "title": item.get('title', ''),
                "channel": item.get('channel', ''),
                "producer": producer,
                "medication": medication_display,
                "published": item.get('published', ''),
            })

        await context.close()
        print(f"  \u2713 {len(reviews)} unique reviews extracted")

    except Exception as e:
        print(f"  \u2717 Failed to scrape YouTube reviews: {e}")

    return reviews


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
        print("  \u2717 Could not find STRAINS_JSON in index.html")
        return False
    bracket_start = html.find('[', start)
    depth = 0
    end = bracket_start
    for i in range(bracket_start, len(html)):
        if html[i] == '[':
            depth += 1
        elif html[i] == ']':
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end < len(html) and html[end] == ';':
        end += 1
    html = html[:start] + f'const STRAINS_JSON = {js_json};' + html[end:]
    with open(html_path, 'w') as f:
        f.write(html)
    print(f"  \u2713 Updated index.html with {len(strains)} strains")
    return True


def update_reviews_html(html_path, reviews):
    """Inject REVIEWS_JSON into index.html using bracket-depth matching."""
    with open(html_path) as f:
        html = f.read()
    js_json = json.dumps(reviews, ensure_ascii=True, separators=(',', ':'))
    js_json = js_json.replace("'", "\\u0027")
    marker = 'const REVIEWS_JSON = '
    start = html.find(marker)
    if start < 0:
        print("  \u2717 Could not find REVIEWS_JSON in index.html")
        return False
    bracket_start = html.find('[', start)
    depth = 0
    end = bracket_start
    for i in range(bracket_start, len(html)):
        if html[i] == '[':
            depth += 1
        elif html[i] == ']':
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end < len(html) and html[end] == ';':
        end += 1
    html = html[:start] + f'const REVIEWS_JSON = {js_json};' + html[end:]
    with open(html_path, 'w') as f:
        f.write(html)
    print(f"  \u2713 Updated index.html with {len(reviews)} reviews")
    return True


def load_existing(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def clean_existing_data(strains):
    """Fix bad strain names and remove duplicates from previous broken scrapes."""
    cleaned = 0
    removed = 0

    # 0. Backfill form="Flower" on any record missing it (legacy records pre-cart support)
    form_backfilled = 0
    for s in strains:
        if "form" not in s or not s.get("form"):
            s["form"] = "Flower"
            form_backfilled += 1
        # Ensure cart fields exist (as empty defaults) so downstream code is safe
        if s["form"] == "Cartridge":
            s.setdefault("thcMg", 0)
            s.setdefault("cbdMg", 0)
            s.setdefault("volume", "")
            s.setdefault("extractType", "")
            s.setdefault("terpeneSource", "")
            s.setdefault("fitment", "")

    # 1. Fix strain names that have page artefacts
    bad_patterns = [
        r'\s*Classification\s.*$',
        r'\s*Chemotype\s.*$',
        r'\s*Type I+\s*$',
        r'\s*Medication Overview.*$',
        r'\s*Indica Hybrid.*$',
        r'\s*Sativa Hybrid.*$',
        r'\s*(?:Indica|Sativa|Hybrid)\s+Chemotype.*$',
        r'\s+(?:Indica|Sativa|Hybrid)$',
    ]
    # Code-like prefixes from URL slugs (e.g. "Hb T24 Hash Burger" → "Hash Burger")
    code_prefix_patterns = [
        r'^[A-Z][a-z]?[a-z]?\s+T\d+\s+',      # "Hb T24 ", "Cf T25 ", "Mfl T25 "
        r'^T\d+:C\d+\s+',                        # "T600:C200 Jack Herer" (MedBud balanced cart)
        r'^T\d+\s+C\d+\s+',                      # "T10 C13 Moon Berry"
        r'^T\d+\s+',                              # "T14 Banana Split"
        r'^[A-Z]{2,5}\s+T\d+\s+',                # "GCR T27 Gas Cream Cake"
        r'^Origins\s+[A-Z]{2,5}\s+T\d+:?C?\d*\s+',   # "Origins GS T800:C30 Grape Soda"
        r'^Cura\s+\d+\s+T\d+\s+',                # "Cura 13 T22 Gelato Og"
        r'^Emt?\d?\s+T?\d+\s+(?:\d+\s+)?',       # "Emt1 T19 20 Cairo"
        r'^Flos\s+T\d+C?\d*\s*',                  # "Flos T21C0"
        r'^Emc\s+\d+\s+C\d+\s*',                  # "Emc 1 C13"
    ]
    for s in strains:
        original = s.get("name", "")
        fixed = original

        # Strip page artefacts
        for pat in bad_patterns:
            fixed = re.sub(pat, '', fixed, flags=re.IGNORECASE).strip()

        # Strip code prefixes (only if there's a real name after)
        for pat in code_prefix_patterns:
            m = re.match(pat, fixed)
            if m and len(fixed[m.end():].strip()) >= 3:
                fixed = fixed[m.end():].strip()
                break

        if fixed != original and len(fixed) >= 2:
            s["name"] = fixed
            cleaned += 1

        # Fix CBD values that are clearly wrong
        if s.get("cbd", 0) > 0 and s.get("cbd", 0) == s.get("thc", 0):
            s["cbd"] = 0
        if s.get("cbd", 0) > 15 and s.get("thc", 0) > 15:
            s["cbd"] = 0

        # Strip youtubeReviews (now stored separately in reviews.json)
        if "youtubeReviews" in s:
            del s["youtubeReviews"]

        # Clean genetics — strip leaked descriptive text
        g = s.get("genetics", "")
        if g:
            g = re.sub(r'\s+(?:strain|strains|if|is|are|the|a|an|this|which|that|with|from)s?\b.*$', '', g, flags=re.IGNORECASE).strip()
            if '.' in g:
                g = g[:g.index('.')].strip()
            if len(g) > 65:
                g = g[:65].rsplit(' ', 1)[0]
            if '×' in g:
                parts = g.split('×')
                if len(parts) == 2 and (len(parts[0].strip()) < 2 or len(parts[1].strip()) < 2):
                    g = ''
            s["genetics"] = g

    # 1b. Drop junk records.
    # Two categories:
    #  - Code-pattern names ("T17 19", "WPT", "T194:C194"): may have been
    #    assigned valid metadata elsewhere on the page, so only drop if
    #    the record has no descriptive data worth keeping.
    #  - Broken-name patterns (starting with a lowercase stopword like "or"):
    #    the name is fundamentally unsalvageable even if terpenes got
    #    extracted from elsewhere. Drop unconditionally.
    code_pattern_names = [
        re.compile(r'^T\d+(\s+\d+)?$'),          # "T17 19", "T19", "T840"
        re.compile(r'^T\d+:C\d+$'),              # "T194:C194"
        re.compile(r'^[A-Z]{2,5}$'),             # "WPT", "JHR", "BSK", "AC"
    ]
    # Names beginning with an English stopword — always a truncated scrape
    # ("or All Vape" from 4C Labs OAV was real product that's been discontinued
    # and its lingering record has a corrupt name).
    broken_name_pattern = re.compile(
        r'^(?:or|and|the|of|for|with|in|on|at|by|to|a|an)\s+',
        re.IGNORECASE
    )

    def _has_descriptive_data(s):
        return (
            len(s.get("terpenes", [])) > 0 or
            len(s.get("effects", [])) > 0 or
            len(s.get("flavours", [])) > 0 or
            len(s.get("helpsWith", [])) > 0 or
            len(s.get("negatives", [])) > 0 or
            bool(s.get("genetics", ""))
        )

    junk_dropped = 0
    filtered = []
    for s in strains:
        name = s.get("name", "")
        is_code_pattern = any(p.match(name) for p in code_pattern_names)
        is_broken = bool(broken_name_pattern.match(name))
        # Broken names: drop unconditionally (name is garbage either way).
        # Code-pattern names: drop only if record has nothing else of value.
        if is_broken or (is_code_pattern and not _has_descriptive_data(s)):
            junk_dropped += 1
            continue
        filtered.append(s)
    strains = filtered

    # 2. Remove duplicates (keep the one with more data).
    # Key includes form so a flower and cartridge of the same name+producer
    # are treated as distinct records. For cartridges we ALSO key on
    # thcMg/cbdMg — two carts with the same name but different potency
    # ratios (e.g. Jack Herer T600:C200 vs T200:C200) are genuinely
    # different products and must not be merged.
    seen = {}
    deduped = []
    for s in strains:
        name_l = s["name"].lower()
        prod_l = s["producer"].lower()
        form_l = s.get("form", "Flower").lower()
        if form_l == "cartridge":
            key = (name_l, prod_l, form_l, s.get("thcMg", 0), s.get("cbdMg", 0))
        else:
            key = (name_l, prod_l, form_l)
        if key in seen:
            existing = seen[key]
            ex_score = len(existing.get("terpenes", [])) + len(existing.get("effects", []))
            new_score = len(s.get("terpenes", [])) + len(s.get("effects", []))
            if new_score > ex_score:
                idx = deduped.index(existing)
                deduped[idx] = s
                seen[key] = s
            removed += 1
        else:
            seen[key] = s
            deduped.append(s)

    # 2b. Acronym-to-full-name merge.
    # Catches MedBud's habit of listing the same cart under both its
    # acronym URL and its full-name URL (e.g. WPT + Wedding Pop Triangle
    # for Curaleaf). If we find an acronym-named record and a full-name
    # record with the same producer/form/thcMg/cbdMg, and the acronym
    # matches the full name's initials, merge them into the full-name record.
    acronym_merged = 0
    by_product_key = {}  # (producer, form, thcMg, cbdMg) -> list of records
    for s in deduped:
        form_l = s.get("form", "Flower").lower()
        if form_l != "cartridge":
            continue
        pk = (s["producer"].lower(), form_l, s.get("thcMg", 0), s.get("cbdMg", 0))
        by_product_key.setdefault(pk, []).append(s)

    to_drop = set()
    for pk, records in by_product_key.items():
        if len(records) < 2:
            continue
        # Separate acronym-named from full-named records in this group
        acronyms = [r for r in records if re.match(r'^[A-Z]{2,5}$', r.get("name", ""))]
        fulls = [r for r in records if not re.match(r'^[A-Z]{2,5}$', r.get("name", ""))]
        for ac in acronyms:
            ac_name = ac["name"]
            ac_initials = ac_name.upper()
            # Find a full-name record whose initials match this acronym
            match = None
            for f in fulls:
                full_words = re.findall(r'\b\w', f["name"])
                full_initials = ''.join(w.upper() for w in full_words)
                if full_initials == ac_initials:
                    match = f
                    break
            if match:
                # Merge acronym record's data into the full-name record
                if ac.get("terpenes") and len(ac["terpenes"]) > len(match.get("terpenes", [])):
                    match["terpenes"] = ac["terpenes"]
                for field in ["effects", "flavours", "helpsWith", "negatives", "genetics",
                              "volume", "extractType", "terpeneSource", "fitment"]:
                    if ac.get(field) and not match.get(field):
                        match[field] = ac[field]
                to_drop.add(id(ac))
                acronym_merged += 1

    if to_drop:
        deduped = [r for r in deduped if id(r) not in to_drop]

    # 3. Fix invalid/duplicate codes
    code_fixed = 0
    used_codes = set()
    for s in deduped:
        code = s.get("code", "")
        if is_valid_code(code) and code not in used_codes:
            used_codes.add(code)
        else:
            new_code = make_code(s["name"], used_codes)
            s["code"] = new_code
            s["id"] = new_code
            code_fixed += 1
    # Ensure id always matches code
    for s in deduped:
        s["id"] = s["code"]

    if cleaned or removed or code_fixed or form_backfilled or junk_dropped or acronym_merged:
        print(f"  \U0001f9f9 Cleaned {cleaned} names, removed {removed} duplicates, "
              f"fixed {code_fixed} codes, backfilled form on {form_backfilled}, "
              f"dropped {junk_dropped} junk records, merged {acronym_merged} acronym dupes")

    return deduped


# ---------------------------------------------------------------------------
# Debug mode
# ---------------------------------------------------------------------------
async def debug_page(url):
    is_cart = '/vape-cartridges/' in url
    print(f"\U0001f50d Debug mode ({'CARTRIDGE' if is_cart else 'FLOWER'}): {url}\n")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=HTTP_HEADERS["User-Agent"])
        page = await context.new_page()

        await page.goto(url, timeout=30000)
        try:
            await page.wait_for_selector('text=Cultivar/Strain', timeout=8000)
        except Exception:
            print("  (Cultivar/Strain selector not found)")
        await page.wait_for_timeout(2000)

        page_text = await page.inner_text('body')
        page_html = await page.content()

        with open('debug_text.txt', 'w') as f:
            f.write(page_text)
        with open('debug_html.html', 'w') as f:
            f.write(page_html)

        print("=" * 60)
        print("FIRST 3000 CHARS OF inner_text:")
        print("=" * 60)
        print(page_text[:3000])

        print("\n" + "=" * 60)
        print("STRAIN NAME REGEX MATCHES:")
        print("=" * 60)
        m = re.search(
            r'Cultivar/Strain\s*\u00b7?\s*'
            r'([A-Za-z][\w\s\'\-\&\.\,\u00e9\u00e8\u00fc#]+?)'
            r'\s*(?:Classification|Chemotype|Flower|THC|Indica|Sativa|Hybrid|Medication)',
            page_text
        )
        if m:
            print(f"  Matched: '{m.group(1)}'")
            print(f"  Cleaned: '{clean_strain_name(m.group(1))}'")
        else:
            print("  No match found!")

        print("\n" + "=" * 60)
        print("TERPENE SECTION:")
        print("=" * 60)
        terp_idx = page_text.find("Terpene Profile")
        if terp_idx >= 0:
            print(page_text[terp_idx:terp_idx+500])
        else:
            terp_idx2 = page_text.lower().find("terpene")
            if terp_idx2 >= 0:
                print(f"  Found at pos {terp_idx2}:")
                print(page_text[terp_idx2:terp_idx2+500])
            else:
                print("  Not found in page text!")

        print("\n" + "=" * 60)
        print("TERPENE TABLE (structural parse):")
        print("=" * 60)
        terp_details = await parse_terpene_table(page)
        if terp_details:
            for td in terp_details:
                print(f"  {td['name']:20s}  {td['designation'] or '(no designation)'}")
        else:
            print("  No terpene table rows found via DOM selectors")

        if is_cart:
            print("\n" + "=" * 60)
            print("CART PAGE TITLE / H1 (used as name fallbacks):")
            print("=" * 60)
            try:
                t = await page.title()
                print(f"  <title>: {t!r}")
            except Exception as e:
                print(f"  <title>: ERROR {e}")
            try:
                h1 = await page.query_selector('h1')
                if h1:
                    print(f"  <h1>:    {(await h1.inner_text())!r}")
                else:
                    print("  <h1>:    (no h1 element found)")
            except Exception as e:
                print(f"  <h1>:    ERROR {e}")

            print("\n" + "=" * 60)
            print("DESIGNATION FIELD + PARSE RESULT:")
            print("=" * 60)
            des_m = re.search(r'Designation\s+([A-Z][^\n]+)', page_text)
            if des_m:
                raw = des_m.group(1).strip()
                name, code = parse_cart_designation(raw)
                print(f"  raw designation: {raw!r}")
                print(f"  parsed name:     {name!r}")
                print(f"  parsed code:     {code!r}")
            else:
                print("  (Designation field not found)")

            print("\n" + "=" * 60)
            print("CARTRIDGE-SPECIFIC FIELDS:")
            print("=" * 60)
            for label, pat in [
                ("THC mg", r'THC[^\d]{0,30}\d{2,4}\s*mg|\d{2,4}\s*mg[^\n\r\d]{0,20}THC'),
                ("CBD mg (pat1)", r'CBD[^\d]{0,30}\d{1,4}\s*mg'),
                ("CBD mg (pat2)", r'\d{1,4}\s*mg[^\n\r\d]{0,20}CBD'),
                ("Volume", r'\d+(?:\.\d+)?\s*ml\b'),
                ("Extract",
                 r'\b(Distillate|Live Rosin|Hash Rosin|Live Resin|Full Spectrum|Broad Spectrum|Solventless|CO2 Extract)\b'),
                ("Terp source",
                 r'\b(Botanical Terpenes?|Cannabis[\s-]Derived Terpenes?|Strain[\s-]Specific Terpenes?|No Additives?)\b'),
                ("Fitment", r'\b(510|Kanabo|Pax[\s-]?Era|Proprietary)\s*(?:Fitment|Thread|Threaded)?'),
                ("Type (cart)",
                 r'\b(Indica|Sativa|Hybrid)\s*[\u2022\u00b7]\s*(?:<?\d+\s*mg|\d+\s*%)'),
            ]:
                found = re.findall(pat, page_text, re.IGNORECASE)
                print(f"  {label}: {found[:3] if found else 'not found'}")

        print("\n" + "=" * 60)
        print("SCRAPE RESULT:")
        print("=" * 60)
        if is_cart:
            data = await scrape_cart_page_pw(page, url, "DEBUG")
        else:
            data = await scrape_strain_page_pw(page, url, "DEBUG")
        if data:
            for k, v in data.items():
                print(f"  {k}: {v}")
        else:
            print("  FAILED - returned None")

        print(f"\n\U0001f4c4 Full dumps saved to debug_text.txt and debug_html.html")
        await browser.close()


# ---------------------------------------------------------------------------
# Re-enrichment mode
# ---------------------------------------------------------------------------
# Reverse-lookup: producer display name → slug (for URL reconstruction)
PRODUCER_SLUG_BY_NAME = {v: k for k, v in {**PRODUCERS, **CART_PRODUCERS}.items()}


async def reenrich(strains_path):
    """
    Re-scrape existing strains that have schema_version < 2 to backfill
    terpeneDetails and any other Phase-1+ fields.
    Phase 2+ will add AllBud/CannaConnection/Weedstrain cascade here.
    """
    print("=" * 60)
    print("Bloomy\u0027s Bud Log \u2014 Re-enrichment Mode")
    print("=" * 60)

    strains = load_existing(strains_path)
    strains = clean_existing_data(strains)
    total = len(strains)

    needs_update = [s for s in strains if s.get("schema_version", 0) < 2]
    print(f"\n  {len(needs_update)} of {total} records need re-enrichment (schema_version < 2)")

    if not needs_update:
        print("  Nothing to do!")
        return 0

    # Build URL for each record that needs updating
    to_scrape = []  # list of (strain_record, url, form)
    skipped = 0
    for s in needs_update:
        producer_name = s.get("producer", "")
        slug = PRODUCER_SLUG_BY_NAME.get(producer_name, "")
        if not slug:
            skipped += 1
            continue
        form = s.get("form", "Flower")
        strain_slug = slugify(s["name"])
        if not strain_slug:
            skipped += 1
            continue
        if form == "Cartridge":
            url = f"{CART_BASE_URL}/{slug}/{strain_slug}/"
        else:
            url = f"{MEDBUD_BASE}/strains/{slug}/{strain_slug}/"
        to_scrape.append((s, url, form))

    if skipped:
        print(f"  Skipped {skipped} records (unknown producer slug or bad name)")
    print(f"  Queued {len(to_scrape)} records for MedBud re-scrape\n")

    if not to_scrape:
        return 0

    # Re-scrape with Playwright
    CONCURRENCY = 4
    progress = {"done": 0, "updated": 0}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)

        async def reenrich_one(browser, strain_record, url, form):
            context = await browser.new_context(user_agent=HTTP_HEADERS["User-Agent"])
            pg = await context.new_page()
            try:
                if form == "Cartridge":
                    data = await scrape_cart_page_pw(pg, url, strain_record["producer"])
                else:
                    data = await scrape_strain_page_pw(pg, url, strain_record["producer"])
            except Exception:
                data = None
            finally:
                await context.close()

            progress["done"] += 1
            if progress["done"] % 25 == 0:
                print(f"    ... re-enriched {progress['done']}/{len(to_scrape)}")

            if not data:
                # MedBud page not found — still bump schema_version so we don't
                # retry endlessly. Phase 2 AllBud will fill the gaps.
                strain_record["schema_version"] = 2
                strain_record.setdefault("terpeneDetails", [])
                return

            # Update terpeneDetails (always overwrite — structural is better)
            if data.get("terpeneDetails"):
                strain_record["terpeneDetails"] = data["terpeneDetails"]
            else:
                strain_record.setdefault("terpeneDetails", [])

            # Update terpenes if new data has more
            if data.get("terpenes") and len(data["terpenes"]) > len(strain_record.get("terpenes", [])):
                strain_record["terpenes"] = data["terpenes"]

            # Fill empty fields
            for field in ["effects", "flavours", "helpsWith", "negatives", "genetics"]:
                if data.get(field) and not strain_record.get(field):
                    strain_record[field] = data[field]

            strain_record["schema_version"] = 2
            progress["updated"] += 1

        queue = asyncio.Queue()
        for item in to_scrape:
            queue.put_nowait(item)

        async def reenrich_worker(browser, queue):
            while not queue.empty():
                try:
                    strain_record, url, form = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                await reenrich_one(browser, strain_record, url, form)
                queue.task_done()

        workers = [asyncio.create_task(reenrich_worker(browser, queue))
                   for _ in range(CONCURRENCY)]
        await asyncio.gather(*workers)

        # Weedstrain fallback for flowers still missing terpenes
        flowers_no_terps = [s for s in needs_update
                            if s.get("form", "Flower") == "Flower"
                            and not s.get("terpenes")]
        if flowers_no_terps:
            print(f"\n\U0001f52c Weedstrain fallback for {len(flowers_no_terps)} strains still missing terpenes...")
            for s in flowers_no_terps:
                ws = scrape_weedstrain(s["name"])
                if ws:
                    for k in ["terpenes", "effects", "flavours", "helpsWith", "negatives", "genetics"]:
                        if not s.get(k) and ws.get(k):
                            s[k] = ws[k]
                    if ws.get("terpenes"):
                        print(f"  \u2713 {s['name']}: {', '.join(ws['terpenes'])}")
                time.sleep(0.5)

        await browser.close()

    # AllBud enrichment — runs on all flowers (MedBud has no helpsWith data)
    flowers_for_allbud = [s for s in needs_update
                          if s.get("form", "Flower") == "Flower"]
    if flowers_for_allbud:
        print(f"\n\U0001f4da AllBud enrichment for {len(flowers_for_allbud)} flowers...")
        allbud_hits = 0
        for s in flowers_for_allbud:
            ab = scrape_allbud(s["name"], s.get("thc", 0), s.get("type", "Hybrid"))
            if ab:
                # Union merge for helpsWith
                existing_helps = set(s.get("helpsWith", []))
                for tag in ab.get("helpsWith", []):
                    if tag not in existing_helps:
                        existing_helps.add(tag)
                        s.setdefault("helpsWith", []).append(tag)
                # Fill-if-empty for other fields
                for field in ["effects", "flavours", "negatives", "genetics"]:
                    if ab.get(field) and not s.get(field):
                        s[field] = ab[field]
                allbud_hits += 1
                helps_str = ', '.join(s.get('helpsWith', [])[:5])
                print(f"  \u2713 {s['name']}: {helps_str}")
            else:
                print(f"  \u2717 {s['name']}")
            time.sleep(1)  # Rate limit: 1 req/sec
        print(f"  AllBud: {allbud_hits}/{len(flowers_for_allbud)} matched")

    # Save
    print(f"\n  Re-enrichment complete: {progress['updated']} records updated via MedBud")
    strains = clean_existing_data(strains)
    print(f"\n\U0001f4be Saving {len(strains)} strains...")
    with open(strains_path, 'w') as f:
        json.dump(strains, f, indent=2, ensure_ascii=False)

    # Stats
    v2_count = sum(1 for s in strains if s.get("schema_version", 0) >= 2)
    td_count = sum(1 for s in strains if s.get("terpeneDetails"))
    hw_count = sum(1 for s in strains if s.get("helpsWith"))
    hw_zero = sum(1 for s in strains if not s.get("helpsWith"))
    all_tags = [tag for s in strains for tag in s.get("helpsWith", [])]
    unique_tags = len(set(all_tags))
    print(f"  schema_version >= 2: {v2_count}/{len(strains)}")
    print(f"  terpeneDetails populated: {td_count}/{len(strains)}")
    print(f"  helpsWith populated: {hw_count}/{len(strains)} ({hw_zero} still empty)")
    print(f"  unique helpsWith tags: {unique_tags}")
    print(f"\n{'='*60}")
    print(f"\u2705 Re-enrichment done!")
    print(f"{'='*60}")
    return 0


def allbud_backfill(strains_path):
    """
    Run AllBud enrichment on all flowers with fewer than 3 helpsWith tags.
    No Playwright needed — AllBud is plain HTTP. No schema_version check.
    """
    print("=" * 60)
    print("Bloomy\u0027s Bud Log \u2014 AllBud Backfill")
    print("=" * 60)

    strains = load_existing(strains_path)
    strains = clean_existing_data(strains)

    flowers = [s for s in strains
               if s.get("form", "Flower") == "Flower"
               and len(s.get("helpsWith", [])) < 3]
    print(f"\n  {len(flowers)} flowers with < 3 helpsWith tags")

    if not flowers:
        print("  Nothing to do!")
        return 0

    allbud_hits = 0
    for i, s in enumerate(flowers):
        ab = scrape_allbud(s["name"], s.get("thc", 0), s.get("type", "Hybrid"))
        if ab:
            existing_helps = set(s.get("helpsWith", []))
            for tag in ab.get("helpsWith", []):
                if tag not in existing_helps:
                    existing_helps.add(tag)
                    s.setdefault("helpsWith", []).append(tag)
            for field in ["effects", "flavours", "negatives", "genetics"]:
                if ab.get(field) and not s.get(field):
                    s[field] = ab[field]
            allbud_hits += 1
            helps_str = ', '.join(s.get('helpsWith', [])[:5])
            print(f"  \u2713 {s['name']}: {helps_str}")
        else:
            print(f"  \u2717 {s['name']}")
        time.sleep(1)

        if (i + 1) % 50 == 0:
            print(f"    ... processed {i + 1}/{len(flowers)}")

    print(f"\n  AllBud: {allbud_hits}/{len(flowers)} matched")

    # Save
    strains = clean_existing_data(strains)
    print(f"\n\U0001f4be Saving {len(strains)} strains...")
    with open(strains_path, 'w') as f:
        json.dump(strains, f, indent=2, ensure_ascii=False)

    # Stats
    hw_count = sum(1 for s in strains if s.get("helpsWith"))
    hw_zero = sum(1 for s in strains if not s.get("helpsWith"))
    all_tags = [tag for s in strains for tag in s.get("helpsWith", [])]
    unique_tags = len(set(all_tags))
    print(f"  helpsWith populated: {hw_count}/{len(strains)} ({hw_zero} still empty)")
    print(f"  unique helpsWith tags: {unique_tags}")
    print(f"\n{'='*60}")
    print(f"\u2705 AllBud backfill done!")
    print(f"{'='*60}")
    return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    if len(sys.argv) >= 3 and sys.argv[1] == '--debug':
        await debug_page(sys.argv[2])
        return 0

    if '--reenrich' in sys.argv:
        repo_root = Path(__file__).parent.parent
        strains_path = repo_root / "strains.json"
        return await reenrich(strains_path)

    if '--allbud' in sys.argv:
        repo_root = Path(__file__).parent.parent
        strains_path = repo_root / "strains.json"
        return allbud_backfill(strains_path)

    repo_root = Path(__file__).parent.parent
    strains_path = repo_root / "strains.json"
    html_path = repo_root / "index.html"

    print("=" * 60)
    print("Bloomy\u0027s Bud Log \u2014 Strain Data Updater")
    print("=" * 60)

    # 1. Load existing
    print("\n\U0001f4c2 Loading existing strains...")
    existing = load_existing(strains_path)
    existing = clean_existing_data(existing)
    existing_by_key = {}
    existing_codes = set()
    for s in existing:
        # Key includes form so flower and cart with same name/producer are distinct
        key = (s["name"].lower(), s["producer"].lower(), s.get("form", "Flower").lower())
        existing_by_key[key] = s
        existing_codes.add(s.get("code", s.get("id", "")))
    flower_count = sum(1 for s in existing if s.get("form", "Flower") == "Flower")
    cart_count = sum(1 for s in existing if s.get("form") == "Cartridge")
    print(f"  Loaded {len(existing)} existing records ({flower_count} flower, {cart_count} cartridges)")

    # 2. Discover strain page URLs via HTTP
    # all_urls maps: url -> (producer_name, form)  where form is "Flower" or "Cartridge"
    print("\n\U0001f50d Discovering strain pages (HTTP)...")
    all_urls = {}
    for slug, producer in PRODUCERS.items():
        urls = discover_strain_urls(slug)
        if urls:
            print(f"  \U0001f4e6 {producer}: {len(urls)} flower pages")
            for u in urls:
                all_urls[u] = (producer, "Flower")
        else:
            print(f"  \U0001f4e6 {producer}: \u2717 not found")
        time.sleep(0.5)

    print("\n\U0001f50d Discovering cartridge pages (HTTP)...")
    cart_found = 0
    for slug, producer in CART_PRODUCERS.items():
        urls = discover_cart_urls(slug)
        if urls:
            print(f"  \U0001f50b {producer}: {len(urls)} cartridge pages")
            for u in urls:
                all_urls[u] = (producer, "Cartridge")
            cart_found += len(urls)
        # Don't log "not found" for carts — many producers won't have carts
        time.sleep(0.5)
    if cart_found == 0:
        print("  (no cartridge pages discovered)")

    flower_urls = sum(1 for v in all_urls.values() if v[1] == "Flower")
    cart_urls = sum(1 for v in all_urls.values() if v[1] == "Cartridge")
    print(f"\n  Total pages to scrape: {len(all_urls)} ({flower_urls} flower, {cart_urls} cartridges)")

    if not all_urls:
        print("  \u26a0 No pages discovered \u2014 keeping existing data")
        return 0

    # 3. Scrape with Playwright (parallel)
    print("\n\U0001f310 Scraping strain pages with headless browser (4 parallel)...")
    new_strains = []
    seen_new = set()
    results_lock = asyncio.Lock()
    progress = {"done": 0, "total": len(all_urls)}
    CONCURRENCY = 4

    async def scrape_one(browser, surl, producer, form):
        context = await browser.new_context(user_agent=HTTP_HEADERS["User-Agent"])
        pg = await context.new_page()
        try:
            if form == "Cartridge":
                data = await scrape_cart_page_pw(pg, surl, producer)
            else:
                data = await scrape_strain_page_pw(pg, surl, producer)
        except Exception:
            data = None
        finally:
            await context.close()
        async with results_lock:
            progress["done"] += 1
            if progress["done"] % 50 == 0:
                print(f"    ... processed {progress['done']}/{progress['total']} pages")
        return data, producer, form

    async def worker(browser, queue):
        while not queue.empty():
            try:
                item = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            surl, (producer, form) = item
            data, prod, form = await scrape_one(browser, surl, producer, form)
            queue.task_done()
            if not data or not data.get("name"):
                continue
            async with results_lock:
                key = (data["name"].lower(), prod.lower(), form.lower())
                if key in existing_by_key:
                    ex = existing_by_key[key]
                    if form == "Cartridge":
                        # Backfill cart-specific numeric fields if missing
                        if data.get("thcMg", 0) > 0 and ex.get("thcMg", 0) == 0:
                            ex["thcMg"] = data["thcMg"]
                        if data.get("cbdMg", 0) > 0 and ex.get("cbdMg", 0) == 0:
                            ex["cbdMg"] = data["cbdMg"]
                        for field in ["volume", "extractType", "terpeneSource", "fitment"]:
                            if data.get(field) and not ex.get(field):
                                ex[field] = data[field]
                    else:
                        if data.get("thc", 0) > 0 and ex.get("thc", 0) == 0:
                            ex["thc"] = data["thc"]
                        if data.get("cbd", 0) > 0 and ex.get("cbd", 0) == 0:
                            ex["cbd"] = data["cbd"]
                    # Update terpenes if new data has more
                    if data.get("terpenes") and len(data["terpenes"]) > len(ex.get("terpenes", [])):
                        ex["terpenes"] = data["terpenes"]
                    # Always update terpeneDetails when available (structural > none)
                    if data.get("terpeneDetails"):
                        ex["terpeneDetails"] = data["terpeneDetails"]
                    if data.get("tier") != "Core" and ex.get("tier", "Core") == "Core":
                        ex["tier"] = data["tier"]
                    # Fill in missing fields
                    for field in ["effects", "flavours", "helpsWith", "negatives", "genetics"]:
                        if data.get(field) and not ex.get(field):
                            ex[field] = data[field]
                    # Bump schema version on re-scrape
                    ex["schema_version"] = 2
                elif key not in seen_new:
                    seen_new.add(key)
                    new_strains.append(data)
                    terp_str = ', '.join(data['terpenes'][:3]) if data.get('terpenes') else 'no terpenes yet'
                    if form == "Cartridge":
                        potency = f"THC {data.get('thcMg', 0)}mg"
                        if data.get('volume'):
                            potency += f" {data['volume']}"
                        print(f"    \U0001f50b {data['name']} ({prod}, {potency}, {terp_str})")
                    else:
                        print(f"    \u271a {data['name']} ({prod}, THC {data.get('thc', 0)}%, {terp_str})")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)

        queue = asyncio.Queue()
        for item in all_urls.items():
            queue.put_nowait(item)
        workers_list = [asyncio.create_task(worker(browser, queue)) for _ in range(CONCURRENCY)]
        await asyncio.gather(*workers_list)

        print(f"\n  New unique strains: {len(new_strains)}")

        # 4. Weedstrain fallback (for terpenes and genetics) — FLOWER ONLY
        # Weedstrain doesn't track cartridges, so skip cart records to avoid
        # mismatching them against flower strains with the same name.
        flower_new = [s for s in new_strains if s.get("form") != "Cartridge"]
        missing_terps = [s for s in flower_new if not s.get("terpenes")]
        missing_genetics = [s for s in flower_new if s.get("terpenes") and not s.get("genetics")]
        fallback_strains = missing_terps + missing_genetics
        if fallback_strains:
            print(f"\n\U0001f52c Weedstrain fallback for {len(fallback_strains)} strains ({len(missing_terps)} missing terpenes, {len(missing_genetics)} missing genetics)...")
            for s in fallback_strains:
                ws = scrape_weedstrain(s["name"])
                if ws:
                    for k in ["terpenes", "effects", "flavours", "helpsWith", "negatives", "genetics"]:
                        if not s.get(k) and ws.get(k):
                            s[k] = ws[k]
                    if ws.get("terpenes"):
                        print(f"  \u2713 {s['name']}: {', '.join(ws['terpenes'])}{' | ' + ws['genetics'] if ws.get('genetics') else ''}")
                    elif ws.get("genetics"):
                        print(f"  \u2713 {s['name']}: genetics={ws['genetics']}")
                    else:
                        print(f"  \u2717 {s['name']}")
                else:
                    print(f"  \u2717 {s['name']}")
                time.sleep(0.5)

        # 4b. AllBud enrichment (helpsWith, effects, flavours, genetics) — FLOWER ONLY
        # MedBud provides no helpsWith data; AllBud is our primary source.
        # Runs on all new flowers regardless of existing data (union merge for helpsWith).
        if flower_new:
            print(f"\n\U0001f4da AllBud enrichment for {len(flower_new)} new flowers...")
            allbud_hits = 0
            for s in flower_new:
                ab = scrape_allbud(s["name"], s.get("thc", 0), s.get("type", "Hybrid"))
                if ab:
                    # Union merge for helpsWith
                    existing_helps = set(s.get("helpsWith", []))
                    for tag in ab.get("helpsWith", []):
                        if tag not in existing_helps:
                            existing_helps.add(tag)
                            s.setdefault("helpsWith", []).append(tag)
                    # Fill-if-empty for other fields
                    for field in ["effects", "flavours", "negatives", "genetics"]:
                        if ab.get(field) and not s.get(field):
                            s[field] = ab[field]
                    allbud_hits += 1
                    helps_str = ', '.join(s.get('helpsWith', [])[:5])
                    print(f"  \u2713 {s['name']}: {helps_str}")
                else:
                    print(f"  \u2717 {s['name']}")
                time.sleep(1)  # Rate limit: 1 req/sec (polite)
            print(f"  AllBud: {allbud_hits}/{len(flower_new)} matched")

        # 5. Merge
        result = list(existing)
        for s in new_strains:
            raw_code = s.get("code", "")
            code = raw_code if is_valid_code(raw_code) and raw_code not in existing_codes else make_code(s["name"], existing_codes)
            if code not in existing_codes:
                existing_codes.add(code)
            form = s.get("form", "Flower")
            record = {
                "name": s["name"], "producer": s["producer"], "code": code,
                "form": form,
                "tier": s.get("tier", "Core"),
                "thc": s.get("thc", 0), "cbd": s.get("cbd", 0),
                "type": s.get("type", "Hybrid"),
                "terpenes": s.get("terpenes", []),
                "terpeneDetails": s.get("terpeneDetails", []),
                "effects": s.get("effects", []),
                "flavours": s.get("flavours", []), "helpsWith": s.get("helpsWith", []),
                "negatives": s.get("negatives", []), "genetics": s.get("genetics", ""),
                "schema_version": 2,
                "id": code,
            }
            if form == "Cartridge":
                record["thcMg"] = s.get("thcMg", 0)
                record["cbdMg"] = s.get("cbdMg", 0)
                record["volume"] = s.get("volume", "")
                record["extractType"] = s.get("extractType", "")
                record["terpeneSource"] = s.get("terpeneSource", "")
                record["fitment"] = s.get("fitment", "")
            result.append(record)

        # 6. YouTube reviews (separate file, not per-strain)
        yt_reviews = await scrape_youtube_reviews(browser)

        await browser.close()

    # 7. Final deduplication
    result = clean_existing_data(result)

    # 8. Save strains
    print(f"\n\U0001f4be Saving {len(result)} strains...")
    with open(strains_path, 'w') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    # 9. Save reviews
    reviews_path = strains_path.parent / "reviews.json"
    print(f"\U0001f4be Saving {len(yt_reviews)} reviews...")
    with open(reviews_path, 'w') as f:
        json.dump(yt_reviews, f, indent=2, ensure_ascii=False)

    # 10. Update HTML
    if html_path.exists():
        update_html(str(html_path), result)
        update_reviews_html(str(html_path), yt_reviews)

    added = len(result) - len(existing)
    flowers = sum(1 for s in result if s.get("form", "Flower") == "Flower")
    carts = sum(1 for s in result if s.get("form") == "Cartridge")
    print(f"\n{'='*60}")
    print(f"\u2705 Done! {len(result)} records ({'+' if added >= 0 else ''}{added} new)")
    print(f"   \U0001f33f Flower: {flowers}   \U0001f50b Cartridges: {carts}")
    print(f"{'='*60}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
