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
        try:
            await page.wait_for_selector('text=Cultivar/Strain', timeout=8000)
        except Exception:
            pass
        await page.wait_for_timeout(1500)
    except Exception:
        return None

    page_text = await page.inner_text('body')

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

    # Find genetics/parents section — MedBud typically uses "Parents"
    gen_section = ""
    for keyword in ["Parents", "Parent Strains", "Parent Strain", "Genetics",
                     "Lineage", "Parentage"]:
        gen_idx = page_text.find(keyword)
        if gen_idx < 0:
            gen_idx = page_text.lower().find(keyword.lower())
        if gen_idx >= 0:
            # Skip past the keyword itself so it doesn't get captured
            start = gen_idx + len(keyword)
            gen_section = page_text[start:start+300]
            break

    if gen_section:
        # Strip parenthetical annotations: "(sativa hybrid)", "(indica)", "(50/50)" etc.
        clean = re.sub(r'\([^)]{0,40}\)', '', gen_section)
        clean = re.sub(r'[ \t]{2,}', ' ', clean)  # Collapse spaces but keep newlines

        # Approach 1: "Name x Name" — use literal space (not \s) so newlines stop the match
        pair_m = re.search(
            r"([A-Z][A-Za-z0-9 '\-\&\#]{1,35}?)\s*[×xX]\s*"
            r"([A-Z][A-Za-z0-9 '\-\&\#]{1,35}?)"
            r"(?:\s*$|\s*[\.\n,]|\s*(?:strain|Classi|Chemo|Terp|THC|CBD|Type|Flower|"
            r"Effect|Flavo|Medication|Medical|Negative|Side|\d+\s*%))",
            clean
        )
        if pair_m:
            p1 = pair_m.group(1).strip()
            p2 = pair_m.group(2).strip()
            if len(p1) >= 2 and len(p2) >= 2:
                genetics = f"{p1} × {p2}"

        # Approach 2: looser — first line only, no stop-word requirement
        if not genetics:
            first_line = clean.split('\n')[0].strip() if '\n' in clean else clean[:150]
            pair_m2 = re.search(
                r"([A-Z][A-Za-z0-9 '\-\&\#]{1,35}?)\s*[×xX]\s*"
                r"([A-Z][A-Za-z0-9 '\-\&\#]{2,35})",
                first_line
            )
            if pair_m2:
                p1 = pair_m2.group(1).strip()
                p2 = pair_m2.group(2).strip()
                if len(p1) >= 2 and len(p2) >= 2:
                    genetics = f"{p1} × {p2}"

    # Approach 3: "cross of X and Y" anywhere near top of page
    if not genetics:
        cross_section = re.sub(r'\([^)]{0,40}\)', '', page_text[:1500])
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
        # Remove trailing leaked descriptive words
        genetics = re.sub(
            r'\s+(?:strain|strains|if|is|are|the|a|an|this|which|that|with|from)s?\s*\.?$',
            '', genetics, flags=re.IGNORECASE
        ).strip()
        if len(genetics) > 65:
            genetics = genetics[:65].rsplit(' ', 1)[0]
        # Validate: both sides of × must have 2+ chars
        if '×' in genetics:
            parts = genetics.split('×')
            if len(parts) != 2 or len(parts[0].strip()) < 2 or len(parts[1].strip()) < 2:
                genetics = ""

    return {
        "name": strain_name, "producer": producer_name,
        "thc": thc, "cbd": cbd, "type": strain_type, "code": code, "tier": tier,
        "terpenes": terpenes, "effects": effects, "flavours": flavours,
        "helpsWith": helps, "negatives": negatives, "genetics": genetics,
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
        r'^T\d+\s+C\d+\s+',                      # "T10 C13 Moon Berry"
        r'^T\d+\s+',                              # "T14 Banana Split"
        r'^[A-Z]{2,5}\s+T\d+\s+',                # "GCR T27 Gas Cream Cake"
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

    # 2. Remove duplicates (keep the one with more data)
    seen = {}
    deduped = []
    for s in strains:
        key = (s["name"].lower(), s["producer"].lower())
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

    if cleaned or removed or code_fixed:
        print(f"  \U0001f9f9 Cleaned {cleaned} names, removed {removed} duplicates, fixed {code_fixed} codes")

    return deduped


# ---------------------------------------------------------------------------
# Debug mode
# ---------------------------------------------------------------------------
async def debug_page(url):
    print(f"\U0001f50d Debug mode: {url}\n")
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
        print("SCRAPE RESULT:")
        print("=" * 60)
        data = await scrape_strain_page_pw(page, url, "DEBUG")
        if data:
            for k, v in data.items():
                print(f"  {k}: {v}")
        else:
            print("  FAILED - returned None")

        print(f"\n\U0001f4c4 Full dumps saved to debug_text.txt and debug_html.html")
        await browser.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    if len(sys.argv) >= 3 and sys.argv[1] == '--debug':
        await debug_page(sys.argv[2])
        return 0

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
        key = (s["name"].lower(), s["producer"].lower())
        existing_by_key[key] = s
        existing_codes.add(s.get("code", s.get("id", "")))
    print(f"  Loaded {len(existing)} existing strains")

    # 2. Discover strain page URLs via HTTP
    print("\n\U0001f50d Discovering strain pages (HTTP)...")
    all_urls = {}
    for slug, producer in PRODUCERS.items():
        urls = discover_strain_urls(slug)
        if urls:
            print(f"  \U0001f4e6 {producer}: {len(urls)} pages")
            for u in urls:
                all_urls[u] = producer
        else:
            print(f"  \U0001f4e6 {producer}: \u2717 not found")
        time.sleep(0.5)

    print(f"\n  Total strain pages to scrape: {len(all_urls)}")

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

    async def scrape_one(browser, surl, producer):
        context = await browser.new_context(user_agent=HTTP_HEADERS["User-Agent"])
        pg = await context.new_page()
        try:
            data = await scrape_strain_page_pw(pg, surl, producer)
        except Exception:
            data = None
        finally:
            await context.close()
        async with results_lock:
            progress["done"] += 1
            if progress["done"] % 50 == 0:
                print(f"    ... processed {progress['done']}/{progress['total']} pages")
        return data, producer

    async def worker(browser, queue):
        while not queue.empty():
            try:
                surl, producer = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            data, prod = await scrape_one(browser, surl, producer)
            queue.task_done()
            if not data or not data.get("name"):
                continue
            async with results_lock:
                key = (data["name"].lower(), prod.lower())
                if key in existing_by_key:
                    ex = existing_by_key[key]
                    if data["thc"] > 0 and ex.get("thc", 0) == 0:
                        ex["thc"] = data["thc"]
                    if data["cbd"] > 0 and ex.get("cbd", 0) == 0:
                        ex["cbd"] = data["cbd"]
                    # Update terpenes if new data has more
                    if data["terpenes"] and len(data["terpenes"]) > len(ex.get("terpenes", [])):
                        ex["terpenes"] = data["terpenes"]
                    if data.get("tier") != "Core" and ex.get("tier", "Core") == "Core":
                        ex["tier"] = data["tier"]
                    # Fill in missing fields
                    for field in ["effects", "flavours", "helpsWith", "negatives", "genetics"]:
                        if data.get(field) and not ex.get(field):
                            ex[field] = data[field]
                elif key not in seen_new:
                    seen_new.add(key)
                    new_strains.append(data)
                    terp_str = ', '.join(data['terpenes'][:3]) if data['terpenes'] else 'no terpenes yet'
                    print(f"    \u271a {data['name']} ({prod}, THC {data['thc']}%, {terp_str})")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)

        queue = asyncio.Queue()
        for item in all_urls.items():
            queue.put_nowait(item)
        workers_list = [asyncio.create_task(worker(browser, queue)) for _ in range(CONCURRENCY)]
        await asyncio.gather(*workers_list)

        print(f"\n  New unique strains: {len(new_strains)}")

        # 4. Weedstrain fallback (for terpenes and genetics)
        missing_terps = [s for s in new_strains if not s.get("terpenes")]
        missing_genetics = [s for s in new_strains if s.get("terpenes") and not s.get("genetics")]
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

        # 5. Merge
        result = list(existing)
        for s in new_strains:
            raw_code = s.get("code", "")
            code = raw_code if is_valid_code(raw_code) and raw_code not in existing_codes else make_code(s["name"], existing_codes)
            if code not in existing_codes:
                existing_codes.add(code)
            result.append({
                "name": s["name"], "producer": s["producer"], "code": code,
                "tier": s.get("tier", "Core"), "thc": s.get("thc", 0),
                "cbd": s.get("cbd", 0), "type": s.get("type", "Hybrid"),
                "terpenes": s.get("terpenes", []), "effects": s.get("effects", []),
                "flavours": s.get("flavours", []), "helpsWith": s.get("helpsWith", []),
                "negatives": s.get("negatives", []), "genetics": s.get("genetics", ""),
                "id": code,
            })

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
    print(f"\n{'='*60}")
    print(f"\u2705 Done! {len(result)} strains ({'+' if added >= 0 else ''}{added} new)")
    print(f"{'='*60}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
