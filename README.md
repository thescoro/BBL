# Strain Data Auto-Updater

This folder contains the scraper that keeps Bloomy's Bud Log up to date.

## How it works

1. **GitHub Actions** runs `update_strains.py` daily at **6am UTC**
2. The script fetches the full catalogue from **MediBud.co.uk's JSON API**
   (strain names, producers, THC/CBD, type, terpenes — flower and vape
   cartridges; oils/capsules/edibles are skipped)
3. Any **new** strains are enriched with terpene, effect, and flavour data
   from **Weedstrain.com** and **AllBud.com**
4. The updated data is saved to `strains.json` and embedded into `index.html`
5. If anything changed, the bot commits and pushes automatically

> **History:** the original source was MedBud.wiki page scraping, but that
> site started returning HTTP 402 ("payment required for automated content
> access") to all bots in June 2026. MediBud.co.uk mirrors the same
> catalogue (its records link back to medbud.wiki) via an open JSON feed.
> YouTube reviews also came from medbud.wiki, so `reviews.json` is frozen
> at its last good state (May 2026).

## Manual trigger

Go to **Actions** → **Update Strain Data** → **Run workflow** to trigger it manually.

## Failure alerts

If the MediBud feed goes missing or comes back suspiciously small, the
updater exits non-zero and the workflow run **fails loudly** (red ✗) —
GitHub emails the repo owner on scheduled-workflow failures. Green runs
mean data was genuinely checked, not just that the script didn't crash.

## Important notes

- The scraper is **polite** — one request to MediBud per run, rate-limited
  enrichment requests elsewhere
- Existing strain data is **never deleted** — only new strains are added
- THC/CBD values are updated if the feed has newer data
- Records are matched to existing ones with an accent/punctuation-tolerant
  key, so "Blue Pavé" and "Blue Pave" don't duplicate
