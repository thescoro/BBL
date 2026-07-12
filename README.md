# Strain Data Auto-Updater

This folder contains the scraper that keeps Bloomy's Bud Log up to date.

## How it works

1. The **home server's cron** runs `scripts/daily_update.sh` daily at **6am**
   (GitHub Actions can't: medibud.co.uk is behind Cloudflare, which blocks
   requests from datacenter IPs with 403 — residential IPs work fine)
2. The script fetches the full catalogue from **MediBud.co.uk's JSON API**
   (strain names, producers, THC/CBD, type, terpenes — flower and vape
   cartridges; oils/capsules/edibles are skipped)
3. Any **new** strains are enriched with terpene, effect, and flavour data
   from **Weedstrain.com** and **AllBud.com**
4. **Patient reviews** are fetched from MediBud's review API (only strains
   that have reviews get a request; photos/videos are skipped)
5. The updated data is saved to `strains.json` / `reviews.json` and
   embedded into `index.html`
6. If anything changed, the bot commits and pushes automatically

> **History:** the original source was MedBud.wiki page scraping, but that
> site started returning HTTP 402 ("payment required for automated content
> access") to all bots in June 2026. MediBud.co.uk mirrors the same
> catalogue (its records link back to medbud.wiki) via an open JSON feed.
> The reviews tab showed YouTube reviews scraped from medbud.wiki; those
> went behind the same 402 wall, so as of July 2026 the tab shows MediBud
> patient reviews instead.

## Manual trigger

Run `~/BBL/scripts/daily_update.sh` on the home server. (The Actions
workflow still exists but will 403 on the feed fetch from GitHub's IPs.)

## Failure alerts

If the MediBud feed goes missing or comes back suspiciously small, the
updater exits non-zero instead of quietly committing nothing. Cron output
lands in `~/BBL/update.log` — check it if the site stops picking up new
strains.

## Important notes

- The scraper is **polite** — one request to MediBud per run, rate-limited
  enrichment requests elsewhere
- Existing strain data is **never deleted** — only new strains are added
- THC/CBD values are updated if the feed has newer data
- Records are matched to existing ones with an accent/punctuation-tolerant
  key, so "Blue Pavé" and "Blue Pave" don't duplicate
