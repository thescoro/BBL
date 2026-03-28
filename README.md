[README.md](https://github.com/user-attachments/files/26324516/README.md)
# Strain Data Auto-Updater

This folder contains the scraper that keeps Bloomy's Bud Log up to date.

## How it works

1. **GitHub Actions** runs `update_strains.py` every **Sunday at 6am UTC**
2. The script scrapes **MedBud.wiki** for strain names, producers, THC/CBD, and tiers
3. Any **new** strains are enriched with terpene, effect, and flavour data from **Weedstrain.com**
4. The updated data is saved to `strains.json` and embedded into `index.html`
5. If anything changed, the bot commits and pushes automatically

## Manual trigger

Go to **Actions** → **Update Strain Data** → **Run workflow** to trigger it manually.

## Adding a new producer

Edit `PRODUCERS` in `update_strains.py`:

```python
PRODUCERS = {
    "4c-labs": "4C Labs",
    "new-producer-slug": "New Producer Display Name",
    # ...
}
```

The slug is the URL path on MedBud (e.g. `medbud.wiki/strains/4c-labs/`).

## Important notes

- The scraper is **polite** — it waits between requests and identifies itself
- Existing strain data is **never deleted** — only new strains are added
- If MedBud changes their site structure, the scraper may need updating
- THC/CBD values are updated if MedBud has newer data
