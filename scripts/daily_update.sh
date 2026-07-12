#!/usr/bin/env bash
# Daily data refresh for Bloomy's Bud Log, run from the home server's cron.
# GitHub Actions can't do this job: medibud.co.uk sits behind Cloudflare,
# which 403s requests from datacenter IPs. Residential IPs are fine.
set -euo pipefail
cd "$(dirname "$0")/.."

git pull --rebase --quiet
python3 scripts/update_strains.py

if git diff --quiet -- strains.json reviews.json index.html; then
  echo "✅ No new strain data — everything is up to date."
else
  git add strains.json reviews.json index.html
  STRAIN_COUNT=$(python3 -c "import json; print(len(json.load(open('strains.json'))))")
  REVIEW_COUNT=$(python3 -c "import json; print(len(json.load(open('reviews.json'))))" 2>/dev/null || echo 0)
  git -c user.name="Bloomy's Bot" -c user.email="bot@bloomys-bud-log.github.io" \
    commit -m "🌿 Auto-update: $STRAIN_COUNT strains, $REVIEW_COUNT reviews"
  git push
fi
