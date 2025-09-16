# scripts/enrich_with_ean.py

import csv
import requests
import time
from pathlib import Path

# ==== Config ====
ICECAT_USER = "Louverius"
ICECAT_PASS = "Komtgoed12!"

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "prisjakt_offers.csv"
OUTPUT_FILE = BASE_DIR / "prisjakt_offers_with_ean.csv"

ean_cache = {}

def search_icecat_rest(title: str):
    """Zoek product in Icecat via de REST Search API (fuzzy search)."""
    url = "https://live.icecat.biz/rest/search/v1/"
    params = {
        "shopname": ICECAT_USER,
        "lang": "en",
        "q": title,
    }
    try:
        r = requests.get(url, params=params, auth=(ICECAT_USER, ICECAT_PASS), timeout=10)
        if r.status_code == 200:
            data = r.json()
            # eerste resultaat pakken
            if "data" in data and "products" in data["data"]:
                products = data["data"]["products"]
                if products:
                    return products[0].get("id")  # ProductId
        else:
            print(f"[WARN] Icecat REST search returned {r.status_code} for {title}")
    except Exception as e:
        print(f"[ERROR] REST search failed for {title}: {e}")
    return None

def fetch_ean(product_id):
    """Haal EAN op uit Icecat productdetails API."""
    if not product_id:
        return None

    url = "https://data.icecat.biz/api/"
    params = {
        "UserName": ICECAT_USER,
        "Language": "en",
        "ProductId": product_id,
    }
    try:
        r = requests.get(url, params=params, auth=(ICECAT_USER, ICECAT_PASS), timeout=10)
        if r.status_code == 200:
            data = r.json()
            return data.get("GeneralInfo", {}).get("EAN")
        else:
            print(f"[WARN] Icecat detail returned {r.status_code} for ID {product_id}")
    except Exception as e:
        print(f"[ERROR] Fetch EAN failed for ID {product_id}: {e}")
    return None

def lookup_ean(title):
    if not title:
        return None
    if title in ean_cache:
        return ean_cache[title]

    product_id = search_icecat_rest(title)
    ean = fetch_ean(product_id) if product_id else None
    ean_cache[title] = ean
    return ean

def main():
    if not INPUT_FILE.exists():
        print(f"[ERROR] Input file not found: {INPUT_FILE}")
        return

    print(f"[INFO] Reading input from: {INPUT_FILE}")
    print(f"[INFO] Writing output to: {OUTPUT_FILE}")

    with open(INPUT_FILE, newline="", encoding="utf-8") as infile, \
         open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as outfile:

        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ["ean"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            title = row.get("product_title", "")
            ean = lookup_ean(title)
            row["ean"] = ean
            writer.writerow(row)

            print(f"[INFO] {title} -> EAN: {ean}")
            time.sleep(0.5)  # niet te agressief ratelen

if __name__ == "__main__":
    main()
