"""
EAPCET College-Wise Allotment Scraper
Scrapes all data from https://eduvale.in/eapcet-college-wise-allotment/
and saves as CSV + Excel.

Features:
- Appends each college's data to CSV immediately (no bulk memory usage)
- Checkpoint file tracks completed year+college combos for resume on restart
"""

import csv
import json
import os
import time
import zipfile
import requests
from bs4 import BeautifulSoup
import pandas as pd

BASE_URL = "https://eduvale.in/eapcet-college-wise-allotment/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
}
DELAY = 0.5

CSV_PATH = "eapcet_all_data.csv"
XLSX_PATH = "eapcet_all_data.xlsx"
CHECKPOINT_PATH = "eapcet_checkpoint.json"

FIELDNAMES = [
    "year", "phase", "college_code", "college_name", "branch",
    "roll_no", "rank", "name", "gender", "region", "category", "seat_category",
]


# ── Checkpoint helpers ───────────────────────────────────────────────────────

def load_checkpoint():
    """Load the set of completed (year, college) keys."""
    if os.path.exists(CHECKPOINT_PATH):
        with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
            return set(tuple(k) for k in json.load(f))
    return set()


def save_checkpoint(done: set):
    """Persist the set of completed (year, college) keys."""
    with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
        json.dump(list(done), f)


# ── API helpers ──────────────────────────────────────────────────────────────

def fetch_page():
    resp = requests.get(BASE_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def get_years(soup):
    select = soup.find("select", {"id": "year"})
    years = []
    for opt in select.find_all("option"):
        val = opt.get("value", "").strip()
        text = opt.get_text(strip=True)
        if val:
            years.append((val, text))
    return years


def get_colleges(year_val):
    url = f"{BASE_URL}fetch_colleges.php?year={year_val}"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    colleges = []
    for opt in soup.find_all("option"):
        val = opt.get("value", "").strip()
        text = opt.get_text(strip=True)
        if val:
            colleges.append((val, text))
    return colleges


def get_branches(year_val, college_code):
    url = f"{BASE_URL}fetch_branches.php?year={year_val}&college={college_code}"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    branches = []
    options = soup.find_all("option")
    if options:
        for opt in options:
            val = opt.get("value", "").strip()
            if val:
                branches.append(val)
    else:
        for line in resp.text.strip().splitlines():
            line = line.strip()
            if line:
                branches.append(line)
    return branches


def get_results(year_val, college_code, branch):
    ts = int(time.time() * 1000)
    url = (
        f"{BASE_URL}fetch_results.php?"
        f"year={year_val}&college={college_code}&branch={branch}&_={ts}"
    )
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", [])


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    # If a zip file exists but the CSV doesn't, extract it first
    if os.path.exists("eapcet_all_data.zip") and not os.path.exists(CSV_PATH):
        print("Found eapcet_all_data.zip. Extracting eapcet_all_data.csv...")
        try:
            with zipfile.ZipFile("eapcet_all_data.zip", "r") as z:
                z.extract(CSV_PATH)
            print("Extraction successful.")
        except Exception as e:
            print(f"Failed to extract zip: {e}")

    done = load_checkpoint()
    print(f"Loaded checkpoint: {len(done)} year+college combo(s) already done")

    # Determine whether the CSV header needs to be written
    csv_exists = os.path.exists(CSV_PATH) and os.path.getsize(CSV_PATH) > 0
    csv_file = open(CSV_PATH, "a", newline="", encoding="utf-8-sig")
    writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES)
    if not csv_exists:
        writer.writeheader()
        csv_file.flush()

    print("Fetching main page...")
    soup = fetch_page()
    years = get_years(soup)
    print(f"Found {len(years)} year(s): {[y[1] for y in years]}")
    print("-" * 60)

    total_rows = 0

    try:
        for year_val, year_display in years:
            print(f"\n=== Year: {year_display} ({year_val}) ===")

            try:
                colleges = get_colleges(year_val)
            except Exception as e:
                print(f"  Failed to fetch colleges: {e}")
                colleges = []
            time.sleep(DELAY)

            if not colleges:
                print("  No colleges found, skipping.")
                continue

            print(f"  {len(colleges)} college(s)")

            for ci, (college_code, college_name) in enumerate(colleges, 1):
                key = (year_val, college_code)
                if key in done:
                    continue  # already scraped in a previous run

                try:
                    branches = get_branches(year_val, college_code)
                except Exception:
                    branches = []
                time.sleep(DELAY)

                college_rows = 0

                for branch in branches:
                    try:
                        students = get_results(year_val, college_code, branch)
                    except Exception:
                        students = []
                    time.sleep(DELAY)

                    if not students:
                        continue

                    for s in students:
                        row = {
                            "year": year_val,
                            "phase": year_display,
                            "college_code": college_code,
                            "college_name": college_name,
                            "branch": branch,
                            "roll_no": s.get("rollno", ""),
                            "rank": s.get("rank", ""),
                            "name": s.get("cand_name", ""),
                            "gender": s.get("gender", ""),
                            "region": s.get("region", ""),
                            "category": s.get("category", ""),
                            "seat_category": s.get("seat_category", ""),
                        }
                        writer.writerow(row)
                        college_rows += 1

                    print(
                        f"  [{ci}/{len(colleges)}] {college_code} | "
                        f"{branch} | {len(students)} student(s)"
                    )

                # Flush CSV and mark this college as done
                csv_file.flush()
                done.add(key)
                save_checkpoint(done)
                total_rows += college_rows

    except KeyboardInterrupt:
        print("\n\nInterrupted! Progress has been saved to checkpoint.")
    finally:
        csv_file.close()

    print(f"\n{'=' * 60}")
    print(f"Total NEW rows written this run: {total_rows}")
    print(f"Total year+college combos completed: {len(done)}")

    # Generate Excel from the full CSV
    if os.path.exists(CSV_PATH) and os.path.getsize(CSV_PATH) > 0:
        print(f"Converting {CSV_PATH} -> {XLSX_PATH} ...")
        df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
        df.to_excel(XLSX_PATH, index=False, engine="openpyxl")
        print(f"Saved {XLSX_PATH}  ({len(df)} total rows)")
    else:
        print("No data to convert to Excel.")


if __name__ == "__main__":
    main()
