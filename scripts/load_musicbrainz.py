import os
from datetime import datetime, timedelta
import pandas as pd
from playwright.sync_api import sync_playwright

URL = "https://kworb.net/spotify/country/global_daily.html"

# Absolute path so it hits the mounted volume no matter what
OUTPUT_DIR = "/opt/airflow/data/raw/kworb"

def scrape_kworb():
    print(f"Starting scrape for {URL}")

    # Ensure the directory exists (it might not if the mount is fresh)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(URL, timeout=60000)

        # Wait for the table to load
        page.wait_for_selector("table")

        # Extract table data
        html_content = page.content()
        browser.close()

    print("Parsing HTML table ")
    dfs = pd.read_html(html_content)

    if not dfs:
        raise ValueError("No tables found on the page")

    df = dfs[0]
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    extraction_date = datetime.now().date()
    # Charts are usually for the previous day
    chart_date = extraction_date - timedelta(days=1)
    df["chart_date"] = chart_date
    df["extracted_at"] = datetime.now()

    filename = f"kworb_spotify_global_daily_{chart_date}.parquet"
    filepath = os.path.join(OUTPUT_DIR, filename)

    print(f"Saving data to {filepath}")
    df.to_parquet(filepath, index=False)
    
    # Verification print
    if os.path.exists(filepath):
        print(f"✅ Success! File confirmed at: {filepath}")
    else:
        raise RuntimeError(f"❌ File write failed at: {filepath}")

if __name__ == "__main__":
    scrape_kworb()