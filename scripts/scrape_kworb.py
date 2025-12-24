import os
import logging
from datetime import datetime, timedelta
import pandas as pd
from playwright.sync_api import sync_playwright
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from playwright._impl._errors import TimeoutError as PlaywrightTimeoutError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

URL = "https://kworb.net/spotify/country/global_daily.html"
# Use DATA_DIR env var (Docker) or default to local ./data path
BASE_DIR = os.getenv("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))
OUTPUT_DIR = os.path.join(BASE_DIR, "raw", "kworb")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=5, max=60),
    retry=retry_if_exception_type((PlaywrightTimeoutError, ConnectionError)),
    before_sleep=lambda retry_state: logger.warning(
        f"Scrape failed, retry {retry_state.attempt_number}/3. Waiting {retry_state.next_action.sleep}s..."
    )
)
def fetch_page_content() -> str:
    """Fetch page content with Playwright, with retry on timeout."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(URL, timeout=60000)
        page.wait_for_selector("table")
        html_content = page.content()
        browser.close()
    return html_content

def scrape_kworb():
    logger.info(f"Starting scrape for {URL}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    html_content = fetch_page_content()

    logger.info("Parsing HTML table")
    dfs = pd.read_html(html_content)

    if not dfs:
        raise ValueError("No tables found on the page")

    df = dfs[0]
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    extraction_date = datetime.now().date()
    chart_date = extraction_date - timedelta(days=1)
    df["chart_date"] = chart_date
    df["extracted_at"] = datetime.now()

    filename = f"kworb_spotify_global_daily_{chart_date}.parquet"
    filepath = os.path.join(OUTPUT_DIR, filename)

    logger.info(f"Saving data to {filepath}")
    df.to_parquet(filepath, index=False)
    logger.info(f"Scrape completed. Saved {len(df)} rows.")

if __name__ == "__main__":
    scrape_kworb()