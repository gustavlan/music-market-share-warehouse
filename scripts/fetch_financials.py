"""
Fetch historical stock prices for the Big 3 music labels.

Tickers:
- UMG.AS: Universal Music Group (Euronext Amsterdam)
- WMG: Warner Music Group (Nasdaq)
- SONY: Sony Group Corporation (NYSE)

This script fetches daily adjusted close prices and stores them in DuckDB
for correlation analysis with streaming market share data.
"""
import os
import logging
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import duckdb
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Stock tickers for the Big 3 music labels
TICKERS = {
    "UMG.AS": "Universal Music Group",
    "WMG": "Warner Music Group",
    "SONY": "Sony Group Corporation"
}

# Use DATA_DIR env var (Docker) or default to local ./data path
BASE_DIR = os.getenv("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))
DB_PATH = os.path.join(BASE_DIR, "music_warehouse.duckdb")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=5, max=30),
    before_sleep=lambda retry_state: logger.warning(
        f"yfinance fetch failed, retry {retry_state.attempt_number}/3..."
    )
)
def fetch_stock_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch historical stock data from Yahoo Finance with retry logic.
    
    Args:
        ticker: Stock ticker symbol
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
    
    Returns:
        DataFrame with OHLCV data
    """
    logger.info(f"Fetching {ticker} from {start_date} to {end_date}")
    stock = yf.Ticker(ticker)
    df = stock.history(start=start_date, end=end_date, auto_adjust=True)
    
    if df.empty:
        logger.warning(f"No data returned for {ticker}")
        return pd.DataFrame()
    
    df = df.reset_index()
    df['ticker'] = ticker
    df['company_name'] = TICKERS[ticker]
    
    # Rename columns to snake_case
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    
    # Select relevant columns
    df = df[['date', 'ticker', 'company_name', 'open', 'high', 'low', 'close', 'volume']]
    
    # Ensure date is datetime without timezone
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None)
    
    logger.info(f"Fetched {len(df)} rows for {ticker}")
    return df


def fetch_all_financials(start_date: str = None, end_date: str = None) -> None:
    """
    Fetch stock data for all Big 3 tickers and store in DuckDB.
    
    Args:
        start_date: Start date (default: 2 years ago)
        end_date: End date (default: today)
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    
    logger.info(f"Connecting to DuckDB at {DB_PATH}")
    con = duckdb.connect(DB_PATH)
    
    # Create table if not exists
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_stock_prices (
            date DATE,
            ticker VARCHAR,
            company_name VARCHAR,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume BIGINT,
            PRIMARY KEY (date, ticker)
        );
    """)
    
    all_data = []
    
    for ticker in TICKERS:
        try:
            df = fetch_stock_data(ticker, start_date, end_date)
            if not df.empty:
                all_data.append(df)
        except Exception as e:
            logger.error(f"Failed to fetch {ticker}: {e}")
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Total rows fetched: {len(combined_df)}")
        
        # Upsert: delete existing then insert
        # Get date range from fetched data
        min_date = combined_df['date'].min()
        max_date = combined_df['date'].max()
        
        con.execute("""
            DELETE FROM raw_stock_prices 
            WHERE date >= ? AND date <= ?
        """, [min_date, max_date])
        
        con.register("combined_df", combined_df)
        con.execute("INSERT INTO raw_stock_prices SELECT * FROM combined_df")
        con.unregister("combined_df")
        
        logger.info(f"Successfully loaded {len(combined_df)} rows into raw_stock_prices")
    else:
        logger.warning("No data fetched for any ticker")
    
    con.close()


def get_stock_summary() -> pd.DataFrame:
    """Return a summary of stock data in the warehouse."""
    con = duckdb.connect(DB_PATH, read_only=True)
    summary = con.sql("""
        SELECT 
            ticker,
            company_name,
            MIN(date) as first_date,
            MAX(date) as last_date,
            COUNT(*) as trading_days,
            ROUND(AVG(close), 2) as avg_close,
            ROUND(MIN(close), 2) as min_close,
            ROUND(MAX(close), 2) as max_close
        FROM raw_stock_prices
        GROUP BY ticker, company_name
        ORDER BY ticker
    """).df()
    con.close()
    return summary


if __name__ == "__main__":
    fetch_all_financials()
    print("\n=== Stock Data Summary ===")
    print(get_stock_summary().to_string(index=False))
