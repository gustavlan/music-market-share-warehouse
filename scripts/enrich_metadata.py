import os
import logging
import duckdb
import spotipy
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from spotipy.exceptions import SpotifyException

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")

# Use DATA_DIR env var (Docker) or default to local ./data path
BASE_DIR = os.getenv("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))
DB_PATH = os.path.join(BASE_DIR, "music_warehouse.duckdb")

def enrich_artist():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("Spotify CLIENT_ID and CLIENT_SECRET must be set as environment variables.")
    
    logger.info("Connecting to DuckDB...")
    con = duckdb.connect(DB_PATH)

    # Initialize Spotipy client, ClientCredentials is used since only public data is needed
    auth_manager = spotipy.oauth2.SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    sp = spotipy.Spotify(auth_manager=auth_manager)

    # Retry decorator for Spotify API calls with exponential backoff
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((SpotifyException, ConnectionError)),
        before_sleep=lambda retry_state: logger.warning(
            f"Retry {retry_state.attempt_number}/5 after error. Waiting {retry_state.next_action.sleep}s..."
        )
    )
    def fetch_track_metadata(track: str, artist: str) -> dict | None:
        """Fetch track metadata from Spotify with retry logic."""
        search_query = f"track:{track} artist:{artist}"
        response = sp.search(q=search_query, type='track', limit=1)
        items = response['tracks']['items']
        if items:
            track_data = items[0]
            album_data = sp.album(track_data['album']['id'])
            return {
                'track_name': track,
                'artist_name': artist,
                'spotify_label': album_data['label'],
                'spotify_track_id': track_data['id'],
                'updated_at': pd.Timestamp.now()
            }
        return None

    # Find artist/track IDs that are missing mapping
    logger.info("Finding artists missing label mapping...")

    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_track_metadata (
            track_name VARCHAR,
            artist_name VARCHAR,
            spotify_label VARCHAR,
            spotify_track_id VARCHAR,
            updated_at TIMESTAMP
        );
    """)

    query = """
        SELECT DISTINCT c.track_name, c.artist_name
        FROM stg_combined_charts c
        LEFT JOIN dim_track_metadata m
            ON c.track_name = m.track_name AND c.artist_name = m.artist_name
        WHERE m.spotify_label IS NULL
        LIMIT 250 -- Increased limit for better coverage
    """

    missing_tracks = con.sql(query).df()

    if missing_tracks.empty:
        logger.info("No new tracks to enrich.")
        return
    logger.info(f"Found {len(missing_tracks)} tracks to enrich, connecting to Spotify API...")

    results = []
    failed_count = 0

    for _, row in missing_tracks.iterrows():
        track = row['track_name']
        artist = row['artist_name']

        try:
            metadata = fetch_track_metadata(track, artist)
            if metadata:
                logger.info(f"Found: {track} by {artist} - Label: {metadata['spotify_label']}")
                results.append(metadata)
            else:
                logger.debug(f"No results for: {track} by {artist}")
        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to fetch data for {track} by {artist} after retries: {e}")

    if results:
        logger.info(f"Inserting {len(results)} enriched records into dim_track_metadata...")
        df_results = pd.DataFrame(results)
        con.register("df_results", df_results)
        con.execute("INSERT INTO dim_track_metadata SELECT * FROM df_results")
        con.unregister("df_results")
        logger.info(f"Success. Enriched {len(results)} tracks, {failed_count} failures.")
    con.close()

if __name__ == "__main__":
    enrich_artist()