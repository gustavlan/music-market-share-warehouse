import os
import duckdb
import spotipy
import pandas as pd
import time

CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")

BASE_DIR = "/opt/airflow/data"
DB_PATH = os.path.join(BASE_DIR, "music_warehouse.duckdb")

def enrich_artist():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("Spotify CLIENT_ID and CLIENT_SECRET must be set as environment variables.")
    
    print("Connecting to DuckDB...")
    con = duckdb.connect(DB_PATH)

    # Initialize Spotipy client, ClientCredentials is used since only public data is needed
    auth_manager = spotipy.oauth2.SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    sp = spotipy.Spotify(auth_manager=auth_manager)

    # Find arist/track IDs that are missing mapping
    print("Finding artists missing label mapping...")

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
        SELECT DISTINCT c.track_name, c.artist_name,
        FROM stg_combined_charts c
        LEFT JOIN dim_track_metadata m
            ON c.track_name = m.track_name AND c.artist_name = m.artist_name
        WHERE m.spotify_label IS NULL
        LIMIT 50 -- Limit to 50 for safety testing purposes
    """

    missing_tracks = con.sql(query).df

    if missing_tracks.empty:
        print("No new tracks to enrich.")
        return
    print(f"Found {len(missing_tracks)} tracks to enrich, connecting to spotify API...")

    results = []

    for _, row in missing_tracks.iterrows():
        track = row['track_name']
        artist = row['artist_name']
        search_query = f"track:{track} artist:{artist}"

        try: # search spotify for the track
            response = sp.search(q=search_query, type='track', limit=1)
            items = response['tracks']['items']
            if items:
                track_data = items[0]
                album_data = sp.album(track_data['album']['id'])
                label = album_data['label']
                spotify_id = track_data['id']
                print(f"Found: {track} by {artist} - Label: {label}")
                results.append({
                    'track_name': track,
                    'artist_name': artist,
                    'spotify_label': label,
                    'spotify_track_id': spotify_id,
                    'updated_at': pd.Timestamp.now()
                })
            else:
                print(f"No results for: {track} by {artist}")
                time.sleep(2)  # brief pause to respect rate limits
        
        except Exception as e:
            print(f"Error fetching data for {track} by {artist}: {e}")
            time.sleep(5)  # longer pause on error

        time.sleep(0.5)  # brief pause to respect rate limits

    if results:
        print(f"Inserting {len(results)} enriched records into dim_track_metadata...")
        df_results = pd.DataFrame(results)
        con.sql("INSERT INTO dim_track_metadata SELECT * FROM df_results")
        print("Success")
    con.close()

if __name__ == "__main__":
    enrich_artist()