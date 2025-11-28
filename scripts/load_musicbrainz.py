import duckdb
import os

DB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/music_warehouse.duckdb")
JSON_FILE_PATH = os.getenv("JSON_FILE_PATH", "/opt/airflow/data/raw/musicbrainz/mbdump/label")

def load_labels_to_duckdb():
    print("Connecting to DuckDB...")
    con = duckdb.connect(DB_PATH)

    if not os.path.exists(JSON_FILE_PATH):
        raise FileNotFoundError(f"Could not find MusicBrainz dump at {JSON_FILE_PATH}. Did you extract it?")

    print("Loading Label Data (this may take a minute)...")

    con.execute(f"""
        CREATE OR REPLACE TABLE raw_musicbrainz_labels AS
        SELECT 
            id AS mb_id,
            name,
            type,
            "label-code" AS label_code,
            relations
        FROM read_json_auto('{JSON_FILE_PATH}', ignore_errors=true);
    """)

    count = con.execute("SELECT COUNT(*) FROM raw_musicbrainz_labels").fetchone()[0]
    print(f"✅ Successfully loaded {count:,} labels into 'raw_musicbrainz_labels'.")

    con.close()

if __name__ == "__main__":
    load_labels_to_duckdb()