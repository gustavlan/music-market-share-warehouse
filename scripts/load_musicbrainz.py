import json
import os
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import duckdb


MUSICBRAINZ_RAW_DIR = Path(os.getenv("MUSICBRAINZ_RAW_DIR", "/opt/airflow/data/raw/musicbrainz"))
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", "/opt/airflow/data/music_warehouse.duckdb"))

LABEL_NDJSON_PATH = MUSICBRAINZ_RAW_DIR / "mbdump" / "label"
LABEL_ARCHIVE_PATH = MUSICBRAINZ_RAW_DIR / "label.tar.xz"

STATE_TABLE = "raw_ingestion_state"
STATE_DATASET = "musicbrainz_label"


def _read_text(path: Path) -> str | None:
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8", errors="replace").strip() or None


def _get_dump_markers() -> tuple[int | None, str | None]:
    replication_sequence = _read_text(MUSICBRAINZ_RAW_DIR / "REPLICATION_SEQUENCE")
    dump_timestamp = _read_text(MUSICBRAINZ_RAW_DIR / "TIMESTAMP")
    seq: int | None
    try:
        seq = int(replication_sequence) if replication_sequence is not None else None
    except ValueError:
        seq = None
    return seq, dump_timestamp


def _is_within_directory(directory: Path, target: Path) -> bool:
    directory = directory.resolve()
    target = target.resolve()
    return str(target).startswith(str(directory) + os.sep) or target == directory


def _safe_extract_member(tar: tarfile.TarFile, member: tarfile.TarInfo, destination_dir: Path) -> None:
    destination_dir.mkdir(parents=True, exist_ok=True)
    member_path = destination_dir / member.name
    if not _is_within_directory(destination_dir, member_path):
        raise RuntimeError(f"Unsafe path in archive: {member.name}")
    tar.extract(member, path=destination_dir)


def ensure_label_dump_present() -> None:
    if LABEL_NDJSON_PATH.exists():
        return

    if not LABEL_ARCHIVE_PATH.exists():
        raise FileNotFoundError(
            "MusicBrainz label dump not found. Expected one of: "
            f"{LABEL_NDJSON_PATH} (preferred) or {LABEL_ARCHIVE_PATH}."
        )

    print(f"Extracting {LABEL_ARCHIVE_PATH}...")
    with tarfile.open(LABEL_ARCHIVE_PATH, mode="r:xz") as tar:
        label_member: tarfile.TarInfo | None = None
        for member in tar.getmembers():
            if member.isdir():
                continue
            if member.name.endswith("mbdump/label") or member.name.endswith("/label"):
                label_member = member
                break
        if label_member is None:
            raise RuntimeError("Could not find 'mbdump/label' inside label.tar.xz")

        _safe_extract_member(tar, label_member, MUSICBRAINZ_RAW_DIR)

    if not LABEL_NDJSON_PATH.exists():
        raise RuntimeError(f"Extraction completed but {LABEL_NDJSON_PATH} was not created")


def _iter_ndjson_lines(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Invalid JSON at line {line_no} in {path}") from exc
            if isinstance(obj, dict):
                yield obj


def _ensure_duckdb_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
            dataset VARCHAR,
            replication_sequence BIGINT,
            dump_timestamp VARCHAR,
            loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def _already_loaded(
    con: duckdb.DuckDBPyConnection,
    replication_sequence: int | None,
    dump_timestamp: str | None,
) -> bool:
    row = con.execute(
        f"""
        SELECT replication_sequence, dump_timestamp
        FROM {STATE_TABLE}
        WHERE dataset = ?
        ORDER BY loaded_at DESC
        LIMIT 1
        """,
        [STATE_DATASET],
    ).fetchone()

    if row is None:
        return False

    prev_seq, prev_ts = row
    return prev_seq == replication_sequence and prev_ts == dump_timestamp


def load_musicbrainz_labels_to_duckdb(batch_size: int = 25_000) -> None:
    ensure_label_dump_present()
    replication_sequence, dump_timestamp = _get_dump_markers()

    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"Connecting to DuckDB at {DUCKDB_PATH}")
    con = duckdb.connect(str(DUCKDB_PATH))

    try:
        _ensure_duckdb_schema(con)
        if _already_loaded(con, replication_sequence, dump_timestamp):
            print("MusicBrainz label dump unchanged; skipping reload.")
            return

        print("Preparing target table main.raw_musicbrainz_labels")
        con.execute(
            """
            CREATE OR REPLACE TABLE raw_musicbrainz_labels (
                mb_id VARCHAR,
                name VARCHAR,
                label_code INTEGER,
                relations JSON
            )
            """
        )

        insert_sql = (
            "INSERT INTO raw_musicbrainz_labels (mb_id, name, label_code, relations) "
            "VALUES (?, ?, ?, CAST(? AS JSON))"
        )

        print(f"Loading NDJSON from {LABEL_NDJSON_PATH}")
        started_at = datetime.utcnow()
        batch: list[tuple[str | None, str | None, int | None, str | None]] = []
        rows = 0

        con.execute("BEGIN")
        for obj in _iter_ndjson_lines(LABEL_NDJSON_PATH):
            mb_id = obj.get("id")
            name = obj.get("name")
            label_code_raw = obj.get("label-code")
            if label_code_raw is None:
                label_code_raw = obj.get("label_code")

            try:
                label_code = int(label_code_raw) if label_code_raw not in (None, "") else None
            except (TypeError, ValueError):
                label_code = None

            relations = obj.get("relations")
            relations_json = (
                json.dumps(relations, ensure_ascii=False) if relations is not None else None
            )

            batch.append((mb_id, name, label_code, relations_json))
            if len(batch) >= batch_size:
                con.executemany(insert_sql, batch)
                rows += len(batch)
                batch.clear()

        if batch:
            con.executemany(insert_sql, batch)
            rows += len(batch)

        con.execute("COMMIT")
        elapsed = (datetime.utcnow() - started_at).total_seconds()
        print(f"Loaded {rows:,} labels in {elapsed:,.1f}s")

        con.execute(
            f"INSERT INTO {STATE_TABLE} (dataset, replication_sequence, dump_timestamp) VALUES (?, ?, ?)",
            [STATE_DATASET, replication_sequence, dump_timestamp],
        )
        print("Ingestion state recorded.")
    except Exception:
        try:
            con.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        con.close()


if __name__ == "__main__":
    load_musicbrainz_labels_to_duckdb()