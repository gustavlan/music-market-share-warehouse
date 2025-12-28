# Music Industry Alternative Data & Price Signals

End-to-end alternative data pipeline that constructs a proprietary dataset tracking the daily streaming market share of major music labels (Universal, Sony, Warner) and correlates this with their stock prices for quantitative research.

```mermaid
flowchart LR
    A["Spotify Charts\n(Alt Data)"] --> B["Label Enrichment"] --> C["Entity Resolution\n(Graph Theory)"] --> D["Market Share\nTime Series"] --> E["Stock Correlation\n& Alpha Signals"]
```

## Results

Market share by parent group (Dec 2024 daily charts, 26.9B streams):

| Parent Group | Streams | Share |
|-------------|--------:|------:|
| Universal Music Group | 11.3B | 41.9% |
| Sony Music Entertainment | 6.3B | 23.3% |
| Warner Music Group | 5.4B | 20.2% |
| Independent / Other | 2.9B | 10.8% |
| Unmapped | 1.0B | 3.8% |
| **Big 3 Total** | **23.0B** | **85.4%** |

The 85% reported here is higher than the commonly cited 65-70% for the big 3 because this project measures chart-topping tracks only, hits skew heavily toward majors who dominate playlist placement and marketing. The 65-70% figure covers all streaming including long-tail catalog.

## Architecture

```mermaid
flowchart TB
    subgraph Airflow["Airflow Orchestration"]
        direction TB
        Daily_Trigger((Daily)) --> Scrape[Scrape Kworb]
        Daily_Trigger --> Stocks[Fetch Yahoo Finance]
        Scrape --> Load[Load to DuckDB]
        Load --> Check{New Tracks?}
        Check -- Yes --> Enrich[Spotify API Enrichment]
        Check -- No --> Skip[Skip API Calls]
    end

    subgraph Data["Data Warehouse"]
        Enrich --> RAW
        Stocks --> RAW
        RAW --> STG --> MART
    end
```

Daily charts are scraped from Kworb via Playwright. A trigger in Airflow checks the staging area for new tracks and only calls the Spotify API if metadata is missing, saving API quota and execution time. Financial data is fetched independently. dbt transforms resolve ownership hierarchies and aggregate streams.

**Stack:** Docker Compose · Airflow 2.10 · DuckDB · dbt-core · NetworkX

## dbt Lineage

```mermaid
flowchart LR
    subgraph sources[Sources]
        S1[raw_musicbrainz_labels]
        S2[raw_kworb_charts]
        S3[dim_track_metadata]
        S4[dim_labels]
    end
    
    subgraph staging[Staging]
        STG1[stg_musicbrainz_labels]
        STG2[stg_combined_charts]
        STG3[stg_track_metadata_normalized]
    end
    
    subgraph intermediate[Intermediate]
        INT1[int_label_relationships]
        INT2[int_charts_with_track_id]
        INT3[int_labels_normalized]
    end
    
    subgraph marts[Marts]
        FACT[fact_market_share]
    end
    
    S1 --> STG1 --> INT1
    S2 --> STG2 --> INT2
    S3 --> STG3 --> INT2
    S4 --> INT3
    STG3 --> FACT
    INT3 --> FACT
    INT2 --> FACT
```

*Note: `dim_labels` is created by Python/NetworkX (not dbt) via graph traversal of MusicBrainz ownership relationships.*

## Entity Resolution via Graph Theory

A key technical challenge is resolving the ultimate parent company to a tradable entity for thousands of sub-labels. For example:
- "pgLang, under exclusive license to Interscope Records" → Interscope → Universal Music Group

The [`scripts/build_hierarchy.py`](scripts/build_hierarchy.py) script uses NetworkX to:
1. Build a directed graph of 64K+ label-to-label ownership relationships from MusicBrainz
2. Traverse the graph recursively to find the root parent node for each label
3. Classify labels into market share groups (UMG, Sony, Warner, Independent)

This approach handles complex nested ownership that would be impossible to resolve with simple pattern matching.

## Quantitative Analysis

The [`notebooks/market_share_alpha.ipynb`](notebooks/market_share_alpha.ipynb) notebook explores market share trends of the big 3 with moving averages, momentum signals, Z-score deviations, rate-of-change indicators, correlations of market share vs UMG.AS, WMG, SONY stock prices, and lead/lag analysis.

To run the analysis:
```bash
# Open the notebook (Data is already fetched by Airflow)
jupyter notebook notebooks/market_share_alpha.ipynb
```

## Quickstart

```bash
cp .env.example .env  # Add Spotify credentials for enrichment
docker compose up --build
```

Open Airflow at `localhost:8080` (admin/admin). Trigger `setup_musicbrainz_data`, then unpause `music_market_share_ingest`.

Query results:
```bash
duckdb data/music_warehouse.duckdb "
  SELECT parent_group, SUM(total_streams) as streams
  FROM fact_market_share GROUP BY 1 ORDER BY 2 DESC
"
```

## Data Sources

| Source | Description | Update |
|--------|-------------|--------|
| [Kworb](https://kworb.net) | Daily Spotify global charts | Daily (Airflow) |
| [MusicBrainz](https://musicbrainz.org) | Label ownership relationships | One-off / Manual |
| Spotify API | Track → album → label metadata | Daily (Airflow) |
| Yahoo Finance | UMG.AS, WMG, SONY stock prices | Daily (Airflow) |

## Future Work

- **Merlin integration** — Cross-reference indie labels against Merlin member list to distinguish independent from indie distributed by major.
- **Backtesting framework** — Build simple trading strategies based on market share momentum signals
- **Granger causality** — Statistical tests to validate predictive relationships
- **Earnings correlation** — Map quarterly market share trends to earnings surprises
- **Multi-platform expansion** — Add Apple Music, Amazon Music for broader coverage

## License

MIT
