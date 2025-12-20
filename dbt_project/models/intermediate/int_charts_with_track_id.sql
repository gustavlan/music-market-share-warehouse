{{ config(materialized='view') }}

with charts as (
    select
        chart_date,
        track_name,
        artist_name,
        daily_streams,
        extracted_at,
        -- Normalized keys for matching
        {{ normalize_text('track_name') }} as normalized_track_name,
        {{ normalize_text('artist_name') }} as normalized_artist_name
    from {{ ref('stg_combined_charts') }}
),

metadata as (
    select
        normalized_track_name,
        normalized_artist_name,
        spotify_track_id,
        -- Use latest metadata if duplicates (deterministic tie-break)
        row_number() over (
            partition by normalized_track_name, normalized_artist_name
            order by updated_at desc
        ) as rn
    from {{ ref('stg_track_metadata_normalized') }}
),

latest_metadata as (
    select * from metadata where rn = 1
)

select
    c.chart_date,
    c.track_name,
    c.artist_name,
    c.daily_streams,
    c.extracted_at,
    c.normalized_track_name,
    c.normalized_artist_name,
    m.spotify_track_id
from charts c
left join latest_metadata m
    on c.normalized_track_name = m.normalized_track_name
    and c.normalized_artist_name = m.normalized_artist_name
