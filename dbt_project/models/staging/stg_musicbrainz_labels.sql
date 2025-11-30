with raw_data as (
    select * from {{ source('musicbrainz', 'raw_musicbrainz_labels') }}
),

flattened as (
    select 
        mb_id as label_id,
        name as label_name,
        label_code,
        relations
    from raw_data
)

select * from flattened