{{ config(materialized='table') }}

with recursive hierarchy as (
    -- Anchor member: select all labels as root nodes, ensures labels with no parent still appear
select 
        label_id,
        label_name,
        label_id as parent_id,
        label_name as parent_name,
        0 as depth,
        cast(label_name as varchar) as path
    from {{ ref('stg_musicbrainz_labels') }}

    union all

    -- Recursive member: join labels to their parents
select 
        h.label_id,
        h.label_name,
        r.parent_label_id as parent_id,
        r.parent_label_name as parent_name,
        h.depth + 1 as depth,
        cast(h.path || ' > ' || r.parent_label_name as varchar) as path
    from hierarchy h
    join {{ ref('int_label_relationships') }} r 
        on h.parent_id = r.child_label_id
    -- CRITICAL: Only follow strong ownership links
    where r.relationship_type in ('label ownership', 'imprint')
    -- Stop infinite loops (safety brake)
    and h.depth < 10
),

ranked_hierarchy as (
    -- Only keep the deepest path for each label (ultimate parent label)
    select
        *,
        row_number() over (partition by label_id order by depth desc) as rn
    from hierarchy
)

select
    label_id,
    label_name,
    parent_name as ultimate_parent_name,
    path as ownership_path,
    case 
        when parent_name ilike '%Universal Music%' then 'Universal Music Group'
        when parent_name ilike '%Sony Music%' then 'Sony Music Entertainment'
        when parent_name ilike '%Warner Music%' then 'Warner Music Group'
        else 'Independent / Other'
    end as market_share_group
from ranked_hierarchy
where rn = 1
