-- Thin pass-through staging view over the silver source table (already
-- deduped/cast/stamped with ingestion_ts by stream_to_silver.py). The
-- gold-layer session aggregation model (next ROADMAP increment) builds
-- on this rather than querying the source table directly.
select
    event_id,
    user_id,
    session_id,
    event_type,
    category,
    product_id,
    price,
    timestamp,
    ingestion_ts
from {{ source('lakehouse', 'silver_clickstream_events') }}
