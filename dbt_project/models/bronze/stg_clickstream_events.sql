-- Thin pass-through staging view over the bronze source table. Keeps
-- downstream/gold models decoupled from the raw table's name/location,
-- and is the attachment point for bronze-level renames or casts if the
-- raw event shape changes later.
select
    event_id,
    user_id,
    session_id,
    event_type,
    category,
    product_id,
    price,
    timestamp
from {{ source('lakehouse', 'bronze_clickstream_events') }}
