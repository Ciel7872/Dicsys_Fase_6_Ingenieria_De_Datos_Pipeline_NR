with staged_events as (
    select * from {{ ref('stg_bronze_events') }}
),

deduplicated as (
    select
        event_id,
        date_id,
        customer_id,
        product_id,
        session_id,
        event_type,
        ingestion_time,
        row_number() over (partition by event_id order by ingestion_time asc) as rn
    from staged_events
    where event_id is not null
      and event_type is not null
)

select
    event_id,
    date_id,
    customer_id,
    product_id,
    session_id,
    event_type,
    ingestion_time,
    current_timestamp() as transformed_at
from deduplicated
where rn = 1
