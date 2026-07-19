with staged_events as (
    select * from {{ ref('stg_bronze_events') }}
),

final as (
    select
        event_id,
        date_id,
        customer_id,
        product_id,
        session_id,
        event_type,
        ingestion_time,
        current_timestamp() as transformed_at
    from staged_events
    where event_id is not null
      and event_type is not null
)

select * from final
