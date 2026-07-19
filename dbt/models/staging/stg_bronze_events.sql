with source as (
    select * from {{ source('bronze', 'bronze_events') }}
),

renamed as (
    select
        event_id,
        date_id,
        customer_id,
        product_id,
        session_id,
        event_type,
        ingestion_time
    from source
    where event_id is not null
)

select * from renamed
