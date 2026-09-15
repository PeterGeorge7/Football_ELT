with 
    raw_generated_data as (
        {{ dbt_date.get_date_dimension("2020-01-01", "2030-12-31") }}
    )

    , date_key as (
        select 
            to_char(date_day, 'YYYYMMDD')::integer as date_key,
            raw_generated_data.*
        from raw_generated_data
    )

select *
from date_key