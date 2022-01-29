{{
    config(
        materialized='incremental',
        cluster_by='block_timestamp',
        tags=['price','luna']
    )
}}

with prices as (

    select *
    from {{ source('terra','oracle_prices') }}
    where {{ incremental_load_filter('block_timestamp') }}
        and symbol = 'LUNA'

)

select * from prices