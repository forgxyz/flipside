{{
    config(
        materialized='incremental',
        cluster_by='block_timestamp',
        tags=['price','beth']
    )
}}

with prices as (

    select *
    from {{ source('terra','oracle_prices') }}
    where {{ incremental_load_filter('block_timestamp') }}
        and currency = 'terra1dzhzukyezv0etz22ud940z7adyv7xgcjkahuun'

)

select * from prices