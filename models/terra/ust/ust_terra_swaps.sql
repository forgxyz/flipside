{{

    config(
        materialized='incremental',
        tags=['ust', 'stablecoins', 'swaps', 'aggregation']
    )

}}

with

sells as (

    select * from {{ ref('ust_terra_swaps_sell') }}

),

buys as (

    select * from {{ ref('ust_terra_swaps_buy') }}

),
-- ISSUE / TODO
-- duplicate column names ...
final as (

    select * from sells
    left join buys using (date)

)

select * from final