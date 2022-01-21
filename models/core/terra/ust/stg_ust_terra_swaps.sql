{{
    config(
        materialized='incremental',
        tags=['core', 'ust', 'stablecoin', 'swaps'],
        cluster_by=['block_timestamp']
    )
}}

with
ust_swaps as (

    select

        *
    
    from {{ source('terra', 'swaps') }}
    where {{ incremental_load_filter("block_timestamp") }}
        and (ask_currency = 'UST' or offer_currency = 'UST')
        and token_0_amount is not null
        and token_1_amount is not null

)

select * from ust_swaps