{{
    config(
        materialized='incremental',
        unique_key='tx_id',
        tags=['core', 'ust', 'stablecoin'],
        cluster_by=['block_timestamp']
    )
}}



with
ust_transfers as (

    select

        *

    from {{ source('terra', 'transfers') }}
    where event_currency = 'UST'

)

select * from ust_transfers