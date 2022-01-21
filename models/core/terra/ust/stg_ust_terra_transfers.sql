{{
    config(
        materialized='incremental',
        unique_key='tx_id',
        tags=['core', 'ust', 'stablecoin', 'transfers'],
        cluster_by=['block_timestamp']
    )
}}



with
ust_transfers as (

    select

        *

    from {{ source('terra', 'transfers') }}
    where {{ incremental_load_filter("block_timestamp") }}
    and event_currency = 'UST'

)

select * from ust_transfers