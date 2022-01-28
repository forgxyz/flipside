{{
    config(
        materialized='incremental',
        cluster_by='block_timestamp',
        unique_key='tx_id',
        tags=['anchor','borrow','collateral','bluna']
    )
}}

with bluna_txs as (

    select
        
        *

    from {{ source('terra_sv', 'msgs') }} 
    where {{ incremental_load_filter('block_timestamp') }}
        and tx_status = 'SUCCEEDED'
        and msg_value:execute_msg LIKE '%withdraw_collateral%'
        and msg_value:contract = 'terra1ptjp2vfjrwh0j0faj9r6katm640kgjxnwwq9kn' -- bLUNA 

)

select * from bluna_txs