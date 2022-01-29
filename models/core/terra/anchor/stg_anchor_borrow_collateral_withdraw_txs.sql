{{
    config(
        materialized='incremental',
        cluster_by='block_timestamp',
        unique_key='tx_id',
        tags=['anchor','borrow','collateral','bluna']
    )
}}

with b_txs as (

    select
        
        *

    from {{ source('terra_sv', 'msgs') }} 
    where {{ incremental_load_filter('block_timestamp') }}
        and tx_status = 'SUCCEEDED'
        and msg_value:execute_msg LIKE '%withdraw_collateral%'

)

select * from b_txs