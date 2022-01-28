{{
    config(
        materialized='incremental',
        cluster_by='block_timestamp',
        unique_key='tx_id',
        tags=['anchor','borrow','collateral','bluna']
    )
}}

with bluna_deposits_col_five as (

    select
        
        *
    
    from {{ source('terra_sv', 'msgs')}}

    where {{ incremental_load_filter('block_timestamp') }}
        and tx_status = 'SUCCEEDED'
        and msg_value:contract = 'terra1kc87mu460fwkqte29rquh4hc20m54fxwtsx7gp' --bLUNA
        and msg_value:execute_msg:send:msg = 'eyJkZXBvc2l0X2NvbGxhdGVyYWwiOnt9fQ=='
        and block_timestamp > '2021-09-30T23:59:59Z'

),

-- message structure for deposits changed with col-5 network upgrade, so have to pull prior txs
bluna_deposits_col_four as (

    select
        
        *

    from {{ source('terra_sv', 'msgs') }}

    where {{ incremental_load_filter('block_timestamp') }}
        and tx_status = 'SUCCEEDED'
        and msg_value:execute_msg:send:msg LIKE '%deposit_collateral%'
        and block_timestamp < '2021-10-01T00:00:00Z'

),

all_bluna as (

    select * from bluna_deposits_col_five
    union
    select * from bluna_deposits_col_four

)

select * from all_bluna
