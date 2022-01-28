{{
    config(
        materialized='table',
        tags=['anchor','borrow','collateral','bluna']
    )
}}

with bluna_txs as (

    select * from {{ ref('stg_anchor_borrow_bluna_deposit_txs') }}

),

tx_info as (

    select
        
        block_timestamp,
        chain_id,
        tx_id,
        msg_value:execute_msg:send:amount::float / pow(10,6) as amount,
        msg_value:sender::string as sender,
        'bluna' as collateral,
        'deposit' as action

    from bluna_txs

)

select * from tx_info