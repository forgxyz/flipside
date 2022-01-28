{{
    config(
        materialized='table',
        tags=['anchor','borrow','collateral','bluna']
    )
}}

with withdraw_txs as (

    select * from {{ ref('stg_anchor_borrow_bluna_withdraw_txs') }}

),

withdrawals as (

    select

        block_timestamp,
        chain_id,
        tx_id,
        msg_value:execute_msg:withdraw_collateral:amount::float / pow(10,6) as amount,
        msg_value:sender as sender,
        'bluna' as collateral,
        'withdraw' as action

    from withdraw_txs

)

select * from withdrawals