{{
    config(
        materialized='view',
        tags=['anchor', 'earn', 'deposit', 'users']
    )
}}

with deposits as (

    select * from {{ ref('stg_anchor_earn_deposits') }}

),

final as (

    select
        
        block_timestamp,
        tx_id,
        chain_id,
        msg_value:coins[0]:amount::float / pow(10,6) as amount,
        msg_value:sender as user_address

    from deposits

)

select * from final