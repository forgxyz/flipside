{{
    config(
        materialized='table',
        tags=['anchor','borrow','collateral']
    )
}}

with b_deposits as (

    select * from {{ ref('int_anchor_borrow_collateral_deposits') }}

),

b_withdrawals as (

    select * from {{ ref('int_anchor_borrow_collateral_withdrawals') }}

),

b_actions as (

    select * from b_deposits
    union
    select * from b_withdrawals

)

select * from b_actions