{{
    config(
        materialized='table',
        tags=['anchor','borrow','collateral','bluna']
    )
}}

with bluna_deposits as (

    select * from {{ ref('int_anchor_borrow_bluna_deposits') }}

),

bluna_withdrawals as (

    select * from {{ ref('int_anchor_borrow_bluna_withdrawals') }}

),

bluna_actions as (

    select * from bluna_deposits
    union
    select * from bluna_withdrawals

)

select * from bluna_actions