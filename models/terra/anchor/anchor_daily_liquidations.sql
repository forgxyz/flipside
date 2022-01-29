{{ 
    config(
        materialized='table',
        tags=['anchor', 'borrow', 'collateral', 'liquidate']
    )
}}

with liquidations as (

    select * from {{ ref('int_anchor_liquidations') }}

),

daily_liquidations as (

    select

        date_trunc('d', block_timestamp) as date,
        collateral_contract,
        sum(liquidated_amount) as gross_liquidated,
        sum(loan_repay_amount) as gross_loan_repaid

    from liquidations
    group by 1,2
    
)

select * from daily_liquidations