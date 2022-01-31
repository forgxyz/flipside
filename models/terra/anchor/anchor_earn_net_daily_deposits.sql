{{
    config(
        materialized='table',
        tags=['anchor','earn','deposit','redemption']
    )
}}

with deposits as (

    select * from {{ ref('anchor_earn_deposit_summary') }}

),

redemptions as (

    select * from {{ ref('anchor_earn_redemption_summary') }}

),

liquidations as (

    select * from {{ ref('anchor_daily_liquidations') }}

),

loans_repaid as (

    select

        date,
        sum(gross_loan_repaid) as gross_loan_repaid

    from liquidations
    group by 1

),

depositor_activity as (

    select

        deposits.date,
        deposits.gross_deposit_amount as daily_gross_deposit,
        redemptions.gross_redemption_amount as daily_gross_redemption,
        deposits.gross_deposit_amount - redemptions.gross_redemption_amount as net_depositor_activity

    from deposits
    left join redemptions using (date)

),

balances as (

    select
        
        depositor_activity.date,
        daily_gross_deposit,
        daily_gross_redemption,
        net_depositor_activity,
        coalesce(gross_loan_repaid,0) as gross_loan_repaid,
        net_depositor_activity - coalesce(gross_loan_repaid,0) as earn_balance_change

    from depositor_activity
    left join loans_repaid using (date)

),

final as (

    select
        
        *,
        sum(earn_balance_change) over (order by date) as cumulative_earn_balance

    from balances
)
select * from final