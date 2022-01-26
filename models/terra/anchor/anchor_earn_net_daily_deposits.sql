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

final as (

    select
        deposits.date,
        deposits.gross_deposit_amount as daily_gross_deposit,
        redemptions.gross_redemption_amount as daily_gross_redemption,
        deposits.gross_deposit_amount - redemptions.gross_redemption_amount as net_depositor_activity

    from deposits
    left join redemptions using (date)

)

select * from final