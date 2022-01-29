{{
    config(
        materialized='table',
        tags=['anchor','collateral','beth']
    )
}}

with beth_actions as (

    select * from {{ ref('anchor_borrow_collateral_actions') }}
    where collateral = 'terra10cxuzggyvvv44magvrh3thpdnk9cmlgk93gmx2' -- bETH
    
),

liquidations as (

    select * from {{ ref('anchor_daily_liquidations') }}
    where collateral_contract = 'terra10cxuzggyvvv44magvrh3thpdnk9cmlgk93gmx2' --bETH

),

net_daily_action as (

    select
        date_trunc('d', block_timestamp) as date,
        case
            when action = 'deposit' then amount
        end as deposit_amount,
        case
            when action = 'withdraw' then amount
        end as withdraw_amount
    
    from beth_actions

),

action_agg as (

    select 
        
        date,
        sum(deposit_amount) as gross_deposit,
        sum(withdraw_amount) as gross_withdraw

    from net_daily_action
    group by 1
),

actions as (

    select

        *,
        gross_deposit - gross_withdraw as net_borrower_action

    from action_agg

),

add_liquidations as (

    select

        actions.date,
        actions.gross_deposit,
        actions.gross_withdraw,
        actions.net_borrower_action,
        coalesce(liquidations.gross_liquidated,0) as gross_beth_liquidated
        

    from actions
    left join liquidations using (date)

),

final_daily_change as (

    select

        *,
        net_borrower_action - gross_beth_liquidated as daily_change

    from add_liquidations

),

final as (

    select

        *,
        sum(daily_change) over (order by date) as cumulative_beth
    
    from final_daily_change

)

select * from final