{{
    config(
        materialized='table',
        tags=['anchor','collateral','bluna']
    )
}}

with bluna_actions as (

    select * from {{ ref('anchor_borrow_collateral_actions') }}
    where collateral = 'terra1ptjp2vfjrwh0j0faj9r6katm640kgjxnwwq9kn' -- bLUNA

),

liquidations as (

    select * from {{ ref('anchor_daily_liquidations') }}
    where collateral_contract = 'terra1ptjp2vfjrwh0j0faj9r6katm640kgjxnwwq9kn' --bLUNA

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
    
    from bluna_actions

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
        coalesce(liquidations.gross_liquidated,0) as gross_bluna_liquidated
        

    from actions
    left join liquidations using (date)

),

final_daily_change as (

    select

        *,
        net_borrower_action - gross_bluna_liquidated as daily_change

    from add_liquidations

),

final as (

    select

        *,
        sum(daily_change) over (order by date) as cumulative_bluna
    
    from final_daily_change

)

select * from final