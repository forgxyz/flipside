{{
    config(
        materialized='table',
        tags=['anchor','borrow','collateral','bluna']
    )
}}

with daily_bluna_change as (

    select * from {{ ref('anchor_borrow_net_daily_bluna') }}
    
),

luna_price as (

    select * from {{ ref('price_luna') }}

),

daily_avg_price as (

    select
    
        date_trunc('d', block_timestamp) as date,
        avg(price_usd) as avg_price
    
    from luna_price
    group by 1

),

combo as (

    select
    
        daily_bluna_change.date,
        daily_bluna_change.cumulative_bluna,
        daily_avg_price.avg_price
    
    from daily_bluna_change
    left join daily_avg_price using (date)
),

final as (

    select 

        *,
        cumulative_bluna * avg_price as bluna_value

    from combo

)

select * from final