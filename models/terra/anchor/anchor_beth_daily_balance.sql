{{
    config(
        materialized='table',
        tags=['anchor','borrow','collateral','beth']
    )
}}

with daily_beth_change as (

    select * from {{ ref('anchor_borrow_net_daily_beth') }}
    
),

beth_price as (

    select * from {{ ref('price_beth') }}

),

daily_avg_price as (

    select
    
        date_trunc('d', block_timestamp) as date,
        avg(price_usd) as avg_price
    
    from beth_price
    group by 1

),

combo as (

    select
    
        daily_beth_change.date,
        daily_beth_change.cumulative_beth,
        daily_avg_price.avg_price
    
    from daily_beth_change
    left join daily_avg_price using (date)
),

final as (

    select 

        *,
        cumulative_beth * avg_price as beth_value

    from combo
    where cumulative_beth is not null

)

select * from final
order by 1