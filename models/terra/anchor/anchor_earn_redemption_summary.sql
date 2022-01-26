{{ 
    config(
        materialized='table',
        tags=['anchor', 'earn', 'redemption']
    )
}}

with redemptions as (

    select *
    from {{ ref('int_anchor_earn_redemption_txs') }}

),

aggregations as (

    select

        date_trunc('d', block_timestamp) as date,
        sum(redemption_amount) as gross_redemption_amount,
        avg(redemption_amount) as avg_redemption,
        min(redemption_amount) as min_redemption,
        max(redemption_amount) as max_redemption,
        count(1) as redemption_tx_count

    from redemptions
    group by 1

),

final as (

    select
        *,
        sum(gross_redemption_amount) over (order by date) as cumulative_gross_redemption_amount,
        sum(avg_redemption) over (order by date rows between 6 preceding and current row) as avg_redemption_ma,
        sum(max_redemption) over (order by date rows between 6 preceding and current row) as max_redemption_ma,
        sum(min_redemption) over (order by date rows between 6 preceding and current row) as min_redemption_ma,
        sum(redemption_tx_count) over (order by date rows between 6 preceding and current row) as redemption_tx_count_ma

    from aggregations

)

select * from final