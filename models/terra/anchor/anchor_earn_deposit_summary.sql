{{ 
    config(
        materialized='table',
        tags=['anchor', 'earn', 'deposit']
    )
}}

with deposits as (

    select * from {{ ref('int_anchor_earn_deposit_txs') }}

),

aggregations as (

    select

        date_trunc('d', block_timestamp) as date,
        sum(amount) as gross_deposit_amount,
        avg(amount) as avg_deposit,
        min(amount) as min_deposit,
        max(amount) as max_deposit,
        count(1) as deposit_tx_count

    from deposits
    group by 1

),

final as (

    select 
        *,
        sum(gross_deposit_amount) over (order by date) as cumulative_gross_deposit_amount,
        avg(avg_deposit) over (order by date rows between 6 preceding and current row) as avg_deposit_ma,
        avg(max_deposit) over (order by date rows between 6 preceding and current row) as max_deposit_ma,
        avg(min_deposit) over (order by date rows between 6 preceding and current row) as min_deposit_ma,
        avg(deposit_tx_count) over (order by date rows between 6 preceding and current row) as deposit_tx_count_ma

    from aggregations

)

select * from final