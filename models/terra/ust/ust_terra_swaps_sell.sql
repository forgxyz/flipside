{{

    config(
        materialized='view',
        tags=['ust', 'stablecoins', 'swaps', 'aggregation']
    )

}}

-- sell means ust is the ask currency and some token is the offer 

with
ust_swaps as (

    select * from {{ ref('stg_ust_terra_swaps') }}
    where ask_currency = 'UST'
),

sell_swaps as (

    select 

        date_trunc('d', block_timestamp) as date,
        avg(token_1_amount) as avg_amount,
        sum(token_1_amount) as gross_amount,
        min(token_1_amount) as min_amount,
        max(token_1_amount) as max_amount,
        median(token_1_amount) as median_amount

    from ust_swaps
    group by 1
    order by 1

),

final as (

    select

        *,
        avg(gross_amount) over (order by date rows between 6 preceding and current row) as gross_amount_ma

    from sell_swaps

)

select * from final