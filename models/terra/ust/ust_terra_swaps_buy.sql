{{

    config(
        materialized='view',
        tags=['ust', 'stablecoins', 'swaps', 'aggregation']
    )

}}

-- buy means ust is the offer currency and some token is the ask 

with
ust_swaps as (

    select * from {{ ref('stg_ust_terra_swaps') }}
    where offer_currency = 'UST'
),

buy_swaps as (

    select 

        date_trunc('d', block_timestamp) as date,
        avg(token_0_amount) as buy_avg_amount,
        sum(token_0_amount) as buy_gross_amount,
        min(token_0_amount) as buy_min_amount,
        max(token_0_amount) as buy_max_amount,
        median(token_0_amount) as buy_median_amount

    from ust_swaps
    group by 1
    order by 1

),

final as (

    select

        *,
        avg(buy_gross_amount) over (order by date rows between 6 preceding and current row) as buy_gross_amount_ma
        
    from buy_swaps

)

select * from final