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
        avg(token_0_amount) as avg_amount,
        sum(token_0_amount) as gross_amount,
        min(token_0_amount) as min_amount,
        max(token_0_amount) as max_amount,
        median(token_0_amount) as median_amount

    from ust_swaps
    group by 1
    order by 1

),

final as (

    select

        *,
        avg(gross_amount) over (order by date rows between 6 preceding and current row) as gross_amount_ma
        
    from buy_swaps

)

select * from final