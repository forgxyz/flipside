{{ 
    config(
        materialized='incremental',
        tags=['core', 'anchor', 'yield reserve'],
        cluster_by=['date']
    )
}}


with
yield_reserve as (

  select

    date,
    balance as yield_reserve

  from {{ source('terra', 'daily_balances') }}
  where {{ incremental_load_filter('date') }}
    and address = 'terra1tmnqgvg567ypvsvk6rwsga3srp7e3lg6u0elp8'
  	and currency = 'UST'

)

select * from yield_reserve