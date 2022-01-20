{{

    config(
        materialized='view',
        tags=['ust', 'stablecoins', 'aggregation']
    )

}}

with
ust_transfers as (
  
    select * from {{ ref('stg_ust_transfers') }}
  
),


agg as (

  select
  
  	date_trunc('d', block_timestamp) as date,
    sum(event_amount) as amount,
  	avg(event_amount) as avg_amount,
  	max(event_amount) as max_amount,
  	min(event_amount) as min_amount,
    median(event_amount) as median_amount,
  	count(1) as tx_count
  
  from ust_transfers
  group by 1
  order by 1
  
),

windows as (

  select
  
    *,
    avg(amount) over (order by date rows between 6 preceding and current row) as gross_weekly_ma,
    avg(avg_amount) over (order by date rows between 6 preceding and current row) as avg_weekly_ma,
    avg(tx_count) over (order by date rows between 6 preceding and current row) as count_weekly_ma,
    avg(median_amount) over (order by date rows between 6 preceding and current row) as median_weekly_ma
  
  
  from agg
  
)

select * from windows