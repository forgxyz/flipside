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

users as (

  select
  	block_timestamp,
    event_from,
  	event_to,
  	event_amount
  from ust_transfers
  
),

agg as (

  select
  	date_trunc('d', block_timestamp) as date,
  	count(distinct event_from) as unique_senders,
  	count(distinct event_to) as unique_recipients
  from users
  group by 1
  
),

windows as (

  select
  
    *,
    avg(unique_senders) over (order by date rows between 6 preceding and current row) as senders_ma,
    avg(unique_recipients) over (order by date rows between 6 preceding and current row) as recipients_ma
  
  from agg

)

select * from windows