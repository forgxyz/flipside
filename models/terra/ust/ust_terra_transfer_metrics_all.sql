{{

    config(
        materialized='incremental',
        tags=['ust', 'stablecoins', 'aggregation']
    )

}}

with
ust_transfer_metrics as (

    select * from {{ ref('ust_transfer_metrics') }}

),

ust_transfer_user_metrics as (

    select * from {{ ref('ust_transfer_user_metrics') }}

),

final as (

    select * from ust_transfer_metrics
    left join ust_transfer_user_metrics using (date)
)

select * from final