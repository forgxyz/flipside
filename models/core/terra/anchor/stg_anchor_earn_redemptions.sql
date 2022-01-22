{{ 
    config(
        materialized='incremental',
        tags=['core', 'anchor', 'earn', 'redemption'],
        cluster_by=['block_timestamp']
    )
}}

with
redemptions as (

    select
    
        *

    from {{ source('terra_sv', 'msg_events') }}

    where {{ incremental_load_filter('block_timestamp') }}
        and event_type = 'from_contract'
        and event_attributes:to = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s' -- anchor market contract
        and event_attributes:"1_action" = 'redeem_stable'
        and event_attributes:"0_contract_address" = 'terra1hzh9vpxhsk8253se0vv5jj6etdvxu3nv8z07zu' -- aUST
        and event_attributes:"1_contract_address" = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s'
        and event_attributes:"2_contract_address" = 'terra1hzh9vpxhsk8253se0vv5jj6etdvxu3nv8z07zu'

)

select * from redemptions