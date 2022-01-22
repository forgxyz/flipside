{{ 
    config(
        materialized='incremental',
        tags=['core', 'anchor', 'earn', 'redemption', 'aust'],
        cluster_by=['block_timestamp']
    )
}}


with
redemptions as (

    select
        
        *

    from {{ source('terra_sv', 'msgs') }}
    where {{ incremental_load_filter('block_timestamp') }}
        and tx_status = 'SUCCEEDED'
  	    and msg_value:contract = 'terra1hzh9vpxhsk8253se0vv5jj6etdvxu3nv8z07zu' -- aUST contract
  	    and 
            -- col 4
            (msg_value:execute_msg:send:msg LIKE '%redeem_stable%'
            or
            -- col 5
                (msg_value:execute_msg:send:contract = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s' -- anchor market contract
                and msg_value:execute_msg:send:msg = 'eyJyZWRlZW1fc3RhYmxlIjp7fX0=') --col 5 withdraw message
  	    )
)

select * from redemptions