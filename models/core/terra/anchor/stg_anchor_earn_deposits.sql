{{ 
    config(
        materialized='incremental',
        tags=['core', 'anchor', 'earn', 'depsoit'],
        cluster_by=['block_timestamp']
    )
}}

with
deposits as (

    select
        
        *
    
    from {{ source('terra_sv', 'msgs' )}}

    where {{ incremental_load_filter('block_timestamp') }}
    and tx_status = 'SUCCEEDED'
  	and msg_value:contract = 'terra1sepfj7s0aeg5967uxnfk4thzlerrsktkpelm5s' -- anchor market contract
  	and msg_value:execute_msg LIKE '%deposit_stable%'

)

select * from deposits