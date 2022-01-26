{{
    config(
        materialized='incremental',
        tags=['anchor', 'borrow', 'collateral', 'liquidation'],
        unique_key='tx_id',
        cluster_by='block_timestamp'
    )
}}

with liquidations as (

    select
        
        *

    from {{ source('terra_sv', 'msg_events') }}
    where {{ incremental_load_filter('block_timestamp') }}
        and event_type = 'from_contract'
        and event_attributes:"0_action" LIKE '%liquidate_collateral%'
        and tx_status = 'SUCCEEDED'

)

select * from liquidations