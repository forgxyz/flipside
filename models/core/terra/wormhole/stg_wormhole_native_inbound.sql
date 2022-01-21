with inbound as (

    select

        block_timestamp,
        tx_id,
        event_attributes:denom as denom,
        event_attributes:amount as amount_raw,
        event_attributes:recipient as recipient

    from {{ source('terra_sv', 'msg_events') }}
    
    where event_type = 'from_contract'
        and block_timestamp > '2021-10-18T00:00:00Z'
        and event_attributes:action = 'complete_transfer_terra_native'
        and event_attributes:contract_address = 'terra10nmmwe8r3g99a9newtqa7a75xfgs2e8z87r2sf'

)

select * from inbound