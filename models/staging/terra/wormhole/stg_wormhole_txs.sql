with transactions as (

    select
        block_timestamp,
        tx_id,
        case
            when event_attributes:"0_action" = 'complete_transfer_wrapped' then 'inbound'
            else 'outbound'
        end as direction,
        event_attributes
    from {{ source('terra_sv', 'msg_events') }}
    where event_type = 'from_contract'
        and block_timestamp > '2021-10-18T00:00:00Z'
        and event_attributes:"0_contract_address" = 'terra10nmmwe8r3g99a9newtqa7a75xfgs2e8z87r2sf'
)

select * from transactions