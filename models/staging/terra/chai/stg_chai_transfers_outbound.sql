-- outbound meaning from the chai dapp
-- aka from a labeled chai address

with outbound as (

    select
        block_timestamp,
        chain_id,
        tx_id,
        event_from as from_address,
        event_from_address_label as from_label,
        event_to as to_address,
        event_to_address_label as to_label,
        event_amount as amount,
        event_amount_usd as amount_usd,
        event_currency as currency

    from {{ source('terra', 'transfers') }}
    
    where tx_status = 'SUCCEEDED' -- filter out failed transactions
      and (event_to_address_label = 'chai' and event_from_address_label is null)

)

select * from outbound