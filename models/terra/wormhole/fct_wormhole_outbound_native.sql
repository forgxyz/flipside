with transactions as (

    select *
    from {{ ref('stg_wormhole_txs') }}
    where direction = 'outbound'
        and event_attributes:"1_contract_address" = 'terra1dq03ugtd40zu9hcgdzrsq6z2z4hwhc9tqk2uy5'
),

parse_attributes as (

    select
        block_timestamp,
        event_attributes:"transfer.recipient_chain" as asset_chain, -- destination
        event_attributes:"transfer.token" as token_id,
        event_attributes:"transfer.amount" / pow(10,6) as amount,
        tx_id
    from transactions

),

chain_ids as (

    select * from {{ ref('stg_wormhole_chain_ids') }}

),

final as (

    select
        parse_attributes.block_timestamp,
        parse_attributes.amount,
        tx_id,
        case when token_id[0] is null then token_id else token_id[0]:denom end as wh_token_id,
        chain_ids.chain_name
    from parse_attributes
    join chain_ids using (asset_chain)
        
)

select * from final