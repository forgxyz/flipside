with transactions as (

    select *
    from {{ ref('stg_wormhole_txs') }}
    where direction = 'outbound'
        and event_attributes:"2_contract_address" = 'terra1dq03ugtd40zu9hcgdzrsq6z2z4hwhc9tqk2uy5'
),

tokens as (

    select * from {{ ref('stg_wormhole_tokens') }}

),

chain_ids as (

    select * from {{ ref('stg_wormhole_chain_ids') }}

),

parse_attributes as (

    select
        block_timestamp,
        event_attributes:"transfer.recipient_chain" as asset_chain, -- destination
        event_attributes:"1_contract_address" as token_address,
        event_attributes:"transfer.amount" as amount_raw,
        tx_id
    from transactions

),

tokens_chains as (

    select 
        tokens.token_address,
        tokens.symbol,
        tokens.decimals,
        tokens.asset_chain,
        chain_ids.chain_name
    from tokens
    join chain_ids using (asset_chain)

),

final as (

    select
        parse_attributes.block_timestamp,
        parse_attributes.amount_raw / pow(10, tokens_chains.decimals) as amount,
        parse_attributes.tx_id,
        tokens_chains.token_address,
        tokens_chains.symbol,
        tokens_chains.chain_name
    from parse_attributes
    join tokens_chains using (token_address)  
)

select * from final