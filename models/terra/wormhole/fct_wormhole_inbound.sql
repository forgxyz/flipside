with transactions as (

    select * from {{ ref('stg_wormhole_txs') }} where direction = 'inbound'

),

tokens as (

    select 
        token_address,
        symbol,
        decimals,
        chain_name
    from {{ ref('stg_wormhole_tokens') }}
    join {{ ref('stg_wormhole_chain_ids') }} using (asset_chain)

),

parse_attributes as (

    select
        block_timestamp,
        event_attributes:"1_contract_address" as token_address,
        event_attributes:"0_amount" as amount_raw,
        event_attributes:recipient as recipient
    from transactions

),

final as (

    select
        parse_attributes.block_timestamp,
        parse_attributes.token_address,
        parse_attributes.amount_raw,
        parse_attributes.amount_raw / tokens.decimals as amount_adj,
        parse_attributes.recipient,
        tokens.symbol,
        tokens.chain_name as source_chain
    from parse_attributes join tokens using (token_address)

)

select * from final
