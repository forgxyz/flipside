with tokens as (

    select
        address as token_address,
        name as symbol,
        asset_chain,
        decimals
    from {{ source('forgash', 'wormhole_token_contracts') }}

)

select * from tokens