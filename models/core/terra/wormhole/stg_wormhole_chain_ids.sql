with chains as (

    select
        id as asset_chain,
        name as chain_name
    from {{ source('forgash', 'wormhole_chain_ids') }}

)

select * from chains