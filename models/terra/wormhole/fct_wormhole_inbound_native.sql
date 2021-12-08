-- as currently laid out, there's no way to see the origin chain from the tables (directly). would need to parse VAA message
with inbound as (

    select
        block_timestamp,
        denom,
        amount_raw / pow(10,6) as amount,
        recipient
    from {{ ref('stg_wormhole_native_inbound') }}

)

select * from inbound