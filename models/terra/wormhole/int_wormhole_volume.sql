with inbound as (

    select
        date_trunc('day', block_timestamp) as date,
        symbol,
        sum(amount) as amount
    from {{ ref('fct_wormhole_inbound') }}
    group by 1,2

),

inbound_native as (

    select
        date_trunc('day', block_timestamp) as date,
        denom as symbol,
        sum(amount) as amount
    from {{ ref('fct_wormhole_inbound_native') }}
    group by 1,2
),

outbound as (

    select
        date_trunc('day', block_timestamp) as date,
        symbol,
        sum(amount) as amount
    from {{ ref('fct_wormhole_outbound') }}
    group by 1,2

),

outbound_native as (

    select
        date_trunc('day', block_timestamp) as date,
        case when wh_token_id = 'c756e61' then 'uluna' else 'uusd' end as symbol,
        sum(amount) as amount
    from {{ ref('fct_wormhole_outbound_native') }}
    group by 1,2

),

inbound_all as (

    select *, 'inbound' as direction from inbound
    union
    select *, 'inbound' as direction from inbound_native

),

outbound_all as (

    select *, 'outbound' as direction from outbound
    union
    select *, 'outbound' as direction from outbound_native

),

wormhole as (

    select * from inbound_all
    union
    select * from outbound_all

),

final as (

    select
        date,
        symbol,
        direction,
        sum(amount) as amount
    from wormhole
    group by 1,2,3
    order by 1,2
)

select * from final