with inbound as (

    select * from {{ ref('stg_chai_transfers_inbound') }}
    where block_timestamp > '2020-12-31T23:59:59Z'

),

inbound_aggregate as (

    select

        date_trunc('day', block_timestamp) as date,
        sum(amount) as amount,
        sum(amount_usd) as amount_usd,
        count(1) as num_transactions,
        count(distinct from_address) as unique_wallet

    from inbound
    group by 1

)

select * from inbound_aggregate