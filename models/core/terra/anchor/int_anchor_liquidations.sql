{{
    config(
        materialized='view',
        tags=['anchor', 'borrow', 'collateral', 'liquidate'],
        unique_key='tx_id'
    )
}}

with liquidations as (

    select * from {{ ref('stg_anchor_liquidations') }}

),

final as (

    select

        block_timestamp,
        tx_id,
        chain_id,
        event_attributes:"0_borrower"::string as borrower,
        event_attributes:"0_contract_address"::string as collateral_contract,

        case
            when event_attributes:"0_collateral_amount" is not null then event_attributes:"0_collateral_amount"::float / pow(10,6)
            else event_attributes:collateral_amount::float / pow(10,6) 
        end as liquidated_amount,

        event_attributes:"0_repay_amount"::float /pow(10,6) as loan_repay_amount,

        case
            when event_attributes:liquidator is not null then event_attributes:liquidator::string
            else event_attributes:"0_liquidator"::string 
        end as liquidator

    from liquidations

)

select * from final