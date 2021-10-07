-- Buying TOKEN with UST // UST -> TOKEN swap
WITH buys AS (
  SELECT
    date_trunc('day', block_timestamp) as date,
    sum(token) as quantity_bought,
    sum(ust) as volume_bought
  FROM (
    SELECT
      *,
      event_attributes:offer_amount / pow(10,6) as ust,
      event_attributes:return_amount / pow(10,6) as token
    FROM terra.msg_events
    WHERE tx_status = 'SUCCEEDED'
      AND event_type = 'from_contract'
      AND event_attributes:"0_action" = 'swap'
      AND event_attributes:"0_contract_address" = 'terra...' -- swap contract
      AND event_attributes:"1_action" = 'transfer'
      AND event_attributes:"1_contract_address" = 'terra...' -- token contract
    )
  GROUP BY 1
  ORDER BY 1),

-- Selling TOKEN to UST
sells AS (
  SELECT
    date_trunc('day', block_timestamp) as date,
    sum(token) as quantity_sold,
    sum(ust) as volume_sold
  FROM (
    SELECT
      *,
      event_attributes:offer_amount / pow(10,6) as token,
      event_attributes:return_amount / pow(10,6) as ust
    FROM terra.msg_events
    WHERE tx_status = 'SUCCEEDED'
      AND event_type = 'from_contract'
      AND event_attributes:"0_action" = 'send'
      AND event_attributes:"0_contract_address" = 'terra...' -- token contract
      AND event_attributes:"1_action" = 'swap'
      AND event_attributes:"1_contract_address" = 'terra...' -- swap contract
    )
  GROUP BY 1
  ORDER BY 1)

SELECT
  buys.date as date,
  volume_bought,
  volume_sold,
  volume_bought + volume_sold as gross_volume_buy_sell
  -- optional, if quantity is of note
  -- quantity_bought,
  -- -quantity_sold as sold_neg,
  -- quantity_bought - quantity_sold as net_daily_token_volume,
FROM buys
JOIN sells ON sells.date = buys.date
WHERE buys.date > GETDATE() - interval '2 weeks'
ORDER BY 1
