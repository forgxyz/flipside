-- Buying TOKEN with UST
WITH buys AS (
  SELECT
    date_trunc('hour', block_timestamp) as date,
    avg(ust/token) as avg_buy_price
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
    date_trunc('hour', block_timestamp) as date,
    avg(ust/token) as avg_sell_price
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
  *,
  avg((avg_buy_price+avg_sell_price)/2) OVER (ORDER BY sells.date ROWS BETWEEN 7 PRECEDING AND CURRENT ROW) as moving_avg -- using 8 hr = 1/3 of a day
FROM buys
JOIN sells ON sells.date = buys.date
WHERE buys.date > GETDATE() - interval '2 weeks'
