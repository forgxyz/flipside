# flipside
dbt models for analysis work

## TODO

### Anchor

#### Earn
- check anchor earn redemption transactions for col4 - col-5 compatability wrt user address
- add yml files and tests

#### Liquidations
- reconfigure to account for multi-liquidation bids
example: https://finder.extraterrestrial.money/mainnet/tx/4340019B042341553FA04EEA0EE80D3612837FD9C4EA0F7ABB27B17E4C6FF388

#### Collateral
- nexus. their collateral deposit messages are messy and i need to add a model for those transactions. bLUNA tracks within 1.5% of Anchor's own app and bETH around 4.5% so the present gap is not massively impactful.
