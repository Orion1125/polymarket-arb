# polymarket-arb

Local paper-trading bot for BTC 15-minute Polymarket fast markets.

## What is included

- `polymarket_paper_trader.py`: live terminal dashboard and local paper execution
- `certainty_bot.py`: Simmer-based bot implementation from the original build
- `tests/`: unit and integration-like coverage for both bots
- `start_paper_trader.cmd`: Windows launcher for the visible paper-trader shell

## Dashboard

The paper trader shows:

- live BTC spot and interval-start BTC price to beat
- current tradeable interval info
- YES and NO orderbooks plus effective share prices
- wallet equity, PnL, and fees
- successful `ENTRY`, `TP`, and `SL` trade logs only

BTC spot refresh follows the main dashboard loop. With:

```env
PAPER_POLL_INTERVAL_SECONDS=0.5
```

the dashboard and BTC spot both refresh every 500ms.

## Local run

Create a `.env` file with:

```env
SIMMER_API_KEY=your_key_here
PAPER_POLL_INTERVAL_SECONDS=0.5
PAPER_STARTING_CASH=10
CERTAINTY_POSITION_SIZE=10
```

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

Run once:

```powershell
python .\polymarket_paper_trader.py --mode once
```

Run the live dashboard:

```powershell
python .\polymarket_paper_trader.py --mode loop
```

## Tests

```powershell
python -m unittest discover -s tests -v
```
