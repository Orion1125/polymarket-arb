# Certainty Trading Bot v5

BTC 15m Up/Down prediction markets on Polymarket/Simmer with fast-market support and dynamic re-entry.

## Core Strategy

### Entry Logic
- **Entry target**: 80c (configurable via `CERTAINTY_ENTRY_TARGET`)
- **Entry band**: ±1c (79-81c, configurable via `CERTAINTY_ENTRY_BAND`)
- **Time gate**: Only enter with 10+ minutes remaining in interval
- **Min time left**: 1 minute (no entries in final minute)

### Exit Logic
- **Take Profit**: 95c (±0.5c band)
- **Stop Loss**: 75c down to 50c floor (priority: 75c first, then 74c, 73c... down to 50c)
- **No stop loss on re-entries**: Hold until TP

## Stop Loss Band (75c → 50c)

- **Priority**: 75c first, then 74c, 73c, 72c... down to 50c floor
- **Below 50c**: SL does NOT trigger (below floor)
- **Fast market support**: If price jumps 80c → 60c, SL triggers at 60c
- **If price jumps below 50c** (e.g., 80c → 45c): No SL trigger (below floor)

## Re-Entry Band (60c and above)

- **Floor**: 60c (re-entry can happen at 60c, 61c, 62c... up to 95c+)
- **Priority**: 60c first, then 61c, 62c, etc. (lower prices have priority)
- **Direction**: WHICHEVER direction (YES or NO) is in the band
- **Selection**: If both YES@62c and NO@68c, re-enter YES@62c (lowest price wins)
- **No stop loss on re-entry**: Hold until TP (95c)

### Same-Side Re-Entry Gap (5c)

If re-entry is on the **SAME side** as the original entry, add a **5c gap** before re-entry:

- **Original entry**: YES@80c
- **SL triggered**: at 60c
- **Price recovery**: YES moves back up to 65c
- **Re-entry**: YES@65c (60c + 5c gap, not at 60c)

**Why?** Prevents re-entering immediately when price is still volatile. The 5c gap gives the market time to stabilize and confirms the reversal is genuine.

**Opposite-side re-entry**: No gap required. If original was YES@80c → SL@60c → NO at 60c, re-enter NO@60c immediately.

## Example Flows

```
Normal Entry + TP:
  Entry: YES@80c → TP@95c → Profit

Normal Entry + SL + Re-entry (opposite side):
  Entry: YES@80c → SL@75c → Re-enter NO@75c → TP@95c

Fast Market Jump (SL triggers):
  Entry: YES@80c → Price jumps to 60c → SL@60c → Re-enter NO@60c → TP@95c

Fast Market Jump (Below floor - no SL):
  Entry: YES@80c → Price jumps to 45c → NO SL (below 50c floor) → Position held

Same-Side Re-Entry with 5c Gap:
  Entry: YES@80c → SL@60c → Price recovers to 65c → Re-enter YES@65c (60c + 5c gap)

Opposite-Side Re-Entry (no gap):
  Entry: YES@80c → SL@60c → NO at 60c → Re-enter NO@60c (immediate, no gap)

Recovery Re-entry (opposite side):
  Entry: YES@80c → SL@65c → Price at 70c (NO side) → Re-enter NO@70c

Multiple Jumps:
  Entry: YES@80c → SL@55c → Re-enter NO@55c → SL@85c → Re-enter YES@85c → TP@95c
```

## Configuration

```bash
CERTAINTY_ENTRY_TARGET=0.80      # Entry target price
CERTAINTY_ENTRY_BAND=0.01        # ±1c band
CERTAINTY_TAKE_PROFIT=0.95       # TP price
CERTAINTY_STOP_LOSS=0.75         # SL price (75c, with 50c floor)
CERTAINTY_MIN_MINUTES_LEFT=10    # Min time for entry
CERTAINTY_POSITION_SIZE=500      # $SIM per trade
CERTAINTY_NOTIFY_CHAT_ID=telegram:6596436573  # Telegram target
TRADING_VENUE=sim                # Paper trading only
```

## Technical Requirements

### Market Discovery
- **Primary**: Polymarket CLOB API (`https://clob.polymarket.com/markets`)
- **Fallback**: Simmer fast-markets endpoint
- **Filter**: BTC 15m intervals only

### Execution
- **Platform**: Simmer (`https://api.simmer.markets`)
- **Venue**: `sim` (paper trading)
- **Rate limiting**: 5s polling interval, 30s position check interval

### State Management
- **State file**: `certainty_bot_state.json`
- **Notification queue**: `notification_queue.json`
- **Logs**: `certainty_bot.log`, `notifications.log`

## Bot Behavior

### Entry Conditions
1. Price in 79-81c band (±1c)
2. 10-1 minutes remaining in interval
3. No open positions
4. Market is active (not resolved/halted)

### Exit Conditions
1. **TP**: Price reaches 95c (±0.5c)
2. **SL**: Price in 75c-50c band (triggers at actual price, floor at 50c)
3. **Interval end**: Flatten all positions at interval close

### Re-Entry Conditions
1. SL triggered on existing position
2. Current price in 60c+ band (60c to 100c)
3. WHICHEVER direction (YES or NO) has the lowest price in band
4. **Same-side re-entry**: Add 5c gap (e.g., SL@60c → re-entry@65c)
5. **Opposite-side re-entry**: No gap (e.g., SL@60c → re-entry@60c)
6. No stop loss on re-entry (hold until TP)

## Files

| File | Purpose |
|------|---------|
| `certainty_bot.py` | Main bot code |
| `certainty_bot_state.json` | Position state and notification history |
| `notification_queue.json` | Pending notifications for retry |
| `notifications.log` | All notification logs |
| `certainty_bot.log` | Bot execution logs |

## Running the Bot

```bash
set -a && source /home/azureuser/.openclaw/workspace/secrets/simmer.env
export CERTAINTY_NOTIFY_CHAT_ID='telegram:6596436573'
export CERTAINTY_POSITION_SIZE='500'
set +a
cd /home/azureuser/.openclaw/workspace-agent-j
python3 certainty_bot.py --mode loop --interval 1
```

## Key Features

✅ **SL Band**: 75c down to 50c floor (priority: 75c first)
✅ **Re-entry Band**: 60c and above (priority: 60c first, lowest price wins)
✅ **Same-side re-entry gap**: 5c gap to avoid volatile re-entries
✅ **Opposite-side re-entry**: No gap, immediate execution
✅ **Fast market support**: Handles price jumps (80c → 60c in one tick)
✅ **Any direction re-entry**: Re-enters in whichever direction has lowest price
✅ **Telegram alerts**: Working via OpenClaw CLI
✅ **Paper trading**: Safe $SIM execution
✅ **State persistence**: Survives restarts
✅ **Notification deduplication**: No spam from repair exits
✅ **Rate limit safe**: 5s polling, 30s position checks

## Risk Management

- **Position size**: 500 $SIM per trade (max Simmer limit)
- **No position carry**: All positions flatten at interval end
- **Paper trading only**: `venue=sim` for safe testing
- **Rate limiting**: 5s polling, 30s position checks to avoid 429 errors

## Troubleshooting

### No Telegram notifications?
- Check `notifications.log` for errors
- Verify `openclaw message send` works manually
- Ensure `CERTAINTY_NOTIFY_CHAT_ID` is set correctly

### No trades?
- Price must hit 79-81c entry zone
- Must have 10+ minutes remaining in interval
- Check `certainty_bot.log` for entry signal logs

### Re-entry not happening?
- Price must be in 60c+ band
- Same-side re-entry requires 5c gap (e.g., SL@60c → re-entry@65c)
- Check `certainty_bot_state.json` for position status

---

**Version**: 5.0
**Last Updated**: 2026-03-31
**Author**: Orion / Jude
