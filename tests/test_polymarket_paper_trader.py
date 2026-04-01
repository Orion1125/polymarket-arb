import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from polymarket_paper_trader import (
    BookLevel,
    PaperConfig,
    PaperStateStore,
    SimmerFastMarketDiscovery,
    calculate_fee_usdc,
    choose_reference_price,
    derive_fee_exponent,
    derive_fee_rate,
    filter_trade_log_entries,
    ratio_or_none,
    select_entry_candidate,
    select_reentry_candidate,
    should_take_profit,
    should_trigger_stop_loss,
    simulate_market_buy,
    simulate_market_sell,
)


def build_config(root: Path) -> PaperConfig:
    return PaperConfig(
        simmer_api_key="test-key",
        asset="BTC",
        window="15m",
        entry_target=0.80,
        entry_band=0.01,
        take_profit=0.95,
        take_profit_band=0.005,
        stop_loss=0.75,
        stop_loss_floor=0.50,
        reentry_floor=0.60,
        same_side_reentry_gap=0.05,
        reentry_stop_loss_gap=0.05,
        min_minutes_left=10,
        final_minute_seconds=60,
        position_size=500.0,
        minimum_fill_ratio=0.90,
        starting_cash=10000.0,
        poll_interval_seconds=1.0,
        request_timeout_seconds=5.0,
        request_retries=0,
        orderbook_levels=8,
        log_limit=18,
        state_path=root / "state.json",
        log_path=root / "paper.log",
    )


class FeeAndFillTests(unittest.TestCase):
    def test_fee_formula_uses_market_fee_rate_and_rounding(self):
        self.assertEqual(calculate_fee_usdc(100.0, 0.50, 0.072, 1.0), 0.9)

    def test_market_buy_charges_fee_in_shares(self):
        quote = simulate_market_buy(
            cash_budget=50.0,
            asks=[BookLevel(price=0.50, size=100.0)],
            fee_rate=0.072,
            exponent=1.0,
        )
        self.assertEqual(quote.spent_cash, 50.0)
        self.assertEqual(quote.gross_shares, 100.0)
        self.assertEqual(quote.fee_usdc, 0.9)
        self.assertEqual(quote.fee_shares, 1.8)
        self.assertEqual(quote.net_shares, 98.2)
        self.assertAlmostEqual(quote.effective_price, 50.0 / 98.2)

    def test_market_sell_charges_fee_in_cash(self):
        quote = simulate_market_sell(
            shares_to_sell=100.0,
            bids=[BookLevel(price=0.50, size=100.0)],
            fee_rate=0.072,
            exponent=1.0,
        )
        self.assertEqual(quote.gross_proceeds, 50.0)
        self.assertEqual(quote.fee_usdc, 0.9)
        self.assertEqual(quote.net_proceeds, 49.1)
        self.assertAlmostEqual(quote.effective_price, 0.491)


class PriceSelectionTests(unittest.TestCase):
    def test_reference_price_prefers_midpoint_then_last_trade(self):
        value, source = choose_reference_price(best_bid=0.48, best_ask=0.52, last_trade_price=0.55)
        self.assertEqual(source, "midpoint")
        self.assertEqual(value, 0.50)

        value, source = choose_reference_price(best_bid=0.10, best_ask=0.30, last_trade_price=0.27)
        self.assertEqual(source, "last")
        self.assertEqual(value, 0.27)

    def test_entry_candidate_uses_lowest_qualifying_effective_price(self):
        config = build_config(Path("."))
        yes_quote = simulate_market_buy(cash_budget=500.0, asks=[BookLevel(price=0.80, size=1000.0)], fee_rate=0.0, exponent=1.0)
        no_quote = simulate_market_buy(cash_budget=500.0, asks=[BookLevel(price=0.79, size=1000.0)], fee_rate=0.0, exponent=1.0)
        chosen = select_entry_candidate(config, {"yes": yes_quote, "no": no_quote}, seconds_left=10 * 60)
        self.assertEqual(chosen, ("no", 0.79))

    def test_repeat_initial_entry_is_allowed_after_tp_with_less_than_ten_minutes_left(self):
        config = build_config(Path("."))
        quote = simulate_market_buy(cash_budget=500.0, asks=[BookLevel(price=0.80, size=1000.0)], fee_rate=0.0, exponent=1.0)
        blocked = select_entry_candidate(config, {"yes": quote}, seconds_left=5 * 60, allow_repeat=False)
        allowed = select_entry_candidate(config, {"yes": quote}, seconds_left=5 * 60, allow_repeat=True)
        self.assertIsNone(blocked)
        self.assertEqual(allowed, ("yes", 0.80))

    def test_reentry_same_side_requires_gap(self):
        config = build_config(Path("."))
        pending = {"original_side": "yes", "last_stop_price": 0.60}

        too_early_same_side = simulate_market_buy(cash_budget=500.0, asks=[BookLevel(price=0.64, size=1000.0)], fee_rate=0.0, exponent=1.0)
        below_floor_other_side = simulate_market_buy(cash_budget=500.0, asks=[BookLevel(price=0.59, size=1000.0)], fee_rate=0.0, exponent=1.0)
        self.assertIsNone(select_reentry_candidate(config, pending, {"yes": too_early_same_side, "no": below_floor_other_side}))

        same_side = simulate_market_buy(cash_budget=500.0, asks=[BookLevel(price=0.65, size=1000.0)], fee_rate=0.0, exponent=1.0)
        chosen = select_reentry_candidate(config, pending, {"yes": same_side, "no": below_floor_other_side})
        self.assertEqual(chosen, ("yes", 0.65))

    def test_take_profit_and_stop_loss_use_effective_sell_prices(self):
        config = build_config(Path("."))
        position = {"side": "yes", "entry_kind": "initial"}

        tp_quote = simulate_market_sell(shares_to_sell=100.0, bids=[BookLevel(price=0.95, size=100.0)], fee_rate=0.0, exponent=1.0)
        self.assertEqual(should_take_profit(config, position, {"yes": tp_quote}), 0.95)

        sl_quote = simulate_market_sell(shares_to_sell=100.0, bids=[BookLevel(price=0.60, size=100.0)], fee_rate=0.0, exponent=1.0)
        self.assertEqual(should_trigger_stop_loss(config, position, {"yes": sl_quote}), 0.60)

    def test_reentry_positions_have_five_cent_stop_loss(self):
        config = build_config(Path("."))
        position = {"side": "yes", "entry_kind": "reentry", "entry_effective_price": 0.65}

        hold_quote = simulate_market_sell(shares_to_sell=100.0, bids=[BookLevel(price=0.61, size=100.0)], fee_rate=0.0, exponent=1.0)
        self.assertIsNone(should_trigger_stop_loss(config, position, {"yes": hold_quote}))

        stop_quote = simulate_market_sell(shares_to_sell=100.0, bids=[BookLevel(price=0.60, size=100.0)], fee_rate=0.0, exponent=1.0)
        self.assertEqual(should_trigger_stop_loss(config, position, {"yes": stop_quote}), 0.60)

    def test_fee_exponent_prefers_crypto_tag(self):
        self.assertEqual(derive_fee_exponent(["sdk-import", "crypto", "fast"]), 1.0)

    def test_fee_rate_prefers_crypto_tag(self):
        self.assertEqual(derive_fee_rate(["sdk-import", "crypto", "fast"]), 0.072)

    def test_ratio_or_none_handles_zero_base(self):
        self.assertEqual(ratio_or_none(2.5, 10.0), 0.25)
        self.assertIsNone(ratio_or_none(2.5, 0.0))

    def test_filter_trade_log_entries_keeps_only_entry_tp_sl(self):
        entries = [
            {"event_type": "info", "message": "status"},
            {"event_type": "entry", "message": "bought"},
            {"event_type": "flatten", "message": "closed"},
            {"event_type": "tp", "message": "took profit"},
            {"event_type": "sl", "message": "stopped"},
        ]
        filtered = filter_trade_log_entries(entries, limit=10)
        self.assertEqual([entry["event_type"] for entry in filtered], ["entry", "tp", "sl"])


class PersistenceTests(unittest.TestCase):
    def test_state_store_initializes_wallet(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = PaperStateStore(Path(temp_dir) / "paper_state.json", 2500.0)
            state = store.load()
            self.assertEqual(state["wallet"]["cash"], 2500.0)


class DiscoverySelectionTests(unittest.TestCase):
    def test_future_market_is_not_tradeable(self):
        discovery = SimmerFastMarketDiscovery(api_key="test", timeout_seconds=5.0, request_retries=0)
        try:
            markets = [
                {
                    "id": "future",
                    "opens_at": "2026-04-01T01:30:00Z",
                    "resolves_at": "2026-04-01T01:45:00Z",
                    "is_live_now": False,
                }
            ]
            now = datetime.fromisoformat("2026-04-01T00:56:00+00:00")
            self.assertIsNone(discovery.select_active_market(markets, now=now))
            self.assertEqual(discovery.select_next_market(markets, now=now)["id"], "future")
        finally:
            discovery.close()


if __name__ == "__main__":
    unittest.main()
