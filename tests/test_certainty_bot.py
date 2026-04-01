import os
import tempfile
import unittest
from pathlib import Path

from certainty_bot import (
    BotConfig,
    CertaintyBot,
    MarketPriceSnapshot,
    NotificationStore,
    PolymarketMarketData,
    PreflightError,
    SidePrice,
    StateStore,
    default_state,
    select_entry_candidate,
    select_reentry_candidate,
    setup_logger,
    should_take_profit,
    should_trigger_stop_loss,
)


def build_config(root: Path) -> BotConfig:
    return BotConfig(
        simmer_api_key="test-key",
        trading_venue="sim",
        entry_target=0.80,
        entry_band=0.01,
        take_profit=0.95,
        take_profit_band=0.005,
        stop_loss=0.75,
        stop_loss_floor=0.50,
        reentry_floor=0.60,
        same_side_reentry_gap=0.05,
        min_minutes_left=10,
        final_minute_seconds=60,
        position_size=500.0,
        poll_interval_seconds=5.0,
        position_refresh_interval_seconds=30.0,
        request_timeout_seconds=5.0,
        request_retries=0,
        source="sdk:certainty-bot-v5",
        state_path=root / "certainty_bot_state.json",
        notification_queue_path=root / "notification_queue.json",
        bot_log_path=root / "certainty_bot.log",
        notification_log_path=root / "notifications.log",
        dry_run=False,
    )


def snapshot(yes_price: float, no_price: float) -> MarketPriceSnapshot:
    return MarketPriceSnapshot(
        yes=SidePrice("yes", best_bid=None, best_ask=yes_price, last_trade_price=None, decision_price=yes_price, source="best_ask"),
        no=SidePrice("no", best_bid=None, best_ask=no_price, last_trade_price=None, decision_price=no_price, source="best_ask"),
    )


class FakeSimmerApi:
    def __init__(self, *, claimed: bool = True, paused: bool = False) -> None:
        self.claimed = claimed
        self.paused = paused

    def get_agent_me(self):
        return {"claimed": self.claimed, "status": "claimed" if self.claimed else "unclaimed", "name": "Test Agent"}

    def get_settings(self):
        return {"trading_paused": self.paused, "sdk_max_trade_amount": 100.0}

    def get_fast_markets(self, *, asset: str, window: str, venue: str):
        return []

    def get_market_context(self, market_id: str):
        return {"market": {"status": "active"}}

    def trade(self, **kwargs):
        return {"success": True, "shares_bought": 625.0, "shares_sold": kwargs.get("shares", 0.0)}

    def close(self):
        return None


class FakePolymarketData:
    def close(self):
        return None

    def get_market_prices(self, **kwargs):
        return snapshot(0.80, 0.20)


class StrategyHelperTests(unittest.TestCase):
    def test_entry_candidate_requires_10_minutes_left(self):
        config = build_config(Path("."))
        chosen = select_entry_candidate(config, snapshot(0.80, 0.82), seconds_left=9 * 60)
        self.assertIsNone(chosen)

        chosen = select_entry_candidate(config, snapshot(0.80, 0.82), seconds_left=10 * 60)
        self.assertEqual(chosen, ("yes", 0.80))

    def test_take_profit_threshold_uses_band(self):
        config = build_config(Path("."))
        position = {"side": "yes", "entry_kind": "initial"}
        hit = should_take_profit(config, position, snapshot(0.946, 0.054))
        self.assertEqual(hit, 0.946)

    def test_stop_loss_triggers_inside_band_only(self):
        config = build_config(Path("."))
        position = {"side": "yes", "entry_kind": "initial"}
        self.assertEqual(should_trigger_stop_loss(config, position, snapshot(0.60, 0.40)), 0.60)
        self.assertIsNone(should_trigger_stop_loss(config, position, snapshot(0.45, 0.55)))
        self.assertIsNone(should_trigger_stop_loss(config, {"side": "yes", "entry_kind": "reentry"}, snapshot(0.60, 0.40)))

    def test_reentry_prefers_lowest_qualifying_side(self):
        config = build_config(Path("."))
        pending = {"original_side": "yes", "last_stop_price": 0.60}
        chosen = select_reentry_candidate(config, pending, snapshot(0.65, 0.62))
        self.assertEqual(chosen, ("no", 0.62))

    def test_same_side_reentry_needs_gap(self):
        config = build_config(Path("."))
        pending = {"original_side": "yes", "last_stop_price": 0.60}
        chosen = select_reentry_candidate(config, pending, snapshot(0.64, 0.59))
        self.assertIsNone(chosen)
        chosen = select_reentry_candidate(config, pending, snapshot(0.65, 0.59))
        self.assertEqual(chosen, ("yes", 0.65))


class PersistenceTests(unittest.TestCase):
    def test_state_store_round_trip(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "certainty_bot_state.json"
            store = StateStore(state_path)
            state = default_state()
            state["current_position"] = {"market_id": "m1", "side": "yes"}
            store.save(state)
            loaded = store.load()
            self.assertEqual(loaded["current_position"]["market_id"], "m1")

    def test_notification_store_records_entry(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            queue_path = Path(temp_dir) / "notification_queue.json"
            log_path = Path(temp_dir) / "notifications.log"
            store = NotificationStore(queue_path, log_path)
            entry = store.record(event_type="entry", message="hello", market_id="m1", side="yes")
            self.assertEqual(entry["event_type"], "entry")
            self.assertTrue(queue_path.exists())
            self.assertTrue(log_path.exists())


class PreflightTests(unittest.TestCase):
    def test_paused_account_fails_preflight(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = build_config(root)
            logger = setup_logger(root / "certainty_bot.log")
            try:
                bot = CertaintyBot(
                    config=config,
                    simmer_api=FakeSimmerApi(paused=True),
                    market_data=FakePolymarketData(),
                    state_store=StateStore(root / "certainty_bot_state.json"),
                    notification_store=NotificationStore(root / "notification_queue.json", root / "notifications.log"),
                    logger=logger,
                )
                with self.assertRaises(PreflightError):
                    bot.preflight()
            finally:
                for handler in list(logger.handlers):
                    logger.removeHandler(handler)
                    handler.close()


class PolymarketNormalizationTests(unittest.TestCase):
    def test_best_ask_uses_lowest_ask_even_if_descending(self):
        book = {
            "bids": [{"price": "0.49"}, {"price": "0.45"}],
            "asks": [{"price": "0.99"}, {"price": "0.75"}, {"price": "0.51"}],
            "last_trade_price": "",
        }
        normalized = PolymarketMarketData.normalize_book_price(side="yes", book=book, fallback_price=0.50)
        self.assertEqual(normalized.best_bid, 0.49)
        self.assertEqual(normalized.best_ask, 0.51)
        self.assertEqual(normalized.decision_price, 0.51)
        self.assertEqual(normalized.source, "best_ask")

    def test_fallback_uses_last_trade_then_simmer_price(self):
        trade_book = {"bids": [], "asks": [], "last_trade_price": "0.67"}
        normalized = PolymarketMarketData.normalize_book_price(side="yes", book=trade_book, fallback_price=0.50)
        self.assertEqual(normalized.decision_price, 0.67)
        self.assertEqual(normalized.source, "last_trade_price")

        fallback_book = {"bids": [], "asks": [], "last_trade_price": ""}
        normalized = PolymarketMarketData.normalize_book_price(side="yes", book=fallback_book, fallback_price=0.50)
        self.assertEqual(normalized.decision_price, 0.50)
        self.assertEqual(normalized.source, "simmer_fallback")


class IntegrationLikeTests(unittest.TestCase):
    def test_simmer_dry_run_trade_when_env_present(self):
        api_key = os.environ.get("SIMMER_API_KEY")
        if not api_key:
            self.skipTest("SIMMER_API_KEY is not set for integration-like dry-run test.")

        from certainty_bot import SimmerApi

        api = SimmerApi(api_key=api_key, timeout_seconds=10.0, request_retries=1, logger=setup_logger(Path("certainty_bot.log")))
        try:
            markets = api.get_fast_markets(asset="BTC", window="15m", venue="polymarket")
            market = next((m for m in markets if m.get("polymarket_token_id") and m.get("polymarket_no_token_id")), None)
            if not market:
                self.skipTest("No eligible BTC 15m market is currently available.")
            response = api.trade(
                market_id=market["id"],
                side="yes",
                amount=10.0,
                venue="sim",
                dry_run=True,
                reasoning="integration test dry run",
                source="sdk:certainty-bot-v5-tests",
            )
            self.assertTrue(response.get("success"))
        finally:
            api.close()


if __name__ == "__main__":
    unittest.main()
