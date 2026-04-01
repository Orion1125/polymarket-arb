from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import httpx

UTC = timezone.utc
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class CertaintyBotError(RuntimeError):
    """Base error for certainty bot failures."""


class PreflightError(CertaintyBotError):
    """Raised when operator action is required before the bot can run."""


def utc_now() -> datetime:
    return datetime.now(UTC)


def to_iso8601(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    normalized = value.strip().replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


def parse_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def opposite_side(side: str) -> str:
    return "no" if side == "yes" else "yes"


def default_state() -> dict[str, Any]:
    return {
        "current_position": None,
        "pending_reentry": None,
        "active_market": None,
        "last_actions": {},
        "updated_at": to_iso8601(utc_now()),
    }


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


class JsonFileStore:
    def __init__(self, path: Path, default_factory: Callable[[], Any]) -> None:
        self.path = path
        self.default_factory = default_factory

    def load(self) -> Any:
        if not self.path.exists():
            data = self.default_factory()
            self.save(data)
            return data
        with self.path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def save(self, data: Any) -> None:
        ensure_parent(self.path)
        temp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, sort_keys=True)
            handle.write("\n")
        temp_path.replace(self.path)


class StateStore:
    def __init__(self, path: Path) -> None:
        self._store = JsonFileStore(path, default_state)

    def load(self) -> dict[str, Any]:
        state = self._store.load()
        if not isinstance(state, dict):
            state = default_state()
        normalized = default_state()
        normalized.update(state)
        normalized["last_actions"] = dict(normalized.get("last_actions") or {})
        return normalized

    def save(self, state: dict[str, Any]) -> None:
        payload = default_state()
        payload.update(state)
        payload["updated_at"] = to_iso8601(utc_now())
        self._store.save(payload)


class NotificationStore:
    def __init__(self, queue_path: Path, log_path: Path) -> None:
        self.queue_store = JsonFileStore(queue_path, list)
        self.log_path = log_path

    def record(
        self,
        *,
        event_type: str,
        message: str,
        market_id: Optional[str] = None,
        side: Optional[str] = None,
        sent: bool = False,
    ) -> dict[str, Any]:
        entry = {
            "id": str(uuid.uuid4()),
            "event_type": event_type,
            "message": message,
            "market_id": market_id,
            "side": side,
            "created_at": to_iso8601(utc_now()),
            "sent": sent,
        }
        queue = self.queue_store.load()
        if not isinstance(queue, list):
            queue = []
        queue.append(entry)
        self.queue_store.save(queue)

        ensure_parent(self.log_path)
        with self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, sort_keys=True))
            handle.write("\n")

        return entry


@dataclass
class BotConfig:
    simmer_api_key: str
    trading_venue: str
    entry_target: float
    entry_band: float
    take_profit: float
    take_profit_band: float
    stop_loss: float
    stop_loss_floor: float
    reentry_floor: float
    same_side_reentry_gap: float
    min_minutes_left: int
    final_minute_seconds: int
    position_size: float
    poll_interval_seconds: float
    position_refresh_interval_seconds: float
    request_timeout_seconds: float
    request_retries: int
    source: str
    state_path: Path
    notification_queue_path: Path
    bot_log_path: Path
    notification_log_path: Path
    dry_run: bool

    @classmethod
    def from_env(cls, *, dry_run: bool) -> "BotConfig":
        api_key = os.environ.get("SIMMER_API_KEY", "").strip()
        if not api_key:
            raise PreflightError("SIMMER_API_KEY is required.")

        return cls(
            simmer_api_key=api_key,
            trading_venue=os.environ.get("TRADING_VENUE", "sim").strip().lower() or "sim",
            entry_target=float(os.environ.get("CERTAINTY_ENTRY_TARGET", "0.80")),
            entry_band=float(os.environ.get("CERTAINTY_ENTRY_BAND", "0.01")),
            take_profit=float(os.environ.get("CERTAINTY_TAKE_PROFIT", "0.95")),
            take_profit_band=float(os.environ.get("CERTAINTY_TAKE_PROFIT_BAND", "0.005")),
            stop_loss=float(os.environ.get("CERTAINTY_STOP_LOSS", "0.75")),
            stop_loss_floor=float(os.environ.get("CERTAINTY_STOP_LOSS_FLOOR", "0.50")),
            reentry_floor=float(os.environ.get("CERTAINTY_REENTRY_FLOOR", "0.60")),
            same_side_reentry_gap=float(os.environ.get("CERTAINTY_SAME_SIDE_REENTRY_GAP", "0.05")),
            min_minutes_left=int(os.environ.get("CERTAINTY_MIN_MINUTES_LEFT", "10")),
            final_minute_seconds=int(os.environ.get("CERTAINTY_FINAL_MINUTE_SECONDS", "60")),
            position_size=float(os.environ.get("CERTAINTY_POSITION_SIZE", "500")),
            poll_interval_seconds=float(os.environ.get("CERTAINTY_POLL_INTERVAL_SECONDS", "5")),
            position_refresh_interval_seconds=float(
                os.environ.get("CERTAINTY_POSITION_REFRESH_INTERVAL_SECONDS", "30")
            ),
            request_timeout_seconds=float(os.environ.get("CERTAINTY_REQUEST_TIMEOUT_SECONDS", "15")),
            request_retries=int(os.environ.get("CERTAINTY_REQUEST_RETRIES", "3")),
            source=os.environ.get("CERTAINTY_TRADE_SOURCE", "sdk:certainty-bot-v5"),
            state_path=Path(os.environ.get("CERTAINTY_STATE_PATH", "certainty_bot_state.json")),
            notification_queue_path=Path(
                os.environ.get("CERTAINTY_NOTIFICATION_QUEUE_PATH", "notification_queue.json")
            ),
            bot_log_path=Path(os.environ.get("CERTAINTY_BOT_LOG_PATH", "certainty_bot.log")),
            notification_log_path=Path(os.environ.get("CERTAINTY_NOTIFICATION_LOG_PATH", "notifications.log")),
            dry_run=dry_run,
        )


@dataclass
class SidePrice:
    side: str
    best_bid: Optional[float]
    best_ask: Optional[float]
    last_trade_price: Optional[float]
    decision_price: Optional[float]
    source: str


@dataclass
class MarketPriceSnapshot:
    yes: SidePrice
    no: SidePrice

    def by_side(self, side: str) -> SidePrice:
        return self.yes if side == "yes" else self.no


def min_remaining_seconds(config: BotConfig) -> int:
    return max(config.min_minutes_left * 60, config.final_minute_seconds)


def choose_lowest_price_candidate(
    price_snapshot: MarketPriceSnapshot,
    predicate: Callable[[str, float], bool],
) -> Optional[tuple[str, float]]:
    candidates: list[tuple[float, str]] = []
    for side in ("yes", "no"):
        price = price_snapshot.by_side(side).decision_price
        if price is None:
            continue
        if predicate(side, price):
            candidates.append((price, side))
    if not candidates:
        return None
    price, side = min(candidates, key=lambda item: (item[0], item[1]))
    return side, price


def select_entry_candidate(
    config: BotConfig,
    price_snapshot: MarketPriceSnapshot,
    *,
    seconds_left: float,
) -> Optional[tuple[str, float]]:
    if seconds_left < min_remaining_seconds(config):
        return None
    lower_bound = config.entry_target - config.entry_band
    upper_bound = config.entry_target + config.entry_band
    return choose_lowest_price_candidate(
        price_snapshot,
        lambda _side, price: lower_bound <= price <= upper_bound,
    )


def should_take_profit(
    config: BotConfig,
    position: dict[str, Any],
    price_snapshot: MarketPriceSnapshot,
) -> Optional[float]:
    held_side = position["side"]
    side_price = price_snapshot.by_side(held_side).decision_price
    if side_price is None:
        return None
    threshold = config.take_profit - config.take_profit_band
    return side_price if side_price >= threshold else None


def should_trigger_stop_loss(
    config: BotConfig,
    position: dict[str, Any],
    price_snapshot: MarketPriceSnapshot,
) -> Optional[float]:
    if position.get("entry_kind") == "reentry":
        return None
    held_side = position["side"]
    side_price = price_snapshot.by_side(held_side).decision_price
    if side_price is None:
        return None
    if config.stop_loss_floor <= side_price <= config.stop_loss:
        return side_price
    return None


def select_reentry_candidate(
    config: BotConfig,
    pending_reentry: dict[str, Any],
    price_snapshot: MarketPriceSnapshot,
) -> Optional[tuple[str, float]]:
    original_side = pending_reentry["original_side"]
    last_stop_price = float(pending_reentry["last_stop_price"])

    def qualifies(side: str, price: float) -> bool:
        if price < config.reentry_floor:
            return False
        if side == original_side:
            return price >= last_stop_price + config.same_side_reentry_gap
        return True

    return choose_lowest_price_candidate(price_snapshot, qualifies)


def setup_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("certainty_bot")
    if logger.handlers:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()

    ensure_parent(log_path)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger


class SimmerApi:
    def __init__(
        self,
        *,
        api_key: str,
        timeout_seconds: float,
        request_retries: int,
        logger: logging.Logger,
    ) -> None:
        self.logger = logger
        self.request_retries = request_retries
        self.client = httpx.Client(
            base_url="https://api.simmer.markets",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            timeout=timeout_seconds,
        )

    def close(self) -> None:
        self.client.close()

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(self.request_retries + 1):
            try:
                response = self.client.request(method, path, **kwargs)
            except httpx.HTTPError as exc:
                last_error = exc
                if attempt >= self.request_retries:
                    raise CertaintyBotError(f"Simmer request failed: {exc}") from exc
                time.sleep(0.5 * (2**attempt))
                continue

            if response.status_code in RETRYABLE_STATUS_CODES and attempt < self.request_retries:
                time.sleep(0.5 * (2**attempt))
                continue

            if response.is_error:
                try:
                    payload = response.json()
                except json.JSONDecodeError:
                    payload = {"detail": response.text}
                detail = payload.get("fix") or payload.get("detail") or response.text
                raise CertaintyBotError(
                    f"Simmer API error {response.status_code} on {method} {path}: {detail}"
                )

            if not response.content:
                return {}
            return response.json()

        if last_error is not None:
            raise CertaintyBotError(f"Simmer request failed: {last_error}") from last_error
        raise CertaintyBotError(f"Simmer request failed: {method} {path}")

    def get_agent_me(self) -> dict[str, Any]:
        return self._request("GET", "/api/sdk/agents/me")

    def get_settings(self) -> dict[str, Any]:
        return self._request("GET", "/api/sdk/settings")

    def get_briefing(self, *, since: Optional[str] = None) -> dict[str, Any]:
        params = {"since": since} if since else None
        return self._request("GET", "/api/sdk/briefing", params=params)

    def get_fast_markets(self, *, asset: str, window: str, venue: str) -> list[dict[str, Any]]:
        payload = self._request(
            "GET",
            "/api/sdk/fast-markets",
            params={"asset": asset, "window": window, "venue": venue, "limit": 50},
        )
        return list(payload.get("markets") or [])

    def get_market_context(self, market_id: str) -> dict[str, Any]:
        return self._request("GET", f"/api/sdk/context/{market_id}")

    def get_positions(self, *, venue: str) -> dict[str, Any]:
        return self._request("GET", "/api/sdk/positions", params={"venue": venue})

    def trade(
        self,
        *,
        market_id: str,
        side: str,
        venue: str,
        dry_run: bool,
        amount: Optional[float] = None,
        shares: Optional[float] = None,
        action: Optional[str] = None,
        reasoning: Optional[str] = None,
        source: Optional[str] = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "market_id": market_id,
            "side": side,
            "venue": venue,
            "dry_run": dry_run,
        }
        if amount is not None:
            body["amount"] = amount
        if shares is not None:
            body["shares"] = shares
        if action:
            body["action"] = action
        if reasoning:
            body["reasoning"] = reasoning
        if source:
            body["source"] = source
        return self._request("POST", "/api/sdk/trade", json=body)


class PolymarketMarketData:
    def __init__(self, *, timeout_seconds: float, request_retries: int) -> None:
        self.request_retries = request_retries
        self.client = httpx.Client(base_url="https://clob.polymarket.com", timeout=timeout_seconds)

    def close(self) -> None:
        self.client.close()

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        for attempt in range(self.request_retries + 1):
            try:
                response = self.client.request(method, path, **kwargs)
            except httpx.HTTPError as exc:
                if attempt >= self.request_retries:
                    raise CertaintyBotError(f"Polymarket request failed: {exc}") from exc
                time.sleep(0.3 * (2**attempt))
                continue

            if response.status_code in RETRYABLE_STATUS_CODES and attempt < self.request_retries:
                time.sleep(0.3 * (2**attempt))
                continue

            if response.is_error:
                raise CertaintyBotError(
                    f"Polymarket API error {response.status_code} on {method} {path}: {response.text}"
                )
            return response.json()

        raise CertaintyBotError(f"Polymarket request failed: {method} {path}")

    @staticmethod
    def _extract_best_bid(book: dict[str, Any]) -> Optional[float]:
        bids = [parse_float(level.get("price")) for level in (book.get("bids") or [])]
        values = [value for value in bids if value is not None]
        return max(values) if values else None

    @staticmethod
    def _extract_best_ask(book: dict[str, Any]) -> Optional[float]:
        asks = [parse_float(level.get("price")) for level in (book.get("asks") or [])]
        values = [value for value in asks if value is not None]
        return min(values) if values else None

    @classmethod
    def normalize_book_price(
        cls,
        *,
        side: str,
        book: dict[str, Any],
        fallback_price: Optional[float],
    ) -> SidePrice:
        best_bid = cls._extract_best_bid(book)
        best_ask = cls._extract_best_ask(book)
        last_trade_price = parse_float(book.get("last_trade_price"))
        if best_ask is not None:
            return SidePrice(
                side=side,
                best_bid=best_bid,
                best_ask=best_ask,
                last_trade_price=last_trade_price,
                decision_price=best_ask,
                source="best_ask",
            )
        if last_trade_price is not None:
            return SidePrice(
                side=side,
                best_bid=best_bid,
                best_ask=best_ask,
                last_trade_price=last_trade_price,
                decision_price=last_trade_price,
                source="last_trade_price",
            )
        return SidePrice(
            side=side,
            best_bid=best_bid,
            best_ask=best_ask,
            last_trade_price=last_trade_price,
            decision_price=fallback_price,
            source="simmer_fallback",
        )

    def get_book(self, token_id: str) -> dict[str, Any]:
        return self._request("GET", "/book", params={"token_id": token_id})

    def get_market_prices(
        self,
        *,
        yes_token_id: str,
        no_token_id: str,
        simmer_yes_price: Optional[float],
    ) -> MarketPriceSnapshot:
        yes_book = self.get_book(yes_token_id)
        no_book = self.get_book(no_token_id)
        fallback_yes = simmer_yes_price
        fallback_no = None if fallback_yes is None else max(0.0, min(1.0, 1.0 - fallback_yes))
        yes = self.normalize_book_price(side="yes", book=yes_book, fallback_price=fallback_yes)
        no = self.normalize_book_price(side="no", book=no_book, fallback_price=fallback_no)
        return MarketPriceSnapshot(yes=yes, no=no)


class CertaintyBot:
    def __init__(
        self,
        *,
        config: BotConfig,
        simmer_api: SimmerApi,
        market_data: PolymarketMarketData,
        state_store: StateStore,
        notification_store: NotificationStore,
        logger: logging.Logger,
    ) -> None:
        self.config = config
        self.simmer_api = simmer_api
        self.market_data = market_data
        self.state_store = state_store
        self.notification_store = notification_store
        self.logger = logger
        self.state = self.state_store.load()

    def load_state(self) -> dict[str, Any]:
        self.state = self.state_store.load()
        return self.state

    def save_state(self) -> None:
        self.state_store.save(self.state)

    def record_event(
        self,
        *,
        event_type: str,
        message: str,
        dedupe_key: str,
        market_id: Optional[str] = None,
        side: Optional[str] = None,
    ) -> None:
        if self.state["last_actions"].get(dedupe_key):
            return
        self.notification_store.record(
            event_type=event_type,
            message=message,
            market_id=market_id,
            side=side,
            sent=False,
        )
        self.state["last_actions"][dedupe_key] = to_iso8601(utc_now())
        self.save_state()

    def log(self, level: int, message: str) -> None:
        self.logger.log(level, message)

    def preflight(self) -> tuple[dict[str, Any], dict[str, Any]]:
        if self.config.trading_venue != "sim":
            raise PreflightError("Only TRADING_VENUE=sim is supported in v1.")

        agent = self.simmer_api.get_agent_me()
        is_claimed = bool(agent.get("claimed")) or agent.get("status") in {"claimed", "active"}
        if not is_claimed:
            raise PreflightError("The configured Simmer agent is not claimed yet.")

        settings = self.simmer_api.get_settings()
        if settings.get("trading_paused"):
            if self.config.dry_run:
                self.log(
                    logging.WARNING,
                    "Simmer trading is paused for this agent, but continuing because --dry-run is enabled.",
                )
            else:
                raise PreflightError(
                    "Simmer trading is currently paused for this agent. Unpause it in Simmer before running the bot."
                )

        return agent, settings

    @staticmethod
    def _market_sort_key(market: dict[str, Any]) -> tuple[datetime, datetime]:
        opens_at = parse_datetime(market.get("opens_at")) or datetime.max.replace(tzinfo=UTC)
        resolves_at = parse_datetime(market.get("resolves_at")) or datetime.max.replace(tzinfo=UTC)
        return resolves_at, opens_at

    def select_active_market(self, markets: list[dict[str, Any]], *, now: datetime) -> Optional[dict[str, Any]]:
        live_candidates = [market for market in markets if market.get("is_live_now")]
        if live_candidates:
            return sorted(live_candidates, key=self._market_sort_key)[0]

        open_candidates: list[dict[str, Any]] = []
        for market in markets:
            opens_at = parse_datetime(market.get("opens_at"))
            resolves_at = parse_datetime(market.get("resolves_at"))
            if not opens_at or not resolves_at:
                continue
            if opens_at <= now < resolves_at:
                open_candidates.append(market)
        if open_candidates:
            return sorted(open_candidates, key=self._market_sort_key)[0]
        return None

    def market_record_from_payload(self, market: dict[str, Any]) -> Optional[dict[str, Any]]:
        market_id = market.get("id")
        resolves_at = parse_datetime(market.get("resolves_at"))
        opens_at = parse_datetime(market.get("opens_at"))
        yes_token_id = market.get("polymarket_token_id")
        no_token_id = market.get("polymarket_no_token_id")
        if not market_id or not resolves_at or not yes_token_id or not no_token_id:
            return None
        return {
            "market_id": market_id,
            "question": market.get("question"),
            "url": market.get("url"),
            "opens_at": to_iso8601(opens_at) if opens_at else None,
            "resolves_at": to_iso8601(resolves_at),
            "yes_token_id": yes_token_id,
            "no_token_id": no_token_id,
            "simmer_yes_price": parse_float(
                market.get("external_price_yes") or market.get("current_probability") or market.get("current_price")
            ),
            "is_live_now": bool(market.get("is_live_now")),
            "tags": list(market.get("tags") or []),
        }

    def update_active_market(self, market_record: Optional[dict[str, Any]]) -> None:
        self.state["active_market"] = market_record
        self.save_state()

    def cleanup_expired_state(self, *, now: datetime) -> None:
        pending = self.state.get("pending_reentry")
        if pending:
            resolves_at = parse_datetime(pending.get("resolves_at"))
            if resolves_at and now >= resolves_at:
                self.log(logging.INFO, "Pending re-entry expired with the market interval.")
                self.state["pending_reentry"] = None

        active_market = self.state.get("active_market")
        current_position = self.state.get("current_position")
        if active_market and not current_position and not self.state.get("pending_reentry"):
            resolves_at = parse_datetime(active_market.get("resolves_at"))
            if resolves_at and now >= resolves_at:
                self.state["active_market"] = None

        self.save_state()

    def fetch_live_market_record(self, *, now: datetime) -> Optional[dict[str, Any]]:
        markets = self.simmer_api.get_fast_markets(asset="BTC", window="15m", venue="polymarket")
        selected = self.select_active_market(markets, now=now)
        if not selected:
            self.log(logging.INFO, "No active imported BTC 15m market is live right now.")
            return None
        market_record = self.market_record_from_payload(selected)
        if not market_record:
            self.log(logging.WARNING, "Skipping market because required Simmer market fields are missing.")
            return None
        return market_record

    def ensure_market_record(self, *, now: datetime, allow_expired_state: bool = False) -> Optional[dict[str, Any]]:
        active_market = self.state.get("active_market")
        if active_market:
            resolves_at = parse_datetime(active_market.get("resolves_at"))
            if allow_expired_state or (resolves_at and now < resolves_at):
                return active_market
        live_market = self.fetch_live_market_record(now=now)
        if live_market:
            self.update_active_market(live_market)
        return live_market

    def fetch_price_snapshot(self, market_record: dict[str, Any]) -> MarketPriceSnapshot:
        simmer_yes = parse_float(market_record.get("simmer_yes_price"))
        return self.market_data.get_market_prices(
            yes_token_id=market_record["yes_token_id"],
            no_token_id=market_record["no_token_id"],
            simmer_yes_price=simmer_yes,
        )

    def execute_buy(
        self,
        *,
        market_record: dict[str, Any],
        side: str,
        price: float,
        entry_kind: str,
    ) -> None:
        message = (
            f"{'DRY RUN would enter' if self.config.dry_run else 'Entering'} {side.upper()} "
            f"on {market_record['question']} at decision price {price:.3f}"
        )
        self.log(logging.INFO, message)
        reasoning = (
            f"Certainty v5 {entry_kind} entry on BTC 15m market. "
            f"Decision price {price:.3f} is inside the configured certainty band."
        )
        response = self.simmer_api.trade(
            market_id=market_record["market_id"],
            side=side,
            amount=self.config.position_size,
            venue=self.config.trading_venue,
            dry_run=self.config.dry_run,
            reasoning=reasoning,
            source=self.config.source,
        )
        if not response.get("success"):
            raise CertaintyBotError(f"Buy trade failed: {response}")

        if self.config.dry_run:
            self.record_event(
                event_type=f"{entry_kind}_entry_dry_run",
                message=message,
                dedupe_key=f"dry-run-entry:{market_record['market_id']}:{entry_kind}:{side}:{price:.3f}",
                market_id=market_record["market_id"],
                side=side,
            )
            return

        shares_bought = parse_float(response.get("shares_bought")) or 0.0
        self.state["current_position"] = {
            "market_id": market_record["market_id"],
            "question": market_record["question"],
            "side": side,
            "shares": shares_bought,
            "entry_price": price,
            "entry_kind": entry_kind,
            "position_size": self.config.position_size,
            "opened_at": to_iso8601(utc_now()),
            "resolves_at": market_record["resolves_at"],
            "original_side": side if entry_kind == "initial" else self.state["pending_reentry"]["original_side"],
        }
        if entry_kind == "reentry":
            self.state["pending_reentry"] = None
        self.state["active_market"] = market_record
        self.save_state()
        self.record_event(
            event_type=f"{entry_kind}_entry",
            message=message,
            dedupe_key=f"entry:{market_record['market_id']}:{entry_kind}:{side}",
            market_id=market_record["market_id"],
            side=side,
        )

    def execute_sell(
        self,
        *,
        position: dict[str, Any],
        price: float,
        reason: str,
        event_type: str,
        dedupe_key: str,
        keep_reentry_pending: bool,
    ) -> None:
        market_id = position["market_id"]
        side = position["side"]
        question = position.get("question", market_id)
        message = (
            f"{'DRY RUN would exit' if self.config.dry_run else 'Exiting'} {side.upper()} "
            f"on {question} at decision price {price:.3f} because {reason}"
        )
        self.log(logging.INFO, message)

        response = self.simmer_api.trade(
            market_id=market_id,
            side=side,
            shares=parse_float(position.get("shares")) or 0.0,
            action="sell",
            venue=self.config.trading_venue,
            dry_run=self.config.dry_run,
            reasoning=f"Certainty v5 exit: {reason}. Decision price {price:.3f}.",
            source=self.config.source,
        )
        if not response.get("success"):
            raise CertaintyBotError(f"Sell trade failed: {response}")

        if not self.config.dry_run:
            self.state["current_position"] = None
            if not keep_reentry_pending:
                self.state["pending_reentry"] = None
            self.save_state()

        self.record_event(
            event_type=event_type,
            message=message,
            dedupe_key=dedupe_key,
            market_id=market_id,
            side=side,
        )

    def handle_open_position(
        self,
        *,
        market_record: dict[str, Any],
        position: dict[str, Any],
        price_snapshot: MarketPriceSnapshot,
        now: datetime,
    ) -> None:
        resolves_at = parse_datetime(position.get("resolves_at") or market_record.get("resolves_at"))
        if resolves_at and now >= resolves_at:
            held_price = price_snapshot.by_side(position["side"]).decision_price or 0.0
            self.execute_sell(
                position=position,
                price=held_price,
                reason="interval closed",
                event_type="flatten",
                dedupe_key=f"flatten:{position['market_id']}",
                keep_reentry_pending=False,
            )
            self.state["active_market"] = None
            self.save_state()
            return

        take_profit_price = should_take_profit(self.config, position, price_snapshot)
        if take_profit_price is not None:
            self.execute_sell(
                position=position,
                price=take_profit_price,
                reason="take profit hit",
                event_type="take_profit",
                dedupe_key=f"tp:{position['market_id']}:{position['side']}",
                keep_reentry_pending=False,
            )
            self.state["active_market"] = None
            self.save_state()
            return

        stop_loss_price = should_trigger_stop_loss(self.config, position, price_snapshot)
        if stop_loss_price is not None:
            if not self.config.dry_run:
                original_side = position.get("original_side") or position["side"]
                pending_reentry = {
                    "market_id": position["market_id"],
                    "question": position.get("question"),
                    "original_side": original_side,
                    "last_stop_price": stop_loss_price,
                    "resolves_at": position.get("resolves_at") or market_record["resolves_at"],
                }
                self.state["pending_reentry"] = pending_reentry
                self.save_state()
            self.execute_sell(
                position=position,
                price=stop_loss_price,
                reason="stop loss hit",
                event_type="stop_loss",
                dedupe_key=f"sl:{position['market_id']}:{position['side']}:{stop_loss_price:.3f}",
                keep_reentry_pending=True,
            )
            if not self.config.dry_run:
                self.attempt_reentry(market_record=market_record, price_snapshot=price_snapshot)
            return

        held_side = position["side"]
        held_price = price_snapshot.by_side(held_side).decision_price
        self.log(logging.INFO, f"Holding {held_side.upper()} at {held_price:.3f} on {position['question']}.")

    def attempt_initial_entry(
        self,
        *,
        market_record: dict[str, Any],
        price_snapshot: MarketPriceSnapshot,
        now: datetime,
    ) -> None:
        resolves_at = parse_datetime(market_record["resolves_at"])
        if not resolves_at:
            return
        seconds_left = (resolves_at - now).total_seconds()
        candidate = select_entry_candidate(self.config, price_snapshot, seconds_left=seconds_left)
        if not candidate:
            self.log(
                logging.INFO,
                f"No initial entry for {market_record['question']}: prices not in band or entry window closed.",
            )
            return

        context = self.simmer_api.get_market_context(market_record["market_id"])
        if context.get("market", {}).get("status") != "active":
            self.log(logging.INFO, "Skipping entry because Simmer context says market is not active.")
            return

        side, price = candidate
        self.execute_buy(market_record=market_record, side=side, price=price, entry_kind="initial")

    def attempt_reentry(
        self,
        *,
        market_record: dict[str, Any],
        price_snapshot: MarketPriceSnapshot,
    ) -> None:
        pending = self.state.get("pending_reentry")
        if not pending:
            return
        candidate = select_reentry_candidate(self.config, pending, price_snapshot)
        if not candidate:
            self.log(logging.INFO, f"Pending re-entry is not qualified yet for {pending.get('question')}.")
            return

        side, price = candidate
        self.execute_buy(market_record=market_record, side=side, price=price, entry_kind="reentry")

    def run_cycle(self) -> None:
        self.load_state()
        now = utc_now()
        self.cleanup_expired_state(now=now)

        current_position = self.state.get("current_position")
        pending_reentry = self.state.get("pending_reentry")
        market_record = self.ensure_market_record(now=now, allow_expired_state=bool(current_position))

        if not market_record:
            if current_position:
                self.log(logging.WARNING, "No live market record available while a position is open.")
            return

        try:
            price_snapshot = self.fetch_price_snapshot(market_record)
        except CertaintyBotError as exc:
            self.record_event(
                event_type="pricing_error",
                message=str(exc),
                dedupe_key=f"pricing-error:{market_record['market_id']}:{datetime.utcnow().date()}",
                market_id=market_record["market_id"],
            )
            raise

        if current_position:
            self.handle_open_position(
                market_record=market_record,
                position=current_position,
                price_snapshot=price_snapshot,
                now=now,
            )
            return

        if pending_reentry:
            resolves_at = parse_datetime(pending_reentry.get("resolves_at"))
            if resolves_at and now >= resolves_at:
                self.log(logging.INFO, "Re-entry window expired before a new position qualified.")
                self.state["pending_reentry"] = None
                self.state["active_market"] = None
                self.save_state()
                return
            self.attempt_reentry(market_record=market_record, price_snapshot=price_snapshot)
            return

        self.attempt_initial_entry(market_record=market_record, price_snapshot=price_snapshot, now=now)

    def run_once(self) -> None:
        agent, settings = self.preflight()
        self.log(
            logging.INFO,
            f"Preflight OK for agent {agent.get('name')} on venue {self.config.trading_venue}. "
            f"Max trade amount={settings.get('sdk_max_trade_amount')}.",
        )
        self.run_cycle()

    def run_loop(self) -> None:
        agent, settings = self.preflight()
        self.log(
            logging.INFO,
            f"Starting loop for agent {agent.get('name')} on venue {self.config.trading_venue}. "
            f"Poll interval={self.config.poll_interval_seconds}s.",
        )
        self.log(logging.INFO, f"Simmer max trade amount is {settings.get('sdk_max_trade_amount')}.")
        try:
            while True:
                try:
                    self.run_cycle()
                except Exception as exc:  # noqa: BLE001
                    self.log(logging.ERROR, f"Cycle failed: {exc}")
                    self.record_event(
                        event_type="cycle_error",
                        message=str(exc),
                        dedupe_key=f"cycle-error:{datetime.utcnow().isoformat(timespec='minutes')}",
                    )
                time.sleep(self.config.poll_interval_seconds)
        except KeyboardInterrupt:
            self.log(logging.INFO, "Loop interrupted by operator.")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Certainty Trading Bot v5")
    parser.add_argument("--mode", choices=("once", "loop"), default="once")
    parser.add_argument("--dry-run", action="store_true", help="Simulate trades without posting them.")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    try:
        config = BotConfig.from_env(dry_run=args.dry_run)
    except PreflightError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    logger = setup_logger(config.bot_log_path)
    state_store = StateStore(config.state_path)
    notification_store = NotificationStore(config.notification_queue_path, config.notification_log_path)
    simmer_api = SimmerApi(
        api_key=config.simmer_api_key,
        timeout_seconds=config.request_timeout_seconds,
        request_retries=config.request_retries,
        logger=logger,
    )
    market_data = PolymarketMarketData(
        timeout_seconds=config.request_timeout_seconds,
        request_retries=config.request_retries,
    )

    bot = CertaintyBot(
        config=config,
        simmer_api=simmer_api,
        market_data=market_data,
        state_store=state_store,
        notification_store=notification_store,
        logger=logger,
    )

    try:
        if args.mode == "loop":
            bot.run_loop()
        else:
            bot.run_once()
        return 0
    except PreflightError as exc:
        logger.error(str(exc))
        return 2
    except Exception as exc:  # noqa: BLE001
        logger.error("Bot failed: %s", exc)
        return 1
    finally:
        simmer_api.close()
        market_data.close()


if __name__ == "__main__":
    raise SystemExit(main())
