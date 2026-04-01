from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx
from rich.columns import Columns
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

UTC = timezone.utc
try:
    ET = ZoneInfo("America/New_York")
    DISPLAY_TZ_LABEL = "ET"
except ZoneInfoNotFoundError:
    ET = UTC
    DISPLAY_TZ_LABEL = "UTC"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
FEE_EXPONENT_BY_TAG = {
    "crypto": 1.0,
    "sports": 1.0,
    "politics": 1.0,
    "finance": 1.0,
    "economics": 0.5,
    "weather": 0.5,
    "tech": 1.0,
    "geopolitics": 0.0,
    "culture": 1.0,
    "mentions": 2.0,
    "general": 2.0,
}
FEE_RATE_BY_TAG = {
    "crypto": 0.072,
}


class PaperTraderError(RuntimeError):
    """Base error for paper-trader failures."""


class PreflightError(PaperTraderError):
    """Raised when the trader cannot safely start."""


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


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def format_price(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value:.3f}"


def format_percent(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.1f}c"


def format_money(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"${value:,.2f}"


def format_signed_money(value: Optional[float]) -> str:
    if value is None:
        return "-"
    sign = "+" if value >= 0 else "-"
    return f"{sign}${abs(value):,.2f}"


def format_signed_percent_change(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value * 100:+.2f}%"


def format_shares(value: Optional[float]) -> str:
    if value is None:
        return "-"
    return f"{value:,.2f}"


def ratio_or_none(value: Optional[float], base: Optional[float]) -> Optional[float]:
    if value is None or base is None or abs(base) <= 1e-12:
        return None
    return value / base


def infer_price_direction(current_price: Optional[float], reference_price: Optional[float]) -> str:
    if current_price is None or reference_price is None:
        return "-"
    if abs(current_price - reference_price) <= 1e-9:
        return "FLAT"
    return "UP" if current_price > reference_price else "DOWN"


def filter_trade_log_entries(entries: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    allowed = {"entry", "sl", "tp"}
    filtered = [entry for entry in entries if str(entry.get("event_type") or "").lower() in allowed]
    if limit <= 0:
        return filtered
    return filtered[-limit:]


def floor_to_interval(value: datetime, *, minutes: int) -> datetime:
    minute_bucket = (value.minute // minutes) * minutes
    return value.replace(minute=minute_bucket, second=0, microsecond=0)


def ceil_to_interval(value: datetime, *, minutes: int) -> datetime:
    floored = floor_to_interval(value, minutes=minutes)
    if floored == value.replace(second=0, microsecond=0):
        return floored
    return floored + timedelta(minutes=minutes)


def market_slug_for_interval(asset: str, window: str, interval_start: datetime) -> str:
    return f"{asset.lower()}-updown-{window}-{int(interval_start.timestamp())}"


def derive_fee_exponent(tags: list[str]) -> float:
    lowered = {tag.lower() for tag in tags}
    for tag, exponent in FEE_EXPONENT_BY_TAG.items():
        if tag in lowered:
            return exponent
    return 1.0


def derive_fee_rate(tags: list[str]) -> float:
    lowered = {tag.lower() for tag in tags}
    for tag, rate in FEE_RATE_BY_TAG.items():
        if tag in lowered:
            return rate
    return 0.0


def calculate_fee_usdc(shares: float, price: float, fee_rate: float, exponent: float) -> float:
    if shares <= 0 or price <= 0 or fee_rate <= 0:
        return 0.0
    fee = shares * price * fee_rate * ((price * (1 - price)) ** exponent)
    rounded = round(fee + 1e-12, 5)
    return rounded if rounded >= 0.00001 else 0.0


def default_state(starting_cash: float) -> dict[str, Any]:
    return {
        "wallet": {
            "starting_cash": starting_cash,
            "cash": starting_cash,
            "realized_pnl": 0.0,
            "fees_paid_usdc": 0.0,
            "trade_count": 0,
            "wins": 0,
            "losses": 0,
        },
        "current_position": None,
        "pending_reentry": None,
        "active_market": None,
        "repeat_entry_market_id": None,
        "recent_logs": [],
        "updated_at": to_iso8601(utc_now()),
    }


class JsonFileStore:
    def __init__(self, path: Path, default_factory) -> None:
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
        temp_path = self.path.with_name(
            f"{self.path.name}.{os.getpid()}.{int(time.time() * 1000)}.tmp"
        )
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, sort_keys=True)
            handle.write("\n")
        for attempt in range(20):
            try:
                temp_path.replace(self.path)
                return
            except PermissionError:
                if attempt >= 19:
                    print(
                        f"Warning: could not update state file {self.path} because it is locked by another process.",
                        file=sys.stderr,
                    )
                    try:
                        temp_path.unlink(missing_ok=True)
                    except OSError:
                        pass
                    return
                time.sleep(0.05 * (attempt + 1))


class PaperStateStore:
    def __init__(self, path: Path, starting_cash: float) -> None:
        self.starting_cash = starting_cash
        self._store = JsonFileStore(path, lambda: default_state(starting_cash))

    def load(self) -> dict[str, Any]:
        state = self._store.load()
        if not isinstance(state, dict):
            state = default_state(self.starting_cash)
        normalized = default_state(self.starting_cash)
        normalized.update(state)
        defaults = default_state(self.starting_cash)["wallet"]
        defaults.update(dict(normalized.get("wallet") or {}))
        normalized["wallet"] = defaults
        normalized["recent_logs"] = list(normalized.get("recent_logs") or [])
        stored_starting_cash = parse_float(normalized["wallet"].get("starting_cash"))
        trade_count = int(normalized["wallet"].get("trade_count") or 0)
        if (
            stored_starting_cash is None
            or abs(stored_starting_cash - self.starting_cash) > 1e-9
        ) and trade_count == 0 and not normalized.get("current_position") and not normalized.get("pending_reentry"):
            reset_state = default_state(self.starting_cash)
            reset_state["recent_logs"] = normalized["recent_logs"]
            normalized = reset_state
        return normalized

    def save(self, state: dict[str, Any]) -> None:
        payload = default_state(self.starting_cash)
        payload.update(state)
        payload["updated_at"] = to_iso8601(utc_now())
        self._store.save(payload)


@dataclass
class PaperConfig:
    simmer_api_key: str
    asset: str
    window: str
    entry_target: float
    entry_band: float
    take_profit: float
    take_profit_band: float
    stop_loss: float
    stop_loss_floor: float
    reentry_floor: float
    same_side_reentry_gap: float
    reentry_stop_loss_gap: float
    min_minutes_left: int
    final_minute_seconds: int
    position_size: float
    minimum_fill_ratio: float
    starting_cash: float
    poll_interval_seconds: float
    request_timeout_seconds: float
    request_retries: int
    orderbook_levels: int
    log_limit: int
    state_path: Path
    log_path: Path

    @classmethod
    def from_env(cls) -> "PaperConfig":
        api_key = os.environ.get("SIMMER_API_KEY", "").strip()
        if not api_key:
            raise PreflightError("SIMMER_API_KEY is required for BTC 15m fast-market discovery.")

        return cls(
            simmer_api_key=api_key,
            asset=os.environ.get("PAPER_MARKET_ASSET", "BTC").strip() or "BTC",
            window=os.environ.get("PAPER_MARKET_WINDOW", "15m").strip() or "15m",
            entry_target=float(os.environ.get("CERTAINTY_ENTRY_TARGET", "0.80")),
            entry_band=float(os.environ.get("CERTAINTY_ENTRY_BAND", "0.01")),
            take_profit=float(os.environ.get("CERTAINTY_TAKE_PROFIT", "0.95")),
            take_profit_band=float(os.environ.get("CERTAINTY_TAKE_PROFIT_BAND", "0.005")),
            stop_loss=float(os.environ.get("CERTAINTY_STOP_LOSS", "0.75")),
            stop_loss_floor=float(os.environ.get("CERTAINTY_STOP_LOSS_FLOOR", "0.50")),
            reentry_floor=float(os.environ.get("CERTAINTY_REENTRY_FLOOR", "0.60")),
            same_side_reentry_gap=float(os.environ.get("CERTAINTY_SAME_SIDE_REENTRY_GAP", "0.05")),
            reentry_stop_loss_gap=float(os.environ.get("CERTAINTY_REENTRY_STOP_LOSS_GAP", "0.05")),
            min_minutes_left=int(os.environ.get("CERTAINTY_MIN_MINUTES_LEFT", "10")),
            final_minute_seconds=int(os.environ.get("CERTAINTY_FINAL_MINUTE_SECONDS", "60")),
            position_size=float(os.environ.get("CERTAINTY_POSITION_SIZE", "500")),
            minimum_fill_ratio=float(os.environ.get("PAPER_MINIMUM_FILL_RATIO", "0.90")),
            starting_cash=float(os.environ.get("PAPER_STARTING_CASH", "10")),
            poll_interval_seconds=float(os.environ.get("PAPER_POLL_INTERVAL_SECONDS", "0.5")),
            request_timeout_seconds=float(os.environ.get("PAPER_REQUEST_TIMEOUT_SECONDS", "15")),
            request_retries=int(os.environ.get("PAPER_REQUEST_RETRIES", "2")),
            orderbook_levels=int(os.environ.get("PAPER_ORDERBOOK_LEVELS", "8")),
            log_limit=int(os.environ.get("PAPER_LOG_LIMIT", "18")),
            state_path=Path(os.environ.get("PAPER_STATE_PATH", "polymarket_paper_trader_state.json")),
            log_path=Path(os.environ.get("PAPER_LOG_PATH", "polymarket_paper_trader.log")),
        )


@dataclass
class BookLevel:
    price: float
    size: float


@dataclass
class OrderBookView:
    token_id: str
    side: str
    best_bid: Optional[float]
    best_ask: Optional[float]
    last_trade_price: Optional[float]
    reference_price: Optional[float]
    reference_source: str
    bids: list[BookLevel]
    asks: list[BookLevel]
    tick_size: Optional[float]
    min_order_size: Optional[float]


@dataclass
class BuyQuote:
    requested_cash: float
    spent_cash: float
    gross_shares: float
    net_shares: float
    fee_usdc: float
    fee_shares: float
    average_gross_price: Optional[float]
    effective_price: Optional[float]
    fill_ratio: float
    fully_filled: bool
    levels: list[tuple[float, float]] = field(default_factory=list)


@dataclass
class SellQuote:
    requested_shares: float
    filled_shares: float
    gross_proceeds: float
    net_proceeds: float
    fee_usdc: float
    average_gross_price: Optional[float]
    effective_price: Optional[float]
    fill_ratio: float
    fully_filled: bool
    levels: list[tuple[float, float]] = field(default_factory=list)


@dataclass
class DashboardSnapshot:
    market: Optional[dict[str, Any]]
    next_market: Optional[dict[str, Any]]
    books: dict[str, OrderBookView]
    buy_quotes: dict[str, BuyQuote]
    sell_quotes: dict[str, SellQuote]
    btc_view: Optional["BitcoinPriceView"]
    wallet_view: dict[str, Any]
    state: dict[str, Any]
    now: datetime
    status_message: str
    expected_next_open: Optional[datetime]
    schedule_gap_seconds: Optional[float]


@dataclass
class BitcoinPriceView:
    symbol: str
    spot_price: Optional[float]
    price_to_beat: Optional[float]
    difference: Optional[float]
    difference_pct: Optional[float]
    direction: str
    spot_source: str
    price_to_beat_source: str


def choose_reference_price(
    *,
    best_bid: Optional[float],
    best_ask: Optional[float],
    last_trade_price: Optional[float],
) -> tuple[Optional[float], str]:
    if best_bid is not None and best_ask is not None and (best_ask - best_bid) <= 0.10:
        return ((best_bid + best_ask) / 2, "midpoint")
    if last_trade_price is not None:
        return (last_trade_price, "last")
    if best_ask is not None:
        return (best_ask, "ask")
    if best_bid is not None:
        return (best_bid, "bid")
    return (None, "none")


def min_remaining_seconds(config: PaperConfig) -> int:
    return max(config.min_minutes_left * 60, config.final_minute_seconds)


def can_take_initial_entry(config: PaperConfig, *, seconds_left: float, allow_repeat: bool) -> bool:
    threshold = config.final_minute_seconds if allow_repeat else min_remaining_seconds(config)
    return seconds_left >= threshold


def simulate_market_buy(
    *,
    cash_budget: float,
    asks: list[BookLevel],
    fee_rate: float,
    exponent: float,
) -> BuyQuote:
    remaining_cash = max(cash_budget, 0.0)
    spent_cash = 0.0
    gross_shares = 0.0
    net_shares = 0.0
    fee_usdc_total = 0.0
    fee_shares_total = 0.0
    levels_used: list[tuple[float, float]] = []

    for level in sorted(asks, key=lambda item: item.price):
        if remaining_cash <= 1e-9:
            break
        if level.price <= 0 or level.size <= 0:
            continue
        affordable_shares = remaining_cash / level.price
        shares = min(level.size, affordable_shares)
        if shares <= 1e-9:
            continue
        cash_spent = shares * level.price
        fee_usdc = calculate_fee_usdc(shares, level.price, fee_rate, exponent)
        fee_shares = min(shares, fee_usdc / level.price if level.price else 0.0)

        spent_cash += cash_spent
        gross_shares += shares
        net_shares += max(shares - fee_shares, 0.0)
        fee_usdc_total += fee_usdc
        fee_shares_total += fee_shares
        levels_used.append((level.price, shares))
        remaining_cash -= cash_spent

    average_gross_price = (spent_cash / gross_shares) if gross_shares else None
    effective_price = (spent_cash / net_shares) if net_shares else None
    fill_ratio = (spent_cash / cash_budget) if cash_budget > 0 else 0.0
    return BuyQuote(
        requested_cash=cash_budget,
        spent_cash=spent_cash,
        gross_shares=gross_shares,
        net_shares=net_shares,
        fee_usdc=fee_usdc_total,
        fee_shares=fee_shares_total,
        average_gross_price=average_gross_price,
        effective_price=effective_price,
        fill_ratio=fill_ratio,
        fully_filled=remaining_cash <= 0.01,
        levels=levels_used,
    )


def simulate_market_sell(
    *,
    shares_to_sell: float,
    bids: list[BookLevel],
    fee_rate: float,
    exponent: float,
) -> SellQuote:
    remaining_shares = max(shares_to_sell, 0.0)
    filled_shares = 0.0
    gross_proceeds = 0.0
    net_proceeds = 0.0
    fee_usdc_total = 0.0
    levels_used: list[tuple[float, float]] = []

    for level in sorted(bids, key=lambda item: item.price, reverse=True):
        if remaining_shares <= 1e-9:
            break
        if level.price <= 0 or level.size <= 0:
            continue
        shares = min(level.size, remaining_shares)
        if shares <= 1e-9:
            continue
        gross = shares * level.price
        fee_usdc = calculate_fee_usdc(shares, level.price, fee_rate, exponent)
        net = max(gross - fee_usdc, 0.0)

        filled_shares += shares
        gross_proceeds += gross
        net_proceeds += net
        fee_usdc_total += fee_usdc
        levels_used.append((level.price, shares))
        remaining_shares -= shares

    average_gross_price = (gross_proceeds / filled_shares) if filled_shares else None
    effective_price = (net_proceeds / filled_shares) if filled_shares else None
    fill_ratio = (filled_shares / shares_to_sell) if shares_to_sell > 0 else 0.0
    return SellQuote(
        requested_shares=shares_to_sell,
        filled_shares=filled_shares,
        gross_proceeds=gross_proceeds,
        net_proceeds=net_proceeds,
        fee_usdc=fee_usdc_total,
        average_gross_price=average_gross_price,
        effective_price=effective_price,
        fill_ratio=fill_ratio,
        fully_filled=remaining_shares <= 0.0001,
        levels=levels_used,
    )


def choose_lowest_candidate(quotes: dict[str, BuyQuote], predicate) -> Optional[tuple[str, float]]:
    candidates: list[tuple[float, str]] = []
    for side, quote in quotes.items():
        if quote.effective_price is None:
            continue
        if predicate(side, quote, quote.effective_price):
            candidates.append((quote.effective_price, side))
    if not candidates:
        return None
    price, side = min(candidates, key=lambda item: (item[0], item[1]))
    return side, price


def select_entry_candidate(
    config: PaperConfig,
    buy_quotes: dict[str, BuyQuote],
    *,
    seconds_left: float,
    allow_repeat: bool = False,
) -> Optional[tuple[str, float]]:
    if not can_take_initial_entry(config, seconds_left=seconds_left, allow_repeat=allow_repeat):
        return None
    lower_bound = config.entry_target - config.entry_band
    upper_bound = config.entry_target + config.entry_band
    return choose_lowest_candidate(
        buy_quotes,
        lambda _side, quote, price: quote.fill_ratio >= config.minimum_fill_ratio and lower_bound <= price <= upper_bound,
    )


def describe_entry_prices(buy_quotes: dict[str, BuyQuote]) -> str:
    parts: list[str] = []
    for side in ("yes", "no"):
        quote = buy_quotes.get(side)
        if not quote or quote.effective_price is None:
            parts.append(f"{side.upper()} n/a")
        else:
            parts.append(f"{side.upper()} {quote.effective_price:.3f}")
    return ", ".join(parts)


def should_take_profit(
    config: PaperConfig,
    position: dict[str, Any],
    sell_quotes: dict[str, SellQuote],
) -> Optional[float]:
    quote = sell_quotes.get(position["side"])
    if not quote or quote.effective_price is None:
        return None
    threshold = config.take_profit - config.take_profit_band
    return quote.effective_price if quote.fill_ratio > 0 and quote.effective_price >= threshold else None


def should_trigger_stop_loss(
    config: PaperConfig,
    position: dict[str, Any],
    sell_quotes: dict[str, SellQuote],
) -> Optional[float]:
    quote = sell_quotes.get(position["side"])
    if not quote or quote.effective_price is None:
        return None
    if position.get("entry_kind") == "reentry":
        entry_effective_price = parse_float(position.get("entry_effective_price"))
        if entry_effective_price is None:
            return None
        reentry_stop = max(entry_effective_price - config.reentry_stop_loss_gap, 0.0)
        return quote.effective_price if quote.fill_ratio > 0 and quote.effective_price <= reentry_stop else None
    if config.stop_loss_floor <= quote.effective_price <= config.stop_loss:
        return quote.effective_price
    return None


def select_reentry_candidate(
    config: PaperConfig,
    pending_reentry: dict[str, Any],
    buy_quotes: dict[str, BuyQuote],
) -> Optional[tuple[str, float]]:
    original_side = pending_reentry["original_side"]
    last_stop_price = float(pending_reentry["last_stop_price"])

    def qualifies(side: str, quote: BuyQuote, price: float) -> bool:
        if quote.fill_ratio < config.minimum_fill_ratio or price < config.reentry_floor:
            return False
        if side == original_side:
            return price >= last_stop_price + config.same_side_reentry_gap
        return True

    return choose_lowest_candidate(buy_quotes, qualifies)


def setup_file_logger(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("polymarket_paper_trader")
    if logger.handlers:
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
    ensure_parent(log_path)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(handler)
    return logger


class SimmerFastMarketDiscovery:
    def __init__(self, *, api_key: str, timeout_seconds: float, request_retries: int) -> None:
        self.request_retries = request_retries
        self.api_key = api_key
        self.client = httpx.Client(
            base_url="https://api.simmer.markets",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            timeout=timeout_seconds,
        )
        self.gamma_client = httpx.Client(base_url="https://gamma-api.polymarket.com", timeout=timeout_seconds)

    def close(self) -> None:
        self.client.close()
        self.gamma_client.close()

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(self.request_retries + 1):
            try:
                response = self.client.request(method, path, **kwargs)
            except httpx.HTTPError as exc:
                last_error = exc
                if attempt >= self.request_retries:
                    raise PaperTraderError(f"Simmer request failed: {exc}") from exc
                time.sleep(0.5 * (2**attempt))
                continue

            if response.status_code in RETRYABLE_STATUS_CODES and attempt < self.request_retries:
                time.sleep(0.5 * (2**attempt))
                continue

            if response.is_error:
                raise PaperTraderError(
                    f"Simmer discovery failed with {response.status_code}: {response.text}"
                )

            return response.json()

        if last_error is not None:
            raise PaperTraderError(f"Simmer request failed: {last_error}") from last_error
        raise PaperTraderError(f"Simmer request failed: {method} {path}")

    def _gamma_request(self, method: str, path: str, **kwargs: Any) -> Any:
        last_error: Optional[Exception] = None
        for attempt in range(self.request_retries + 1):
            try:
                response = self.gamma_client.request(method, path, **kwargs)
            except httpx.HTTPError as exc:
                last_error = exc
                if attempt >= self.request_retries:
                    raise PaperTraderError(f"Gamma request failed: {exc}") from exc
                time.sleep(0.3 * (2**attempt))
                continue

            if response.status_code in RETRYABLE_STATUS_CODES and attempt < self.request_retries:
                time.sleep(0.3 * (2**attempt))
                continue

            if response.is_error:
                raise PaperTraderError(
                    f"Gamma request failed with {response.status_code}: {response.text}"
                )

            return response.json()

        if last_error is not None:
            raise PaperTraderError(f"Gamma request failed: {last_error}") from last_error
        raise PaperTraderError(f"Gamma request failed: {method} {path}")

    def get_fast_markets(self, *, asset: str, window: str) -> list[dict[str, Any]]:
        payload = self._request(
            "GET",
            "/api/sdk/fast-markets",
            params={"asset": asset, "window": window, "venue": "polymarket", "limit": 50},
        )
        return list(payload.get("markets") or [])

    @staticmethod
    def _market_sort_key(market: dict[str, Any]) -> tuple[datetime, datetime]:
        opens_at = parse_datetime(market.get("opens_at")) or datetime.max.replace(tzinfo=UTC)
        resolves_at = parse_datetime(market.get("resolves_at")) or datetime.max.replace(tzinfo=UTC)
        return opens_at, resolves_at

    def select_active_market(self, markets: list[dict[str, Any]], *, now: datetime) -> Optional[dict[str, Any]]:
        live_candidates = [market for market in markets if market.get("is_live_now")]
        if live_candidates:
            return sorted(live_candidates, key=self._market_sort_key)[0]

        open_candidates: list[dict[str, Any]] = []
        for market in markets:
            opens_at = parse_datetime(market.get("opens_at"))
            resolves_at = parse_datetime(market.get("resolves_at"))
            if opens_at and resolves_at and opens_at <= now < resolves_at:
                open_candidates.append(market)
        if open_candidates:
            return sorted(open_candidates, key=self._market_sort_key)[0]
        return None

    def select_next_market(self, markets: list[dict[str, Any]], *, now: datetime) -> Optional[dict[str, Any]]:
        upcoming_candidates: list[dict[str, Any]] = []
        for market in markets:
            opens_at = parse_datetime(market.get("opens_at"))
            if opens_at and now < opens_at:
                upcoming_candidates.append(market)
        if upcoming_candidates:
            return sorted(upcoming_candidates, key=self._market_sort_key)[0]
        return None

    def _normalize_market(self, selected: dict[str, Any]) -> Optional[dict[str, Any]]:
        if not selected.get("polymarket_token_id") or not selected.get("polymarket_no_token_id"):
            return None
        tags = list(selected.get("tags") or [])
        fee_rate = derive_fee_rate(tags)
        fee_exponent = derive_fee_exponent(tags)
        return {
            "market_id": selected["id"],
            "question": selected.get("question"),
            "url": selected.get("url"),
            "opens_at": selected.get("opens_at"),
            "resolves_at": selected.get("resolves_at"),
            "yes_token_id": selected.get("polymarket_token_id"),
            "no_token_id": selected.get("polymarket_no_token_id"),
            "tags": tags,
            "fee_rate": fee_rate,
            "fee_exponent": fee_exponent,
            "is_live_now": bool(selected.get("is_live_now")),
        }

    @staticmethod
    def _parse_gamma_token_ids(value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(item) for item in value if item]
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                parsed = []
            if isinstance(parsed, list):
                return [str(item) for item in parsed if item]
        return []

    def _normalize_gamma_market(
        self,
        selected: dict[str, Any],
        *,
        interval_start: datetime,
        asset: str,
        window: str,
    ) -> Optional[dict[str, Any]]:
        token_ids = self._parse_gamma_token_ids(selected.get("clobTokenIds"))
        if len(token_ids) < 2:
            return None
        resolves_at = parse_datetime(selected.get("endDate"))
        if not resolves_at:
            return None
        tags = ["polymarket", "crypto", "fast", f"fast-{window}"]
        fee_schedule = dict(selected.get("feeSchedule") or {})
        fee_rate = parse_float(fee_schedule.get("rate"))
        fee_exponent = parse_float(fee_schedule.get("exponent"))
        return {
            "market_id": str(selected.get("id")),
            "question": selected.get("question"),
            "url": f"https://polymarket.com/event/{selected.get('slug')}",
            "opens_at": to_iso8601(interval_start),
            "resolves_at": to_iso8601(resolves_at),
            "yes_token_id": token_ids[0],
            "no_token_id": token_ids[1],
            "tags": tags,
            "fee_bps": int(selected.get("takerBaseFee") or selected.get("makerBaseFee") or 0),
            "fee_rate": fee_rate if fee_rate is not None else derive_fee_rate(tags),
            "fee_exponent": fee_exponent if fee_exponent is not None else derive_fee_exponent(tags),
            "is_live_now": bool(selected.get("acceptingOrders")) and not bool(selected.get("closed")),
        }

    def _fetch_gamma_market_for_interval(
        self,
        *,
        asset: str,
        window: str,
        interval_start: datetime,
    ) -> Optional[dict[str, Any]]:
        slug = market_slug_for_interval(asset, window, interval_start)
        payload = self._gamma_request("GET", "/markets", params={"slug": slug, "limit": 5})
        if not isinstance(payload, list) or not payload:
            return None
        selected = payload[0]
        if selected.get("closed") or not selected.get("acceptingOrders"):
            return None
        return self._normalize_gamma_market(selected, interval_start=interval_start, asset=asset, window=window)

    def fetch_current_gamma_market(self, *, asset: str, window: str, now: datetime) -> Optional[dict[str, Any]]:
        interval_start = floor_to_interval(now, minutes=15)
        market = self._fetch_gamma_market_for_interval(asset=asset, window=window, interval_start=interval_start)
        if not market:
            return None
        resolves_at = parse_datetime(market.get("resolves_at"))
        if resolves_at and interval_start <= now < resolves_at:
            return market
        return None

    def fetch_next_gamma_market(self, *, asset: str, window: str, now: datetime) -> Optional[dict[str, Any]]:
        for step in range(1, 25):
            interval_start = ceil_to_interval(now, minutes=15) + timedelta(minutes=15 * (step - 1))
            market = self._fetch_gamma_market_for_interval(asset=asset, window=window, interval_start=interval_start)
            if market:
                return market
        return None

    def fetch_live_market(self, *, asset: str, window: str, now: datetime) -> Optional[dict[str, Any]]:
        gamma_market = self.fetch_current_gamma_market(asset=asset, window=window, now=now)
        if gamma_market:
            return gamma_market
        markets = self.get_fast_markets(asset=asset, window=window)
        selected = self.select_active_market(markets, now=now)
        if not selected:
            return None
        return self._normalize_market(selected)

    def fetch_next_market(self, *, asset: str, window: str, now: datetime) -> Optional[dict[str, Any]]:
        gamma_market = self.fetch_next_gamma_market(asset=asset, window=window, now=now)
        if gamma_market:
            return gamma_market
        markets = self.get_fast_markets(asset=asset, window=window)
        selected = self.select_next_market(markets, now=now)
        if not selected:
            return None
        return self._normalize_market(selected)


class PolymarketCLOB:
    def __init__(self, *, timeout_seconds: float, request_retries: int) -> None:
        self.request_retries = request_retries
        self.client = httpx.Client(base_url="https://clob.polymarket.com", timeout=timeout_seconds)
        self.fee_cache: dict[str, int] = {}

    def close(self) -> None:
        self.client.close()

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        for attempt in range(self.request_retries + 1):
            try:
                response = self.client.request(method, path, **kwargs)
            except httpx.HTTPError as exc:
                if attempt >= self.request_retries:
                    raise PaperTraderError(f"Polymarket request failed: {exc}") from exc
                time.sleep(0.3 * (2**attempt))
                continue

            if response.status_code in RETRYABLE_STATUS_CODES and attempt < self.request_retries:
                time.sleep(0.3 * (2**attempt))
                continue

            if response.is_error:
                raise PaperTraderError(
                    f"Polymarket request failed with {response.status_code} on {path}: {response.text}"
                )

            return response.json()

        raise PaperTraderError(f"Polymarket request failed: {method} {path}")

    def get_fee_bps(self, token_id: str) -> int:
        if token_id in self.fee_cache:
            return self.fee_cache[token_id]
        payload = self._request("GET", "/fee-rate", params={"token_id": token_id})
        fee_bps = int(payload.get("base_fee") or 0)
        self.fee_cache[token_id] = fee_bps
        return fee_bps

    @staticmethod
    def _parse_levels(levels: list[dict[str, Any]], *, reverse: bool) -> list[BookLevel]:
        parsed: list[BookLevel] = []
        for level in levels or []:
            price = parse_float(level.get("price"))
            size = parse_float(level.get("size"))
            if price is None or size is None:
                continue
            parsed.append(BookLevel(price=price, size=size))
        return sorted(parsed, key=lambda item: item.price, reverse=reverse)

    def get_book(self, *, token_id: str, side: str) -> OrderBookView:
        payload = self._request("GET", "/book", params={"token_id": token_id})
        bids = self._parse_levels(list(payload.get("bids") or []), reverse=True)
        asks = self._parse_levels(list(payload.get("asks") or []), reverse=False)
        best_bid = bids[0].price if bids else None
        best_ask = asks[0].price if asks else None
        last_trade_price = parse_float(payload.get("last_trade_price"))
        reference_price, reference_source = choose_reference_price(
            best_bid=best_bid,
            best_ask=best_ask,
            last_trade_price=last_trade_price,
        )
        return OrderBookView(
            token_id=token_id,
            side=side,
            best_bid=best_bid,
            best_ask=best_ask,
            last_trade_price=last_trade_price,
            reference_price=reference_price,
            reference_source=reference_source,
            bids=bids,
            asks=asks,
            tick_size=parse_float(payload.get("tick_size")),
            min_order_size=parse_float(payload.get("min_order_size")),
        )


class BitcoinReferenceData:
    def __init__(self, *, timeout_seconds: float, request_retries: int) -> None:
        self.request_retries = request_retries
        self.client = httpx.Client(base_url="https://api.binance.us", timeout=timeout_seconds)
        self.interval_open_cache: dict[str, float] = {}

    def close(self) -> None:
        self.client.close()

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        for attempt in range(self.request_retries + 1):
            try:
                response = self.client.request(method, path, **kwargs)
            except httpx.HTTPError as exc:
                if attempt >= self.request_retries:
                    raise PaperTraderError(f"BTC price request failed: {exc}") from exc
                time.sleep(0.3 * (2**attempt))
                continue

            if response.status_code in RETRYABLE_STATUS_CODES and attempt < self.request_retries:
                time.sleep(0.3 * (2**attempt))
                continue

            if response.is_error:
                raise PaperTraderError(
                    f"BTC price request failed with {response.status_code} on {path}: {response.text}"
                )

            return response.json()

        raise PaperTraderError(f"BTC price request failed: {method} {path}")

    def get_spot_price(self, *, symbol: str) -> Optional[float]:
        payload = self._request("GET", "/api/v3/ticker/price", params={"symbol": symbol})
        return parse_float(payload.get("price"))

    def get_interval_open_price(self, *, symbol: str, interval_start: datetime, interval: str) -> Optional[float]:
        cache_key = f"{symbol}:{interval}:{to_iso8601(interval_start)}"
        if cache_key in self.interval_open_cache:
            return self.interval_open_cache[cache_key]
        payload = self._request(
            "GET",
            "/api/v3/klines",
            params={
                "symbol": symbol,
                "interval": interval,
                "startTime": int(interval_start.timestamp() * 1000),
                "limit": 1,
            },
        )
        if not isinstance(payload, list) or not payload:
            return None
        first = payload[0]
        if not isinstance(first, list) or len(first) < 2:
            return None
        open_price = parse_float(first[1])
        if open_price is not None:
            self.interval_open_cache[cache_key] = open_price
        return open_price

    def get_price_view(
        self,
        *,
        asset: str,
        window: str,
        interval_start: Optional[datetime],
    ) -> BitcoinPriceView:
        symbol = f"{asset.upper()}USDT"
        spot_price = self.get_spot_price(symbol=symbol)
        price_to_beat = None
        if interval_start is not None:
            price_to_beat = self.get_interval_open_price(symbol=symbol, interval_start=interval_start, interval=window)
        difference = None
        difference_pct = None
        if spot_price is not None and price_to_beat is not None:
            difference = spot_price - price_to_beat
            difference_pct = ratio_or_none(difference, price_to_beat)
        return BitcoinPriceView(
            symbol=symbol,
            spot_price=spot_price,
            price_to_beat=price_to_beat,
            difference=difference,
            difference_pct=difference_pct,
            direction=infer_price_direction(spot_price, price_to_beat),
            spot_source="Binance US spot",
            price_to_beat_source="Binance US 15m open",
        )


class PolymarketPaperTrader:
    def __init__(
        self,
        *,
        config: PaperConfig,
        discovery: SimmerFastMarketDiscovery,
        market_data: PolymarketCLOB,
        price_data: BitcoinReferenceData,
        state_store: PaperStateStore,
        logger: logging.Logger,
    ) -> None:
        self.config = config
        self.discovery = discovery
        self.market_data = market_data
        self.price_data = price_data
        self.state_store = state_store
        self.state = state_store.load()
        self.logger = logger
        self.console = Console()

    def preflight(self) -> None:
        wallet = self.state.get("wallet") or {}
        if parse_float(wallet.get("cash")) is None:
            raise PreflightError("Wallet state is invalid.")
        now = utc_now()
        market = self.discovery.fetch_live_market(asset=self.config.asset, window=self.config.window, now=now)
        if market:
            self.state["active_market"] = market
            self.record_event(
                event_type="info",
                message=(
                    f"Paper trader ready on {market['question']}. "
                    f"Entry band={(self.config.entry_target - self.config.entry_band):.3f}-"
                    f"{(self.config.entry_target + self.config.entry_band):.3f}."
                ),
            )
        else:
            self.state["active_market"] = None
            next_market = self.discovery.fetch_next_market(asset=self.config.asset, window=self.config.window, now=now)
            if next_market:
                opens_at = parse_datetime(next_market.get("opens_at"))
                opens_in = (opens_at - now).total_seconds() if opens_at else None
                self.record_event(
                    event_type="info",
                    message=(
                        f"No tradeable {self.config.asset} {self.config.window} market is live right now. "
                        f"Next listed market opens in {opens_in:.1f}s."
                        if opens_in is not None
                        else f"No tradeable {self.config.asset} {self.config.window} market is live right now."
                    ),
                )
            else:
                self.record_event(
                    event_type="info",
                    message=f"No live {self.config.asset} {self.config.window} market is available right now.",
                )
        self.state_store.save(self.state)

    def record_event(self, *, event_type: str, message: str) -> None:
        entry = {"ts": to_iso8601(utc_now()), "event_type": event_type, "message": message}
        recent_logs = list(self.state.get("recent_logs") or [])
        recent_logs.append(entry)
        self.state["recent_logs"] = recent_logs[-max(self.config.log_limit * 4, 40) :]
        self.logger.info("[%s] %s", event_type.upper(), message)

    def _ensure_market(self, now: datetime) -> Optional[dict[str, Any]]:
        active_market = self.state.get("active_market")
        if active_market:
            opens_at = parse_datetime(active_market.get("opens_at"))
            resolves_at = parse_datetime(active_market.get("resolves_at"))
            if opens_at and resolves_at and opens_at <= now < resolves_at:
                return active_market

        market = self.discovery.fetch_live_market(asset=self.config.asset, window=self.config.window, now=now)
        self.state["active_market"] = market
        return market

    def _expire_pending_reentry(self, now: datetime) -> None:
        pending = self.state.get("pending_reentry")
        if not pending:
            return
        resolves_at = parse_datetime(pending.get("resolves_at"))
        if resolves_at and now >= resolves_at:
            self.record_event(event_type="info", message="Pending re-entry expired with the interval.")
            self.state["pending_reentry"] = None

    def _build_buy_quotes(self, market: dict[str, Any], books: dict[str, OrderBookView]) -> dict[str, BuyQuote]:
        wallet_cash = float((self.state.get("wallet") or {}).get("cash") or 0.0)
        cash_budget = min(self.config.position_size, wallet_cash)
        fee_rate = float(market.get("fee_rate") or 0.0)
        exponent = float(market.get("fee_exponent") or 1.0)
        return {
            "yes": simulate_market_buy(cash_budget=cash_budget, asks=books["yes"].asks, fee_rate=fee_rate, exponent=exponent),
            "no": simulate_market_buy(cash_budget=cash_budget, asks=books["no"].asks, fee_rate=fee_rate, exponent=exponent),
        }

    def _build_sell_quotes(self, market: dict[str, Any], books: dict[str, OrderBookView]) -> dict[str, SellQuote]:
        position = self.state.get("current_position")
        if not position:
            return {}
        shares = float(position.get("shares") or 0.0)
        if shares <= 0:
            return {}
        fee_rate = float(market.get("fee_rate") or 0.0)
        exponent = float(market.get("fee_exponent") or 1.0)
        side = position["side"]
        return {
            side: simulate_market_sell(
                shares_to_sell=shares,
                bids=books[side].bids,
                fee_rate=fee_rate,
                exponent=exponent,
            )
        }

    def _execute_buy(self, *, market: dict[str, Any], side: str, quote: BuyQuote, entry_kind: str) -> None:
        if quote.net_shares <= 0 or quote.spent_cash <= 0:
            self.record_event(event_type="info", message=f"Skipped {entry_kind} {side.upper()} entry because no ask liquidity was fillable.")
            return

        wallet = self.state["wallet"]
        wallet["cash"] = float(wallet["cash"]) - quote.spent_cash
        wallet["fees_paid_usdc"] = float(wallet["fees_paid_usdc"]) + quote.fee_usdc
        wallet["trade_count"] = int(wallet["trade_count"]) + 1

        pending = self.state.get("pending_reentry") or {}
        self.state["current_position"] = {
            "market_id": market["market_id"],
            "question": market["question"],
            "side": side,
            "shares": quote.net_shares,
            "gross_shares": quote.gross_shares,
            "cost_basis_cash": quote.spent_cash,
            "entry_kind": entry_kind,
            "entry_effective_price": quote.effective_price,
            "opened_at": to_iso8601(utc_now()),
            "resolves_at": market["resolves_at"],
            "original_side": pending.get("original_side", side),
            "stop_loss_trigger": (
                max(float(quote.effective_price) - self.config.reentry_stop_loss_gap, 0.0)
                if entry_kind == "reentry" and quote.effective_price is not None
                else None
            ),
        }
        self.state["active_market"] = market
        if entry_kind == "reentry":
            self.state["pending_reentry"] = None
        self.state["repeat_entry_market_id"] = None

        self.record_event(
            event_type="entry",
            message=(
                f"{entry_kind.upper()} {side.upper()} buy: spent {format_money(quote.spent_cash)}, "
                f"received {format_shares(quote.net_shares)} shares at effective {format_price(quote.effective_price)} "
                f"(fee {format_money(quote.fee_usdc)} / {format_shares(quote.fee_shares)} shares)."
            ),
        )

    def _execute_sell(
        self,
        *,
        market: dict[str, Any],
        reason: str,
        quote: SellQuote,
        stop_price: Optional[float] = None,
    ) -> None:
        position = self.state.get("current_position")
        if not position:
            return
        if quote.filled_shares <= 0 or quote.net_proceeds <= 0:
            self.record_event(
                event_type="info",
                message=f"Tried to exit {position['side'].upper()} for {reason}, but there were no bid fills available.",
            )
            return

        wallet = self.state["wallet"]
        wallet["cash"] = float(wallet["cash"]) + quote.net_proceeds
        wallet["fees_paid_usdc"] = float(wallet["fees_paid_usdc"]) + quote.fee_usdc
        wallet["trade_count"] = int(wallet["trade_count"]) + 1

        shares_before = float(position["shares"])
        cost_basis_cash = float(position["cost_basis_cash"])
        closed_fraction = clamp(quote.filled_shares / shares_before if shares_before else 0.0, 0.0, 1.0)
        realized_cost_basis = cost_basis_cash * closed_fraction
        pnl = quote.net_proceeds - realized_cost_basis
        wallet["realized_pnl"] = float(wallet["realized_pnl"]) + pnl
        if pnl >= 0:
            wallet["wins"] = int(wallet["wins"]) + 1
        else:
            wallet["losses"] = int(wallet["losses"]) + 1

        remaining_shares = max(shares_before - quote.filled_shares, 0.0)
        remaining_cost_basis = max(cost_basis_cash - realized_cost_basis, 0.0)

        self.record_event(
            event_type=reason.lower(),
            message=(
                f"{reason.upper()} {position['side'].upper()} sell: sold {format_shares(quote.filled_shares)} shares "
                f"at effective {format_price(quote.effective_price)}, proceeds {format_money(quote.net_proceeds)}, "
                f"PnL {format_money(pnl)}."
            ),
        )

        if remaining_shares > 0.0001 and not quote.fully_filled:
            position["shares"] = remaining_shares
            position["cost_basis_cash"] = remaining_cost_basis
            self.state["current_position"] = position
            self.record_event(event_type="info", message=f"Exit was partial. {format_shares(remaining_shares)} shares remain open.")
            return

        if reason.lower() == "sl":
            self.state["pending_reentry"] = {
                "market_id": position["market_id"],
                "question": position["question"],
                "original_side": position.get("original_side", position["side"]),
                "last_stop_price": stop_price,
                "resolves_at": position["resolves_at"],
            }
            self.state["repeat_entry_market_id"] = None
        elif reason.lower() == "tp":
            self.state["pending_reentry"] = None
            self.state["repeat_entry_market_id"] = position["market_id"]
        else:
            self.state["pending_reentry"] = None
            self.state["repeat_entry_market_id"] = None

        self.state["current_position"] = None
        if reason.lower() in {"tp", "flatten"}:
            self.state["active_market"] = None

    def _wallet_view(self, market: Optional[dict[str, Any]], books: dict[str, OrderBookView]) -> tuple[dict[str, Any], dict[str, SellQuote]]:
        wallet = self.state["wallet"]
        position = self.state.get("current_position")
        sell_quotes: dict[str, SellQuote] = {}
        position_value = 0.0

        if position and market:
            sell_quotes = self._build_sell_quotes(market, books)
            quote = sell_quotes.get(position["side"])
            if quote:
                position_value = quote.net_proceeds

        realized = float(wallet["realized_pnl"])
        cash = float(wallet["cash"])
        starting_cash = float(wallet["starting_cash"])
        total_equity = cash + position_value
        total_pnl = total_equity - starting_cash
        cost_basis = float(position["cost_basis_cash"]) if position else 0.0
        unrealized = position_value - cost_basis
        return (
            {
                "starting_cash": starting_cash,
                "cash": cash,
                "position_value": position_value,
                "realized_pnl": realized,
                "realized_pnl_pct": ratio_or_none(realized, starting_cash),
                "unrealized_pnl": unrealized,
                "unrealized_pnl_pct": ratio_or_none(unrealized, starting_cash),
                "total_equity": total_equity,
                "total_pnl": total_pnl,
                "total_pnl_pct": ratio_or_none(total_pnl, starting_cash),
                "fees_paid_usdc": float(wallet["fees_paid_usdc"]),
                "trade_count": int(wallet["trade_count"]),
                "wins": int(wallet["wins"]),
                "losses": int(wallet["losses"]),
            },
            sell_quotes,
        )

    def _process_strategy(
        self,
        *,
        now: datetime,
        market: Optional[dict[str, Any]],
        buy_quotes: dict[str, BuyQuote],
        sell_quotes: dict[str, SellQuote],
    ) -> str:
        position = self.state.get("current_position")
        pending_reentry = self.state.get("pending_reentry")

        if not market:
            return "Waiting for the next live BTC 15m market."

        opens_at = parse_datetime(market.get("opens_at"))
        resolves_at = parse_datetime(market["resolves_at"])
        if not resolves_at:
            return "Market timing is unavailable."
        if opens_at and now < opens_at:
            expected_next_open = ceil_to_interval(now, minutes=15)
            if opens_at > expected_next_open:
                missing_slots = int((opens_at - expected_next_open).total_seconds() // (15 * 60))
                return (
                    f"Schedule gap detected: {missing_slots} expected 15m slot(s) missing before the next listed market. "
                    f"Watching the listed market opening in {(opens_at - now).total_seconds():.1f}s."
                )
            return f"Watching the next interval. Market opens in {(opens_at - now).total_seconds():.1f}s."
        seconds_left = (resolves_at - now).total_seconds()

        if position:
            if seconds_left <= 0:
                quote = sell_quotes.get(position["side"])
                if quote:
                    self._execute_sell(market=market, reason="flatten", quote=quote)
                return "Flattening at interval close."

            take_profit_price = should_take_profit(self.config, position, sell_quotes)
            if take_profit_price is not None:
                quote = sell_quotes.get(position["side"])
                if quote:
                    self._execute_sell(market=market, reason="tp", quote=quote)
                return f"Take-profit armed at {take_profit_price:.3f}."

            stop_loss_price = should_trigger_stop_loss(self.config, position, sell_quotes)
            if stop_loss_price is not None:
                quote = sell_quotes.get(position["side"])
                if quote:
                    self._execute_sell(market=market, reason="sl", quote=quote, stop_price=stop_loss_price)
                return f"Stop-loss triggered at {stop_loss_price:.3f}."

            exit_quote = sell_quotes.get(position["side"])
            return (
                f"Holding {position['side'].upper()} {format_shares(position['shares'])} shares. "
                f"Exit mark {format_price(exit_quote.effective_price if exit_quote else None)}."
            )

        if pending_reentry:
            candidate = select_reentry_candidate(self.config, pending_reentry, buy_quotes)
            if candidate:
                side, _ = candidate
                self._execute_buy(market=market, side=side, quote=buy_quotes[side], entry_kind="reentry")
                return f"Re-entry candidate found on {side.upper()}."
            return "Watching for re-entry above the configured floor."

        allow_repeat = self.state.get("repeat_entry_market_id") == market["market_id"]
        threshold_seconds = self.config.final_minute_seconds if allow_repeat else min_remaining_seconds(self.config)
        candidate = select_entry_candidate(
            self.config,
            buy_quotes,
            seconds_left=seconds_left,
            allow_repeat=allow_repeat,
        )
        if candidate:
            side, _ = candidate
            self._execute_buy(market=market, side=side, quote=buy_quotes[side], entry_kind="initial")
            return f"Entry candidate found on {side.upper()}."
        if seconds_left < threshold_seconds:
            if allow_repeat:
                return f"Repeat entry window closed with {seconds_left:.1f}s left."
            return f"Initial entry window closed with {seconds_left:.1f}s left."
        if allow_repeat and seconds_left < min_remaining_seconds(self.config):
            return "Waiting for another 80c repeat entry in this interval."
        lower_bound = self.config.entry_target - self.config.entry_band
        upper_bound = self.config.entry_target + self.config.entry_band
        return (
            f"No entry: effective buy prices {describe_entry_prices(buy_quotes)} are outside "
            f"{lower_bound:.3f}-{upper_bound:.3f}."
        )

    def snapshot(self) -> DashboardSnapshot:
        now = utc_now()
        self._expire_pending_reentry(now)
        market = self._ensure_market(now)
        next_market: Optional[dict[str, Any]] = None
        books: dict[str, OrderBookView] = {}
        buy_quotes: dict[str, BuyQuote] = {}
        btc_view: Optional[BitcoinPriceView] = None
        status_message = "Waiting for data."
        expected_next_open = ceil_to_interval(now, minutes=15)
        schedule_gap_seconds: Optional[float] = None

        display_market = market
        if not display_market:
            next_market = self.discovery.fetch_next_market(asset=self.config.asset, window=self.config.window, now=now)
            display_market = next_market

        if display_market:
            if not display_market.get("fee_bps"):
                display_market["fee_bps"] = self.market_data.get_fee_bps(display_market["yes_token_id"])
            opens_at = parse_datetime(display_market.get("opens_at"))
            if opens_at and opens_at > expected_next_open:
                schedule_gap_seconds = (opens_at - expected_next_open).total_seconds()
            try:
                btc_view = self.price_data.get_price_view(
                    asset=self.config.asset,
                    window=self.config.window,
                    interval_start=opens_at,
                )
            except PaperTraderError as exc:
                self.logger.warning("Failed to refresh BTC reference prices: %s", exc)
            books = {
                "yes": self.market_data.get_book(token_id=display_market["yes_token_id"], side="yes"),
                "no": self.market_data.get_book(token_id=display_market["no_token_id"], side="no"),
            }
            buy_quotes = self._build_buy_quotes(display_market, books)

        wallet_view, sell_quotes = self._wallet_view(market, books)
        if market:
            status_message = self._process_strategy(
                now=now,
                market=market,
                buy_quotes=buy_quotes,
                sell_quotes=sell_quotes,
            )
        elif next_market:
            opens_at = parse_datetime(next_market.get("opens_at"))
            opens_in = (opens_at - now).total_seconds() if opens_at else None
            if opens_at and opens_at > expected_next_open:
                missing_slots = int((opens_at - expected_next_open).total_seconds() // (15 * 60))
                status_message = (
                    f"No tradeable interval right now. Previewing the next listed market only. "
                    f"Feed is missing {missing_slots} expected 15m slot(s)."
                )
            else:
                status_message = (
                    f"No tradeable interval right now. Previewing the next listed market opening in {opens_in:.1f}s."
                    if opens_in is not None
                    else "No tradeable interval right now."
                )
        else:
            status_message = "No tradeable or upcoming interval is available from the feed."
        wallet_view, sell_quotes = self._wallet_view(market, books)
        self.state_store.save(self.state)
        return DashboardSnapshot(
            market=market,
            next_market=next_market,
            books=books,
            buy_quotes=buy_quotes,
            sell_quotes=sell_quotes,
            btc_view=btc_view,
            wallet_view=wallet_view,
            state=self.state,
            now=now,
            status_message=status_message,
            expected_next_open=expected_next_open,
            schedule_gap_seconds=schedule_gap_seconds,
        )

    def run_once(self) -> None:
        self.console.print(self.render_dashboard(self.snapshot()))

    def run_loop(self, *, duration_seconds: Optional[float] = None) -> None:
        start = time.time()
        with Live(self.render_dashboard(self.snapshot()), console=self.console, screen=True, auto_refresh=False) as live:
            while True:
                snapshot = self.snapshot()
                live.update(self.render_dashboard(snapshot), refresh=True)
                if duration_seconds is not None and (time.time() - start) >= duration_seconds:
                    break
                time.sleep(self.config.poll_interval_seconds)

    def render_dashboard(self, snapshot: DashboardSnapshot):
        layout = Layout()
        layout.split_column(
            Layout(name="top", size=11),
            Layout(name="middle", size=21),
            Layout(name="bottom", size=12),
        )
        layout["top"].split_row(Layout(name="reference"), Layout(name="market"))
        layout["middle"].update(self.build_orderbooks_panel(snapshot))
        layout["bottom"].split_row(Layout(name="wallet"), Layout(name="logs"))
        layout["reference"].update(self.build_reference_panel(snapshot))
        layout["market"].update(self.build_market_panel(snapshot))
        layout["wallet"].update(self.build_wallet_panel(snapshot))
        layout["logs"].update(self.build_logs_panel(snapshot))
        return layout

    def build_reference_panel(self, snapshot: DashboardSnapshot) -> Panel:
        btc_view = snapshot.btc_view
        table = Table.grid(expand=True)
        table.add_column()
        table.add_column(justify="right")
        table.add_row("Spot BTC", format_money(btc_view.spot_price if btc_view else None))
        table.add_row("Price To Beat", format_money(btc_view.price_to_beat if btc_view else None))
        table.add_row("Difference", format_signed_money(btc_view.difference if btc_view else None))
        table.add_row("Difference %", format_signed_percent_change(btc_view.difference_pct if btc_view else None))
        table.add_row("Direction", btc_view.direction if btc_view else "-")
        table.add_row("Sources", f"{btc_view.spot_source} / {btc_view.price_to_beat_source}" if btc_view else "-")

        position = snapshot.state.get("current_position")
        pending_reentry = snapshot.state.get("pending_reentry")
        lines = [table, Text("")]
        if position:
            sell_quote = snapshot.sell_quotes.get(position["side"])
            stop_text = f"SL {self.config.stop_loss_floor:.3f}-{self.config.stop_loss:.3f}"
            if position.get("entry_kind") == "reentry":
                stop_text = f"SL <= {format_price(parse_float(position.get('stop_loss_trigger')))}"
            lines.append(
                Text(
                    f"Held {position['side'].upper()} exit mark: {format_percent(sell_quote.effective_price if sell_quote else None)}   "
                    f"TP >= {self.config.take_profit - self.config.take_profit_band:.3f}   "
                    f"{stop_text}",
                    style="bold cyan",
                )
            )
        elif pending_reentry:
            same_side_min = float(pending_reentry["last_stop_price"]) + self.config.same_side_reentry_gap
            lines.append(
                Text(
                    f"Re-entry floor >= {self.config.reentry_floor:.3f}. "
                    f"Same-side {pending_reentry['original_side'].upper()} needs >= {same_side_min:.3f}.",
                    style="bold yellow",
                )
            )
        else:
            lines.append(
                Text(
                    f"Entry band to beat: {(self.config.entry_target - self.config.entry_band):.3f}-"
                    f"{(self.config.entry_target + self.config.entry_band):.3f} effective buy price.",
                    style="bold green",
                )
            )
        if not snapshot.market and snapshot.next_market:
            lines.append(Text("Preview only: next listed market is not tradeable yet.", style="bold yellow"))
        lines.append(Text(snapshot.status_message, style="white"))
        return Panel(Group(*lines), title="BTC Reference / Price To Beat", border_style="cyan")

    def build_market_panel(self, snapshot: DashboardSnapshot) -> Panel:
        market = snapshot.market or snapshot.next_market
        if not market:
            return Panel("No live market detected yet.", title="Market Interval Info", border_style="yellow")

        opens_at = parse_datetime(market["opens_at"])
        resolves_at = parse_datetime(market["resolves_at"])
        opens_in = (opens_at - snapshot.now).total_seconds() if opens_at else None
        resolves_in = (resolves_at - snapshot.now).total_seconds() if resolves_at else None
        yes_book = snapshot.books.get("yes")

        info = Table.grid(expand=True)
        info.add_column()
        info.add_column()
        info.add_row("Tradeable Now", "yes" if snapshot.market else "no")
        info.add_row("Question", market["question"] or "-")
        info.add_row(
            "Open",
            opens_at.astimezone(ET).strftime(f"%Y-%m-%d %I:%M:%S %p {DISPLAY_TZ_LABEL}") if opens_at else "-",
        )
        info.add_row(
            "Resolve",
            resolves_at.astimezone(ET).strftime(f"%Y-%m-%d %I:%M:%S %p {DISPLAY_TZ_LABEL}") if resolves_at else "-",
        )
        info.add_row("Opens In", f"{max(opens_in, 0.0):,.1f}s" if opens_in is not None else "-")
        info.add_row("Resolves In", f"{max(resolves_in, 0.0):,.1f}s" if resolves_in is not None else "-")
        info.add_row("Interval", "900.0s")
        if snapshot.expected_next_open:
            info.add_row(
                "Expected Next",
                snapshot.expected_next_open.astimezone(ET).strftime(f"%Y-%m-%d %I:%M:%S %p {DISPLAY_TZ_LABEL}"),
            )
        if snapshot.schedule_gap_seconds:
            info.add_row("Schedule Gap", f"{snapshot.schedule_gap_seconds:,.1f}s")
        info.add_row(
            "Fee",
            f"{market.get('fee_bps', 0)} bps, rate {float(market.get('fee_rate', 0.0)):.3f}, "
            f"exponent {float(market.get('fee_exponent', 1.0)):.1f}",
        )
        info.add_row("Tick / Min", f"{format_price(yes_book.tick_size if yes_book else None)} / {format_shares(yes_book.min_order_size if yes_book else None)}")
        info.add_row("Tags", ", ".join(market.get("tags") or []) or "-")
        info.add_row("Market ID", market["market_id"])
        return Panel(info, title="Market Interval Info", border_style="magenta")

    def build_orderbooks_panel(self, snapshot: DashboardSnapshot) -> Panel:
        if not snapshot.books:
            return Panel("Waiting for Polymarket order books.", title="Orderbooks / Share Prices", border_style="blue")

        header_lines: list[Any] = []
        if not snapshot.market and snapshot.next_market:
            header_lines.append(Text("Previewing the next listed market only. No live tradeable interval right now.", style="bold yellow"))
            header_lines.append(Text(""))

        summary = Table(expand=True)
        summary.add_column("Side")
        summary.add_column("Bid")
        summary.add_column("Ask")
        summary.add_column("Ref")
        summary.add_column("Last")
        summary.add_column("Buy@Size")
        for side in ("yes", "no"):
            book = snapshot.books[side]
            quote = snapshot.buy_quotes.get(side)
            summary.add_row(
                side.upper(),
                format_percent(book.best_bid),
                format_percent(book.best_ask),
                format_percent(book.reference_price),
                format_percent(book.last_trade_price),
                format_percent(quote.effective_price if quote else None),
            )

        def build_book_table(side: str) -> Table:
            book = snapshot.books[side]
            table = Table(title=f"{side.upper()} Book", expand=True)
            table.add_column("Ask Px", justify="right")
            table.add_column("Ask Sz", justify="right")
            table.add_column("Bid Px", justify="right")
            table.add_column("Bid Sz", justify="right")
            depth = self.config.orderbook_levels
            asks = book.asks[:depth]
            bids = book.bids[:depth]
            rows = max(len(asks), len(bids), depth)
            for index in range(rows):
                ask = asks[index] if index < len(asks) else None
                bid = bids[index] if index < len(bids) else None
                table.add_row(
                    format_percent(ask.price if ask else None),
                    format_shares(ask.size if ask else None),
                    format_percent(bid.price if bid else None),
                    format_shares(bid.size if bid else None),
                )
            return table

        return Panel(
            Group(*header_lines, summary, Text(""), Columns([build_book_table("yes"), build_book_table("no")], expand=True)),
            title="Orderbooks / Share Prices",
            border_style="blue",
        )

    def build_wallet_panel(self, snapshot: DashboardSnapshot) -> Panel:
        wallet_view = snapshot.wallet_view
        position = snapshot.state.get("current_position")

        def pnl_cell(amount: Optional[float], pct: Optional[float]) -> str:
            amount_text = format_signed_money(amount)
            pct_text = format_signed_percent_change(pct)
            if pct_text == "-":
                return amount_text
            return f"{amount_text} ({pct_text})"

        table = Table.grid(expand=True)
        table.add_column()
        table.add_column(justify="right")
        table.add_row("Starting Cash", format_money(wallet_view["starting_cash"]))
        table.add_row("Cash", format_money(wallet_view["cash"]))
        table.add_row("Position Value", format_money(wallet_view["position_value"]))
        table.add_row("Total Equity", format_money(wallet_view["total_equity"]))
        table.add_row("Realized PnL", pnl_cell(wallet_view["realized_pnl"], wallet_view["realized_pnl_pct"]))
        table.add_row("Unrealized PnL", pnl_cell(wallet_view["unrealized_pnl"], wallet_view["unrealized_pnl_pct"]))
        table.add_row("Total PnL", pnl_cell(wallet_view["total_pnl"], wallet_view["total_pnl_pct"]))
        table.add_row("Fees Paid", format_money(wallet_view["fees_paid_usdc"]))
        table.add_row("Trades", str(wallet_view["trade_count"]))
        table.add_row("W / L", f"{wallet_view['wins']} / {wallet_view['losses']}")
        if position:
            table.add_row("Held Side", position["side"].upper())
            table.add_row("Shares", format_shares(position["shares"]))
            table.add_row("Avg Entry", format_percent(position.get("entry_effective_price")))
            table.add_row("Entry Kind", position.get("entry_kind", "-"))
        return Panel(table, title="Wallet Balance / PnL", border_style="green")

    def build_logs_panel(self, snapshot: DashboardSnapshot) -> Panel:
        recent_logs = filter_trade_log_entries(
            list(snapshot.state.get("recent_logs") or []),
            limit=self.config.log_limit,
        )
        if not recent_logs:
            return Panel("No successful entry, TP, or SL trades yet.", title="Entry / TP / SL Logs", border_style="yellow")
        log_lines: list[Text] = []
        styles = {
            "entry": "bold green",
            "tp": "bold cyan",
            "sl": "bold red",
        }
        for entry in recent_logs:
            ts = parse_datetime(entry.get("ts"))
            label = (entry.get("event_type") or "entry").lower()
            timestamp = ts.astimezone(ET).strftime("%H:%M:%S") if ts else "--:--:--"
            log_lines.append(Text(f"[{timestamp}] {entry.get('message')}", style=styles.get(label, "white")))
        return Panel(Group(*log_lines), title="Entry / TP / SL Logs", border_style="yellow")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Paper trade BTC 15m Polymarket fast markets with a live terminal dashboard.")
    parser.add_argument("--mode", choices=["loop", "once"], default="loop")
    parser.add_argument("--duration-seconds", type=float, default=None, help="Optional max runtime for loop mode.")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    load_dotenv(Path(".env"))
    args = parse_args(argv or sys.argv[1:])
    config = PaperConfig.from_env()
    logger = setup_file_logger(config.log_path)
    discovery = SimmerFastMarketDiscovery(
        api_key=config.simmer_api_key,
        timeout_seconds=config.request_timeout_seconds,
        request_retries=config.request_retries,
    )
    market_data = PolymarketCLOB(
        timeout_seconds=config.request_timeout_seconds,
        request_retries=config.request_retries,
    )
    price_data = BitcoinReferenceData(
        timeout_seconds=config.request_timeout_seconds,
        request_retries=config.request_retries,
    )
    state_store = PaperStateStore(config.state_path, config.starting_cash)
    trader = PolymarketPaperTrader(
        config=config,
        discovery=discovery,
        market_data=market_data,
        price_data=price_data,
        state_store=state_store,
        logger=logger,
    )

    try:
        trader.preflight()
        if args.mode == "once":
            trader.run_once()
        else:
            trader.run_loop(duration_seconds=args.duration_seconds)
        return 0
    except KeyboardInterrupt:
        trader.record_event(event_type="info", message="Paper trader stopped by operator.")
        state_store.save(trader.state)
        return 130
    finally:
        discovery.close()
        market_data.close()
        price_data.close()
        state_store.save(trader.state)


if __name__ == "__main__":
    raise SystemExit(main())
