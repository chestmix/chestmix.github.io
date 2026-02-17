"""
data.markets.kalshi – Kalshi REST API v2 client.

Kalshi is a regulated US prediction market exchange.
Docs: https://trading-api.kalshi.com/trade-api/v2

Authentication: HMAC-SHA256 signed requests using API key + secret.
Weather markets on Kalshi are well categorised (series ticker prefix "KXWEATHER").
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

from .base import (
    BaseMarketClient,
    Market,
    Order,
    OrderBook,
    OrderStatus,
    PriceLevel,
    Side,
)

logger = logging.getLogger(__name__)

_WEATHER_SERIES = re.compile(
    r"^KXWEATHER|^KXPRECIP|^KXSNOW|^KXRAIN|^KXTEMP|^KXWIND|^KXHURR",
    re.IGNORECASE,
)

_CITY_COORDS: Dict[str, Dict[str, float]] = {
    "new york": {"lat": 40.71, "lon": -74.01},
    "los angeles": {"lat": 34.05, "lon": -118.24},
    "chicago": {"lat": 41.88, "lon": -87.63},
    "seattle": {"lat": 47.61, "lon": -122.33},
    "miami": {"lat": 25.77, "lon": -80.19},
    "boston": {"lat": 42.36, "lon": -71.06},
    "denver": {"lat": 39.74, "lon": -104.98},
    "dallas": {"lat": 32.78, "lon": -96.80},
    "atlanta": {"lat": 33.75, "lon": -84.39},
    "san francisco": {"lat": 37.77, "lon": -122.42},
}


def _extract_location(text: str) -> Optional[Dict[str, Any]]:
    lower = text.lower()
    for city, coords in _CITY_COORDS.items():
        if city in lower:
            return {**coords, "city": city.title()}
    return None


class KalshiClient(BaseMarketClient):
    PLATFORM = "kalshi"
    WEATHER_CATEGORY_TAGS = ["weather"]

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        base_url: str = "https://trading-api.kalshi.com/trade-api/v2",
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._base_url = base_url.rstrip("/")
        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    # ── Auth ──────────────────────────────────────────────────────────────────

    def _sign(self, method: str, path: str, body: str = "") -> Dict[str, str]:
        """
        Generate Kalshi HMAC-SHA256 signature headers.
        Timestamp is in milliseconds.
        """
        ts_ms = str(int(time.time() * 1000))
        message = ts_ms + method.upper() + path + body
        signature = hmac.new(
            self._api_secret.encode("utf-8"),
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        sig_b64 = base64.b64encode(signature).decode("utf-8")
        return {
            "KALSHI-ACCESS-KEY": self._api_key,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": sig_b64,
        }

    def _get(self, path: str, params: Optional[Dict] = None) -> Any:
        query = ("?" + urlencode(params)) if params else ""
        full_path = path + query
        headers = self._sign("GET", full_path)
        url = self._base_url + path
        resp = self._session.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, body: Dict) -> Any:
        import json
        body_str = json.dumps(body)
        headers = self._sign("POST", path, body_str)
        url = self._base_url + path
        resp = self._session.post(url, data=body_str, headers=headers, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def _delete(self, path: str) -> Any:
        headers = self._sign("DELETE", path)
        url = self._base_url + path
        resp = self._session.delete(url, headers=headers, timeout=15)
        resp.raise_for_status()
        return resp.json()

    # ── Market scanning ───────────────────────────────────────────────────────

    def get_markets(
        self,
        category: Optional[str] = None,
        tags: Optional[List[str]] = None,
        limit: int = 100,
    ) -> List[Market]:
        params: Dict[str, Any] = {
            "status": "open",
            "limit": min(limit, 200),
        }
        # Kalshi supports series_ticker prefix filtering
        if category == "weather" or (tags and "weather" in tags):
            params["series_ticker"] = "KXWEATHER"

        try:
            data = self._get("/markets", params=params)
        except Exception as exc:
            logger.error("Kalshi get_markets failed: %s", exc)
            return []

        markets: List[Market] = []
        for item in data.get("markets", []):
            market = self._parse_market(item)
            if market:
                markets.append(market)
        return markets

    def get_weather_markets(self, limit: int = 200) -> List[Market]:
        """Fetch only weather-series markets from Kalshi."""
        params: Dict[str, Any] = {
            "status": "open",
            "limit": min(limit, 200),
        }
        try:
            # Kalshi weather series tickers all start with KXWEATHER*
            data = self._get("/markets", params={**params, "series_ticker": "KXWEATHER"})
            markets = [self._parse_market(m) for m in data.get("markets", [])]
            return [m for m in markets if m is not None]
        except Exception as exc:
            logger.error("Kalshi get_weather_markets failed: %s", exc)
            return []

    def _parse_market(self, item: dict) -> Optional[Market]:
        try:
            ticker = item.get("ticker", "")
            title = item.get("title", "")
            subtitle = item.get("subtitle", "")
            question = f"{title} {subtitle}".strip()

            # Filter to weather-only
            is_weather = bool(
                _WEATHER_SERIES.match(ticker)
                or any(kw in question.lower() for kw in
                       ("rain", "snow", "precip", "storm", "temperature", "wind", "weather"))
            )
            if not is_weather:
                return None

            close_time = item.get("close_time") or item.get("expiration_time")
            resolution_date = (
                datetime.fromisoformat(close_time.replace("Z", "+00:00"))
                if close_time
                else datetime.now(timezone.utc)
            )

            # Kalshi prices are in cents [0–100]; convert to [0–1]
            yes_bid = float(item.get("yes_bid", 50)) / 100.0
            yes_ask = float(item.get("yes_ask", 50)) / 100.0
            no_bid = float(item.get("no_bid", 50)) / 100.0
            no_ask = float(item.get("no_ask", 50)) / 100.0

            yes_price = (yes_bid + yes_ask) / 2.0
            no_price = (no_bid + no_ask) / 2.0

            return Market(
                market_id=ticker,
                platform=self.PLATFORM,
                question=question,
                category="weather",
                tags=["weather"],
                resolution_date=resolution_date,
                yes_price=yes_price,
                no_price=no_price,
                volume_usd=float(item.get("volume", 0) or 0),
                open_interest=float(item.get("open_interest", 0) or 0),
                location=_extract_location(question),
                raw=item,
            )
        except Exception as exc:
            logger.warning("Kalshi _parse_market error: %s | item=%s", exc, item)
            return None

    # ── Order book ────────────────────────────────────────────────────────────

    def get_order_book(self, market_id: str) -> OrderBook:
        try:
            data = self._get(f"/markets/{market_id}/orderbook")
            book = data.get("orderbook", {})
            # Kalshi: bids/asks are lists of [price_cents, size]
            yes_bids = [
                PriceLevel(price=float(b[0]) / 100.0, size=float(b[1]))
                for b in sorted(book.get("yes", []), key=lambda x: -x[0])
            ]
            yes_asks = [
                PriceLevel(price=float(a[0]) / 100.0, size=float(a[1]))
                for a in sorted(book.get("yes", []), key=lambda x: x[0])
            ]
            return OrderBook(
                market_id=market_id,
                platform=self.PLATFORM,
                yes_bids=yes_bids,
                yes_asks=yes_asks,
            )
        except Exception as exc:
            logger.error("Kalshi get_order_book failed for %s: %s", market_id, exc)
            return OrderBook(
                market_id=market_id, platform=self.PLATFORM, yes_bids=[], yes_asks=[]
            )

    # ── Order management ──────────────────────────────────────────────────────

    def place_order(self, order: Order) -> Order:
        if order.dry_run:
            logger.info("[DRY RUN] Would place %s order on Kalshi: %s", order.side, order)
            order.status = OrderStatus.FILLED
            order.filled_price = order.price
            order.filled_size = order.size_usd
            return order

        try:
            # Kalshi expects price in cents
            body = {
                "ticker": order.market_id,
                "action": "buy",
                "side": order.side.value.lower(),
                "type": "limit",
                "yes_price": int(round(order.price * 100)),
                "count": int(order.size_usd),   # Kalshi: count = number of contracts ($1 each)
                "time_in_force": "GTC",
            }
            resp = self._post("/portfolio/orders", body)
            order.order_id = resp.get("order", {}).get("order_id")
            order.status = OrderStatus.OPEN
            logger.info("Kalshi order placed: %s", order.order_id)
        except Exception as exc:
            logger.error("Kalshi place_order failed: %s", exc)
        return order

    def cancel_order(self, order_id: str, market_id: str) -> bool:
        try:
            self._delete(f"/portfolio/orders/{order_id}")
            return True
        except Exception as exc:
            logger.error("Kalshi cancel_order failed for %s: %s", order_id, exc)
            return False

    def get_positions(self) -> List[Order]:
        try:
            data = self._get("/portfolio/positions")
            positions = []
            for p in data.get("market_positions", []):
                if p.get("position", 0) == 0:
                    continue
                side = Side.YES if p["position"] > 0 else Side.NO
                positions.append(Order(
                    market_id=p["ticker"],
                    platform=self.PLATFORM,
                    side=side,
                    price=float(p.get("market_exposure", 0)),
                    size_usd=abs(float(p.get("position", 0))),
                    status=OrderStatus.OPEN,
                ))
            return positions
        except Exception as exc:
            logger.error("Kalshi get_positions failed: %s", exc)
            return []
