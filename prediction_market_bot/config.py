"""
config.py – centralised settings loaded from environment / .env file.
All other modules import from here; nothing reads os.environ directly.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the project root (parent of this file's directory)
_ENV_PATH = Path(__file__).parent / ".env"
load_dotenv(_ENV_PATH, override=False)


def _require(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            f"Copy .env.example → .env and fill in your credentials."
        )
    return value


def _get(key: str, default: str = "") -> str:
    return os.getenv(key, default)


# ── Kalshi ────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class KalshiConfig:
    api_key: str
    api_secret: str
    env: str  # "prod" | "demo"
    base_url: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "base_url",
            "https://trading-api.kalshi.com/trade-api/v2"
            if self.env == "prod"
            else "https://demo-api.kalshi.co/trade-api/v2",
        )

    @classmethod
    def from_env(cls) -> "KalshiConfig":
        return cls(
            api_key=_require("KALSHI_API_KEY"),
            api_secret=_require("KALSHI_API_SECRET"),
            env=_get("KALSHI_ENV", "demo"),
        )


# ── Polymarket ────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class PolymarketConfig:
    api_key: str
    api_secret: str
    api_passphrase: str
    private_key: str
    funder_address: str
    host: str = "https://clob.polymarket.com"
    chain_id: int = 137  # Polygon mainnet

    @classmethod
    def from_env(cls) -> "PolymarketConfig":
        return cls(
            api_key=_require("POLYMARKET_API_KEY"),
            api_secret=_require("POLYMARKET_API_SECRET"),
            api_passphrase=_require("POLYMARKET_API_PASSPHRASE"),
            private_key=_require("POLYMARKET_PRIVATE_KEY"),
            funder_address=_require("POLYMARKET_FUNDER_ADDRESS"),
        )


# ── Bot runtime settings ──────────────────────────────────────────────────────

@dataclass(frozen=True)
class BotConfig:
    min_edge_threshold: float       # Minimum edge fraction to trade (e.g. 0.05)
    kelly_fraction: float           # Fractional Kelly multiplier (e.g. 0.25)
    max_position_fraction: float    # Max single position as fraction of bankroll
    max_total_exposure: float       # Max total exposure as fraction of bankroll
    bankroll_usd: float             # Starting / current bankroll in USD
    poll_interval_seconds: int      # How often to run the pipeline
    dry_run: bool                   # If True, never place real orders

    @classmethod
    def from_env(cls) -> "BotConfig":
        return cls(
            min_edge_threshold=float(_get("MIN_EDGE_THRESHOLD", "0.05")),
            kelly_fraction=float(_get("KELLY_FRACTION", "0.25")),
            max_position_fraction=float(_get("MAX_POSITION_FRACTION", "0.08")),
            max_total_exposure=float(_get("MAX_TOTAL_EXPOSURE", "0.25")),
            bankroll_usd=float(_get("BANKROLL_USD", "1000.0")),
            poll_interval_seconds=int(_get("POLL_INTERVAL_SECONDS", "300")),
            dry_run=_get("DRY_RUN", "true").lower() != "false",
        )


# ── Aggregate config ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class AppConfig:
    kalshi: KalshiConfig
    polymarket: PolymarketConfig
    bot: BotConfig

    @classmethod
    def load(cls) -> "AppConfig":
        return cls(
            kalshi=KalshiConfig.from_env(),
            polymarket=PolymarketConfig.from_env(),
            bot=BotConfig.from_env(),
        )
