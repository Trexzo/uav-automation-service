from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

from .profile import ProfileError, load_profile

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if load_dotenv is not None:
    load_dotenv(PROJECT_ROOT / ".env")
if hasattr(time, "tzset"):
    time.tzset()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    return default if raw is None else raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return default if raw is None or not raw.strip() else int(raw)


def _env_path(name: str, default: Path) -> Path:
    raw = os.getenv(name)
    value = Path(raw).expanduser() if raw and raw.strip() else default
    if not value.is_absolute():
        value = PROJECT_ROOT / value
    return value.resolve()


@dataclass(frozen=True)
class Settings:
    project_root: Path = PROJECT_ROOT
    data_dir: Path = field(default_factory=lambda: _env_path("DATA_DIR", PROJECT_ROOT / "data"))
    state_dir: Path = field(default_factory=lambda: _env_path("STATE_DIR", PROJECT_ROOT / "runtime"))
    profile_path: Path = field(default_factory=lambda: _env_path("PROFILE_PATH", PROJECT_ROOT / "config" / "profile.json"))

    discord_bot_token: str = field(default_factory=lambda: os.getenv("DISCORD_BOT_TOKEN", "").strip())
    discord_guild_id: int = field(default_factory=lambda: _env_int("DISCORD_GUILD_ID", 0))
    source_channel_id: int = field(default_factory=lambda: _env_int("SOURCE_CHANNEL_ID", 0))
    control_channel_id: int = field(default_factory=lambda: _env_int("CONTROL_CHANNEL_ID", 0))
    owner_user_id: int = field(default_factory=lambda: _env_int("OWNER_USER_ID", 0))

    scraper_enabled: bool = field(default_factory=lambda: _env_bool("SCRAPER_ENABLED", True))
    scraper_workers: int = field(default_factory=lambda: _env_int("SCRAPER_WORKERS", 2))
    scraper_max_workers: int = field(default_factory=lambda: _env_int("SCRAPER_MAX_WORKERS", 4))
    polling_interval: int = field(default_factory=lambda: _env_int("POLLING_INTERVAL", 60))
    tracking_interval: int = field(default_factory=lambda: _env_int("TRACKING_INTERVAL", 60))
    market_data_url: str = field(default_factory=lambda: os.getenv("MARKET_DATA_URL", "").strip())

    api_enabled: bool = field(default_factory=lambda: _env_bool("API_ENABLED", False))
    api_host: str = field(default_factory=lambda: os.getenv("API_HOST", "127.0.0.1").strip())
    api_port: int = field(default_factory=lambda: _env_int("API_PORT", 8080))
    api_token: str = field(default_factory=lambda: os.getenv("API_TOKEN", "").strip())
    api_cors_origins: tuple[str, ...] = field(default_factory=lambda: tuple(origin.strip() for origin in os.getenv("API_CORS_ORIGINS", "").split(",") if origin.strip()))

    command_prefix: str = field(default_factory=lambda: os.getenv("COMMAND_PREFIX", "!").strip() or "!")
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO").upper())
    webhook_queue_size: int = field(default_factory=lambda: max(1, _env_int("WEBHOOK_QUEUE_SIZE", 500)))

    @property
    def tradingpost_url(self) -> str:  # compatibility for market_service
        return self.market_data_url

    @property
    def db_path(self) -> Path:
        return self.data_dir / "entiredatabase.db"

    @property
    def items_json(self) -> Path:
        return self.data_dir / "items.json"

    @property
    def currencies_json(self) -> Path:
        return self.data_dir / "currencies.json"

    def ensure_directories(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        (self.data_dir / "backups").mkdir(parents=True, exist_ok=True)

    def profile(self) -> dict:
        return load_profile(self.profile_path)

    @property
    def webhook_urls(self) -> dict[str, str]:
        try:
            routes = self.profile().get("routes", {})
        except ProfileError:
            return {}
        return {name: os.getenv(str(route.get("webhook_env", "")), "").strip() for name, route in routes.items()}

    def route_mention(self, route_name: str) -> str:
        route = self.profile().get("routes", {}).get(route_name, {})
        env_name = str(route.get("mention_env", "")).strip()
        return os.getenv(env_name, "").strip() if env_name else ""

    def validate_runtime(self) -> list[str]:
        errors: list[str] = []
        if not self.discord_bot_token:
            errors.append("DISCORD_BOT_TOKEN is not configured")
        if self.source_channel_id <= 0:
            errors.append("SOURCE_CHANNEL_ID is not configured")
        if self.control_channel_id <= 0:
            errors.append("CONTROL_CHANNEL_ID is not configured")
        if self.source_channel_id > 0 and self.source_channel_id == self.control_channel_id:
            errors.append("SOURCE_CHANNEL_ID and CONTROL_CHANNEL_ID must be different")
        if self.scraper_enabled and not self.market_data_url:
            errors.append("MARKET_DATA_URL is required when SCRAPER_ENABLED=true")
        if self.scraper_enabled and not self.items_json.exists():
            errors.append(f"items.json is missing at {self.items_json}")
        if self.api_enabled and not self.api_token:
            errors.append("API_TOKEN is required when API_ENABLED=true")
        if not 1 <= self.api_port <= 65535:
            errors.append("API_PORT must be between 1 and 65535")
        if self.scraper_workers < 1 or self.scraper_max_workers < 1:
            errors.append("SCRAPER_WORKERS and SCRAPER_MAX_WORKERS must be at least 1")
        if self.polling_interval < 5 or self.tracking_interval < 5:
            errors.append("POLLING_INTERVAL and TRACKING_INTERVAL must be at least 5 seconds")
        try:
            self.profile()
        except ProfileError as exc:
            errors.append(str(exc))
        return errors


settings = Settings()
