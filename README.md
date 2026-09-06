# UAV Automation Service

A configurable Discord automation and market-data service for processing configured sources, routing matching events to notifications, maintaining counters and rotations, tracking selected account activity, querying a local SQLite market database, and optionally exposing an authenticated local API.

The public repository deliberately contains **no deployment-specific monitoring rules, channel IDs, role IDs, webhook URLs, account lists, or runtime state**. Those values belong in ignored local configuration.

## Highlights

- Profile-driven message matching and notification routing
- Discord slash commands for market statistics and database-backed queries
- Configurable watch rules with direct-message notifications
- Generic timed events, counters, named lists, rotations, and history counters
- SQLite persistence with safe writer shutdown and backup scripts
- Optional authenticated Flask API
- Bounded duplicate-delivery protection for source messages
- Public/private configuration separation designed for safe GitHub publishing
- Automated tests, security scanning, and release builds

## Repository layout

```text
.
├── uav_service/            # application package
├── config/
│   └── profile.example.json
├── examples/data/         # synthetic catalog examples only
├── data/                  # ignored deployment catalog/database
├── runtime/               # ignored deployment state (created locally)
├── scripts/
├── systemd/
├── tests/
├── .env.example
└── pyproject.toml
```

## Install

Python 3.11 or newer is required.

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
python -m pip install -e .[dev]
cp .env.example .env             # Windows: copy .env.example .env
python scripts/bootstrap_config.py
```

Edit `.env` and `config/profile.json` for your deployment. Do not commit either file.

For a configuration-only installation with no market scraper, leave `SCRAPER_ENABLED=false`. If ingestion is enabled, provide `MARKET_DATA_URL` and a compatible item catalog in `data/items.json` (start from `examples/data/items.example.json`).

Run the service with:

```bash
uav-service
```

or:

```bash
python -m uav_service
```

## Configuration model

`.env` stores credentials, IDs, paths, and endpoint values. `config/profile.json` stores operational matching rules and maps logical routes to environment-variable names. This separation means the reusable source can remain public while the actual deployment profile stays private.

Start from `config/profile.example.json`. The example uses synthetic terms only. A profile can define:

- logical notification routes;
- static text/regex rules;
- timed rules with relative expiry timestamps;
- configurable reward counters;
- operator-maintained named lists;
- rotating locations/states;
- historical counters over source-message history;
- aliases for private control commands.

## Discord commands

Public slash commands include:

- `/status`
- `/price`
- `/recent`
- `/player`
- `/hotitems`
- `/totals`
- `/daily`
- `/track`
- `/untrack`
- `/ask`
- `/historycount`

Operator-only text controls are intentionally generic. The engine supports ignore lists, watch rules, profile-defined named lists/rotations/counter aliases, and a simulation command. Exact private aliases live in the untracked profile.

## Development

```bash
python -m pip install -e .[dev]
pytest
python scripts/security_scan.py
```

The test suite uses synthetic fixtures. It should not require a live Discord bot, real webhook, production database, or private deployment profile.

## Releases

Tags matching `v*` trigger the release workflow. GitHub Actions runs the test and security checks, builds a source distribution and wheel, and attaches them to a GitHub Release.

## Deployment

`systemd/uav-automation-service.service` is a hardened example unit. Adjust its paths and service account for your host. Keep the deployment environment file outside the repository and place writable data/state in a dedicated directory.

## Security and privacy

Never commit:

- `.env`;
- `config/profile.json` or `config/private/`;
- `runtime/`;
- live database files;
- webhook URLs, bot tokens, private API tokens, or real deployment IDs.

Run `python scripts/security_scan.py` before publishing or tagging a release. See `SECURITY.md` for reporting guidance.

## License

MIT. See `LICENSE`.
