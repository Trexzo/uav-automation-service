from __future__ import annotations

import asyncio
import logging
import re
from collections import deque
from datetime import datetime
from typing import Iterable

import discord
from discord import app_commands
from discord.ext import commands

from . import market_service, queries
from .config import settings
from .engine import AutomationEngine, DmRequest
from .webhook_service import queue_webhook

logger = logging.getLogger(__name__)


def _trim(text: str, limit: int = 1900) -> str:
    return text if len(text) <= limit else text[: limit - 20] + "\n…truncated"


def _format_trade_lines(trades: Iterable[dict], player: bool = False) -> str:
    lines: list[str] = []
    for trade in trades:
        role = f"{trade['role']} " if player else ""
        lines.append(
            f"• `{trade['timestamp']}` — {role}{trade['quantity']}x **{trade['item']}** "
            f"at **{trade['price']:,}**"
        )
    return "\n".join(lines)


class TrexzoBot(commands.Bot):
    def __init__(self, engine: AutomationEngine, shutdown_event) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.messages = True
        intents.message_content = True
        super().__init__(
            command_prefix=settings.command_prefix,
            intents=intents,
            help_command=None,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        self.engine = engine
        self.shutdown_event = shutdown_event
        self._commands_synced = False
        self._background_tasks: set[asyncio.Task] = set()
        self._processed_source_ids = deque()
        self._processed_source_id_set: set[object] = set()
        self._processed_source_limit = 5000
        self._source_processing_lock = None
        self._history_count_lock = None
        self._history_count_cache: dict[str, tuple[float, int]] = {}
        self._history_count_cache_seconds = 600.0

    def _start_background_task(self, coroutine, *, name: str) -> None:
        task = asyncio.create_task(coroutine, name=name)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_task_done)

    def _background_task_done(self, task: asyncio.Task) -> None:
        self._background_tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error(
                "Discord background task %s failed",
                task.get_name(),
                exc_info=(type(error), error, error.__traceback__),
            )
            self.shutdown_event.set()

    def _mark_source_message(self, delivery_key) -> bool:
        if delivery_key in self._processed_source_id_set:
            return False
        if len(self._processed_source_ids) >= self._processed_source_limit:
            expired = self._processed_source_ids.popleft()
            self._processed_source_id_set.discard(expired)
        self._processed_source_ids.append(delivery_key)
        self._processed_source_id_set.add(delivery_key)
        return True

    def _forget_source_message(self, delivery_key) -> None:
        if delivery_key not in self._processed_source_id_set:
            return
        self._processed_source_id_set.discard(delivery_key)
        try:
            self._processed_source_ids.remove(delivery_key)
        except ValueError:
            pass

    async def setup_hook(self) -> None:
        self._start_background_task(self._shutdown_watch(), name="shutdown-watch")
        self._start_background_task(self._trade_alert_watch(), name="trade-alert-watch")
        try:
            if settings.discord_guild_id:
                guild = discord.Object(id=settings.discord_guild_id)
                self.tree.copy_global_to(guild=guild)
                synced = await self.tree.sync(guild=guild)
            else:
                synced = await self.tree.sync()
            self._commands_synced = True
            logger.info("Synced %d application commands", len(synced))
        except Exception:
            logger.exception("Application command sync failed")

    async def _shutdown_watch(self) -> None:
        while not self.shutdown_event.is_set():
            await asyncio.sleep(0.5)
        logger.info("Shutdown event observed by Discord bot")
        await self.close()

    async def _trade_alert_watch(self) -> None:
        await self.wait_until_ready()
        while not self.shutdown_event.is_set() and not self.is_closed():
            try:
                alerts = await asyncio.to_thread(market_service.get_pending_discord_alerts, 50)
                for alert in alerts:
                    raw_price = int(alert["price"] or 0)
                    price = raw_price * 100_000_000 if int(alert["currency"] or 0) == 1 else raw_price
                    content = (
                        f"📈 **Tracked trade: {alert['username']}**\n"
                        f"`{alert['timestamp']}` — {alert['role']} "
                        f"{alert['quantity']}x **{alert['item_name']}** at **{price:,}**"
                    )
                    try:
                        user = self.get_user(int(alert["discord_user_id"])) or await self.fetch_user(int(alert["discord_user_id"]))
                        await user.send(_trim(content))
                    except (discord.Forbidden, discord.NotFound):
                        await asyncio.to_thread(market_service.mark_trade_alert_seen, alert["alert_id"])
                    except Exception:
                        logger.exception("Transient DM failure for tracked alert %s", alert["alert_id"])
                        break
                    else:
                        await asyncio.to_thread(market_service.mark_trade_alert_seen, alert["alert_id"])
            except Exception:
                logger.exception("Tracked-trade alert watcher failed")
            await asyncio.sleep(5)

    @staticmethod
    def _can_manage_control(message: discord.Message) -> bool:
        if settings.owner_user_id and message.author.id == settings.owner_user_id:
            return True
        permissions = getattr(message.author, "guild_permissions", None)
        return bool(permissions and permissions.manage_guild)

    async def on_ready(self) -> None:
        logger.info("Discord bot connected as %s (%s)", self.user, getattr(self.user, "id", "?"))

    async def _send_dms(self, requests: Iterable[DmRequest]) -> None:
        for request in requests:
            try:
                user = self.get_user(request.user_id) or await self.fetch_user(request.user_id)
                await user.send(_trim(request.content))
            except Exception:
                logger.exception("Failed to DM Discord user %s", request.user_id)

    async def _history_count(self, metric_name: str) -> tuple[dict, int]:
        metric = self.engine.history_counter(metric_name)
        if metric is None:
            raise KeyError(metric_name)
        canonical = str(metric.get("name", metric_name))
        if self._history_count_lock is None:
            self._history_count_lock = asyncio.Lock()
        async with self._history_count_lock:
            now = asyncio.get_running_loop().time()
            cached = self._history_count_cache.get(canonical)
            if cached is not None and now - cached[0] < self._history_count_cache_seconds:
                return metric, cached[1]

            channel = self.get_channel(settings.source_channel_id)
            if channel is None:
                channel = await self.fetch_channel(settings.source_channel_id)
            if not isinstance(channel, (discord.TextChannel, discord.Thread)):
                raise RuntimeError("SOURCE_CHANNEL_ID does not point to a text channel")

            after_raw = str(metric.get("after", "")).strip()
            after = datetime.fromisoformat(after_raw) if after_raw else None
            search_term = str(metric.get("search_term", "")).lower()
            quantity_pattern = re.compile(str(metric.get("quantity_pattern", r"(?:\b(\d+)x\b|\bx(\d+)\b)")), re.IGNORECASE)
            total = 0
            kwargs = {"limit": None, "oldest_first": True}
            if after is not None:
                kwargs["after"] = after
            async for message in channel.history(**kwargs):
                for embed in message.embeds:
                    title = embed.title or ""
                    if search_term and search_term not in title.lower():
                        continue
                    match = quantity_pattern.search(title)
                    total += int(next((g for g in match.groups() if g), 1)) if match else int(metric.get("default_quantity", 0))
            self._history_count_cache[canonical] = (asyncio.get_running_loop().time(), total)
            return metric, total

    async def _process_source_embeds(self, message: discord.Message) -> bool:
        if self._source_processing_lock is None:
            self._source_processing_lock = asyncio.Lock()
        async with self._source_processing_lock:
            processed_any = False
            for index, embed in enumerate(message.embeds):
                if not embed.title:
                    continue
                delivery_key = (message.id, index)
                if not self._mark_source_message(delivery_key):
                    logger.warning("Ignored duplicate source embed %s[%s]", message.id, index)
                    continue
                try:
                    dms = await asyncio.to_thread(self.engine.process_message, embed.title)
                    await self._send_dms(dms)
                except Exception:
                    self._forget_source_message(delivery_key)
                    raise
                processed_any = True
            return processed_any

    async def _process_simulated_embed(self, content: str) -> None:
        if self._source_processing_lock is None:
            self._source_processing_lock = asyncio.Lock()
        async with self._source_processing_lock:
            await asyncio.to_thread(self.engine.process_test_message, content)

    async def on_message(self, message: discord.Message) -> None:
        if message.author == self.user:
            return
        try:
            if message.channel.id == settings.source_channel_id:
                await self._process_source_embeds(message)

            if message.channel.id == settings.control_channel_id:
                content = message.content.strip()
                is_command = content.startswith(settings.command_prefix)
                is_simulation = self.engine.looks_like_simulated_input(content)
                if (is_command or is_simulation) and not self._can_manage_control(message):
                    logger.warning("Ignored unauthorized control message from Discord user %s", message.author.id)
                    return

                history_alias = None
                if is_command:
                    command = content[len(settings.command_prefix):].split(maxsplit=1)[0].lower()
                    for metric in self.engine.profile.get("history_counters", []):
                        names = {str(metric.get("name", "")).lower(), *{str(v).lower() for v in metric.get("aliases", [])}}
                        if command in names:
                            history_alias = command
                            break
                if history_alias:
                    try:
                        metric, total = await self._history_count(history_alias)
                        label = str(metric.get("label", metric.get("name", "Configured history count")))
                        route = str(metric.get("route", self.engine.profile.get("counter_reply_route")))
                        queue_webhook(route, f"**{label}:** {total}")
                    except Exception:
                        logger.exception("History counter failed")
                else:
                    handled, replies, dms = await asyncio.to_thread(self.engine.handle_control_command, content)
                    route = str(self.engine.profile.get("control_reply_route"))
                    for reply in replies:
                        queue_webhook(route, _trim(reply))
                    await self._send_dms(dms)
                    if not handled and is_simulation:
                        await self._process_simulated_embed(content)
        except Exception:
            logger.exception("Failed to process Discord message %s", message.id)

        await self.process_commands(message)


def create_bot(engine: AutomationEngine, shutdown_event) -> TrexzoBot:
    bot = TrexzoBot(engine, shutdown_event)

    async def item_autocomplete(interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
        names = await asyncio.to_thread(queries.search_items, current, 20)
        return [app_commands.Choice(name=name[:100], value=name[:100]) for name in names[:20]]

    @bot.tree.command(name="status", description="Show ingestion and database status")
    async def status_command(interaction: discord.Interaction) -> None:
        await interaction.response.defer(thinking=True)
        status = await asyncio.to_thread(queries.get_status)
        text = (
            f"**Trexzo status**\n"
            f"Trades: **{status['total_trades']:,}**\n"
            f"Backfilled items: **{status['backfilled_items']:,}**\n"
            f"Latest trade: `{status['latest_trade'] or 'none'}`\n"
            f"Workers: **{status['workers']}**\n"
            f"Cycle delay: **{status['polling_interval']}s**"
        )
        await interaction.followup.send(text)

    @bot.tree.command(name="price", description="Show recent price statistics for an item")
    @app_commands.describe(item="Item name", days="Number of days, 1–3650")
    @app_commands.autocomplete(item=item_autocomplete)
    async def price_command(interaction: discord.Interaction, item: str, days: int = 30) -> None:
        await interaction.response.defer(thinking=True)
        stats = await asyncio.to_thread(queries.get_item_stats, item, days)
        if not stats:
            await interaction.followup.send(f"No recent trades found for **{item}**.")
            return
        await interaction.followup.send(
            f"**{stats['item']}** — last {stats['days']} days\n"
            f"Median: **{stats['median']:,}**\nAverage: **{stats['average']:,}**\n"
            f"Typical range: **{stats['low']:,}–{stats['high']:,}**\n"
            f"Latest: **{stats['latest']:,}** at `{stats['latest_at']}`\n"
            f"Trades sampled: **{stats['trades']:,}{'+' if stats.get('sample_limited') else ''}**"
        )

    @bot.tree.command(name="recent", description="Show recent trades for an item")
    @app_commands.describe(item="Item name", limit="Number of rows, 1–25")
    @app_commands.autocomplete(item=item_autocomplete)
    async def recent_command(interaction: discord.Interaction, item: str, limit: int = 10) -> None:
        await interaction.response.defer(thinking=True)
        trades = await asyncio.to_thread(queries.get_recent_item_trades, item, limit)
        if not trades:
            await interaction.followup.send(f"No trades found for **{item}**.")
            return
        await interaction.followup.send(_trim(f"**Recent {trades[0]['item']} trades**\n" + _format_trade_lines(trades)))

    @bot.tree.command(name="player", description="Show recent trades involving a configured account name")
    @app_commands.describe(username="Account name", limit="Number of rows, 1–25")
    async def player_command(interaction: discord.Interaction, username: str, limit: int = 10) -> None:
        await interaction.response.defer(thinking=True)
        trades = await asyncio.to_thread(queries.get_player_trades, username, limit)
        if not trades:
            await interaction.followup.send(f"No trades found for **{username}**.")
            return
        await interaction.followup.send(_trim(f"**Recent trades for {username}**\n" + _format_trade_lines(trades, player=True)))

    @bot.tree.command(name="hotitems", description="Show the most traded items in a time window")
    async def hotitems_command(interaction: discord.Interaction, hours: int = 24, limit: int = 10) -> None:
        hours = max(1, min(hours, 720))
        limit = max(1, min(limit, 25))
        await interaction.response.defer(thinking=True)
        items = await asyncio.to_thread(queries.get_hot_items, hours, limit)
        text = "\n".join(f"• **{entry['item']}** — {entry['trades']:,} trades" for entry in items)
        await interaction.followup.send(_trim(text or f"No trades found in the last {hours} hours."))

    @bot.tree.command(name="totals", description="Show totals for a configured counter group")
    async def totals_command(interaction: discord.Interaction, group: str = "") -> None:
        await interaction.response.send_message(_trim(engine.counter_totals_text(group or None)))

    @bot.tree.command(name="daily", description="Show daily totals for a configured counter group")
    async def daily_command(interaction: discord.Interaction, group: str = "") -> None:
        await interaction.response.send_message(_trim(engine.counter_daily_text(group or None)))

    @bot.tree.command(name="track", description="Track new trades involving an account name")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.checks.has_permissions(manage_guild=True)
    async def track_command(interaction: discord.Interaction, username: str) -> None:
        _ok, message = await asyncio.to_thread(market_service.add_tracked_user, username, interaction.user.id)
        await interaction.response.send_message(message, ephemeral=True)

    @bot.tree.command(name="untrack", description="Stop tracking an account's new trades")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.checks.has_permissions(manage_guild=True)
    async def untrack_command(interaction: discord.Interaction, username: str) -> None:
        _ok, message = await asyncio.to_thread(market_service.remove_tracked_user, username)
        await interaction.response.send_message(message, ephemeral=True)

    @bot.tree.command(name="ask", description="Ask a database-backed market question")
    async def ask_command(interaction: discord.Interaction, question: str) -> None:
        await interaction.response.defer(thinking=True)
        answer = await asyncio.to_thread(queries.ask_database, question)
        await interaction.followup.send(_trim(answer))

    @bot.tree.command(name="historycount", description="Count a configured historical metric")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.checks.has_permissions(manage_guild=True)
    async def historycount_command(interaction: discord.Interaction, metric: str) -> None:
        await interaction.response.defer(thinking=True)
        try:
            config, total = await bot._history_count(metric)
        except Exception as exc:
            logger.exception("/historycount failed")
            await interaction.followup.send(f"History count failed: `{type(exc).__name__}`", ephemeral=True)
            return
        await interaction.followup.send(f"**{config.get('label', metric)}:** {total}")

    @bot.tree.error
    async def application_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
        if isinstance(error, app_commands.MissingPermissions):
            text = "You need **Manage Server** permission to use that command."
        else:
            logger.error("Application command failed: %s", error, exc_info=(type(error), error, error.__traceback__))
            text = "That command failed. Check the service logs for details."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(text, ephemeral=True)
            else:
                await interaction.response.send_message(text, ephemeral=True)
        except Exception:
            logger.exception("Could not send application-command error response")

    return bot


def run_discord_bot(engine: AutomationEngine, shutdown_event) -> None:
    bot = create_bot(engine, shutdown_event)
    bot.run(settings.discord_bot_token, log_handler=None)
