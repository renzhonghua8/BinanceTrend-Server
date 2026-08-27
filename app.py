import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import aiohttp
from aiohttp import web


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("binance-trend")

REST_BASE = os.getenv("BINANCE_REST_BASE", "https://fapi.binance.com")
WS_URL = os.getenv("BINANCE_WS_URL", "wss://fstream.binance.com/market/ws/!ticker@arr")
WEBHOOK = os.getenv("DINGTALK_WEBHOOK", "")
STATE_PATH = Path(os.getenv("STATE_PATH", "/data/state.json"))
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8080"))
ANALYSIS_CACHE_SECONDS = float(os.getenv("ANALYSIS_CACHE_SECONDS", "30"))


@dataclass
class Ticker:
    symbol: str
    price: float
    percent: float


class TrendService:
    def __init__(self) -> None:
        self.session: aiohttp.ClientSession | None = None
        self.tickers: dict[str, Ticker] = {}
        self.leader_initialized, self.leader = self._load_leader()
        self.last_event_at = 0.0
        self.last_notification_at = 0.0
        self.last_error: str | None = None
        self.notification_lock = asyncio.Lock()
        self.evaluation_lock = asyncio.Lock()
        self.evaluation_task: asyncio.Task[None] | None = None
        self.kline_limit = asyncio.Semaphore(10)
        self.analysis_cache: dict[str, tuple[float, str, float]] = {}

    def _load_leader(self) -> tuple[bool, str | None]:
        try:
            state = json.loads(STATE_PATH.read_text())
            if "qualifiedLeader" not in state:
                # Ignore the old raw-ranking leader state when upgrading.
                return False, None
            return True, state["qualifiedLeader"]
        except (FileNotFoundError, KeyError, json.JSONDecodeError, TypeError):
            return False, None

    def _save_leader(self, leader: str | None) -> None:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        temp = STATE_PATH.with_suffix(".tmp")
        temp.write_text(
            json.dumps(
                {"version": 2, "qualifiedLeader": leader}, ensure_ascii=False
            )
        )
        temp.replace(STATE_PATH)

    @staticmethod
    def eligible(symbol: str) -> bool:
        return symbol.endswith("USDT") and "_" not in symbol

    def top10(self) -> list[Ticker]:
        return sorted(self.tickers.values(), key=lambda item: item.percent, reverse=True)[:10]

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=20)
        self.session = aiohttp.ClientSession(timeout=timeout)
        await self.load_snapshot()
        await self.stream_forever()

    async def load_snapshot(self) -> None:
        assert self.session
        async with self.session.get(f"{REST_BASE}/fapi/v1/ticker/24hr") as response:
            response.raise_for_status()
            values = await response.json()
        self.apply_tickers(values)
        log.info("Loaded %d eligible futures tickers", len(self.tickers))

    def apply_tickers(self, values: list[dict[str, Any]]) -> None:
        for value in values:
            if "st" in value and value["st"] != 1:
                continue
            symbol = value.get("s") or value.get("symbol", "")
            if not self.eligible(symbol):
                continue
            try:
                self.tickers[symbol] = Ticker(
                    symbol=symbol,
                    price=float(value.get("c") or value["lastPrice"]),
                    percent=float(value.get("P") or value["priceChangePercent"]),
                )
            except (KeyError, TypeError, ValueError):
                continue

        ranking = self.top10()
        if not ranking:
            return
        if self.evaluation_task is None or self.evaluation_task.done():
            self.evaluation_task = asyncio.create_task(self.evaluate_qualified_leader())

    async def evaluate_qualified_leader(self) -> None:
        async with self.evaluation_lock:
            ranking = self.top10()
            if not ranking:
                return
            try:
                analyses = await asyncio.gather(
                    *(self.cached_classification(item.symbol) for item in ranking)
                )
            except Exception as error:
                self.last_error = f"analysis: {error}"
                log.exception("Failed to analyze qualified leaderboard")
                return

            qualified: list[tuple[int, Ticker, float]] = []
            for index, (ticker, analysis) in enumerate(zip(ranking, analyses), start=1):
                mark, line = analysis
                if mark != "qualified":
                    continue
                distance = (ticker.price - line) / ticker.price * 100
                qualified.append((index, ticker, distance))

            current = qualified[0][1].symbol if qualified else None
            previous = self.leader
            if not self.leader_initialized:
                self.leader_initialized = True
                self.leader = current
                self._save_leader(current)
                log.info(
                    "Initial qualified leader recorded: %s (no notification)",
                    current or "none",
                )
                return
            if previous == current:
                return

            self.leader = current
            self._save_leader(current)
            if current is None:
                log.info("No qualified leader; previous was %s (no notification)", previous)
                return

            log.info("Qualified leader changed: %s -> %s", previous or "none", current)
            await self.notify_leader_change(previous, current, qualified)

    async def cached_classification(self, symbol: str) -> tuple[str, float]:
        cached = self.analysis_cache.get(symbol)
        now = time.monotonic()
        if cached and cached[0] > now:
            return cached[1], cached[2]
        mark, line = await self.classify(symbol)
        self.analysis_cache[symbol] = (
            now + ANALYSIS_CACHE_SECONDS,
            mark,
            line,
        )
        return mark, line

    async def stream_forever(self) -> None:
        assert self.session
        delay = 1
        while True:
            try:
                async with self.session.ws_connect(WS_URL, heartbeat=20, receive_timeout=10) as ws:
                    log.info("Binance ranking stream connected")
                    delay = 1
                    async for message in ws:
                        if message.type == aiohttp.WSMsgType.TEXT:
                            values = json.loads(message.data)
                            self.last_event_at = time.time()
                            self.last_error = None
                            self.apply_tickers(values)
                        elif message.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                            break
                raise ConnectionError("ranking stream closed")
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.last_error = str(error)
                log.warning("Stream error: %s; reconnecting in %ss", error, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
                try:
                    await self.load_snapshot()
                except Exception as snapshot_error:
                    self.last_error = str(snapshot_error)
                    log.warning("Snapshot refresh failed: %s", snapshot_error)

    async def notify_leader_change(
        self,
        previous: str | None,
        current: str,
        qualified: list[tuple[int, Ticker, float]],
    ) -> None:
        async with self.notification_lock:
            try:
                rows = []
                for index, ticker, distance in qualified:
                    rows.append(
                        f"{index}. {ticker.symbol[:-4]}  24H {ticker.percent:+.2f}%  "
                        f"可回撤 {max(0.0, distance):.2f}%  价格 {self.format_price(ticker.price)}"
                    )
                row_text = "\n".join(rows)
                text = (
                    "Binance 合约榜单提醒\n"
                    f"合格第一名变化：{previous[:-4] if previous else '暂无'} → {current[:-4]}\n"
                    f"时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                    f"方向合格榜单：\n{row_text}"
                )
                await self.send_dingtalk(text)
                self.last_notification_at = time.time()
                log.info("DingTalk leaderboard notification sent")
            except Exception as error:
                self.last_error = f"notification: {error}"
                log.exception("Failed to send DingTalk notification")

    async def send_dingtalk(self, text: str) -> None:
        if not WEBHOOK:
            raise RuntimeError("DINGTALK_WEBHOOK is not configured")
        assert self.session
        async with self.session.post(
            WEBHOOK,
            json={"msgtype": "text", "text": {"content": text}},
        ) as response:
            response.raise_for_status()
            result = await response.json()
        if result.get("errcode") != 0:
            raise RuntimeError(result.get("errmsg", "DingTalk rejected message"))

    async def classify(self, symbol: str) -> tuple[str, float]:
        signals = await asyncio.gather(
            *(self.signal(symbol, interval) for interval in ("5m", "15m", "1h", "4h", "1d"))
        )
        m5 = signals[0]
        high_qualified = all(item[0] for item in signals[1:])
        if not high_qualified:
            return "normal", m5[1]
        return ("qualified" if m5[0] else "watch"), m5[1]

    async def signal(self, symbol: str, interval: str) -> tuple[bool, float]:
        assert self.session
        async with self.kline_limit:
            async with self.session.get(
                f"{REST_BASE}/fapi/v1/klines",
                params={"symbol": symbol, "interval": interval, "limit": 140},
            ) as response:
                response.raise_for_status()
                raw = await response.json()
        now_ms = time.time() * 1000
        candles = [
            (float(row[2]), float(row[3]), float(row[4]))
            for row in raw
            if float(row[6]) < now_ms
        ]
        result = self.supertrend(candles)
        if result is None:
            raise RuntimeError(f"not enough candles for {symbol} {interval}")
        return result

    @staticmethod
    def supertrend(
        candles: list[tuple[float, float, float]], length: int = 10, multiplier: float = 3
    ) -> tuple[bool, float] | None:
        if len(candles) < length + 3:
            return None
        count = len(candles)
        true_ranges = [0.0] * count
        for index, (high, low, _) in enumerate(candles):
            if index == 0:
                true_ranges[index] = high - low
            else:
                previous_close = candles[index - 1][2]
                true_ranges[index] = max(
                    high - low, abs(high - previous_close), abs(low - previous_close)
                )
        atr = [float("nan")] * count
        atr[length - 1] = sum(true_ranges[:length]) / length
        for index in range(length, count):
            atr[index] = (atr[index - 1] * (length - 1) + true_ranges[index]) / length
        upper = [float("nan")] * count
        lower = [float("nan")] * count
        bullish = False
        latest_line = float("nan")
        for index in range(length - 1, count):
            high, low, close = candles[index]
            mid = (high + low) / 2
            basic_upper = mid + multiplier * atr[index]
            basic_lower = mid - multiplier * atr[index]
            if (
                index == length - 1
                or basic_upper < upper[index - 1]
                or candles[index - 1][2] > upper[index - 1]
            ):
                upper[index] = basic_upper
            else:
                upper[index] = upper[index - 1]
            if (
                index == length - 1
                or basic_lower > lower[index - 1]
                or candles[index - 1][2] < lower[index - 1]
            ):
                lower[index] = basic_lower
            else:
                lower[index] = lower[index - 1]
            if index == length - 1:
                bullish = close > upper[index]
            elif bullish and close < lower[index]:
                bullish = False
            elif not bullish and close > upper[index]:
                bullish = True
            latest_line = lower[index] if bullish else upper[index]
        return candles[-1][2] > latest_line, latest_line

    @staticmethod
    def format_price(price: float) -> str:
        if price >= 1000:
            return f"{price:.2f}"
        if price >= 1:
            return f"{price:.4f}"
        if price >= 0.01:
            return f"{price:.6f}"
        return f"{price:.8f}"

    def health(self) -> dict[str, Any]:
        event_age = time.time() - self.last_event_at if self.last_event_at else None
        healthy = event_age is not None and event_age < 10
        return {
            "ok": healthy,
            "qualifiedLeader": self.leader,
            # Kept for compatibility with existing health checks.
            "leader": self.leader,
            "tickerCount": len(self.tickers),
            "lastEventAgeSeconds": event_age,
            "lastNotificationAt": self.last_notification_at or None,
            "lastError": self.last_error,
        }


service = TrendService()


async def health_handler(_: web.Request) -> web.Response:
    payload = service.health()
    return web.json_response(payload, status=200 if payload["ok"] else 503)


async def main() -> None:
    app = web.Application()
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HEALTH_PORT)
    await site.start()
    log.info("Health endpoint listening on :%d/health", HEALTH_PORT)
    try:
        await service.start()
    finally:
        if service.session:
            await service.session.close()
        await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
