"""Read Kafka order-book snapshots without joining the trading command group."""
from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
import hashlib
import json
import logging
import threading
import time
import uuid


def number(value, name):
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        raise ValueError(f"Kafka price book has an invalid {name}.") from None
    if not result.is_finite() or result <= 0:
        raise ValueError(f"Kafka price book requires a positive {name}.")
    return result


@dataclass(frozen=True)
class BookPrice:
    price: str
    timestamp_ns: int
    available_quantity: str
    levels_used: int
    topic: str = ""
    partition: int = -1
    offset: int = -1

    def details(self):
        return {"priceSource": "kafka", "referencePrice": self.price,
                "priceBookTimestampNs": self.timestamp_ns,
                "priceBookAvailableQuantity": self.available_quantity,
                "priceBookLevelsUsed": self.levels_used, "priceBookTopic": self.topic,
                "priceBookPartition": self.partition, "priceBookOffset": self.offset}


def select_price(book, symbol, side, quantity, *, now, max_age):
    """Walk the opposite side of a snapshot; impose no price/slippage band."""
    if book.get("symbol") != symbol or side not in {"Buy", "Sell"}:
        raise ValueError("Kafka price book does not match the requested symbol/side.")
    side_book = book.get("offer_side" if side == "Buy" else "bid_side", {})
    if not isinstance(side_book, dict):
        raise ValueError("Kafka price book has an invalid side.")
    stamps = [book.get("update_time"), side_book.get("update_time")]
    if any(type(value) is not int or value <= 0 for value in stamps):
        raise ValueError("Kafka price book requires nanosecond update_time values.")
    if any(now < value / 1e9 or (max_age is not None and now - value / 1e9 > max_age) for value in stamps):
        raise ValueError("Kafka price book is stale or future-dated.")
    levels = side_book.get("levels")
    if not isinstance(levels, list) or not levels:
        raise ValueError("Kafka price book has no executable levels on the requested side.")
    prices = {}
    for level in levels:
        if not isinstance(level, dict):
            raise ValueError("Kafka price book contains an invalid level.")
        raw_quantity = level.get("total_qty", level.get("qty"))
        if str(raw_quantity) in {"0", "0.0"}:
            continue
        size = number(raw_quantity, "level quantity")
        price = number(level.get("prc"), "level price")
        if price in prices:
            raise ValueError("Kafka price book contains duplicate price levels.")
        prices[price] = size
    if not prices:
        raise ValueError("Kafka price book has no executable levels on the requested side.")
    wanted, covered, used = number(quantity, "order quantity"), Decimal(0), 0
    for price in sorted(prices, reverse=side == "Sell"):
        covered += prices[price]
        used += 1
        if covered >= wanted:
            break
    # Insufficient visible depth uses its outermost level; it may partially fill.
    return BookPrice(format(price, "f"), min(stamps), format(covered, "f"), used)


class KafkaPriceBooks:
    """Background Kafka reader; order-time price selection only reads its cache."""

    def __init__(self, settings, *, consumer_factory=None, clock=time.time, monotonic=time.monotonic):
        self.settings = settings
        self.consumer_factory = consumer_factory
        self.clock, self.monotonic = clock, monotonic
        self.cache_path = settings.work_dir / "api" / "price-books.json"
        self.cached = {}
        self._consumer = None
        self._offsets = {}
        self._stop = threading.Event()
        self._thread = None

    @property
    def topic(self):
        return str(self.settings.get("kafka.priceBooksTopic", "price-books"))

    @property
    def max_age(self):
        # Legacy null settings must not disable the five-minute freshness rule.
        return min(float(self.settings.get("kafka.priceBookMaxAgeSeconds") or 300), 300)

    def _source(self):
        route = [self.settings.get("kafka.bootstrapServers", ""), self.topic]
        return hashlib.sha256(json.dumps(route).encode()).hexdigest()

    def _load_cache(self):
        try:
            state = json.loads(self.cache_path.read_text())
            if state.get("version") == 1 and state.get("source") == self._source() and isinstance(state.get("books"), dict):
                return state["books"]
        except (OSError, ValueError, AttributeError):
            pass
        return {}

    def price(self, symbol, side, quantity):
        # No consumer construction, polling, or network access on this path.
        entry = self._load_cache().get(symbol)
        if not isinstance(entry, dict):
            raise ValueError(f"No cached Kafka price book for {symbol}; background feed is not ready.")
        if entry.get("error"):
            raise ValueError(f"Cached Kafka price book for {symbol} is inconsistent across partitions.")
        try:
            selected = select_price(entry["book"], symbol, side, quantity,
                                    now=self.clock(), max_age=self.max_age)
            return replace(selected, topic=self.topic, partition=entry["partition"], offset=entry["offset"])
        except (KeyError, TypeError, AttributeError):
            raise ValueError(f"Cached Kafka price book for {symbol} is malformed.") from None

    @staticmethod
    def _merge(cache, book, partition, offset):
        symbol = book["symbol"]
        previous = cache.get(symbol)
        entry = {"book": book, "partition": partition, "offset": offset}
        if previous is None:
            cache[symbol] = entry
        elif previous["partition"] == partition:
            if offset > previous["offset"]:
                # Newer invalid/empty data replaces old data too; never silently
                # trade against an older snapshot after a newer one arrives.
                cache[symbol] = entry
        else:
            stamp, old_stamp = book.get("update_time"), previous["book"].get("update_time")
            if type(stamp) is not int or type(old_stamp) is not int:
                cache[symbol] = {**entry, "error": True}
            elif stamp > old_stamp:
                cache[symbol] = entry
            elif stamp == old_stamp and book != previous["book"]:
                cache[symbol] = {**entry, "error": True}

    def refresh(self):
        """Catch up a retained consumer in the worker, then atomically publish."""
        from confluent_kafka import Consumer, TopicPartition
        from .api import save_session

        started = self.monotonic()
        deadline = started + float(self.settings.get("kafka.priceBookTimeoutSeconds", 30))
        def remaining():
            left = deadline - self.monotonic()
            if left <= 0 or self._stop.is_set():
                raise ValueError("Background Kafka price-book refresh did not complete before its deadline.")
            return left

        if self._consumer is None:
            self._consumer = (self.consumer_factory or Consumer)({
                **self.settings.kafka_client_config,
                "bootstrap.servers": self.settings.require("kafka.bootstrapServers"),
                "group.id": "gigamoney-price-books-" + uuid.uuid4().hex,
                "enable.auto.commit": False, "enable.auto.offset.store": False,
                "allow.auto.create.topics": False, "auto.offset.reset": "error",
            })
        consumer = self._consumer
        metadata = consumer.list_topics(self.topic, timeout=remaining()).topics.get(self.topic)
        if metadata is None or metadata.error or not metadata.partitions:
            raise ValueError("Kafka price-books topic is unavailable to the background reader.")
        # A forward scan of the freshness window replaces repeated 500-record
        # backwards seeks. On subsequent polls retain the consumer and offsets.
        since = int((self.clock() - self.max_age) * 1000)
        floors = consumer.offsets_for_times([
            TopicPartition(self.topic, partition, since) for partition in sorted(metadata.partitions)
        ], timeout=remaining())
        assignments, ends, cursors = [], {}, {}
        for floor in floors:
            if floor.error:
                raise ValueError("Background Kafka price-book offset lookup failed.")
            low, high = consumer.get_watermark_offsets(floor, timeout=remaining())
            minimum = high if floor.offset < 0 else max(low, floor.offset)
            previous = self._offsets.get(floor.partition, minimum)
            cursor = max(minimum, previous) if previous <= high else minimum
            assignments.append(TopicPartition(self.topic, floor.partition, cursor))
            cursors[floor.partition], ends[floor.partition] = cursor, high
        if cursors != self._offsets:
            consumer.assign(assignments)
        cache = dict(self.cached or self._load_cache())
        count = 0
        batch_size = int(self.settings.get("kafka.priceBookBatchSize", 500))
        while any(cursors[part] < end for part, end in ends.items()):
            messages = consumer.consume(batch_size, timeout=min(1.0, remaining()))
            for message in messages:
                if message.error():
                    raise ValueError("Background Kafka price-book consumption failed.")
                partition, offset = message.partition(), message.offset()
                cursors[partition] = max(cursors[partition], offset + 1)
                count += 1
                try:
                    book = json.loads(message.value())
                except (TypeError, ValueError):
                    continue
                if not isinstance(book, dict) or not isinstance(book.get("symbol"), str):
                    continue
                self._merge(cache, book, partition, offset)
        state = {"version": 1, "source": self._source(), "topic": self.topic,
                 "refreshed_at": self.clock(), "books": cache}
        save_session(self.cache_path, state)
        self.cached, self._offsets = cache, cursors
        logging.getLogger("gigamoney").info(
            "Kafka price-book cache refreshed topic=%s symbols=%s messages=%d elapsed_s=%.3f",
            self.topic, ",".join(sorted(cache)), count, self.monotonic() - started)
        return state

    def _close_consumer(self):
        if self._consumer is not None:
            try:
                self._consumer.close()
            finally:
                self._consumer = None
                self._offsets = {}

    def _run(self):
        log = logging.getLogger("gigamoney")
        interval = float(self.settings.get("kafka.priceBookPollIntervalSeconds", 30))
        try:
            while not self._stop.is_set():
                started = self.monotonic()
                try:
                    self.refresh()
                except Exception as error:
                    if not self._stop.is_set():
                        # Keep the last completed cache. Its original source
                        # timestamp still expires at order time, even on outage.
                        log.warning("Kafka price-book background refresh failed error_type=%s; cached books still expire after %gs",
                                    type(error).__name__, self.max_age)
                    self._close_consumer()
                self._stop.wait(max(0, interval - (self.monotonic() - started)))
        finally:
            self._close_consumer()

    def start(self):
        if self._thread is not None and self._thread.is_alive():
            return
        self.cached = self._load_cache()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="gigamoney-price-books", daemon=True)
        self._thread.start()

    def close(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(float(self.settings.get("kafka.priceBookTimeoutSeconds", 30)) + 2)
        else:
            self._close_consumer()
