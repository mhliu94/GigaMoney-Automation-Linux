"""Read Kafka order-book snapshots without joining the trading command group."""
from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal, InvalidOperation
import json
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
    def __init__(self, settings, *, consumer_factory=None, clock=time.time, monotonic=time.monotonic):
        self.settings = settings
        self.consumer_factory = consumer_factory
        self.clock, self.monotonic = clock, monotonic
        self.cached = {}
        self.scanned = {}

    def price(self, symbol, side, quantity):
        from confluent_kafka import Consumer, TopicPartition

        topic = self.settings.require("kafka.priceBooksTopic")
        max_age = self.settings.get("kafka.priceBookMaxAgeSeconds")
        lookback = float(self.settings.get("kafka.priceBookLookbackSeconds", 3600))
        timeout = float(self.settings.get("kafka.priceBookTimeoutSeconds", 30))
        batch_size = int(self.settings.get("kafka.priceBookBatchSize", 500))
        deadline = self.monotonic() + timeout

        def remaining():
            value = deadline - self.monotonic()
            if value <= 0:
                raise ValueError("Timed out reading a current Kafka price book.")
            return value

        consumer = (self.consumer_factory or Consumer)({
            **self.settings.kafka_client_config,
            "bootstrap.servers": self.settings.require("kafka.bootstrapServers"),
            "group.id": "gigamoney-price-books-" + uuid.uuid4().hex,
            "enable.auto.commit": False, "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
        })
        try:
            metadata = consumer.list_topics(topic, timeout=remaining()).topics.get(topic)
            if metadata is None or metadata.error or not metadata.partitions:
                raise ValueError("Kafka price-books topic is unavailable.")
            partitions = sorted(metadata.partitions)
            offsets = consumer.offsets_for_times([
                TopicPartition(topic, p, int((self.clock() - (max_age or lookback)) * 1000)) for p in partitions
            ], timeout=remaining())
            assignments, ends, floors, starts, caught_up = [], {}, {}, {}, set()
            candidate = self.cached.get(symbol)
            scan_from = self.scanned.get(symbol, {})
            for offset in offsets:
                if offset.error:
                    raise ValueError("Kafka price-book offset lookup failed.")
                low, high = consumer.get_watermark_offsets(offset, timeout=remaining())
                floor = high if offset.offset < 0 else max(low, offset.offset)
                floors[offset.partition] = floor
                if candidate and low <= scan_from.get(offset.partition, -1) <= high:
                    start = scan_from[offset.partition]
                elif len(partitions) == 1:
                    # Search recent windows first instead of downloading every
                    # other symbol's history before finding a quiet stock's book.
                    start = max(floor, high - batch_size)
                else:
                    start = floor
                starts[offset.partition] = start
                assignments.append(TopicPartition(topic, offset.partition, start))
                ends[offset.partition] = high
                if start >= high:
                    caught_up.add(offset.partition)
            consumer.assign(assignments)
            initial_ends = dict(ends)

            def finish():
                book, partition, offset = candidate
                selected = select_price(book, symbol, side, quantity, now=self.clock(), max_age=max_age)
                self.cached[symbol] = candidate
                self.scanned[symbol] = initial_ends
                return replace(selected, topic=topic, partition=partition, offset=offset)

            if candidate is not None and len(caught_up) == len(ends):
                return finish()
            while self.monotonic() < deadline:
                messages = consumer.consume(batch_size, timeout=min(
                    float(self.settings.get("kafka.pollTimeoutSeconds", 1)), remaining()))
                for message in messages:
                    if message.error():
                        raise ValueError("Kafka price-book consumption failed.")
                    if message.offset() + 1 >= ends[message.partition()]:
                        caught_up.add(message.partition())
                    if message.offset() >= ends[message.partition()] and ends != initial_ends:
                        continue
                    try:
                        book = json.loads(message.value())
                    except (TypeError, ValueError):
                        continue
                    if not isinstance(book, dict) or book.get("symbol") != symbol:
                        continue
                    # Invalid newer snapshots must not fall back to an old price.
                    if candidate is None or (message.partition() == candidate[1] and message.offset() > candidate[2]):
                        candidate = book, message.partition(), message.offset()
                    elif message.partition() != candidate[1]:
                        stamp, previous = book.get("update_time"), candidate[0].get("update_time")
                        if type(stamp) is not int or type(previous) is not int:
                            raise ValueError("Kafka price-book timestamp is invalid.")
                        if stamp == previous and book != candidate[0]:
                            raise ValueError("Kafka price books disagree at the same timestamp.")
                        if stamp > previous:
                            candidate = book, message.partition(), message.offset()
                if candidate is not None and len(caught_up) == len(ends):
                    return finish()
                if len(partitions) == 1 and len(caught_up) == 1:
                    partition = partitions[0]
                    if starts[partition] > floors[partition]:
                        ends[partition] = starts[partition]
                        starts[partition] = max(floors[partition], ends[partition] - batch_size)
                        consumer.assign([TopicPartition(topic, partition, starts[partition])])
                        caught_up.clear()
            raise ValueError(f"No Kafka price book for {symbol} within the configured history/timeout on {topic}.")
        finally:
            # Direct assignment and disabled commits never change runner offsets.
            consumer.close()
