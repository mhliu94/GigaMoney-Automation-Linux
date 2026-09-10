import copy
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

from gigamoney.config import Settings
from gigamoney.price_books import KafkaPriceBooks, select_price


def book(symbol="BIYA", stamp=999_000_000_000):
    def side(levels):
        return {"update_time": stamp, "levels": [{"prc": price, "total_qty": qty} for price, qty in levels]}
    return {"symbol": symbol, "update_time": stamp,
            "bid_side": side([("2.20", "80"), ("2.22", "30")]),
            "offer_side": side([("2.30", "80"), ("2.24", "30")])}


class Message:
    def __init__(self, value, offset, partition=0):
        self.data, self.index, self.part = value, offset, partition

    def value(self):
        return self.data if isinstance(self.data, bytes) else json.dumps(self.data).encode()

    def offset(self): return self.index
    def partition(self): return self.part
    def error(self): return None


class Consumer:
    def __init__(self, batches, highs=None):
        self.batches = list(batches)
        self.highs = highs or {0: 3}
        self.closed = False
        self.now = 0

    def list_topics(self, topic, **kwargs):
        return SimpleNamespace(topics={topic: SimpleNamespace(error=None, partitions=self.highs)})

    def offsets_for_times(self, partitions, **kwargs):
        self.lookups = partitions
        return [SimpleNamespace(partition=p, offset=0, error=None) for p in self.highs]

    def get_watermark_offsets(self, partition, **kwargs):
        return 0, self.highs[partition.partition]

    def assign(self, partitions): self.assignments = partitions

    def consume(self, count, timeout):
        self.now += timeout
        return self.batches.pop(0) if self.batches else []

    def close(self): self.closed = True


class PriceBookTests(unittest.TestCase):
    def source(self, consumer):
        settings = Settings({"kafka": {"bootstrapServers": "unused:9092", "priceBooksTopic": "price-books",
                                      "priceBookMaxAgeSeconds": 30, "priceBookTimeoutSeconds": 5}},
                            Path("/unused/config.json"), Path("/unused"))
        def factory(options):
            consumer.options = options
            return consumer
        return KafkaPriceBooks(settings, consumer_factory=factory, clock=lambda: 1000, monotonic=lambda: consumer.now)

    def test_walks_asks_for_buys_and_bids_for_sells_in_price_order(self):
        for side, qty, expected, levels in (("Buy", "20", "2.24", 1), ("Buy", "100", "2.30", 2),
                                            ("Sell", "20", "2.22", 1), ("Sell", "100", "2.20", 2)):
            with self.subTest(side=side, qty=qty):
                price = select_price(book(), "BIYA", side, qty, now=1000, max_age=30)
                self.assertEqual((price.price, price.levels_used), (expected, levels))

    def test_has_no_price_or_slippage_band_and_reports_insufficient_depth(self):
        data = book()
        data["offer_side"]["levels"][0]["prc"] = "99999999.99"
        price = select_price(data, "BIYA", "Buy", "200", now=1000, max_age=30)
        self.assertEqual(price.price, "99999999.99")
        self.assertEqual(price.available_quantity, "110")

    def test_validates_source_and_side_freshness_not_only_message_arrival(self):
        for stamp in (1_001_000_000_000, 960_000_000_000):
            for target in ("book", "offer_side"):
                with self.subTest(stamp=stamp, target=target):
                    data = book()
                    (data if target == "book" else data[target])["update_time"] = stamp
                    with self.assertRaisesRegex(ValueError, "stale or future"):
                        select_price(data, "BIYA", "Buy", "1", now=1000, max_age=30)

    def test_missing_wrong_or_invalid_prices_fail(self):
        for mutation in (lambda x: x.update(symbol="AAPL"), lambda x: x.update(update_time=999),
                         lambda x: x["offer_side"].update(levels=[]),
                         lambda x: x["offer_side"]["levels"][0].update(prc="NaN"),
                         lambda x: x["offer_side"]["levels"][0].update(total_qty="-1"),
                         lambda x: x["offer_side"]["levels"].append(copy.deepcopy(x["offer_side"]["levels"][0]))):
            data = book(); mutation(data)
            with self.assertRaises(ValueError):
                select_price(data, "BIYA", "Buy", "1", now=1000, max_age=30)

    def test_catches_up_and_uses_latest_symbol_snapshot_without_committing(self):
        newer = book(stamp=999_500_000_000)
        newer["offer_side"]["levels"][1]["prc"] = "2.25"
        consumer = Consumer([[Message(book(), 0)], [Message(book("AAPL"), 1), Message(newer, 2)]])
        result = self.source(consumer).price("BIYA", "Buy", "10")
        self.assertEqual((result.price, result.offset, result.topic), ("2.25", 2, "price-books"))
        self.assertFalse(consumer.options["enable.auto.commit"])
        self.assertFalse(consumer.options["enable.auto.offset.store"])
        self.assertTrue(consumer.options["group.id"].startswith("gigamoney-price-books-"))
        self.assertEqual(consumer.lookups[0].offset, 970000)
        self.assertTrue(consumer.closed)

    def test_newer_empty_snapshot_does_not_reuse_previous_price(self):
        empty = book(); empty["offer_side"]["levels"] = []
        consumer = Consumer([[Message(book(), 0), Message(book("AAPL"), 1), Message(empty, 2)]])
        with self.assertRaisesRegex(ValueError, "no executable"):
            self.source(consumer).price("BIYA", "Buy", "1")
        self.assertTrue(consumer.closed)

    def test_no_matching_quote_has_bounded_wait_and_closes_consumer(self):
        consumer = Consumer([[Message(book("AAPL"), 2)]])
        with self.assertRaisesRegex(ValueError, "No Kafka price book"):
            self.source(consumer).price("BIYA", "Buy", "1")
        self.assertLessEqual(consumer.now, 5)
        self.assertTrue(consumer.closed)

    def test_unchanged_book_can_be_used_when_age_limit_is_disabled(self):
        price = select_price(book(stamp=100_000_000_000), "BIYA", "Buy", "1", now=1000, max_age=None)
        self.assertEqual(price.price, "2.24")
        with self.assertRaisesRegex(ValueError, "future"):
            select_price(book(stamp=1_001_000_000_000), "BIYA", "Buy", "1", now=1000, max_age=None)

    def test_cache_advances_offsets_even_when_symbol_book_has_not_changed(self):
        consumer = Consumer([[Message(book(), 0), Message(book("AAPL"), 1), Message(book("AAPL"), 2)]])
        source = self.source(consumer)
        self.assertEqual(source.price("BIYA", "Buy", "1").price, "2.24")
        consumer.highs = {0: 4}
        consumer.batches = [[Message(book("AAPL"), 3)]]
        self.assertEqual(source.price("BIYA", "Buy", "1").price, "2.24")
        self.assertEqual(consumer.assignments[0].offset, 3)
        self.assertEqual(source.scanned["BIYA"], {0: 4})
        consumer.highs = {0: 5}
        empty = book(); empty["offer_side"]["levels"] = []
        consumer.batches = [[Message(empty, 4)]]
        with self.assertRaisesRegex(ValueError, "no executable"):
            source.price("BIYA", "Buy", "1")

    def test_searches_backwards_for_quiet_symbol_then_returns_latest_in_window(self):
        consumer = Consumer([[Message(book("AAPL"), 6), Message(book("AAPL"), 7)],
                             [Message(book(), 4), Message(book("AAPL"), 5), Message(book("AAPL"), 6)]],
                            highs={0: 8})
        source = self.source(consumer)
        source.settings.data["kafka"]["priceBookBatchSize"] = 2
        result = source.price("BIYA", "Buy", "1")
        self.assertEqual(result.offset, 4)
        self.assertEqual(source.scanned["BIYA"], {0: 8})

    def test_cached_book_is_still_checked_against_optional_max_age(self):
        consumer = Consumer([[Message(book(), 0), Message(book("AAPL"), 1), Message(book("AAPL"), 2)]])
        source = self.source(consumer)
        source.price("BIYA", "Buy", "1")
        source.clock = lambda: 1100
        with self.assertRaisesRegex(ValueError, "stale"):
            source.price("BIYA", "Buy", "1")

    def test_conflicting_partition_snapshots_are_rejected(self):
        other = book(); other["offer_side"]["levels"][0]["prc"] = "500"
        consumer = Consumer([[Message(book(), 0, 0), Message(other, 0, 1)]], highs={0: 1, 1: 1})
        with self.assertRaisesRegex(ValueError, "disagree"):
            self.source(consumer).price("BIYA", "Buy", "1")
        self.assertTrue(consumer.closed)
