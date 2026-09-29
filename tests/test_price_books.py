import copy
import json
import tempfile
from unittest.mock import Mock
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
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        settings = Settings({"kafka": {"bootstrapServers": "unused:9092", "priceBooksTopic": "price-books",
                                      "priceBookMaxAgeSeconds": 300, "priceBookTimeoutSeconds": 5}},
                            root / "config.json", root)
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

    def test_background_refresh_caches_all_symbols_and_prices_never_consume(self):
        newer = book(stamp=999_500_000_000)
        newer["offer_side"]["levels"][1]["prc"] = "2.25"
        consumer = Consumer([[Message(book(), 0)], [Message(book("AAPL"), 1), Message(newer, 2)]])
        source = self.source(consumer)
        source.settings.data["api"] = {"allowedSymbols": ["BIYA"]}
        source.refresh()
        consumer.consume = Mock(side_effect=AssertionError("Order path must not poll Kafka"))
        result = source.price("BIYA", "Buy", "10")
        self.assertEqual((result.price, result.offset, result.topic), ("2.25", 2, "price-books"))
        self.assertEqual(source.price("AAPL", "Sell", "10").price, "2.22")
        self.assertFalse(consumer.options["enable.auto.commit"])
        self.assertFalse(consumer.options["enable.auto.offset.store"])
        self.assertEqual(consumer.options["auto.offset.reset"], "error")
        self.assertTrue(consumer.options["group.id"].startswith("gigamoney-price-books-"))
        self.assertEqual(consumer.lookups[0].offset, 700000)
        source.close()
        self.assertTrue(consumer.closed)

    def test_cli_reads_same_persisted_cache_without_constructing_consumer(self):
        consumer = Consumer([[Message(book(), 2)]])
        source = self.source(consumer)
        source.refresh()
        factory = Mock(side_effect=AssertionError("No Kafka connection at order time"))
        reader = KafkaPriceBooks(source.settings, consumer_factory=factory, clock=lambda: 1000)
        self.assertEqual(reader.price("BIYA", "Buy", "100").price, "2.30")
        factory.assert_not_called()
        reader.settings.data["kafka"]["bootstrapServers"] = "different-cluster:9092"
        with self.assertRaisesRegex(ValueError, "No cached"):
            reader.price("BIYA", "Buy", "100")

    def test_refresh_keeps_connection_and_advances_offsets_without_reseeking(self):
        consumer = Consumer([[Message(book(), 0), Message(book("AAPL"), 1), Message(book("AAPL"), 2)]])
        source = self.source(consumer)
        source.refresh()
        original_assignments = consumer.assignments
        consumer.highs = {0: 4}
        consumer.batches = [[Message(book("AAPL"), 3)]]
        source.refresh()
        self.assertIs(consumer.assignments, original_assignments)
        self.assertEqual(source._offsets, {0: 4})
        self.assertEqual(source.price("BIYA", "Buy", "1").offset, 0)
        self.assertFalse(consumer.closed)

    def test_polling_does_not_make_old_source_price_fresh(self):
        consumer = Consumer([[Message(book(), 2)]])
        source = self.source(consumer)
        source.refresh()
        source.clock = lambda: 1299
        self.assertEqual(source.price("BIYA", "Buy", "1").price, "2.24")
        source.clock = lambda: 1300
        source.refresh()
        with self.assertRaisesRegex(ValueError, "stale or future"):
            source.price("BIYA", "Buy", "1")
        # Neither legacy null nor an oversized setting disables five minutes.
        for configured in (None, 3600):
            source.settings.data["kafka"]["priceBookMaxAgeSeconds"] = configured
            with self.assertRaisesRegex(ValueError, "stale or future"):
                source.price("BIYA", "Buy", "1")

    def test_missing_book_fails_immediately_without_starting_feed(self):
        source = self.source(Consumer([]))
        source.consumer_factory = Mock(side_effect=AssertionError("No on-demand lookup"))
        with self.assertRaisesRegex(ValueError, "No cached"):
            source.price("BIYA", "Buy", "1")
        source.consumer_factory.assert_not_called()

    def test_newer_empty_or_future_snapshot_invalidates_previous_price(self):
        for mutation in (lambda x: x["offer_side"].update(levels=[]),
                         lambda x: x.update(update_time=1_001_000_000_000)):
            with self.subTest(mutation=mutation):
                consumer = Consumer([[Message(book(), 2)]])
                source = self.source(consumer)
                source.refresh()
                invalid = book(); mutation(invalid)
                consumer.highs = {0: 4}
                consumer.batches = [[Message(invalid, 3)]]
                source.refresh()
                with self.assertRaises(ValueError):
                    source.price("BIYA", "Buy", "1")

    def test_incomplete_background_refresh_retains_only_previous_complete_cache(self):
        consumer = Consumer([[Message(book(), 2)]])
        source = self.source(consumer)
        source.refresh()
        previous = source.cache_path.read_bytes()
        consumer.highs = {0: 5}
        newer = book(); newer["offer_side"]["levels"][1]["prc"] = "2.25"
        consumer.batches = [[Message(newer, 3)]]
        with self.assertRaisesRegex(ValueError, "deadline"):
            source.refresh()
        self.assertEqual(source.cache_path.read_bytes(), previous)
        self.assertEqual(source.price("BIYA", "Buy", "1").price, "2.24")

    def test_conflicting_partition_snapshots_are_not_tradeable(self):
        other = book(); other["offer_side"]["levels"][0]["prc"] = "500"
        consumer = Consumer([[Message(book(), 0, 0), Message(other, 0, 1)]], highs={0: 1, 1: 1})
        source = self.source(consumer)
        source.refresh()
        with self.assertRaisesRegex(ValueError, "inconsistent"):
            source.price("BIYA", "Buy", "1")

    def test_worker_refreshes_on_thirty_second_cadence_and_closes(self):
        consumer = Consumer([])
        source = self.source(consumer)
        elapsed = [0.0]
        source.monotonic = lambda: elapsed[0]
        waits = []
        class Stop:
            stopped = False
            def is_set(self): return self.stopped
            def wait(self, seconds):
                waits.append(seconds)
                elapsed[0] += seconds
                self.stopped = len(waits) == 2
        source._stop = Stop()
        calls = []
        def refresh():
            calls.append(elapsed[0])
            elapsed[0] += 2
        source.refresh = refresh
        source._consumer = consumer
        source._run()
        self.assertEqual(calls, [0, 30])
        self.assertEqual(waits, [28, 28])
        self.assertTrue(consumer.closed)
