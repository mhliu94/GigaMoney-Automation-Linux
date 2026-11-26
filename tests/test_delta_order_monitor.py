from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from gigamoney.delta_order_monitor import DeltaOrderMonitor
import gigamoney.delta_order_monitor as module

def test_partial_fill_wakes_holdings_before_delivery_and_retries_transient_read(monkeypatch):
    runner = Mock(settings={}, running=True, stop_path=Path('nonexistent-stop-file'))
    api = Mock()
    api.poll_order_status.side_effect = [
        {'orders': [{'order_id': '1', 'status': 'PARTIALLY_FILLED', 'filled_quantity': 1}]},
        RuntimeError('temporary'),
        {'orders': [{'order_id': '1', 'status': 'PARTIALLY_FILLED', 'filled_quantity': 2}]},
        {'orders': [{'order_id': '1', 'status': 'PARTIALLY_FILLED', 'filled_quantity': 2}]},
    ]
    monkeypatch.setattr(module, 'ApiTrading', lambda settings: api)
    monitor = DeltaOrderMonitor(runner, Mock())
    waits = []
    def wait(delay):
        waits.append(delay)
        if len(waits) == 4: monitor.stop.set()
    monkeypatch.setattr(monitor.wake_event, 'wait', wait)
    ordered = []
    runner.request_holdings_refresh.side_effect = lambda: ordered.append('assets')
    runner._publish_orders.side_effect = lambda *args: ordered.append('orders')
    monitor.run()
    assert ordered == ['assets', 'orders', 'assets', 'orders', 'orders']
    assert len(waits) == 4 and all(.05 <= delay <= 1 for delay in waits)

def test_order_receipt_is_offered_before_another_broker_round_trip(monkeypatch):
    runner = Mock(settings={}, running=True, stop_path=Path("absent"))
    api = Mock()
    snapshot = {"order_id": "ticket", "command_id": "child", "status": "FILLED"}
    api.journal.read.return_value = {"order_snapshot": snapshot}
    events = []
    api.poll_order_status.side_effect = lambda: events.append("broker_read") or {"orders": []}
    runner._publish_orders.side_effect = lambda producer, rows, **kwargs: events.append(("publish", rows, kwargs))
    monkeypatch.setattr(module, "ApiTrading", lambda settings: api)
    monitor = DeltaOrderMonitor(runner, Mock())
    monkeypatch.setattr(monitor.wake_event, "wait", lambda delay: monitor.stop.set())
    monitor.wake("child")
    monitor.run()
    assert events[:2] == [("publish", [snapshot], {"first_observation": True}), "broker_read"]
