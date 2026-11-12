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
    monkeypatch.setattr(monitor.stop, 'wait', wait)
    ordered = []
    runner.request_holdings_refresh.side_effect = lambda: ordered.append('assets')
    runner._publish_orders.side_effect = lambda *args: ordered.append('orders')
    monitor.run()
    assert ordered == ['assets', 'orders', 'assets', 'orders', 'orders']
    assert len(waits) == 4 and all(.05 <= delay <= 1 for delay in waits)
