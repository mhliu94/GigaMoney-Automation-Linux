
import pytest
@pytest.fixture(autouse=True)
def isolate_background_order_observer(request, monkeypatch):
    if request.node.path.name.startswith(('test_runner_', 'test_api_trading')):
        from gigamoney.runner import TradingRunner
        monkeypatch.setattr(TradingRunner, '_make_order_monitor', lambda self, producer: None)

import pytest
@pytest.fixture(autouse=True)
def isolate_background_order_observer(request, monkeypatch):
    if request.node.path.name.startswith(('test_runner_', 'test_api_trading')):
        from gigamoney.runner import TradingRunner
        monkeypatch.setattr(TradingRunner, '_make_order_monitor', lambda self, producer: None)

import pytest
@pytest.fixture(autouse=True)
def isolate_background_order_observer(request, monkeypatch):
    if request.node.path.name.startswith(('test_runner', 'test_api_trading')):
        from gigamoney.runner import TradingRunner
        monkeypatch.setattr(TradingRunner, '_make_order_monitor', lambda self, producer: None)

import pytest
@pytest.fixture(autouse=True)
def isolate_background_order_observer(request, monkeypatch):
    if request.node.path.name.startswith(('test_runner', 'test_api_trading')):
        from gigamoney.runner import TradingRunner
        monkeypatch.setattr(TradingRunner, '_make_order_monitor', lambda self, producer: None)

import pytest
@pytest.fixture(autouse=True)
def isolate_background_order_observer(request, monkeypatch):
    if request.node.path.name.startswith(('test_runner', 'test_api_trading')):
        from gigamoney.runner import TradingRunner
        monkeypatch.setattr(TradingRunner, '_make_order_monitor', lambda self, producer: None)

import pytest
@pytest.fixture(autouse=True)
def isolate_background_order_observer(request, monkeypatch):
    if request.node.path.name.startswith(('test_runner', 'test_api_trading')):
        from gigamoney.runner import TradingRunner
        monkeypatch.setattr(TradingRunner, '_make_order_monitor', lambda self, producer: None)

import pytest
@pytest.fixture(autouse=True)
def isolate_background_order_observer(request, monkeypatch):
    if request.node.path.name.startswith(('test_runner', 'test_api_trading')):
        from gigamoney.runner import TradingRunner
        monkeypatch.setattr(TradingRunner, '_make_order_monitor', lambda self, producer: None)
