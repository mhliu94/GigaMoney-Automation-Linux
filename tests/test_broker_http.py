import http.client
from unittest.mock import Mock

import pytest
from urllib.request import Request

from gigamoney.broker_http import BrokerOpener


def test_two_successive_requests_share_tls_without_replaying_a_failed_write(monkeypatch):
    from email.message import Message
    from io import BytesIO

    class Reply(BytesIO):
        status = 200
        headers = Message()
        reason = "OK"
        will_close = False

    conn = Mock(sock=None)
    conn.getresponse.side_effect = lambda: Reply(b"{}")
    factory = Mock(return_value=conn)
    monkeypatch.setattr(http.client, "HTTPSConnection", factory)
    op = BrokerOpener(100)
    request = Request("https://broker.example.test/api/order", data=b"{}", method="POST")
    for _ in range(2):
        with op.open(request, timeout=2) as reply:
            assert reply.read() == b"{}"
    assert factory.call_count == 1
    conn.getresponse.side_effect = http.client.RemoteDisconnected("lost response")
    with pytest.raises(OSError):
        op.open(request, timeout=2)
    assert conn.request.call_count == 3
    assert conn.close.called
    # The next independently issued request creates a fresh connection.
    conn.getresponse.side_effect = lambda: Reply(b"{}")
    op.open(request, timeout=2).close()
    assert factory.call_count == 2


def test_transport_never_follows_a_redirect_or_sends_over_an_unapproved_cleartext_origin(monkeypatch):
    from email.message import Message
    from io import BytesIO
    from urllib.error import HTTPError

    class Redirect(BytesIO):
        status = 307
        headers = Message()
        reason = "Temporary Redirect"
        will_close = False

    conn = Mock(sock=None)
    conn.getresponse.return_value = Redirect(b"redirect")
    factory = Mock(return_value=conn)
    monkeypatch.setattr(http.client, "HTTPSConnection", factory)
    op = BrokerOpener(100)
    with pytest.raises(ValueError):
        op.open(Request("http://broker.example.test/"), timeout=2)
    with pytest.raises(HTTPError) as info:
        op.open(Request("https://broker.example.test/", data=b"order"), timeout=2)
    assert info.value.code == 307
    assert conn.request.call_count == 1
