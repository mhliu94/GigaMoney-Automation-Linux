"""Verified, per-thread HTTP connections for signed broker requests."""
import http.client
from io import BytesIO
import ssl
import threading
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.response import addinfourl


class BrokerOpener:
    def __init__(self, max_bytes, *, loopback_capture=False):
        self.max_bytes = int(max_bytes)
        if self.max_bytes < 1:
            raise ValueError("max response length must be positive")
        self.loopback_capture = loopback_capture
        self.tls = ssl.create_default_context()
        self.local = threading.local()

    def open(self, request, *, timeout):
        url = urlsplit(request.full_url)
        capture = self.loopback_capture and url.scheme == "http" and url.hostname == "127.0.0.1" and url.port
        if not url.hostname or url.username is not None or url.password is not None or url.fragment or (url.scheme != "https" and not capture):
            raise ValueError("Invalid broker request origin")
        origin = (url.scheme, url.hostname, url.port)
        connections = getattr(self.local, "connections", None)
        if connections is None:
            connections = self.local.connections = {}
        conn = connections.get(origin)
        if conn is None:
            if capture:
                conn = http.client.HTTPConnection(url.hostname, url.port, timeout=timeout)
            else:
                conn = http.client.HTTPSConnection(url.hostname, url.port, timeout=timeout, context=self.tls)
            connections[origin] = conn
        conn.timeout = timeout
        try:
            if conn.sock is not None:
                conn.sock.settimeout(timeout)
            target = (url.path or "/") + ("?" + url.query if url.query else "")
            conn.request(request.get_method(), target, body=request.data, headers=dict(request.header_items()))
            with conn.getresponse() as response:
                body = response.read(self.max_bytes + 1)
                if len(body) > self.max_bytes:
                    raise ValueError("Broker response exceeds size limit")
                if getattr(response, "length", None):
                    raise http.client.IncompleteRead(body)
                status, headers, reason = response.status, response.headers, response.reason
                if response.will_close:
                    connections.pop(origin, None)
                    conn.close()
        except (OSError, http.client.HTTPException, ValueError) as exc:
            connections.pop(origin, None)
            conn.close()
            # A stale socket may have transmitted a write. Never replay it here.
            if isinstance(exc, http.client.HTTPException):
                raise URLError("Broker connection failed") from exc
            raise
        if status >= 300:
            raise HTTPError(request.full_url, status, reason, headers, BytesIO(body))
        return addinfourl(BytesIO(body), headers, request.full_url, status)
