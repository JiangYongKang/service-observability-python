"""Downstream failure classification: stable categories, no detail leakage."""
from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from obs.client import DownstreamClient
from obs.errors import (
    DependencyError,
    DependencyErrorCategory as C,
    classify_exception,
)

REQUEST = httpx.Request("GET", "https://downstream.example/api/quote")
SECRET = "internal-host-db-1-SECRET"


def response(code: int, text: str = "x") -> httpx.Response:
    return httpx.Response(code, text=text, request=REQUEST)


CLASSIFY_CASES = [
    ("read_timeout", httpx.ReadTimeout(f"timed out host={SECRET}"), C.TIMEOUT),
    ("write_timeout", httpx.WriteTimeout("slow"), C.TIMEOUT),
    ("stdlib_timeout", TimeoutError("stdlib timeout"), C.TIMEOUT),
    ("connect_timeout", httpx.ConnectTimeout(f"host={SECRET}"), C.CONNECTION_ERROR),
    ("pool_timeout", httpx.PoolTimeout("pool exhausted"), C.CONNECTION_ERROR),
    ("connect_error", httpx.ConnectError(f"[Errno -2] {SECRET}"), C.CONNECTION_ERROR),
    ("network_error", httpx.NetworkError("reset", request=REQUEST), C.CONNECTION_ERROR),
    ("http_500", httpx.HTTPStatusError("500", request=REQUEST, response=response(500, SECRET)), C.UPSTREAM_5XX),
    ("http_503", httpx.HTTPStatusError("503", request=REQUEST, response=response(503)), C.UPSTREAM_5XX),
    ("http_404", httpx.HTTPStatusError("404", request=REQUEST, response=response(404, SECRET)), C.UPSTREAM_4XX),
    ("http_429", httpx.HTTPStatusError("429", request=REQUEST, response=response(429)), C.UPSTREAM_4XX),
    ("decoding", httpx.DecodingError("broken encoding", request=REQUEST), C.BAD_RESPONSE),
    ("bad_json", json.JSONDecodeError("expecting value", "doc{", 0), C.BAD_RESPONSE),
    ("unknown", RuntimeError("totally unexpected"), C.UNKNOWN),
]


@pytest.mark.parametrize("name,exc,expected", CLASSIFY_CASES, ids=[c[0] for c in CLASSIFY_CASES])
def test_classification_is_type_and_status_based(name, exc, expected):
    got = classify_exception(exc)
    print(f"[输入] {name}: {type(exc).__name__} [判定依据] -> {got.value}")
    assert got is expected


@pytest.mark.parametrize("name,exc,expected", CLASSIFY_CASES[:10], ids=[c[0] for c in CLASSIFY_CASES[:10]])
def test_classification_independent_of_message_text(name, exc, expected):
    """Rebuilding the same exception type with another text keeps the category."""
    if isinstance(exc, httpx.HTTPStatusError):
        alt = httpx.HTTPStatusError(
            "zzz different text", request=exc.request, response=exc.response
        )
    else:
        alt = type(exc)("zzz different text")
    assert classify_exception(alt) is expected


def _run(handler):
    async def go():
        return await DownstreamClient(
            transport=httpx.MockTransport(handler)
        ).fetch_quote()
    return asyncio.run(go())


CLIENT_CASES = [
    ("timeout", lambda r: (_ for _ in ()).throw(httpx.ReadTimeout(SECRET)), C.TIMEOUT),
    ("connection", lambda r: (_ for _ in ()).throw(httpx.ConnectError(SECRET)), C.CONNECTION_ERROR),
    ("upstream_5xx", lambda r: httpx.Response(500, text=SECRET), C.UPSTREAM_5XX),
    ("upstream_4xx", lambda r: httpx.Response(404, text=SECRET), C.UPSTREAM_4XX),
    ("malformed_json", lambda r: httpx.Response(200, content=b"{not json",
                                               headers={"content-type": "application/json"}), C.BAD_RESPONSE),
    ("missing_field", lambda r: httpx.Response(200, json={"other": 1}), C.BAD_RESPONSE),
    ("wrong_type", lambda r: httpx.Response(200, json=["not", "object"]), C.BAD_RESPONSE),
    ("empty_quote", lambda r: httpx.Response(200, json={"quote": "  "}), C.BAD_RESPONSE),
]


@pytest.mark.parametrize("name,handler,expected", CLIENT_CASES, ids=[c[0] for c in CLIENT_CASES])
def test_client_maps_every_failure_to_category(name, handler, expected):
    with pytest.raises(DependencyError) as ei:
        _run(handler)
    print(f"[输入] {name} [判定依据] category={ei.value.category.value}")
    assert ei.value.category is expected
    # No underlying detail (hostnames/upstream bodies) rides on the exception.
    assert SECRET not in str(ei.value)


def test_client_success_returns_validated_quote():
    text = _run(lambda r: httpx.Response(200, json={"quote": "hello there"}))
    assert text == "hello there"


def test_already_classified_error_passes_category_through():
    err = DependencyError(C.BAD_RESPONSE)
    assert classify_exception(err) is C.BAD_RESPONSE
