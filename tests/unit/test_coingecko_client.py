import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from requests.exceptions import ReadTimeout, RequestException

from feature_pipeline.ingestion import coingecko_client
from feature_pipeline.ingestion.coingecko_client import CoinGeckoClient


def test_initialization_sets_attributes() -> None:
    client = CoinGeckoClient(
        base_url="https://api.coingecko.com/api/v3/coins",
        endpoint="market_chart/range",
        timeout=10,
        n_retries=2,
    )

    assert client.base_url == "https://api.coingecko.com/api/v3/coins"
    assert client.endpoint == "market_chart/range"
    assert client.timeout == 10
    assert client.n_retries == 2


def test_datetime_to_unix_returns_string_timestamp() -> None:
    client = CoinGeckoClient(
        base_url="https://api.coingecko.com/api/v3/coins",
        endpoint="market_chart/range",
    )

    result = client._datetime_to_unix(datetime(2026, 1, 1, tzinfo=UTC))

    assert result == "1767225600"


def test_build_url_combines_base_url_coin_and_endpoint() -> None:
    client = CoinGeckoClient(
        base_url="https://api.coingecko.com/api/v3/coins",
        endpoint="market_chart/range",
    )

    result = client.build_url("bitcoin")

    assert result == (
        "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart/range"
    )


def test_build_params_contains_expected_coingecko_parameters() -> None:
    client = CoinGeckoClient(
        base_url="https://api.coingecko.com/api/v3/coins",
        endpoint="market_chart/range",
    )

    result = client.build_params(
        vs_currency="usd",
        starting_date=datetime(2026, 1, 1, tzinfo=UTC),
        ending_date=datetime(2026, 1, 2, tzinfo=UTC),
    )

    assert result == {
        "vs_currency": "usd",
        "from": "1767225600",
        "to": "1767312000",
        "precision": "full",
        "interval": "hourly",
    }


def test_build_chunks_splits_date_range_into_100_day_chunks() -> None:
    client = CoinGeckoClient(
        base_url="https://api.coingecko.com/api/v3/coins",
        endpoint="market_chart/range",
    )

    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 5, 1, tzinfo=UTC)

    result = client.build_chunks(start, end)

    assert result == [
        (
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 4, 11, tzinfo=UTC),
        ),
        (
            datetime(2026, 4, 11, tzinfo=UTC),
            datetime(2026, 5, 1, tzinfo=UTC),
        ),
    ]


def test_build_chunks_keeps_short_range_as_single_chunk() -> None:
    client = CoinGeckoClient(
        base_url="https://api.coingecko.com/api/v3/coins",
        endpoint="market_chart/range",
    )

    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 1, 10, tzinfo=UTC)

    result = client.build_chunks(start, end)

    assert result == [(start, end)]


def test_get_coin_ids_reads_requested_number_of_coins(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    coin_file = tmp_path / "top100_coins.json"
    coin_file.write_text(
        json.dumps(
            {
                "bitcoin": {"symbol": "btc"},
                "ethereum": {"symbol": "eth"},
                "solana": {"symbol": "sol"},
            }
        )
    )

    monkeypatch.setattr(coingecko_client, "COIN_ID_DATA", coin_file)

    client = CoinGeckoClient(
        base_url="https://api.coingecko.com/api/v3/coins",
        endpoint="market_chart/range",
    )

    result = client.get_coin_ids(number_of_coins=2)

    assert result == ["bitcoin", "ethereum"]


class FakeSuccessfulResponse:
    status_code = 200

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return {
            "prices": [[1767225600000, 100.0]],
            "market_caps": [[1767225600000, 1_000_000.0]],
            "total_volumes": [[1767225600000, 50_000.0]],
        }


def test_fetch_coin_data_returns_json_when_request_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_get(url: str, params: dict, headers: dict, timeout: int):
        assert url == "https://example.com"
        assert params == {"vs_currency": "usd"}
        assert headers == {"accept": "application/json"}
        assert timeout == 10
        return FakeSuccessfulResponse()

    monkeypatch.setattr(coingecko_client.requests, "get", fake_get)

    client = CoinGeckoClient(
        base_url="https://api.coingecko.com/api/v3/coins",
        endpoint="market_chart/range",
        timeout=10,
        n_retries=1,
    )

    result = client.fetch_coin_data(
        url="https://example.com",
        params={"vs_currency": "usd"},
        headers={"accept": "application/json"},
    )

    assert result == {
        "prices": [[1767225600000, 100.0]],
        "market_caps": [[1767225600000, 1_000_000.0]],
        "total_volumes": [[1767225600000, 50_000.0]],
    }


def test_fetch_coin_data_retries_after_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"count": 0}

    class FakeRateLimitResponse:
        status_code = 429

        def raise_for_status(self) -> None:
            pass

        def json(self) -> dict:
            return {}

    def fake_get(url: str, params: dict, headers: dict, timeout: int):
        calls["count"] += 1

        if calls["count"] == 1:
            return FakeRateLimitResponse()

        return FakeSuccessfulResponse()

    monkeypatch.setattr(coingecko_client.requests, "get", fake_get)
    monkeypatch.setattr(coingecko_client.time, "sleep", lambda seconds: None)

    client = CoinGeckoClient(
        base_url="https://api.coingecko.com/api/v3/coins",
        endpoint="market_chart/range",
        n_retries=2,
    )

    result = client.fetch_coin_data(
        url="https://example.com",
        params={},
        headers={},
    )

    assert calls["count"] == 2
    assert result["prices"] == [[1767225600000, 100.0]]


def test_fetch_coin_data_raises_runtime_error_after_read_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_get(url: str, params: dict, headers: dict, timeout: int):
        raise ReadTimeout("request timed out")

    monkeypatch.setattr(coingecko_client.requests, "get", fake_get)
    monkeypatch.setattr(coingecko_client.time, "sleep", lambda seconds: None)

    client = CoinGeckoClient(
        base_url="https://api.coingecko.com/api/v3/coins",
        endpoint="market_chart/range",
        n_retries=2,
    )

    with pytest.raises(RuntimeError, match="Max retries exceeded"):
        client.fetch_coin_data(
            url="https://example.com",
            params={},
            headers={},
        )


def test_fetch_coin_data_raises_runtime_error_after_request_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_get(url: str, params: dict, headers: dict, timeout: int):
        raise RequestException("request failed")

    monkeypatch.setattr(coingecko_client.requests, "get", fake_get)
    monkeypatch.setattr(coingecko_client.time, "sleep", lambda seconds: None)

    client = CoinGeckoClient(
        base_url="https://api.coingecko.com/api/v3/coins",
        endpoint="market_chart/range",
        n_retries=2,
    )

    with pytest.raises(RuntimeError, match="Max retries exceeded"):
        client.fetch_coin_data(
            url="https://example.com",
            params={},
            headers={},
        )
