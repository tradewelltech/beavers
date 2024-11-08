"""
Example of ETF nav (Net Asset Value) calculation
"""

import dataclasses
import random
from operator import attrgetter
from typing import Callable, Generic, Optional, TypeVar

import numpy as np
import pandas as pd

from beavers import Dag

K = TypeVar("K")
V = TypeVar("V")


@dataclasses.dataclass(frozen=True)
class PriceRecord:
    timestamp: pd.Timestamp
    ticker: str
    price: Optional[float]


@dataclasses.dataclass(frozen=True)
class EtfComposition:
    timestamp: pd.Timestamp
    ticker: str
    weights: dict[str, float]


class GetLatest(Generic[K, V]):
    def __init__(self, key_extractor: Callable[[V], K]):
        self._key_extractor = key_extractor
        self._latest = {}

    def __call__(self, updates: list[V]) -> dict[str, V]:
        for update in updates:
            self._latest[self._key_extractor(update)] = update
        return self._latest


class GetUnique(Generic[K, V]):
    def __init__(self, key_extractor: Callable[[V], K]):
        self._key_extractor = key_extractor

    def __call__(self, updates: list[V]) -> list[str]:
        return sorted(list({self._key_extractor(update) for update in updates}))


def create_day_test_prices(date: pd.Timestamp) -> list[PriceRecord]:
    end = date + pd.offsets.Day()
    return sorted(
        [
            PriceRecord(
                timestamp=pd.Timestamp(
                    np.random.randint(date.value, end.value), unit="ns"
                ),
                ticker=random.choice(["AAPL", "GOOGL", "MSFT"]),  # nosec B311
                price=random.random(),  # nosec B311
            )
            for _ in range(random.randint(0, 1000))  # nosec B311
        ],
        key=lambda x: x.timestamp,
    )


def calculate_nav(
    composition: EtfComposition, prices: dict[str, PriceRecord]
) -> PriceRecord:
    timestamp = composition.timestamp
    quotient = 0.0
    dividend = 0.0
    error = False
    for ticker, weight in composition.weights.items():
        try:
            price = prices[ticker]
        except KeyError:
            error = True
        else:
            quotient += price.price * weight
            dividend += weight
            timestamp = max(timestamp, price.timestamp)

    return PriceRecord(
        timestamp,
        composition.ticker,
        None if dividend == 0.0 or error else quotient / dividend,
    )


def calculate_navs(
    updated_tickers: set[str],
    etf_compositions: dict[str, EtfComposition],
    prices: dict[str, PriceRecord],
) -> list[PriceRecord]:
    return [
        calculate_nav(etf_composition, prices)
        for etf_composition in etf_compositions.values()
        if (
            etf_composition.ticker in updated_tickers
            or (updated_tickers & etf_composition.weights.keys())
        )
    ]


def get_updated_tickers(
    updated_prices: list[PriceRecord],
    updated_etf_compositions: list[EtfComposition],
) -> set[str]:
    return set(p.ticker for p in updated_prices) | set(
        e.ticker for e in updated_etf_compositions
    )


def create_dag() -> Dag:
    dag = Dag()
    price_stream = dag.source_stream([], name="price")
    etf_composition_stream = dag.source_stream([], name="etf_composition")
    price_latest = dag.state(GetLatest(attrgetter("ticker"))).map(price_stream)
    etf_composition_latest = dag.state(GetLatest(attrgetter("ticker"))).map(
        etf_composition_stream
    )

    updated_tickers = dag.stream(get_updated_tickers, set()).map(
        price_stream, etf_composition_stream
    )
    updated_navs = dag.stream(calculate_navs, []).map(
        updated_tickers, etf_composition_latest, price_latest
    )
    dag.sink("etf_price", updated_navs)
    return dag
