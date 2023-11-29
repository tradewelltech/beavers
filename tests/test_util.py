import collections
import dataclasses
import random
from typing import Callable, Dict, Generic, TypeVar

import pandas as pd
import pyarrow as pa

from beavers.engine import UTC_MAX, Dag, TimerManager
from beavers.replay import DataSink, DataSource

T = TypeVar("T")

TEST_TABLE = pa.table(
    {
        "timestamp": [
            pd.to_datetime("2023-01-01T00:00:00Z"),
            pd.to_datetime("2023-01-02T00:00:00Z"),
        ],
        "value": [1, 2],
    }
)


class GetLatest(Generic[T]):
    def __init__(self, default: T):
        self._value = default

    def __call__(self, values: list[T]) -> T:
        if values:
            self._value = values[-1]
        return self._value


def add(left, right):
    return left + right


def add_with_noise(left, right):
    return left + right + random.randint(0, 1000)  # nosec


def add_no_42(left, right):
    results = add(left, right)
    if results == 42:
        raise ValueError(f"{left} + {right} == 42")
    else:
        return results


class AddOther:
    def __init__(self, other):
        self._other = other

    def set_other(self, other):
        self._other = other

    def __call__(self, value):
        return self._other + value


def select(key, **values):
    return values[key]


class WordCount:
    def __init__(self):
        self._counts = collections.defaultdict(lambda: 0)

    def __call__(self, words: list[str]) -> dict[str, int]:
        for word in words:
            self._counts[word] += 1

        return self._counts


def join_counts(**kwargs: Dict[str, int]) -> pd.DataFrame:
    return pd.concat(
        [pd.Series(value, name=key) for key, value in kwargs.items()], axis=1
    ).fillna(0)


@dataclasses.dataclass(frozen=True)
class TimerEntry:
    timestamp: pd.Timestamp
    values: list[int]


class SetATimer:
    def __init__(self):
        self._entry = None

    def __call__(
        self, entries: list[TimerEntry], now: pd.Timestamp, timer_manager: TimerManager
    ) -> list[int]:
        if entries:
            self._entry = entries[-1]
            timer_manager.set_next_timer(self._entry.timestamp)
        if self._entry is not None and now >= self._entry.timestamp:
            results = self._entry.values
            self._entry = None
            return results
        else:
            return []


def create_word_count_dag() -> tuple[Dag, WordCount]:
    dag = Dag()
    messages_stream = dag.source_stream([], name="messages")
    word_count = WordCount()
    state = dag.state(word_count).map(messages_stream)
    changed_key = dag.stream(lambda x: sorted(set(x)), []).map(messages_stream)
    records = dag.stream(lambda x, y: {v: y[v] for v in x}, {}).map(changed_key, state)
    dag.sink("results", records)
    return dag, word_count


class ListDataSource(DataSource[list[T]]):
    def __init__(self, data: list[T], extractor: Callable[[T], pd.Timestamp]):
        self._data = data
        self._extractor = extractor
        self._position = 0

    def read_to(self, timestamp: pd.Timestamp) -> list[T]:
        results = []
        while (
            self._position < len(self._data)
            and self._extractor(self._data[self._position]) <= timestamp
        ):
            results.append(self._data[self._position])
            self._position += 1
        return results

    def get_next(self) -> pd.Timestamp:
        if self._position >= len(self._data):
            return UTC_MAX
        else:
            return self._extractor(self._data[self._position])


class ListDataSink(DataSink[list[T]]):
    def __init__(self):
        self._data = []

    def append(self, timestamp: pd.Timestamp, data: list[T]):
        self._data.extend(data)

    def close(self):
        pass
