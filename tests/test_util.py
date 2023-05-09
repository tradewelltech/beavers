import collections
import dataclasses
import random
from typing import Dict, Generic, TypeVar

import pandas as pd

from beavers.engine import Dag, TimerManager

T = TypeVar("T")


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
    messages_stream = dag.source_stream([], "messages")
    word_count = WordCount()
    state = dag.state(word_count).map(messages_stream)
    changed_key = dag.stream(lambda x: sorted(set(x)), []).map(messages_stream)
    records = dag.stream(lambda x, y: {v: y[v] for v in x}, {}).map(changed_key, state)
    dag.sink("results", records)
    return dag, word_count
