import dataclasses
from operator import attrgetter

import pandas as pd
import pytest

from beavers.engine import UTC_MAX, Dag
from beavers.replay import (
    DataSource,
    IteratorDataSourceAdapter,
    ReplayContext,
    ReplayDriver,
    _create_sinks,
    _create_sources,
)
from tests.test_util import ListDataSink, ListDataSource


@dataclasses.dataclass(frozen=True)
class Word:
    timestamp: pd.Timestamp
    value: str


@pytest.fixture
def replay_context() -> ReplayContext:
    return ReplayContext(
        pd.to_datetime("2023-01-01", utc=True),
        pd.to_datetime("2023-01-02", utc=True),
        pd.to_timedelta("1min"),
    )


def create_data_source(context: ReplayContext):
    return ListDataSource(
        [Word(context.start + pd.Timedelta(minutes=i), "hello") for i in range(10)],
        attrgetter("timestamp"),
    )


def test_create_sources_mismatch(replay_context: ReplayContext):
    with pytest.raises(
        ValueError,
        match=r"Source node and DataSource names don't match: \[\]  vs \['words'\]",
    ):
        _create_sources(Dag(), replay_context, {"words": create_data_source})


def test_create_sources_match(replay_context: ReplayContext):
    dag = Dag()
    node = dag.source_stream(empty=[], name="words")

    results = _create_sources(dag, replay_context, {"words": create_data_source})
    assert len(results) == 1
    assert results[0].name == "words"
    assert results[0].node == node
    assert isinstance(results[0].data_source, ListDataSource)


def test_create_sinks_mismatch(replay_context: ReplayContext):
    sink = ListDataSink()
    with pytest.raises(
        ValueError,
        match=r"Sink node and DataSink names don't match: \[\]  vs \['words'\]",
    ):
        _create_sinks(Dag(), replay_context, {"words": lambda _: sink})


def test_create_sinks_match(replay_context: ReplayContext):
    sink = ListDataSink()
    dag = Dag()
    source_node = dag.source_stream(empty=[], name="words")
    sink_node = dag.sink("words", source_node)
    results = _create_sinks(dag, replay_context, {"words": lambda _: sink})
    assert len(results) == 1
    assert results[0].name == "words"
    assert results[0].nodes == [sink_node]
    assert results[0].data_sink is sink


def test_pass_through_replay(replay_context: ReplayContext):
    source = create_data_source(replay_context)
    sink = ListDataSink()
    dag = Dag()
    source_node = dag.source_stream(empty=[], name="words")
    dag.sink("words", source_node)

    driver = ReplayDriver.create(
        dag,
        replay_context,
        {"words": lambda _: source},
        {"words": lambda _: sink},
    )
    driver.run()
    assert sink._data == source._data


def create_data_groups() -> list[list[Word]]:
    timestamp = pd.to_datetime("2022-01-01", utc=True)
    return [
        [
            Word(timestamp + pd.Timedelta(minutes=0), "hello"),
            Word(timestamp + pd.Timedelta(minutes=1), "world"),
        ],
        [
            Word(timestamp + pd.Timedelta(minutes=2), "hello"),
            Word(timestamp + pd.Timedelta(minutes=2), "world"),
        ],
        [
            Word(timestamp + pd.Timedelta(minutes=3), "hello"),
            Word(timestamp + pd.Timedelta(minutes=3), "world"),
            Word(timestamp + pd.Timedelta(minutes=3), "world"),
            Word(timestamp + pd.Timedelta(minutes=4), "world"),
        ],
        [],
        [
            Word(timestamp + pd.Timedelta(minutes=5), "hello"),
            Word(timestamp + pd.Timedelta(minutes=5), "world"),
        ],
    ]


def create_adapter(data_groups: list[list[Word]]) -> DataSource[list[Word]]:
    return IteratorDataSourceAdapter(
        (
            ListDataSource(data_group, attrgetter("timestamp"))
            for data_group in data_groups
        ),
        [],
        lambda left, right: left + right,
    )


def test_iterator_data_source_adapter_run_all():
    data_groups = create_data_groups()
    adapter = create_adapter(data_groups)
    assert adapter.read_to(UTC_MAX) == [
        word for data_group in data_groups for word in data_group
    ]
    assert adapter.read_to(UTC_MAX) == []


def test_iterator_data_source_adapter_run_one_by_one():
    timestamp = pd.to_datetime("2022-01-01", utc=True)
    data_groups = create_data_groups()
    adapter = create_adapter(data_groups)
    assert adapter.get_next() == timestamp
    assert adapter.read_to(timestamp) == [data_groups[0][0]]
    assert adapter.read_to(timestamp) == []
    assert adapter.read_to(timestamp + pd.Timedelta(minutes=1)) == [data_groups[0][1]]
    assert adapter.read_to(timestamp + pd.Timedelta(minutes=1)) == []
    assert (
        adapter.read_to(timestamp + pd.Timedelta(minutes=3))
        == data_groups[1] + data_groups[2][:-1]
    )
    assert adapter.read_to(timestamp + pd.Timedelta(minutes=4)) == data_groups[2][-1:]
    assert adapter.read_to(timestamp + pd.Timedelta(minutes=5)) == data_groups[4]
    assert adapter.read_to(timestamp + pd.Timedelta(minutes=6)) == []
    assert adapter.read_to(UTC_MAX) == []


def test_iterator_data_source_empty():
    adapter = create_adapter([])
    assert adapter.get_next() == UTC_MAX
    assert adapter.get_next() == UTC_MAX
