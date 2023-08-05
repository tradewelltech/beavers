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
    T,
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


def test_no_op_through_replay(replay_context: ReplayContext):
    """
    Test a corner case of the driver were a sink did not update during a cycle
    """
    sink = ListDataSink()
    dag = Dag()
    dag.source_stream(empty=[], name="words_1")
    source_2 = dag.source_stream(empty=[], name="words_2")
    dag.sink("words", source_2)

    driver = ReplayDriver.create(
        dag,
        replay_context,
        {
            "words_1": create_data_source,
            "words_2": lambda _: ListDataSource([], attrgetter("timestamp")),
        },
        {"words": lambda _: sink},
    )
    driver.run()
    assert sink._data == []


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
    assert adapter.read_to(UTC_MAX) == []
    assert adapter.get_next() == UTC_MAX
    assert adapter.read_to(UTC_MAX) == []


def test_iterator_data_source_all_empty():
    adapter = create_adapter([[], []])
    assert adapter.get_next() == UTC_MAX
    assert adapter.read_to(UTC_MAX) == []
    assert adapter.get_next() == UTC_MAX
    assert adapter.read_to(UTC_MAX) == []


class CornerCaseTester(DataSource[list[Word]]):
    def __init__(self, timestamp: pd.Timestamp):
        self._timestamp = timestamp
        self._read = False

    def read_to(self, timestamp: pd.Timestamp) -> list[T]:
        self._read = True
        return []

    def get_next(self) -> pd.Timestamp:
        if self._read:
            return UTC_MAX
        else:
            return self._timestamp


def test_iterator_data_source_cutoff():
    """
    Test a tricky corner case were the underlying DataSource of
     IteratorDataSourceAdapter doesn't behave as expected.
    """
    timestamp = pd.to_datetime("2022-01-01", utc=True)
    adapter = IteratorDataSourceAdapter(
        (
            source
            for source in [
                CornerCaseTester(timestamp + pd.Timedelta(minutes=1)),
                ListDataSource(
                    [Word(timestamp + pd.Timedelta(minutes=2), "hello")],
                    attrgetter("timestamp"),
                ),
            ]
        ),
        [],
        lambda left, right: left + right,
    )

    assert adapter.read_to(UTC_MAX) == [
        Word(
            timestamp=pd.Timestamp("2022-01-01 00:02:00+0000", tz="UTC"), value="hello"
        )
    ]


def test_replay_read_sources():
    source = ListDataSource(
        [
            Word(pd.to_datetime("2023-01-01 00:01:00Z"), "1"),
            Word(pd.to_datetime("2023-01-01 00:02:00Z"), "2"),
            Word(pd.to_datetime("2023-01-01 12:01:00Z"), "3"),
            Word(pd.to_datetime("2023-01-01 12:04:00Z"), "4"),
        ],
        attrgetter("timestamp"),
    )

    dag = Dag()
    dag.source_stream([], "hello")
    driver = ReplayDriver.create(
        dag=dag,
        replay_context=ReplayContext(
            pd.to_datetime("2023-01-01", utc=True),
            pd.to_datetime("2023-01-02", utc=True) - pd.to_timedelta("1ns"),
            pd.to_timedelta("12h"),
        ),
        data_source_providers={"hello": lambda x: source},
        data_sink_providers={},
    )

    records, timestamp = driver.read_sources()
    assert timestamp == pd.to_datetime("2023-01-01 00:01:00Z", utc=True)
    assert records == 0


def test_replay_run_cycle():
    source = ListDataSource(
        [
            Word(pd.to_datetime("2023-01-01 00:01:00Z"), "1"),
            Word(pd.to_datetime("2023-01-01 00:02:00Z"), "2"),
            Word(pd.to_datetime("2023-01-01 12:01:00Z"), "3"),
            Word(pd.to_datetime("2023-01-01 12:04:00Z"), "4"),
        ],
        attrgetter("timestamp"),
    )

    dag = Dag()
    dag.source_stream([], "hello")
    driver = ReplayDriver.create(
        dag=dag,
        replay_context=ReplayContext(
            pd.to_datetime("2023-01-01", utc=True),
            pd.to_datetime("2023-01-02", utc=True) - pd.to_timedelta("1ns"),
            pd.to_timedelta("12h"),
        ),
        data_source_providers={"hello": lambda x: source},
        data_sink_providers={},
    )

    metrics = driver.run_cycle()
    assert metrics is None
    assert driver.current_time == pd.to_datetime("2023-01-01 12:00:00Z")

    metrics = driver.run_cycle()
    assert metrics.timestamp == pd.to_datetime("2023-01-01 12:00:00Z")
    assert metrics.source_records == 2
    assert metrics.sink_records == 0
    assert metrics.cycle_time_ns > 0
    assert metrics.warp_ratio > 0.0
    assert driver.current_time == pd.to_datetime("2023-01-02 00:00:00Z")

    metrics = driver.run_cycle()
    assert metrics.timestamp == pd.to_datetime("2023-01-01 23:59:59.999999999Z")
    assert metrics.source_records == 2
    assert metrics.sink_records == 0
    assert metrics.cycle_time_ns > 0
    assert metrics.warp_ratio > 0.0
    assert driver.current_time == pd.to_datetime("2023-01-02 12:00:00Z")
    assert driver.is_done()
