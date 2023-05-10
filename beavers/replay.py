"""
API and implementation for replaying data
"""
import abc
import dataclasses
import time
from typing import Callable, Generic, Iterator, Optional, Protocol, TypeVar

import pandas as pd

from beavers.engine import UTC_MAX, Dag, Node

T = TypeVar("T")


@dataclasses.dataclass(frozen=True)
class ReplayContext:
    start: pd.Timestamp
    end: pd.Timestamp
    frequency: pd.Timedelta


class DataSource(abc.ABC, Generic[T]):
    """
    Interface for replaying historical data from a file or database
    """

    @abc.abstractmethod
    def read_to(self, timestamp: pd.Timestamp) -> T:
        """Read from the data source, all the way to the provided timestamp (inclusive)

        This function is stateful and must remember the previous timestamp for which
         data was read

        Parameters
        ----------
        timestamp
            End of the time interval for which data is required (inclusive)
        Returns
        -------
        data
            The data for the interval (or empty if no data is found)
        """

    @abc.abstractmethod
    def get_next(self) -> pd.Timestamp:
        """Return the next timestamp for which there is data

        If no data is available this should return `UTC_MAX`


        Returns
        -------
        timestamp
            Timestamp of the next available data point (or `UTC_MAX` if done)
        """


class DataSink(abc.ABC, Generic[T]):
    @abc.abstractmethod
    def append(self, timestamp: pd.Timestamp, data: T):
        pass

    @abc.abstractmethod
    def close(self):
        pass


class DataSourceProvider(Protocol[T]):
    @abc.abstractmethod
    def __call__(self, context: ReplayContext) -> DataSource[T]:
        pass


class DataSinkProvider(Protocol[T]):
    @abc.abstractmethod
    def __call__(self, context: ReplayContext) -> DataSink[T]:
        pass


@dataclasses.dataclass(frozen=True)
class ReplaySource(Generic[T]):
    name: str
    node: Node[T]
    data_source: DataSource[T]


@dataclasses.dataclass(frozen=True)
class ReplaySink(Generic[T]):
    name: str
    nodes: list[Node[T]]
    data_sink: DataSink[T]


class IteratorDataSourceAdapter(DataSource[T]):
    def __init__(
        self,
        sources: Iterator[DataSource[T]],
        empty: T,
        concatenator: Callable[[T, T], T],
    ):
        self._sources = sources
        self._empty = empty
        self._concatenator = concatenator
        self._current = self._next()

    def read_to(self, timestamp: pd.Timestamp) -> T:
        if self._current is None:
            return self._empty
        else:
            this_batch = self._current.read_to(timestamp)
            while self._current is not None and self._current.get_next() == UTC_MAX:
                self._current = self._next()
                next_batch = (
                    self._empty
                    if self._current is None
                    else self._current.read_to(timestamp)
                )
                if next_batch and this_batch:
                    this_batch = self._concatenator(this_batch, next_batch)
                elif next_batch:
                    this_batch = next_batch

            return this_batch

    def get_next(self) -> pd.Timestamp:
        if self._current is None:
            return UTC_MAX
        else:
            return self._current.get_next()

    def _next(self) -> Optional[DataSource]:
        try:
            return next(self._sources)
        except StopIteration:
            return None


@dataclasses.dataclass(frozen=True)
class ReplayCycleInfo:
    cycle_id: int
    cycle_time: pd.Timestamp
    records_count: int
    compute_time: pd.Timedelta
    warp_ratio: float


@dataclasses.dataclass
class ReplayDriver:
    dag: Dag
    context: ReplayContext
    sources: list[ReplaySource]
    sinks: list[ReplaySink]
    current_time: pd.Timestamp

    @staticmethod
    def create(
        dag: Dag,
        context: ReplayContext,
        data_source_providers: dict[str, DataSourceProvider],
        data_sink_providers: dict[str, DataSinkProvider],
    ) -> "ReplayDriver":
        return ReplayDriver(
            dag,
            context,
            _create_sources(dag, context, data_source_providers),
            _create_sinks(dag, context, data_sink_providers),
            current_time=context.start,
        )

    def run(self):
        while not self.is_done():
            self.run_cycle()
        for sink in self.sinks:
            sink.data_sink.close()

    def is_done(self) -> bool:
        return self.current_time > self.context.end

    def run_cycle(self) -> ReplayCycleInfo:
        start_nanos = time.time_ns()
        records, next_timestamp = self.read_sources()
        if records or self.dag.get_next_timer() <= self.current_time:
            self.update_dag()
            self.flush_sinks()
        compute_time = pd.to_timedelta(time.time_ns() - start_nanos, "ns")

        results = ReplayCycleInfo(
            cycle_id=self.dag.get_cycle_id(),
            cycle_time=self.current_time,
            records_count=records,
            compute_time=compute_time,
            warp_ratio=self.context.frequency / compute_time,
        )

        self.current_time = max(
            next_timestamp, self.current_time + self.context.frequency
        ).ceil(self.context.frequency)
        return results

    def read_sources(self) -> tuple[int, pd.Timestamp]:
        records = 0
        next_timestamp = self.context.end
        for replay_source in self.sources:
            source_data = replay_source.data_source.read_to(self.current_time)
            next_timestamp = min(next_timestamp, replay_source.data_source.get_next())
            if len(source_data) > 0:
                replay_source.node.set_stream(source_data)
                records += len(source_data)
        return records, next_timestamp

    def update_dag(self):
        self.dag.stabilize(self.current_time)

    def flush_sinks(self):
        for sink in self.sinks:
            for node in sink.nodes:
                if node.get_cycle_id() == self.dag.get_cycle_id():
                    sink.data_sink.append(self.current_time, node.get_sink_value())


def _create_sources(
    dag: Dag,
    context: ReplayContext,
    data_source_providers: dict[str, DataSourceProvider],
) -> list[ReplaySource]:
    source_nodes = dag.get_sources()
    assert sorted(source_nodes.keys()) == sorted(data_source_providers.keys())
    return [
        ReplaySource(name, source_nodes[name], data_source_providers[name](context))
        for name in data_source_providers.keys()
    ]


def _create_sinks(
    dag: Dag, context: ReplayContext, data_sink_providers: dict[str, DataSinkProvider]
) -> list[ReplaySink]:
    sink_nodes = dag.get_sinks()
    assert sorted(sink_nodes.keys()) == sorted(data_sink_providers.keys())
    return [
        ReplaySink(name, sink_nodes[name], data_sink_providers[name](context))
        for name in data_sink_providers.keys()
    ]
