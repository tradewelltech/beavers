"""Module for replaying historical data."""

import abc
import collections.abc
import dataclasses
import logging
import time
from typing import Callable, Generic, Iterator, Optional, Protocol, TypeVar

import pandas as pd

from beavers.dag import UTC_MAX, Dag, Node

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclasses.dataclass(frozen=True)
class ReplayContext:
    """
    Stores the information about a replay.

    Attributes
    ----------
    start: pd.Timestamp
        Start of the replay
    end: pd.Timestamp
        End of the replay.
        This is exclusive, the replay will stop 1ns before
    frequency:
        How often should the replay run

    """

    start: pd.Timestamp
    end: pd.Timestamp
    frequency: pd.Timedelta

    def __post_init__(self):
        """Check arguments are valid."""
        assert self.start.tzname() == "UTC"
        assert self.end.tzname() == "UTC"


class DataSource(Protocol[T]):
    """Interface for replaying historical data from a file or database."""

    def read_to(self, timestamp: pd.Timestamp) -> T:
        """
        Read from the data source, all the way to the provided timestamp (inclusive).

        This function is stateful and must remember the previous timestamp
         for which data was read.

        Parameters
        ----------
        timestamp
            End of the time interval for which data is required (inclusive)

        Returns
        -------
        data
            The data for the interval (or empty if no data is found)

        """

    def get_next(self) -> pd.Timestamp:
        """
        Return the next timestamp for which there is data.

        If no data is available this should return `UTC_MAX`


        Returns
        -------
        timestamp: pd.Timestamp
            Timestamp of the next available data point (or `UTC_MAX` if no more data
             is available)

        """


class DataSink(Protocol[T]):
    """Interface for saving the results of a replay to a file or database."""

    def append(self, timestamp: pd.Timestamp, data: T):
        """
        Append data for the current cycle.

        Parameters
        ----------
        timestamp:
            End of the time interval for which data was replayed (inclusive)
        data:
            The generated data

        """

    def close(self):
        """Flush the data and clean up resources."""


class DataSourceProvider(Protocol[T]):
    """Interface for the provision of `DataSource`."""

    def __call__(self, replay_context: ReplayContext) -> DataSource[T]:
        """
        Create a `DataSource` for the given replay_context.

        Parameters
        ----------
        replay_context:
            Information about the replay that's about to run

        Returns
        -------
        DataSource[T]:
            Source for the replay

        """


class DataSinkProvider(Protocol[T]):
    """Interface for the provision of `DataSink`."""

    @abc.abstractmethod
    def __call__(self, replay_context: ReplayContext) -> DataSink[T]:
        """
        Create a `DataSink` for the given replay_context.

        Parameters
        ----------
        replay_context:
            Information about the replay that's about to run

        Returns
        -------
        DataSink[T]:
            Sink for the replay

        """


@dataclasses.dataclass(frozen=True)
class _ReplaySource(Generic[T]):
    """Internal class used to store `DataSource` at runtime."""

    name: str
    node: Node[T]
    data_source: DataSource[T]


@dataclasses.dataclass(frozen=True)
class _ReplaySink(Generic[T]):
    """Internal class used to store `DataSink` at runtime."""

    name: str
    nodes: list[Node[T]]
    data_sink: DataSink[T]


@dataclasses.dataclass(frozen=True)
class ReplayCycleMetrics:
    """Metrics for each replay cycle."""

    timestamp: pd.Timestamp
    cycle_id: int
    source_records: int
    sink_records: int
    cycle_time_ns: int
    warp_ratio: float


@dataclasses.dataclass
class ReplayDriver:
    """
    Orchestrate the replay of data for dag.

    This will:

    - create the relevant `DataSource`s
    - create the relevant `DataSink`s
    - stream the data from the sources
    - inject the input data in the dag source nodes
    - execute the dag
    - collect the output data and pass it to the sink
    - close the sink at the end of the run

    Notes
    -----
    Do not call the constructor directly, use `create` instead

    """

    dag: Dag
    replay_context: ReplayContext
    sources: list[_ReplaySource]
    sinks: list[_ReplaySink]
    current_time: pd.Timestamp

    @staticmethod
    def create(
        dag: Dag,
        replay_context: ReplayContext,
        data_source_providers: dict[str, DataSourceProvider],
        data_sink_providers: dict[str, DataSinkProvider],
    ) -> "ReplayDriver":
        return ReplayDriver(
            dag,
            replay_context,
            _create_sources(dag, replay_context, data_source_providers),
            _create_sinks(dag, replay_context, data_sink_providers),
            current_time=replay_context.start,
        )

    def run(self):
        while not self.is_done():
            self.run_cycle()
        for sink in self.sinks:
            sink.data_sink.close()

    def is_done(self) -> bool:
        return self.current_time > self.replay_context.end

    def run_cycle(self) -> Optional[ReplayCycleMetrics]:
        st = time.time_ns()
        source_records, next_timestamp = self.read_sources()
        if source_records or self.dag.get_next_timer() <= self.current_time:
            timestamp = min(self.current_time, self.replay_context.end)
            self.dag.execute(timestamp)
            sink_records = self.flush_sinks()
            et = time.time_ns()
            warp_ratio = self.replay_context.frequency.value / (et - st)
            metrics = ReplayCycleMetrics(
                timestamp=timestamp,
                cycle_id=self.dag.get_cycle_id(),
                source_records=source_records,
                sink_records=sink_records,
                cycle_time_ns=et - st,
                warp_ratio=warp_ratio,
            )
            logger.info(
                f"Running cycle={metrics.cycle_id} "
                f"timestamp={metrics.timestamp} "
                f"source_records={metrics.source_records} "
                f"sink_records={metrics.sink_records} "
                f"warp={warp_ratio:.1f}"
            )
        else:
            metrics = None

        self.current_time = max(
            next_timestamp, self.current_time + self.replay_context.frequency
        ).ceil(self.replay_context.frequency)
        return metrics

    def read_sources(self) -> tuple[int, pd.Timestamp]:
        records = 0
        next_timestamp = self.replay_context.end
        for replay_source in self.sources:
            source_data = replay_source.data_source.read_to(self.current_time)
            next_timestamp = min(next_timestamp, replay_source.data_source.get_next())
            if len(source_data) > 0:
                replay_source.node.set_stream(source_data)
                records += len(source_data)
        return records, next_timestamp

    def flush_sinks(self) -> int:
        records = 0
        for sink in self.sinks:
            for node in sink.nodes:
                if node.get_cycle_id() == self.dag.get_cycle_id():
                    sink_value = node.get_sink_value()
                    records += (
                        len(sink_value)
                        if isinstance(sink_value, collections.abc.Sized)
                        else 1
                    )
                    sink.data_sink.append(self.current_time, node.get_sink_value())
        return records


def _create_sources(
    dag: Dag,
    replay_context: ReplayContext,
    data_source_providers: dict[str, DataSourceProvider],
) -> list[_ReplaySource]:
    source_nodes = dag.get_sources()
    nodes_names = sorted(source_nodes.keys())
    source_names = sorted(data_source_providers.keys())
    if nodes_names != source_names:
        raise ValueError(
            "Source node and DataSource names don't match: "
            f"{nodes_names}  vs {source_names}"
        )
    return [
        _ReplaySource(
            name, source_nodes[name], data_source_providers[name](replay_context)
        )
        for name in data_source_providers
    ]


def _create_sinks(
    dag: Dag,
    replay_context: ReplayContext,
    data_sink_providers: dict[str, DataSinkProvider],
) -> list[_ReplaySink]:
    sink_nodes = dag.get_sinks()
    nodes_names = sorted(sink_nodes.keys())
    sink_names = sorted(data_sink_providers.keys())
    if nodes_names != sink_names:
        raise ValueError(
            f"Sink node and DataSink names don't match: {nodes_names}  vs {sink_names}"
        )
    return [
        _ReplaySink(name, sink_nodes[name], data_sink_providers[name](replay_context))
        for name in data_sink_providers
    ]


class IteratorDataSourceAdapter(DataSource[T]):
    """
    Adapter between an iterator of `DataSource` and a DataSource.

    This can be used to stitch together various `DataSource` for incremental date range
    """

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


class NoOpDataSink(DataSink):
    """DataSink that does nothing."""

    def append(self, timestamp: pd.Timestamp, data: T):
        pass

    def close(self):
        pass


class NoOpDataSinkProvider:
    """DataSinkProvider that provides a NoOpDataSink."""

    def __call__(self, context: ReplayContext) -> DataSink[T]:
        return NoOpDataSink()
