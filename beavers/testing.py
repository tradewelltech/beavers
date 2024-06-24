from typing import Any, Optional, Sequence, TypeVar

import pandas as pd

from beavers.dag import Dag

T = TypeVar("T")


class DagTestBench:
    def __init__(self, dag: Dag):
        self.dag = dag
        for output_name, output_sinks in self.dag.get_sinks().items():
            assert len(output_sinks) == 1, output_name

    def set_source(
        self,
        source_name: str,
        source_data: Any,
    ) -> "DagTestBench":
        source = self.dag.get_sources()[source_name]
        source.set_stream(source_data)
        return self

    def execute(self, now: Optional[pd.Timestamp] = None) -> "DagTestBench":
        self.dag.execute(now)
        return self

    def assert_sink_list(
        self,
        sink_name: str,
        expected_messages: Sequence[T],
    ) -> "DagTestBench":
        sinks = self.dag.get_sinks()[sink_name]
        assert len(sinks) == 1
        cycle_id = sinks[0].get_cycle_id()
        assert cycle_id == self.dag.get_cycle_id()
        actual_messages = sinks[0].get_sink_value()
        assert len(actual_messages) == len(
            expected_messages
        ), f"Sink {sink_name} value size mismatch"
        for actual_message, expected_message in zip(actual_messages, expected_messages):
            assert actual_message == expected_message
        return self

    def assert_sink_not_updated(self, sink_name: str) -> "DagTestBench":
        sinks = self.dag.get_sinks()[sink_name]
        assert len(sinks) == 1
        cycle_id = sinks[0].get_cycle_id()
        assert (
            cycle_id < self.dag.get_cycle_id()
        ), f"Sink {sink_name} got updated this cycle"
        return self
