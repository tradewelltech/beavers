from typing import Protocol, Sized

import pandas as pd

from beavers.dag import Node
from beavers.kafka import _ConsumerManager, _RuntimeSourceTopic


class ReadCallback(Protocol):
    def __call__(self, high_watermark: pd.Timestamp, data: dict[Node, Sized]):
        pass


class LiveSourceManager(Protocol):
    def poll_some(self, callback: ReadCallback) -> pd.Timestamp:
        pass


class LiveSourceCreator(Protocol):
    def __call__(
        self,
        start: pd.Timestamp,
        source_nodes: dict[str, Node],
    ) -> tuple[LiveSourceManager, list[Node]]:
        pass


class KafkaLiveSourceManager(LiveSourceManager):
    def __init__(
        self,
        runtime_source_topics: list[_RuntimeSourceTopic],
        consumer_manager: _ConsumerManager,
    ):
        self.runtime_source_topics = runtime_source_topics
        self.consumer_manager = consumer_manager

    def poll_some(self, callback: ReadCallback) -> pd.Timestamp:
        messages = self.consumer_manager.poll(1.0)

        if self._run_cycle(messages):
            self._produce_records(self._dag.get_cycle_id())
            self._producer_manager.poll()
            return True
        else:
            self._producer_manager.poll()
            return False
