import dataclasses
from typing import Callable

import pandas as pd
import pyarrow as pa

from beavers.engine import UTC_MAX
from beavers.replay import DataSink, DataSource


class ArrowTableDataSource(DataSource[pa.Table]):
    def __init__(
        self, table: pa.Table, timestamp_extractor: Callable[[pa.Table], pa.Array]
    ):
        assert callable(timestamp_extractor)
        self._table = table
        self._empty_table = table.schema.empty_table()
        self._timestamp_column = timestamp_extractor(table).to_pandas(
            date_as_object=False
        )
        assert (
            self._timestamp_column.is_monotonic_increasing
        ), "Timestamp column should be monotonic increasing"
        self._index = 0

    def read_to(self, timestamp: pd.Timestamp) -> pa.Table:
        new_index = self._timestamp_column.searchsorted(timestamp, side="right")
        if new_index > self._index:
            from_index = self._index
            self._index = new_index
            return self._table.slice(from_index, new_index - from_index)
        else:
            results = self._empty_table
        return results

    def get_next(self) -> pd.Timestamp:
        if self._index >= len(self._table):
            return UTC_MAX
        else:
            return self._timestamp_column.iloc[self._index]


@dataclasses.dataclass
class ArrowTableDataSink(DataSink[pa.Table]):
    saver: Callable[[pa.Table], None]
    chunks: list[pa.Table] = dataclasses.field(default_factory=list)

    def append(self, timestamp: pd.Timestamp, data: pa.Table):
        self.chunks.append(data)

    def close(self):
        if self.chunks:
            results = pa.concat_tables(self.chunks)
            self.saver(results)
