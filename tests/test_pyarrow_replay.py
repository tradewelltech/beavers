from operator import itemgetter

import pandas as pd
import pyarrow as pa
import pytest

from beavers.engine import UTC_MAX
from beavers.pyarrow_replay import ArrowTableDataSource


def test_arrow_table_data_source():
    table = pa.table(
        {
            "timestamp": [
                pd.to_datetime("2023-01-01T00:00:00Z"),
                pd.to_datetime("2023-01-02T00:00:00Z"),
            ],
            "value": [1, 2],
        }
    )
    source = ArrowTableDataSource(
        table,
        itemgetter("timestamp"),
    )
    assert source.get_next() == pd.to_datetime("2023-01-01T00:00:00Z")
    assert source.read_to(pd.to_datetime("2023-01-01T00:00:00Z")) == table[:1]
    assert source.read_to(pd.to_datetime("2023-01-01T00:00:00Z")) == table[:0]
    assert source.get_next() == pd.to_datetime("2023-01-02T00:00:00Z")
    assert source.read_to(pd.to_datetime("2023-01-02T00:00:00Z")) == table[1:]
    assert source.get_next() == UTC_MAX
    assert source.read_to(UTC_MAX) == table[:0]


def test_arrow_table_data_source_ooo():
    with pytest.raises(
        AssertionError, match="Timestamp column should be monotonic increasing"
    ):
        ArrowTableDataSource(
            pa.table(
                {
                    "timestamp": [
                        pd.to_datetime("2023-01-02T00:00:00Z"),
                        pd.to_datetime("2023-01-01T00:00:00Z"),
                    ],
                    "value": [1, 2],
                }
            ),
            itemgetter("timestamp"),
        )
