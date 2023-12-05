from operator import itemgetter

import pandas as pd
import pyarrow as pa
import pyarrow.csv
import pytest

from beavers.engine import UTC_MAX
from beavers.pyarrow_replay import ArrowTableDataSink, ArrowTableDataSource
from tests.test_util import TEST_TABLE


def test_arrow_table_data_source():
    source = ArrowTableDataSource(TEST_TABLE, itemgetter("timestamp"))

    assert source.get_next() == pd.to_datetime("2023-01-01T00:00:00Z")
    assert source.read_to(pd.to_datetime("2023-01-01T00:00:00Z")) == TEST_TABLE[:1]
    assert source.read_to(pd.to_datetime("2023-01-01T00:00:00Z")) == TEST_TABLE[:0]
    assert source.get_next() == pd.to_datetime("2023-01-02T00:00:00Z")
    assert source.read_to(pd.to_datetime("2023-01-02T00:00:00Z")) == TEST_TABLE[1:]
    assert source.get_next() == UTC_MAX
    assert source.read_to(UTC_MAX) == TEST_TABLE[:0]


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


def test_arrow_table_data_sink(tmpdir):
    file = tmpdir / "file.csv"
    sink = ArrowTableDataSink(lambda table: pyarrow.csv.write_csv(table, file))

    sink.close()
    assert not file.exists()

    sink.append(UTC_MAX, TEST_TABLE)
    sink.close()
    assert file.exists()
