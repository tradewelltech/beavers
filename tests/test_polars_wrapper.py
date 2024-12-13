import polars as pl
import polars.testing
import pytest

from beavers import Dag
from beavers.polars_wrapper import _get_stream_schema, _get_stream_dtype

SIMPLE_SCHEMA = pl.Schema(
    [
        ("col1", pl.Int32()),
        ("col2", pl.Utf8()),
    ]
)
EMPTY_FRAME = pl.DataFrame(schema=SIMPLE_SCHEMA)
SIMPLE_FRAME = pl.DataFrame([[1, 2, 3], ["a", "b", "c"]], schema=SIMPLE_SCHEMA)
SIMPLE_FRAME_2 = table = pl.DataFrame([[1, 2], ["d", "e"]], schema=SIMPLE_SCHEMA)


def test_source_stream():
    dag = Dag()

    node = dag.pl.source_table(schema=SIMPLE_SCHEMA)
    polars.testing.assert_frame_equal(
        node._empty_factory(), pl.DataFrame(schema=SIMPLE_SCHEMA)
    )

    node.set_stream(SIMPLE_FRAME)
    dag.execute()
    polars.testing.assert_frame_equal(node.get_value(), SIMPLE_FRAME)

    dag.execute()
    polars.testing.assert_frame_equal(
        node.get_value(), pl.DataFrame(schema=SIMPLE_SCHEMA)
    )


def test_table_stream():
    dag = Dag()

    schema = pl.Schema([("col1", pl.Int32())])
    source = dag.pl.source_table(SIMPLE_SCHEMA)
    node = dag.pl.table_stream(lambda x: x.select(["col1"]), schema).map(source)

    dag.execute()
    polars.testing.assert_frame_equal(node.get_value(), pl.DataFrame(schema=schema))

    source.set_stream(SIMPLE_FRAME)
    dag.execute()
    polars.testing.assert_frame_equal(node.get_value(), SIMPLE_FRAME.select(["col1"]))


def test_filter_stream():
    dag = Dag()

    source = dag.pl.source_table(SIMPLE_SCHEMA)
    filtered = dag.pl.filter_stream(source, pl.col("col1") > 1, pl.col("col2") == "a")

    dag.execute()
    polars.testing.assert_frame_equal(
        filtered.get_value(), pl.DataFrame(schema=SIMPLE_SCHEMA)
    )

    source.set_stream(SIMPLE_FRAME)
    dag.execute()
    polars.testing.assert_frame_equal(
        filtered.get_value(),
        SIMPLE_FRAME.filter(pl.col("col1") > 1, pl.col("col2") == "a"),
    )


def test_get_stream_schema():
    dag = Dag()

    polars_source = dag.pl.source_table(SIMPLE_SCHEMA)
    assert _get_stream_schema(polars_source) == SIMPLE_SCHEMA

    list_source = dag.source_stream(empty=[], name="source1")
    with pytest.raises(TypeError, match=r"Argument should be a Node\[pl\.DataFrame\]"):
        _get_stream_schema(list_source)


def test_last_by():
    dag = Dag()

    source = dag.pl.source_table(SIMPLE_SCHEMA)
    last_by = dag.pl.last_by_keys(source, ["col1"])

    dag.execute()
    polars.testing.assert_frame_equal(
        last_by.get_value(), pl.DataFrame(schema=SIMPLE_SCHEMA)
    )

    source.set_stream(SIMPLE_FRAME)
    dag.execute()
    polars.testing.assert_frame_equal(last_by.get_value(), SIMPLE_FRAME)

    source.set_stream(SIMPLE_FRAME_2)
    dag.execute()
    assert str(last_by.get_value()) == str(
        pl.DataFrame([[1, 2, 3], ["d", "e", "c"]], schema=SIMPLE_SCHEMA)
    )


def test_last_by_order_of_column():
    dag = Dag()

    source = dag.pl.source_table(SIMPLE_SCHEMA)
    last_by = dag.pl.last_by_keys(source, ["col2"])

    dag.execute()
    polars.testing.assert_frame_equal(
        last_by.get_value(), pl.DataFrame(schema=SIMPLE_SCHEMA)
    )

    source.set_stream(SIMPLE_FRAME)
    dag.execute()
    polars.testing.assert_frame_equal(last_by.get_value(), SIMPLE_FRAME)


def test_last_by_bad_keys():
    dag = Dag()
    source = dag.pl.source_table(SIMPLE_SCHEMA)
    with pytest.raises(AssertionError, match="Keys must be strings"):
        dag.pl.last_by_keys(source, [1])


def test_concat_series():
    dag = Dag()
    left_source = dag.pl.source_table(SIMPLE_SCHEMA)
    left = dag.pl.get_series(left_source, "col1")
    right_source = dag.pl.source_table(SIMPLE_SCHEMA)
    right = dag.pl.get_series(right_source, "col1")

    both = dag.pl.concat_series(left, right)

    dag.execute()
    polars.testing.assert_series_equal(
        both.get_value(), pl.Series(dtype=pl.Int32(), name="col1")
    )

    left_source.set_stream(SIMPLE_FRAME)
    dag.execute()
    polars.testing.assert_series_equal(
        both.get_value(), pl.Series(values=[1, 2, 3], dtype=pl.Int32(), name="col1")
    )

    left_source.set_stream(SIMPLE_FRAME)
    right_source.set_stream(SIMPLE_FRAME_2)
    dag.execute()
    polars.testing.assert_series_equal(
        both.get_value(),
        pl.Series(values=[1, 2, 3, 1, 2], dtype=pl.Int32(), name="col1"),
    )

    right_source.set_stream(SIMPLE_FRAME_2)
    dag.execute()
    polars.testing.assert_series_equal(
        both.get_value(),
        pl.Series(values=[1, 2], dtype=pl.Int32(), name="col1"),
    )


def test_concat_series_bad_no_series():
    dag = Dag()
    with pytest.raises(ValueError, match="Must pass at least one series"):
        dag.pl.concat_series()


def test_concat_series_bad_mismatching_series():
    dag = Dag()
    source = dag.pl.source_table(SIMPLE_SCHEMA)
    left = dag.pl.get_series(source, "col1")
    right = dag.pl.get_series(source, "col2")
    with pytest.raises(TypeError, match="Series type mismatch Int32 vs String"):
        dag.pl.concat_series(left, right)


def test_get_series():
    dag = Dag()
    left_source = dag.pl.source_table(SIMPLE_SCHEMA)
    left_series = dag.pl.get_series(left_source, "col1")

    dag.execute()
    polars.testing.assert_series_equal(left_series.get_value(), EMPTY_FRAME["col1"])

    left_source.set_stream(SIMPLE_FRAME)
    dag.execute()
    polars.testing.assert_series_equal(left_series.get_value(), SIMPLE_FRAME["col1"])

    dag.execute()
    polars.testing.assert_series_equal(left_series.get_value(), EMPTY_FRAME["col1"])


def test_get_stream_dtype_bad():
    with pytest.raises(TypeError, match=r"Argument should be a Node\[pl\.Series\]"):
        _get_stream_dtype(Dag().source_stream())
