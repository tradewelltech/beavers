import numpy as np
import pandas as pd
import pytest

from beavers import Dag
from beavers.pandas_wrapper import _empty_df, _get_stream_dtypes, _LatestTracker

DTYPES = pd.Series(
    {
        "col1": np.int64,
        "col2": np.object_,
    }
)
DF = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
DF_UPDATE = pd.DataFrame({"col1": [1, 2, 2], "col2": ["e", "f", "g"]})


def test_dtypes():
    df = _empty_df(dtypes=DTYPES)
    pd.testing.assert_series_equal(df.dtypes, DTYPES)


def test_source_df():
    dag = Dag()
    source = dag.pd.source_df(dtypes=DTYPES)

    dag.execute()
    pd.testing.assert_series_equal(source.get_value().dtypes, DTYPES)

    source.set_stream(DF)
    dag.execute()
    pd.testing.assert_frame_equal(source.get_value(), DF)


def test_table_stream():
    dag = Dag()
    source = dag.pd.source_df(dtypes=DTYPES)
    stream = dag.pd.df_stream(lambda x: x[x["col1"] > 1], DTYPES).map(source)

    dag.execute()
    pd.testing.assert_frame_equal(stream.get_value(), _empty_df(DTYPES))

    source.set_stream(DF)
    dag.execute()
    pd.testing.assert_frame_equal(stream.get_value(), DF[lambda x: x["col1"] > 1])


def test_get_stream_dtypes():
    dag = Dag()
    source = dag.pd.source_df(dtypes=DTYPES)
    pd.testing.assert_series_equal(_get_stream_dtypes(source), DTYPES)

    state = dag.state(lambda: "foo").map()
    with pytest.raises(TypeError, match=r"Argument should be a stream Node"):
        pd.testing.assert_series_equal(_get_stream_dtypes(state), DTYPES)

    list_node = dag.source_stream()
    with pytest.raises(TypeError, match=r"Argument should be a Node\[pd.DataFrame\]"):
        pd.testing.assert_series_equal(_get_stream_dtypes(list_node), DTYPES)


def test_latest_tracker():
    tracker = _LatestTracker(["col1"], _empty_df(DTYPES))
    pd.testing.assert_frame_equal(tracker(_empty_df(DTYPES)), _empty_df(DTYPES))
    pd.testing.assert_frame_equal(tracker(DF), DF)
    pd.testing.assert_frame_equal(tracker(DF), DF)

    pd.testing.assert_frame_equal(
        tracker(DF_UPDATE), pd.DataFrame({"col1": [3, 1, 2], "col2": ["c", "e", "g"]})
    )


def test_latest_by_keys():
    dag = Dag()
    source = dag.pd.source_df(dtypes=DTYPES)
    latest = dag.pd.latest_by_keys(source, ["col1"])

    dag.execute()
    pd.testing.assert_frame_equal(latest.get_value(), _empty_df(DTYPES))

    source.set_stream(DF)
    dag.execute()
    pd.testing.assert_frame_equal(latest.get_value(), DF)

    source.set_stream(DF)
    dag.execute()
    pd.testing.assert_frame_equal(latest.get_value(), DF)
