import numpy as np
import pandas as pd

from beavers import Dag
from beavers.pandas_wrapper import _empty_df

DTYPES = pd.Series(
    {
        "col1": np.int64,
        "col2": np.object_,
    }
)
DF = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})


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
