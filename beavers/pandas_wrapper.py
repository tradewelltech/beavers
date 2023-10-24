"""Module for building dags using pandas."""
import dataclasses
from typing import Callable, Optional, ParamSpec

import pandas as pd

from beavers import Dag, Node
from beavers.engine import NodePrototype

P = ParamSpec("P")


def _empty_df(dtypes: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(columns=dtypes.index).astype(dtypes)


def _get_stream_dtypes(node: Node[pd.DataFrame]) -> pd.Series:
    empty = node._get_empty()
    if not isinstance(empty, pd.DataFrame):
        raise TypeError(f"Argument should be a {Node.__name__}[pa.Table]")
    else:
        return empty.dtypes


@dataclasses.dataclass()
class _LatestTracker:
    key_columns: list[str]
    current: pd.DataFrame

    def __call__(self, stream: pd.DataFrame):
        self.current = (
            pd.concat([self.current, stream])
            .groupby(self.key_columns, as_index=False)
            .tail(1)
        )

        return self.current


@dataclasses.dataclass(frozen=True)
class PandasWrapper:
    """Helper call for adding pandas Nodes to a Dag."""

    _dag: Dag

    def source_df(
        self, dtypes: pd.Series, name: Optional[str] = None
    ) -> Node[pd.DataFrame]:
        empty = _empty_df(dtypes)
        return self._dag.source_stream(empty, name=name)

    def df_stream(
        self, function: Callable[P, pd.DataFrame], dtypes: pd.Series
    ) -> NodePrototype[pd.DataFrame]:
        return self._dag.stream(function, empty=_empty_df(dtypes))

    def latest_by_keys(
        self, stream: Node[pd.DataFrame], keys: list[str]
    ) -> Node[pd.DataFrame]:
        """Build a state of the latest row by keys."""
        dtypes = _get_stream_dtypes(stream)
        for key in keys:
            assert key in dtypes, key
        return self._dag.state(_LatestTracker(keys, _empty_df(dtypes))).map(stream)
