"""Module for building dags using pandas."""
import dataclasses
from typing import Callable, Optional, ParamSpec

import pandas as pd

from beavers import Dag, Node
from beavers.engine import NodePrototype

P = ParamSpec("P")


def _empty_df(dtypes: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(columns=dtypes.index).astype(dtypes)


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
