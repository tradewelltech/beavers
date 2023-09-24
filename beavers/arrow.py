import dataclasses
from typing import Callable, Iterable, Optional, ParamSpec

import numpy as np
import pyarrow as pa

from beavers.engine import Dag, Node, NodePrototype, _check_function

P = ParamSpec("P")


@dataclasses.dataclass(frozen=True)
class _TableFiler:
    predicate: Callable[[pa.Table, ...], pa.Array]

    def __call__(self, table: pa.Table, *args, **kwargs) -> pa.Table:
        return table.filter(self.predicate(table, *args, **kwargs))


def _get_latest(table: pa.Table, keys: list[str]) -> pa.Table:
    return table.take(
        table.select(keys)
        .append_column("_beavers_index", pa.array(np.arange(len(table))))
        .group_by(keys)
        .aggregate([("_beavers_index", "max")])["_beavers_index_max"]
        .sort()
    )


def _check_columns(columns: list[str], schema: pa.Schema) -> list[str]:
    if not isinstance(columns, Iterable):
        raise TypeError(columns)
    for column in columns:
        if not isinstance(column, str):
            raise TypeError(column)
        elif column not in schema.names:
            raise TypeError(f"field {column} no in schema: {schema.names}")
    return list(columns)


@dataclasses.dataclass()
class _LatestTracker:
    key_columns: list[str]
    current: pa.Table

    def __call__(self, stream: pa.Table):
        self.current = _get_latest(
            pa.concat_tables([self.current, stream]), self.key_columns
        )
        return self.current


def _get_stream_schema(node: Node[pa.Table]) -> pa.Schema:
    if not isinstance(node, Node):
        raise TypeError(f"Argument should be a {Node.__name__}")
    elif not node._is_stream():
        raise TypeError(f"Argument should be a stream {Node.__name__}")
    empty = node._empty_factory()
    if not isinstance(empty, pa.Table):
        raise TypeError(f"Argument should be a {Node.__name__}[pa.Table]")
    else:
        return empty.schema


@dataclasses.dataclass(frozen=True)
class ArrowDagWrapper:
    dag: Dag

    def source_stream(
        self, schema: pa.Schema, name: Optional[str] = None
    ) -> Node[pa.Table]:
        return self.dag.source_stream(empty=schema.empty_table(), name=name)

    def table_stream(
        self, function: Callable[P, pa.Table], schema: pa.Schema
    ) -> NodePrototype[pa.Table]:
        return self.dag.stream(function, empty=schema.empty_table())

    def filter_stream(
        self,
        predicate: Callable[[pa.Table, ...], pa.Array],
        stream: Node[pa.Table],
        *args: Node,
        **kwargs: Node,
    ) -> Node[pa.Table]:
        function = _TableFiler(predicate)
        schema = _get_stream_schema(stream)
        _check_function(function)
        return self.dag.stream(function, empty=schema.empty_table()).map(
            stream, *args, **kwargs
        )

    def latest_by_keys(self, stream: Node[pa.Table], keys: list[str]) -> Node[pa.Table]:
        schema = _get_stream_schema(stream)
        keys = _check_columns(keys, schema)
        return self.dag.state(_LatestTracker(keys, schema.empty_table())).map(stream)
