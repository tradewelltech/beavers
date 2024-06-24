"""Module for building dags using pyarrow."""

import dataclasses
from typing import Callable, Iterable, Optional, ParamSpec, Sequence

import numpy as np
import pyarrow as pa

from beavers.dag import Dag, Node, NodePrototype, _check_function

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


def _concat_arrow_arrays(
    arrow_arrays: Sequence[pa.ChunkedArray],
) -> [pa.Array | pa.ChunkedArray]:
    arrays: list[pa.Array] = []
    for arrow_array in arrow_arrays:
        if isinstance(arrow_array, pa.ChunkedArray):
            arrays.extend(arrow_array.iterchunks())
        elif isinstance(arrow_array, pa.Array):
            arrays.append(arrow_array)
        else:
            raise TypeError(arrow_array)

    return pa.chunked_array(arrays)


def _check_column(column: str, schema: pa.Schema):
    if not isinstance(column, str):
        raise TypeError(column)
    elif column not in schema.names:
        raise TypeError(f"field {column} no in schema: {schema.names}")


def _check_array(node: Node[pa.Array | pa.ChunkedArray]) -> pa.DataType:
    empty = node._get_empty()
    if not isinstance(empty, (pa.Array, pa.ChunkedArray)):
        raise TypeError(f"Argument should be a {Node.__name__}[pa.Array]")
    else:
        return empty.type


def _check_columns(columns: list[str], schema: pa.Schema) -> list[str]:
    if not isinstance(columns, Iterable):
        raise TypeError(columns)
    for column in columns:
        if not isinstance(column, str):
            raise TypeError(column)
        elif column not in schema.names:
            raise TypeError(f"field {column} no in schema: {schema.names}")
    return list(columns)


def _get_stream_schema(node: Node[pa.Table]) -> pa.Schema:
    empty = node._get_empty()
    if not isinstance(empty, pa.Table):
        raise TypeError(f"Argument should be a {Node.__name__}[pa.Table]")
    else:
        return empty.schema


@dataclasses.dataclass()
class _LatestTracker:
    key_columns: list[str]
    current: pa.Table

    def __call__(self, stream: pa.Table):
        self.current = _get_latest(
            pa.concat_tables([self.current, stream]), self.key_columns
        )
        return self.current


@dataclasses.dataclass(frozen=True)
class ArrowDagWrapper:
    """Helper call for adding pyarrow Nodes to a Dag."""

    _dag: Dag

    def source_table(
        self, schema: pa.Schema, name: Optional[str] = None
    ) -> Node[pa.Table]:
        """Add a source stream of type `pa.Table`."""
        return self._dag.source_stream(empty=schema.empty_table(), name=name)

    def table_stream(
        self, function: Callable[P, pa.Table], schema: pa.Schema
    ) -> NodePrototype[pa.Table]:
        """Add a stream node of output type `pa.Table`"""
        return self._dag.stream(function, empty=schema.empty_table())

    def filter_stream(
        self,
        predicate: Callable[[pa.Table, ...], pa.Array],
        stream: Node[pa.Table],
        *args: Node,
        **kwargs: Node,
    ) -> Node[pa.Table]:
        """Filter a stream Node of type `pa.Table`."""
        function = _TableFiler(predicate)
        schema = _get_stream_schema(stream)
        _check_function(function)
        return self._dag.stream(function, empty=schema.empty_table()).map(
            stream, *args, **kwargs
        )

    def latest_by_keys(self, stream: Node[pa.Table], keys: list[str]) -> Node[pa.Table]:
        """Build a state of the latest row by keys."""
        schema = _get_stream_schema(stream)
        keys = _check_columns(keys, schema)
        return self._dag.state(_LatestTracker(keys, schema.empty_table())).map(stream)

    def get_column(self, stream: Node[pa.Table], key: str) -> Node[pa.ChunkedArray]:
        """Return a column from a stream node of type pa.Table."""
        schema = _get_stream_schema(stream)
        _check_column(key, schema)
        field = schema.field(key)
        empty = pa.chunked_array([pa.array([], field.type)])
        return self._dag.stream(lambda x: x[key], empty=empty).map(stream)

    def concat_arrays(
        self, *streams: Node[pa.Array | pa.ChunkedArray]
    ) -> Node[pa.ChunkedArray]:
        if len(streams) == 0:
            raise ValueError("Must pass at least one array")
        array_type = None
        for stream in streams:
            each_type = _check_array(stream)
            if array_type is None:
                array_type = each_type
            elif array_type != each_type:
                raise TypeError(f"Array type mismatch {array_type} vs {each_type}")

        empty = pa.chunked_array([pa.array([], array_type)])
        return self._dag.stream(lambda *x: _concat_arrow_arrays(x), empty=empty).map(
            *streams
        )
