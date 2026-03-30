"""Module for building dags using polars."""

import dataclasses
from operator import itemgetter
from typing import ParamSpec, Any
from collections.abc import Callable, Iterable

import polars as pl
from polars._typing import IntoExprColumn

from beavers.dag import Dag, Node, NodePrototype

P = ParamSpec("P")


@dataclasses.dataclass()
class _LastByKey:
    key_columns: tuple[str, ...]
    current: pl.DataFrame

    def __call__(self, stream: pl.DataFrame) -> pl.DataFrame:
        self.current = (
            pl.concat([self.current, stream])
            .group_by(self.key_columns, maintain_order=True)
            .last()
            .select(self.current.columns)
        )
        return self.current


def _get_stream_schema(node: Node[pl.DataFrame]) -> pl.Schema:
    empty = node._get_empty()
    if not isinstance(empty, pl.DataFrame):
        raise TypeError(f"Argument should be a {Node.__name__}[pl.DataFrame]")
    else:
        return empty.schema


def _get_stream_dtype(node: Node[pl.Series]) -> pl.DataType:
    empty = node._get_empty()
    if not isinstance(empty, pl.Series):
        raise TypeError(f"Argument should be a {Node.__name__}[pl.Series]")
    else:
        return empty.dtype


@dataclasses.dataclass(frozen=True)
class _TableFilter:
    predicate: tuple[IntoExprColumn | Iterable[IntoExprColumn], ...]
    constraints: dict[str, Any]

    def __call__(self, table: pl.DataFrame) -> pl.DataFrame:
        return table.filter(*self.predicate, **self.constraints)


@dataclasses.dataclass(frozen=True)
class PolarsDagWrapper:
    """Helper for adding polars Nodes to a Dag."""

    _dag: Dag

    def source_table(
        self, schema: pl.Schema, name: str | None = None
    ) -> Node[pl.DataFrame]:
        """Add a source stream of type `pl.DataFrame`."""

        return self._dag.source_stream(empty=schema.to_frame(), name=name)

    def table_stream(
        self, function: Callable[P, pl.DataFrame], schema: pl.Schema
    ) -> NodePrototype[pl.DataFrame]:
        """Add a stream node of output type `pl.DataFrame`"""
        return self._dag.stream(function, empty=schema.to_frame())

    def filter_stream(
        self,
        stream: Node[pl.DataFrame],
        *predicates: IntoExprColumn | Iterable[IntoExprColumn],
        **constraints: Any,
    ) -> Node[pl.DataFrame]:
        """Filter a stream Node of type `pl.DataFrame`."""
        schema = _get_stream_schema(stream)
        return self._dag.stream(
            _TableFilter(tuple(predicates), dict(constraints)),
            empty=schema.to_frame(),
        ).map(stream)

    def last_by_keys(
        self, stream: Node[pl.DataFrame], keys: list[str]
    ) -> Node[pl.DataFrame]:
        """Build a state of the latest row by keys."""
        schema = _get_stream_schema(stream)
        for key in keys:
            assert isinstance(key, str), "Keys must be strings"
        return self._dag.state(_LastByKey(tuple(keys), schema.to_frame())).map(stream)

    def concat_series(self, *streams: Node[pl.Series]) -> Node[pl.Series]:
        if len(streams) == 0:
            raise ValueError("Must pass at least one series")
        series_type = None
        for stream in streams:
            each_type = _get_stream_dtype(stream)
            if series_type is None:
                series_type = each_type
            elif series_type != each_type:
                raise TypeError(f"Series type mismatch {series_type} vs {each_type}")

        empty = pl.Series(dtype=series_type)
        return self._dag.stream(lambda *x: pl.concat(x), empty=empty).map(*streams)

    def get_series(self, stream: Node[pl.DataFrame], name: str) -> Node[pl.Series]:
        empty = _get_stream_schema(stream).to_frame()[name]
        return self._dag.stream(itemgetter(name), empty=empty).map(stream)
