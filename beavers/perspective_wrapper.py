import dataclasses
import pathlib
import threading
from typing import Any, Literal, Optional, Sequence

import perspective
import pyarrow as pa
import tornado
from perspective import PerspectiveManager, PerspectiveTornadoHandler

from beavers import Dag, Node
from beavers.kafka import KafkaDriver

COMPARATORS = (
    "==",
    "!=",
    ">",
    ">=",
    "<",
    "<=",
    "begins with",
    "contains",
    "ends with",
    "in",
    "not in",
    "is not null",
    "is null",
)

_SOURCE_DIRECTORY = pathlib.Path(__file__).parent
TABLE_PATH = str(_SOURCE_DIRECTORY / "table.html")
ASSETS_DIRECTORY = str(_SOURCE_DIRECTORY / "assets")


@dataclasses.dataclass(frozen=True)
class PerspectiveTableDefinition:
    """
    API table definition
    """

    name: str
    index_column: str
    remove_column: Optional[str] = None
    sort: list[tuple[str, Literal["asc", "desc"]]] = dataclasses.field(
        default_factory=list
    )
    filters: list[tuple[str, str, Any]] = dataclasses.field(default_factory=list)
    hidden_columns: Sequence[str] = tuple()
    limit: Optional[int] = None

    def validate(self, schema: pa.Schema):
        assert self.index_column in schema.names, self.index_column
        if self.remove_column is not None:
            assert isinstance(self.remove_column, str)
            assert self.remove_column in schema.names, self.remove_column

        assert isinstance(self.sort, list)
        for column, order in self.sort:
            assert isinstance(column, str)
            assert column in schema.names
            assert order in ("asc", "desc")
        for column in self.hidden_columns:
            assert isinstance(column, str)
            assert column in schema.names
        for each_filter in self.filters:
            assert len(each_filter) in (2, 3)
            assert isinstance(each_filter[0], str), each_filter
            assert each_filter[1] in COMPARATORS


@dataclasses.dataclass(frozen=True)
class _TableConfig:
    """
    Internal perspective table config, which is passed to the html template
    """

    name: str
    index: str
    columns: list[str]
    sort: Sequence[tuple[str, Literal["asc", "desc"]]]
    filters: Sequence[tuple[str, str, Any]]

    @staticmethod
    def from_definition(definition: PerspectiveTableDefinition, schema: pa.Schema):
        return _TableConfig(
            name=definition.name,
            index=definition.index_column,
            columns=[f for f in schema.names if f not in definition.hidden_columns],
            sort=[] if definition.sort is None else definition.sort,
            filters=definition.filters,
        )


class TableRequestHandler(tornado.web.RequestHandler):
    """Renders the table.html template, using the provided configurations"""

    _tables: Optional[dict[str, _TableConfig]] = None
    _default_table: Optional[str] = None

    def initialize(self, table_configs: list[_TableConfig]) -> None:
        self._tables = {
            table_config.name: table_config for table_config in table_configs
        }
        self._default_table = table_configs[0].name

    async def get(self, path: str) -> None:
        table_name = path or self._default_table
        table_config = self._tables[table_name]

        await self.render(
            TABLE_PATH,
            table_config=table_config,
            perspective_version=perspective.__version__,
        )


def _table_to_bytes(table: pa.Table) -> bytes:
    """Serialize a table as bytes, to pass it to a perspective table"""
    with pa.BufferOutputStream() as sink:
        with pa.ipc.new_stream(sink, table.schema) as writer:
            for batch in table.to_batches():
                writer.write_batch(batch)
        return sink.getvalue().to_pybytes()


@dataclasses.dataclass(frozen=True)
class _UpdateRunner:
    kafka_driver: KafkaDriver

    def __call__(self):
        self.kafka_driver.run_cycle(0.0)


@dataclasses.dataclass(frozen=True)
class _PerspectiveNode:
    table_definition: PerspectiveTableDefinition
    schema: pa.Schema
    table: perspective.Table = None

    def __call__(self, table: pa.Table) -> None:
        """Pass the arrow data to perspective"""
        self.table.update(_table_to_bytes(table))

    def get_table_config(self) -> _TableConfig:
        return _TableConfig.from_definition(self.table_definition, self.schema)


@dataclasses.dataclass(frozen=True)
class PerspectiveDagWrapper:
    """Helper for adding perspective Nodes to a Dag."""

    _dag: Dag

    def to_perspective(
        self,
        node: Node,
        table_definition: PerspectiveTableDefinition,
        schema: Optional[pa.Schema] = None,
    ) -> None:
        """Add a source stream of type `pa.Table`."""
        if schema is None:
            assert node._is_stream(), "Must provide a schema for state nodes"
            empty = node._empty_factory()
            assert isinstance(empty, pa.Table), "Only pyarrow.Table nodes supported"
            schema = empty.schema
        table_definition.validate(schema)
        self._dag.state(
            _PerspectiveNode(
                table_definition,
                schema,
                table=perspective.Table(
                    _table_to_bytes(schema.empty_table()),
                    limit=table_definition.limit,
                    index=table_definition.index_column,
                ),
            )
        ).map(node)


def perspective_thread(
    manager: perspective.PerspectiveManager,
    kafka_driver: KafkaDriver,
    nodes: list[_PerspectiveNode],
):
    psp_loop = tornado.ioloop.IOLoop()

    manager.set_loop_callback(psp_loop.add_callback)
    for node in nodes:
        manager.host_table(node.table_definition.name, node.table)

    callback = tornado.ioloop.PeriodicCallback(
        callback=_UpdateRunner(kafka_driver), callback_time=1_000
    )
    callback.start()
    psp_loop.start()


def create_web_application(
    kafka_driver: KafkaDriver,
    assets_directory: str = ASSETS_DIRECTORY,
) -> tornado.web.Application:
    manager = PerspectiveManager()

    nodes: list[_PerspectiveNode] = []
    for node in kafka_driver._dag._nodes:
        if isinstance(node._function, _PerspectiveNode):
            nodes.append(node._function)
    assert len(nodes) > 0, "No perspective table nodes"
    assert len({n.table_definition.name for n in nodes}) == len(
        nodes
    ), "Duplicate table name"

    thread = threading.Thread(
        target=perspective_thread,
        args=(manager, kafka_driver, nodes),
    )
    thread.daemon = True
    thread.start()

    return tornado.web.Application(
        [
            (
                r"/websocket",
                PerspectiveTornadoHandler,
                {"manager": manager, "check_origin": True},
            ),
            (
                r"/assets/(.*)",
                tornado.web.StaticFileHandler,
                {"path": assets_directory, "default_filename": None},
            ),
            (
                r"/([a-z0-9_]*)",
                TableRequestHandler,
                {"table_configs": [node.get_table_config() for node in nodes]},
            ),
        ],
        serve_traceback=True,
    )


def run_web_application(web_app: tornado.web.Application, port: int):
    web_app.listen(port)
    loop = tornado.ioloop.IOLoop.current()
    loop.start()
