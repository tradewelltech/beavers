from unittest.mock import MagicMock

import perspective
import pyarrow as pa
import pytest
from mock import mock
from perspective import Server
from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application

from beavers import Dag
from beavers.perspective_wrapper import (
    DATA_TYPES,
    PerspectiveTableDefinition,
    TableRequestHandler,
    _PerspectiveNode,
    _table_to_bytes,
    _TableConfig,
    _UpdateRunner,
    _perspective_thread,
)

PERSPECTIVE_TABLE_SCHEMA = pa.schema(
    [
        pa.field("index", pa.string()),
        pa.field("remove", pa.string()),
    ]
)
PERSPECTIVE_TABLE_DEFINITION = config = PerspectiveTableDefinition(
    name="name",
    index_column="index",
    remove_column="remove",
)


def test_config_validate():
    definition = PERSPECTIVE_TABLE_DEFINITION

    with pytest.raises(AssertionError, match="index"):
        definition.validate(pa.schema([]))

    with pytest.raises(AssertionError, match="remove"):
        definition.validate(pa.schema([pa.field("index", pa.string())]))

    definition.validate(PERSPECTIVE_TABLE_SCHEMA)


def test_to_table_config():
    assert _TableConfig.from_definition(
        PERSPECTIVE_TABLE_DEFINITION, PERSPECTIVE_TABLE_SCHEMA
    ) == _TableConfig(
        name="name", index="index", columns=["index", "remove"], sort=[], filters=[]
    )


def test_table_to_bytes():
    results = _table_to_bytes(PERSPECTIVE_TABLE_SCHEMA.empty_table())
    assert isinstance(results, bytes)
    assert len(results) > 100


def test_update_runner():
    mock = MagicMock()

    runner = _UpdateRunner(mock)
    runner()
    assert mock.run_cycle.called


def test_add_node():
    dag = Dag()
    source = dag.pa.source_table(schema=PERSPECTIVE_TABLE_SCHEMA)
    state = dag.state(lambda x: x).map(source)
    assert dag.psp.to_perspective(source, PERSPECTIVE_TABLE_DEFINITION) is None

    with pytest.raises(AssertionError, match="Must provide a schema for state nodes"):
        dag.psp.to_perspective(state, PERSPECTIVE_TABLE_DEFINITION)

    dag.psp.to_perspective(
        state, PERSPECTIVE_TABLE_DEFINITION, schema=PERSPECTIVE_TABLE_SCHEMA
    )

    for node in dag._nodes:
        if isinstance(node._function, _PerspectiveNode):
            assert node._function.table is None
            node._function.table = MagicMock()

    dag.execute()

    nodes = [
        n._function for n in dag._nodes if isinstance(n._function, _PerspectiveNode)
    ]
    assert len(nodes) == 2
    assert nodes[0].get_table_config() == _TableConfig(
        name="name", index="index", columns=["index", "remove"], sort=[], filters=[]
    )


class FakeLoop:
    @staticmethod
    def current():
        return FakeLoop()

    def add_callback(self):
        pass

    def time(self):
        return 0

    def add_timeout(self, *args, **kwargs):
        pass

    def start(self):
        pass


@mock.patch("tornado.ioloop.IOLoop", FakeLoop)
def test_perspective_thread():
    manager = Server()

    _perspective_thread(manager, MagicMock(), [])


class TestHandler(AsyncHTTPTestCase):
    def get_app(self):
        table_configs = [
            _TableConfig(
                "table1", index="col_1", columns=["col_1", "col_2"], sort=(), filters=()
            )
        ]
        return Application(
            [
                (
                    r"/([a-z0-9_]*)",
                    TableRequestHandler,
                    {"table_configs": table_configs},
                ),
            ]
        )

    def test_table(self):
        response = self.fetch("/")
        assert response.code == 200
        assert b'["col_1", "col_2"]' in response.body


def test_schema():
    server = perspective.Server()
    client = server.new_local_client()

    client.table({str(i): v[1] for i, v in enumerate(DATA_TYPES)})
