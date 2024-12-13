import pyarrow as pa
import pyarrow.compute as pc
import pytest

from beavers import Dag
from beavers.pyarrow_wrapper import _concat_arrow_arrays, _get_last_by, _LastByKey

SIMPLE_SCHEMA = pa.schema(
    [
        pa.field("col1", pa.int32()),
        pa.field("col2", pa.string()),
        pa.field("col3", pa.timestamp("ns", "UTC")),
    ]
)
SIMPLE_TABLE = pa.table([[1, 2, 3], ["a", "b", "c"], [0, 0, 0]], schema=SIMPLE_SCHEMA)
SIMPLE_TABLE_2 = table = pa.table([[1, 2], ["d", "e"], [0, 0]], schema=SIMPLE_SCHEMA)


def test_source_stream():
    dag = Dag()

    node = dag.pa.source_table(schema=SIMPLE_SCHEMA)
    assert node._empty_factory() == SIMPLE_SCHEMA.empty_table()

    node.set_stream(SIMPLE_TABLE)
    dag.execute()
    assert node.get_value() == SIMPLE_TABLE

    dag.execute()
    assert node.get_value() == SIMPLE_SCHEMA.empty_table()


def test_source_stream_name():
    dag = Dag()

    node = dag.pa.source_table(schema=SIMPLE_SCHEMA, name="source_1")
    assert dag.get_sources() == {"source_1": node}


def test_table_stream():
    dag = Dag()

    source = dag.pa.source_table(SIMPLE_SCHEMA)
    node = dag.pa.table_stream(
        lambda x: x.select(["col1"]),
        pa.schema([pa.field("col1", pa.int32())]),
    ).map(source)

    source.set_stream(SIMPLE_TABLE)
    dag.execute()
    assert node.get_value() == SIMPLE_TABLE.select(["col1"])


def test_filter_stream():
    dag = Dag()

    source = dag.pa.source_table(SIMPLE_SCHEMA)
    node = dag.pa.filter_stream(
        lambda x, y: pc.equal(x["col1"], y), source, dag.const(1)
    )
    SIMPLE_SCHEMA.empty_table()
    source.set_stream(SIMPLE_TABLE)
    dag.execute()
    assert node.get_value() == SIMPLE_TABLE[0:1]

    dag.execute()
    assert node.get_value() == SIMPLE_SCHEMA.empty_table()


def _predicate(table: pa.Table) -> pa.Array:
    return pc.equal(table["col1"], 1)


def test_filter_stream_bad_arguments():
    dag = Dag()

    state_node = dag.state(lambda: "HELLO").map()
    with pytest.raises(TypeError, match=r"Argument should be a stream Node"):
        dag.pa.filter_stream(_predicate, state_node)

    list_stream_node = dag.source_stream()
    with pytest.raises(TypeError, match=r"Argument should be a Node\[pa\.Table\]"):
        dag.pa.filter_stream(_predicate, list_stream_node)


def test_learn_expression_type():
    field = pc.field("col1")
    assert isinstance(field, pc.Expression)
    greater_with_pc = pc.greater(field, 2)
    assert SIMPLE_TABLE.filter(greater_with_pc) == SIMPLE_TABLE[-1:]
    greater_with_python = field > 2
    assert SIMPLE_TABLE.filter(greater_with_python) == SIMPLE_TABLE[-1:]
    with pytest.raises(TypeError):
        pc.min(SIMPLE_TABLE, field)


def test_group_by_last():
    with pytest.raises(
        pa.ArrowNotImplementedError,
        match="Using ordered aggregator"
        " in multiple threaded execution is not supported",
    ):
        SIMPLE_TABLE.group_by("col1").aggregate([("col2", "last")])


def test_get_latest():
    table = pa.table(
        [[1, 2, 3, 1, 2], ["a", "b", "c", "d", "e"], [0] * 5], schema=SIMPLE_SCHEMA
    )
    assert _get_last_by(table, ["col1"]) == table[2:]
    assert _get_last_by(table, ["col1", "col2"]) == table


def test_get_last_by_batches():
    table = pa.concat_tables([SIMPLE_TABLE, SIMPLE_TABLE])
    assert _get_last_by(table, ["col1"]) == SIMPLE_TABLE


def test_get_last_by_all_columns():
    table = pa.concat_tables([SIMPLE_TABLE, SIMPLE_TABLE])
    assert _get_last_by(table, ["col1", "col2"]) == SIMPLE_TABLE


def test_latest_tracker():
    tracker = _LastByKey(["col1"], SIMPLE_SCHEMA.empty_table())

    assert tracker(SIMPLE_SCHEMA.empty_table()) == SIMPLE_SCHEMA.empty_table()
    assert tracker(SIMPLE_TABLE) == SIMPLE_TABLE
    assert tracker(SIMPLE_TABLE_2) == pa.table(
        [[3, 1, 2], ["c", "d", "e"], [0] * 3], schema=SIMPLE_SCHEMA
    )


def test_last_by_keys():
    dag = Dag()
    source = dag.pa.source_table(SIMPLE_SCHEMA)
    latest = dag.pa.last_by_keys(source, ["col1"])

    dag.execute()
    assert latest.get_value() == SIMPLE_SCHEMA.empty_table()

    source.set_stream(SIMPLE_TABLE)
    dag.execute()
    assert latest.get_value() == SIMPLE_TABLE

    dag.execute()
    assert latest.get_value() == SIMPLE_TABLE

    source.set_stream(SIMPLE_TABLE_2)
    dag.execute()
    assert latest.get_value() == pa.table(
        [[3, 1, 2], ["c", "d", "e"], [0] * 3], schema=SIMPLE_SCHEMA
    )


def test_last_by_keys_bad():
    dag = Dag()

    with pytest.raises(
        AttributeError, match=r"'str' object has no attribute '_get_empty'"
    ):
        dag.pa.last_by_keys("Not a node", ["col1"])
    with pytest.raises(TypeError, match=r"Argument should be a Node\[pa.Table\]"):
        dag.pa.last_by_keys(dag.source_stream(), ["col1"])
    with pytest.raises(TypeError, match=r"Argument should be a stream Node"):
        dag.pa.last_by_keys(dag.state(lambda: None).map(), ["col1"])

    source = dag.pa.source_table(SIMPLE_SCHEMA)

    with pytest.raises(TypeError, match="123"):
        dag.pa.last_by_keys(source, 123)
    with pytest.raises(TypeError, match="123"):
        dag.pa.last_by_keys(source, [123])
    with pytest.raises(
        TypeError, match=r"field colz no in schema: \['col1', 'col2', 'col3'\]"
    ):
        dag.pa.last_by_keys(source, ["colz"])


def test_get_column():
    dag = Dag()
    source = dag.pa.source_table(SIMPLE_SCHEMA)
    array = dag.pa.get_column(source, "col1")

    dag.execute()
    assert array.get_value() == pa.chunked_array([pa.array([], pa.int32())])

    source.set_stream(SIMPLE_TABLE)
    dag.execute()
    assert array.get_value() == SIMPLE_TABLE["col1"]

    dag.execute()
    assert array.get_value() == pa.chunked_array([pa.array([], pa.int32())])

    source.set_stream(SIMPLE_TABLE_2)
    dag.execute()
    assert array.get_value() == SIMPLE_TABLE_2["col1"]


def test_get_column_bad():
    dag = Dag()

    with pytest.raises(
        AttributeError, match=r"'str' object has no attribute '_get_empty'"
    ):
        dag.pa.get_column("Not a node", "col1")
    with pytest.raises(TypeError, match=r"Argument should be a Node\[pa.Table\]"):
        dag.pa.get_column(dag.source_stream(), "col1")
    with pytest.raises(TypeError, match=r"Argument should be a stream Node"):
        dag.pa.get_column(dag.state(lambda: None).map(), "col1")

    source = dag.pa.source_table(SIMPLE_SCHEMA)

    with pytest.raises(TypeError, match="123"):
        dag.pa.get_column(source, 123)
    with pytest.raises(
        TypeError, match=r"field colz no in schema: \['col1', 'col2', 'col3'\]"
    ):
        dag.pa.get_column(source, "colz")


def test_concat_arrays_ok():
    dag = Dag()
    left = dag.source_stream(empty=pa.array([], pa.string()))
    right = dag.source_stream(empty=pa.array([], pa.string()))
    both = dag.pa.concat_arrays(left, right)

    dag.execute()
    assert both.get_value() == pa.chunked_array([], pa.string())

    left.set_stream(pa.array(["a", "b"]))
    right.set_stream(pa.array(["c"]))
    dag.execute()
    assert both.get_value() == pa.chunked_array(["a", "b", "c"], pa.string())

    dag.execute()
    assert both.get_value() == pa.chunked_array([], pa.string())


def test_concat_arrays_bad():
    dag = Dag()

    with pytest.raises(ValueError, match=r"Must pass at least one array"):
        dag.pa.concat_arrays()
    with pytest.raises(TypeError, match=r"Argument should be a stream Node"):
        dag.pa.concat_arrays(dag.state(lambda: None).map())
    with pytest.raises(TypeError, match=r"Argument should be a Node\[pa\.Array\]"):
        dag.pa.concat_arrays(dag.source_stream())
    with pytest.raises(TypeError, match=r"Array type mismatch string vs int32"):
        dag.pa.concat_arrays(
            dag.source_stream(empty=pa.array([], pa.string())),
            dag.source_stream(empty=pa.array([], pa.int32())),
        )


def test_concat_arrow_arrays_mixed():
    assert _concat_arrow_arrays(
        [
            pa.array([], pa.string()),
            pa.chunked_array(pa.array([], pa.string())),
        ]
    ) == pa.chunked_array([], pa.string())


def test_concat_arrow_arrays_bad():
    with pytest.raises(TypeError, match="123"):
        _concat_arrow_arrays([123])
