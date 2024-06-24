import asyncio
import operator
import time

import pandas as pd
import pytest

from beavers.dag import (
    _STATE_UNCHANGED,
    UTC_EPOCH,
    UTC_MAX,
    Dag,
    DagMetrics,
    TimerManager,
    _check_empty,
    _NodeInputs,
    _unchanged_callback,
)
from tests.test_util import (
    GetLatest,
    SetATimer,
    TimerEntry,
    WordCount,
    add,
    add_no_42,
    join_counts,
)


def test_state_positional():
    dag = Dag()
    x_source = dag.source_stream([], name="x")
    x = dag.state(GetLatest(1)).map(x_source)
    y_source = dag.source_stream([], name="y")
    y = dag.state(GetLatest(2)).map(y_source)

    z = dag.state(add).map(x, y)

    dag.execute()

    assert 3 == z.get_value()

    x_source.set_stream([5])
    dag.execute()
    assert 7 == z.get_value()


def test_map_state_key_word():
    dag = Dag()
    x_source = dag.source_stream([], name="x")
    x = dag.state(GetLatest(1)).map(x_source)
    y_source = dag.source_stream([], name="y")
    y = dag.state(GetLatest(2)).map(y_source)

    z = dag.state(add).map(left=x, right=y)

    dag.execute()
    assert 3 == z.get_value()

    x_source.set_stream([5])
    dag.execute()
    assert 7 == z.get_value()


def test_map_positional_and_key_word_not_valid():
    dag = Dag()
    x_source = dag.source_stream([], name="x")
    x = dag.state(GetLatest(1)).map(x_source)
    y_source = dag.source_stream([], name="y")
    y = dag.state(GetLatest(2)).map(y_source)

    dag.state(add).map(x, left=y)

    with pytest.raises(TypeError, match=r"got multiple values for argument"):
        dag.execute()


def test_map_runtime_failure():
    dag = Dag()
    x_source = dag.source_stream([], name="x")
    x = dag.state(GetLatest(40)).map(x_source)
    y_source = dag.source_stream([], name="y")
    y = dag.state(GetLatest(1)).map(y_source)

    z = dag.state(add_no_42).map(x, y)

    dag.execute()
    assert 41 == z.get_value()

    y_source.set_stream([2])
    with pytest.raises(ValueError, match=r".* == 42$"):
        dag.execute()


def test_using_lambda():
    dag = Dag()
    x_source = dag.source_stream([], name="x")
    x = dag.state(GetLatest(40)).map(x_source)
    y_source = dag.source_stream([], name="y")
    y = dag.state(GetLatest(41)).map(y_source)
    z = dag.state(add).map(x, y)

    w = dag.state(lambda left, right: left - right).map(y, z)

    dag.execute()
    assert w.get_value() == -40


def test_scalar():
    dag = Dag()
    x = dag.const(40)
    y_source = dag.source_stream([], name="y")
    y = dag.state(GetLatest(1)).map(y_source)
    z = dag.state(add).map(x, y)

    dag.execute()
    assert z.get_value() == 41

    y_source.set_stream([2])
    assert z.get_value() == 41
    dag.execute()
    assert z.get_value() == 42

    with pytest.raises(TypeError, match="Only _SourceStreamFunction can be set"):
        z.set_stream(34)


def test_stream_no_empty():
    dag = Dag()
    source1 = dag.source_stream()
    assert source1.get_value() == []
    assert source1._empty_factory is list

    stream = dag.stream(lambda x: x).map(source1)
    assert stream.get_value() == []
    assert stream._empty_factory is list


def test_stream_to_state():
    dag = Dag()

    hello_stream = dag.source_stream([], name="hello")
    world_stream = dag.source_stream([], name="world")

    hello_count = dag.state(WordCount()).map(hello_stream)
    world_count = dag.state(WordCount()).map(world_stream)

    both = dag.state(join_counts).map(hello=hello_count, world=world_count)

    hello_stream.set_stream(["foo", "bar", "foo"])
    world_stream.set_stream(["z", "x", "y"])
    dag.execute()
    assert 1 == both.get_cycle_id()
    assert both.get_value()["hello"]["foo"] == 2
    assert both.get_value()["hello"]["z"] == 0
    assert both.get_value()["world"]["foo"] == 0
    assert both.get_value()["world"]["z"] == 1

    hello_stream.set_stream(["foo"])
    world_stream.set_stream(["z"])
    dag.execute()
    assert 2 == both.get_cycle_id()
    assert both.get_value()["hello"]["foo"] == 3
    assert both.get_value()["hello"]["z"] == 0
    assert both.get_value()["world"]["foo"] == 0
    assert both.get_value()["world"]["z"] == 2

    dag.execute()  # Nothing should happen here as inputs are flushed
    assert 2 == both.get_cycle_id()

    hello_stream.set_stream([])
    world_stream.set_stream([])
    dag.execute()
    assert 2, both.get_cycle_id()


def test_map_stream():
    dag = Dag()

    source = dag.source_stream([])
    source.set_stream([1, 2, 3])
    plus_one = dag.stream(lambda x: [v + 1 for v in x], []).map(source)
    dag.execute()

    assert [2, 3, 4] == plus_one.get_value()


def test_add_stream():
    dag = Dag()

    source = dag.source_stream([])
    source.set_stream([1, 2, 3])
    plus_one = dag.stream(lambda x: [v + 1 for v in x], []).map(source)
    dag.execute()

    assert [2, 3, 4] == plus_one.get_value()


def test_map_stream_with_async_calls():
    async def get_square(x: int) -> int:
        """Waits for one second and then computes the square of the given int"""
        await asyncio.sleep(0.1)
        return x * x

    async def get_squares(xs: list[int]) -> list[int]:
        """Executes get_square on the given micro-batch"""
        coros = []
        for x in xs:
            coros.append(get_square(x))

        return await asyncio.gather(*coros)  # type: ignore

    def run_get_squares(xs: list[int]) -> list[int]:
        """Synchronous wrapper for the async function get_squares"""
        return asyncio.run(get_squares(xs))

    dag = Dag()
    source = dag.source_stream([])
    async_node = dag.stream(run_get_squares, []).map(source)
    source.set_stream([0, 1, 2, 3, 4, 5, 6])

    start_time = time.time()
    dag.execute()
    end_time = time.time()

    # It should take just barely over a second to call dag.execute() since it should
    # run all calls to get_square(x) concurrently. If get_square(x) were a synchronous
    # function that took 1 second per call then we'd expect it to take about 7 seconds.
    assert 0.1 <= (end_time - start_time) < 0.2
    assert async_node.get_value() == [0, 1, 4, 9, 16, 25, 36]


def test_time():
    dag = Dag()
    source = dag.source_stream([], name="x")
    add_time = dag.state(lambda x, t: [(v, t) for v in x]).map(source, dag.now())

    time0 = pd.to_datetime("2022-09-15", utc=True)
    dag.execute(time0)
    assert add_time.get_value() == []

    time1 = time0 + pd.to_timedelta("2s")
    source.set_stream(["a"])
    dag.execute(time1)
    assert add_time.get_value() == [("a", time1)]

    time2 = time0 + pd.to_timedelta("2s")
    dag.execute(time2)
    assert add_time.get_value() == [
        ("a", time1)
    ], "Change of time isn't notified to clean/not stale node"

    time3 = time1 + pd.to_timedelta("4s")
    source.set_stream(["b"])
    dag.execute(time3)
    assert add_time.get_value() == [
        ("b", time3)
    ], "Change of time is notified to stale node"


def test_cutoff_update():
    dag = Dag()
    x_source = dag.source_stream([], name="x")
    x = dag.state(GetLatest(1)).map(x_source)
    x_change_only = dag.cutoff(x)

    x_source.set_stream(["a"])
    dag.execute()
    assert x.get_value() == "a"
    assert x_change_only.get_value() == "a"
    assert x.get_cycle_id() == dag.get_cycle_id()
    assert x_change_only.get_cycle_id() == dag.get_cycle_id()

    dag.execute()
    assert x.get_cycle_id() == dag.get_cycle_id() - 1
    assert x_change_only.get_cycle_id() == dag.get_cycle_id() - 1

    x_source.set_stream(["a"])
    dag.execute()
    assert x.get_cycle_id() == dag.get_cycle_id()
    assert x_change_only.get_cycle_id() == dag.get_cycle_id() - 2


def test_cutoff_custom():
    dag = Dag()
    x_source = dag.source_stream([], name="x")
    x = dag.state(GetLatest(1)).map(x_source)
    x_change_only = dag.cutoff(x, comparator=lambda x, y: abs(x - y) < 0.1)

    x_source.set_stream([1.0])
    dag.execute()
    assert x.get_value() == 1.0
    assert x_change_only.get_value() == 1.0
    assert x.get_cycle_id() == dag.get_cycle_id()
    assert x_change_only.get_cycle_id() == dag.get_cycle_id()

    dag.execute()
    assert x.get_cycle_id() == dag.get_cycle_id() - 1
    assert x_change_only.get_cycle_id() == dag.get_cycle_id() - 1

    x_source.set_stream([1.01])
    dag.execute()
    assert x.get_cycle_id() == dag.get_cycle_id()
    assert x_change_only.get_cycle_id() == dag.get_cycle_id() - 2

    x_source.set_stream([1.09])
    dag.execute()
    assert x.get_cycle_id() == dag.get_cycle_id()
    assert x_change_only.get_cycle_id() == dag.get_cycle_id() - 3

    x_source.set_stream([1.11])
    dag.execute()
    assert x.get_cycle_id() == dag.get_cycle_id()
    assert x_change_only.get_cycle_id() == dag.get_cycle_id()
    assert x_change_only.get_value() == 1.11


def test_cutoff_not_callable():
    dag = Dag()
    x_source = dag.source_stream([], name="x")
    x = dag.state(GetLatest(1)).map(x_source)
    with pytest.raises(TypeError, match="`comparator` should be callable"):
        dag.cutoff(x, comparator="not a callable")


def test_silence_state():
    dag = Dag()
    x_source = dag.source_stream([], name="x")
    x = dag.state(GetLatest(1)).map(x_source)
    x_silent = dag.silence(x)

    x_source.set_stream(["a"])
    dag.execute()
    assert x.get_value() == "a"
    assert x_silent.get_value() == "a"
    assert x.get_cycle_id() == dag.get_cycle_id()
    assert x_silent.get_cycle_id() == 0

    x_source.set_stream(["b"])
    dag.execute()
    assert x.get_value() == "b"
    assert x_silent.get_value() == "b"
    assert x.get_cycle_id() == dag.get_cycle_id()
    assert x_silent.get_cycle_id() == 0


def test_silence_stream():
    dag = Dag()
    x_source = dag.source_stream([], name="x")
    x_silent = dag.silence(x_source)

    x_source.set_stream(["a", "b"])
    dag.execute()
    assert x_silent.get_cycle_id() == 0
    assert x_source.get_cycle_id() == 1
    assert x_silent.get_value() == ["a", "b"]


def test_now():
    dag = Dag()

    now = dag.now()
    assert now.get_value() == UTC_EPOCH

    dag.execute(pd.to_datetime("2022-09-22", utc=True))
    assert now.get_value() == pd.to_datetime("2022-09-22", utc=True)
    assert now.get_cycle_id() == 0

    dag.execute(pd.to_datetime("2022-09-23", utc=True))
    assert now.get_value() == pd.to_datetime("2022-09-23", utc=True)
    assert now.get_cycle_id() == 0


def test_timers():
    set_a_timer = SetATimer()

    dag = Dag()
    timer_source = dag.source_stream([])
    node = dag.stream(set_a_timer, []).map(timer_source, dag.now(), dag.timer_manager())

    assert dag.get_next_timer() == UTC_MAX
    dag.execute(pd.to_datetime("2022-09-22", utc=True))
    assert dag.get_next_timer() == UTC_MAX
    assert node.get_value() == []
    assert node.get_cycle_id() == 0

    timer_source.set_stream(
        [TimerEntry(pd.to_datetime("2022-09-24", utc=True), [1, 2, 3])]
    )
    dag.execute(pd.to_datetime("2022-09-23", utc=True))
    assert node.get_cycle_id() == 0
    assert node.get_value() == []
    assert dag.get_next_timer() == pd.to_datetime("2022-09-24", utc=True)

    dag.execute(pd.to_datetime("2022-09-23", utc=True))
    assert node.get_cycle_id() == 0
    assert node.get_value() == []
    assert dag.get_next_timer() == pd.to_datetime("2022-09-24", utc=True)

    dag.execute(pd.to_datetime("2022-09-24", utc=True))
    assert node.get_value() == [1, 2, 3]
    assert node.get_cycle_id() == 4
    assert dag.get_next_timer() == UTC_MAX

    dag.execute(pd.to_datetime("2022-09-25", utc=True))
    assert node.get_value() == []
    assert node.get_cycle_id() == 4
    assert dag.get_next_timer() == UTC_MAX

    timer_source.set_stream(
        [TimerEntry(pd.to_datetime("2022-09-27", utc=True), [4, 5, 6])]
    )
    dag.execute(pd.to_datetime("2022-09-26", utc=True))
    assert node.get_value() == []
    assert node.get_cycle_id() == 4
    assert dag.get_next_timer() == pd.to_datetime("2022-09-27", utc=True)

    dag.execute(pd.to_datetime("2022-09-30", utc=True))
    assert node.get_value() == [4, 5, 6]
    assert node.get_cycle_id() == 7
    assert dag.get_next_timer() == UTC_MAX


def test_timer_manager():
    timer_manager = TimerManager()
    t0 = pd.to_datetime("2022-10-19", utc=True)
    t1 = pd.to_datetime("2022-10-20", utc=True)
    timer_manager.set_next_timer(t1)
    assert timer_manager._flush(t0) is False
    assert timer_manager.just_triggered() is False
    assert timer_manager.has_next_timer() is True
    assert timer_manager._flush(t1) is True
    assert timer_manager.just_triggered() is True
    assert timer_manager.has_next_timer() is False


def test_sinks_and_sources():
    dag = Dag()
    source_1 = dag.source_stream([], name="source_1")
    source_2 = dag.source_stream([], name="source_2")
    both = dag.stream(lambda left, right: left + right, []).map(source_1, source_2)
    sink = dag.sink("sink", both)

    assert dag.get_sources() == {"source_1": source_1, "source_2": source_2}
    assert dag.get_sinks() == {"sink": [sink]}

    source_1.set_stream([1, 2, 3])
    dag.execute()
    assert dag.get_sinks()["sink"][0].get_sink_value() == [1, 2, 3]

    source_1.set_stream([4, 5, 6])
    dag.execute()
    assert dag.get_sinks()["sink"][0].get_sink_value() == [4, 5, 6]

    source_1.set_stream([7])
    source_2.set_stream([8, 9])
    dag.execute()
    assert sink is dag.get_sinks()["sink"][0]
    assert sink.get_sink_value() == [7, 8, 9]
    # Nodes know about their inputs and their observer
    # As such this creates a circular dependency.
    # This causes `__str__` to stack overflow
    assert "Node" in str(sink)


def test_duplicate_source():
    dag = Dag()
    source_1 = dag.source_stream(name="source")
    source_2 = dag.source_stream(name="source")

    assert source_1 is source_2


def test_duplicate_source_different_empty():
    dag = Dag()
    dag.source_stream([], name="source_1")
    with pytest.raises(ValueError, match=r"Duplicate source: source_1"):
        dag.source_stream({}, name="source_1")


def test_node_with_same_input_positional():
    dag = Dag()
    source_1 = dag.source_stream([], name="source")
    node = dag.stream(lambda a, b: a + b, []).map(source_1, source_1)
    assert node._inputs.positional == (source_1, source_1)
    assert node._inputs.key_word == {}
    assert node._inputs.nodes == (source_1,)


def test_node_with_same_input_key_word():
    dag = Dag()
    source_1 = dag.source_stream([], name="source")
    node = dag.stream(lambda a, b: a + b, []).map(a=source_1, b=source_1)
    assert node._inputs.positional == ()
    assert node._inputs.key_word == {"a": source_1, "b": source_1}
    assert node._inputs.nodes == (source_1,)


def test_node_with_same_input_mixed():
    dag = Dag()
    source_1 = dag.source_stream([], name="source")
    node = dag.stream(lambda a, b: a + b, []).map(source_1, b=source_1)
    assert node._inputs.positional == (source_1,)
    assert node._inputs.key_word == {"b": source_1}
    assert node._inputs.nodes == (source_1,)


def test_wrong_usage():
    dag = Dag()
    with pytest.raises(TypeError, match="`empty` should implement `__len__`"):
        dag.stream(lambda x: x, 123)

    with pytest.raises(TypeError, match="`empty` should implement `__len__`"):
        dag.stream(lambda x: x, 0)


def test_add_existing_node():
    dag = Dag()
    source = dag.source_stream([], name="source")
    node = dag.stream(lambda x: x, []).map(source)
    with pytest.raises(ValueError, match="New Node can't have observers"):
        dag._add_node(source)
    with pytest.raises(ValueError, match="Node already in dag"):
        dag._add_node(node)


def test_mixed_dags():
    dag = Dag()
    other_dag = Dag()
    other_source = other_dag.source_stream([], name="source")
    with pytest.raises(ValueError, match="Input Node not in dag"):
        dag.stream(lambda x: x, []).map(other_source)


def test_get_sink_value_on_other_node():
    dag = Dag()
    source = dag.source_stream([], name="source")
    node = dag.stream(lambda x: x, []).map(source)
    with pytest.raises(TypeError, match="Only _SinkFunction can be read"):
        node.get_sink_value()


def test_node_inputs_kwargs_not_str():
    dag = Dag()
    source = dag.source_stream([], name="source")
    with pytest.raises(TypeError, match="class 'int'"):
        _NodeInputs.create([], {1: source})


def test_node_inputs_not_node():
    with pytest.raises(TypeError, match="Inputs should be `Node`, got <class 'str'>"):
        _NodeInputs.create(["foo"], {})


def test_node_not_function():
    dag = Dag()
    with pytest.raises(TypeError, match="`function` should be a callable"):
        dag.stream("foo", [])
    with pytest.raises(TypeError, match="`function` should be a callable"):
        dag.state("foo")


def test_stream_empty_not_empty():
    dag = Dag()
    with pytest.raises(TypeError, match=r"`len\(empty\)` should be 0"):
        dag.stream("foo", [1])
    with pytest.raises(TypeError, match="`empty` should implement `__len__`"):
        dag.stream("foo", 1)


def test_recalculate_clean_node():
    dag = Dag()
    source = dag.source_stream([])
    node = dag.stream(lambda x: x, []).map(source)
    node._recalculate(1)
    with pytest.raises(RuntimeError, match="Calling recalculate on un-notified node"):
        node._recalculate(2)


def test_can_add_node_copy():
    dag = Dag()
    source = dag.source_stream([])
    node_one = dag.stream(operator.__add__).map(source, source)
    node_two = dag.stream(operator.__add__).map(source, source)
    assert node_one is not node_two
    assert node_one != node_two


def test_can_not_add_node_back():
    dag = Dag()
    source = dag.source_stream([])
    with pytest.raises(ValueError, match="Node already in dag"):
        dag._add_node(source)


def test_unchanged_callback():
    assert _unchanged_callback() is _STATE_UNCHANGED


def test_metrics():
    dag = Dag()
    x_source = dag.source_stream([], name="x")
    x = dag.state(GetLatest(40)).map(x_source)
    y_source = dag.source_stream([], name="y")
    y = dag.state(GetLatest(41)).map(y_source)
    z = dag.state(add).map(x, y)

    dag.state(lambda left, right: left - right).map(y, z)

    dag.execute()
    assert dag.flush_metrics() == DagMetrics(13, 8, 1, 8)

    dag.execute()
    assert dag.flush_metrics() == DagMetrics(0, 0, 1, 8)
    assert dag.flush_metrics() == DagMetrics(0, 0, 0, 8)

    x_source.set_stream([1, 2, 3])
    y_source.set_stream([1, 2, 3])
    dag.execute()
    assert dag.flush_metrics() == DagMetrics(8, 6, 1, 8)

    x_source.set_stream([1, 2, 3])
    dag.execute()
    assert dag.flush_metrics() == DagMetrics(4, 4, 1, 8)


def test_check_empty():
    assert _check_empty(None, None) is list
    empty_list_empty = _check_empty([], None)
    assert callable(empty_list_empty)
    assert empty_list_empty() == []

    with pytest.raises(TypeError, match=r"`len\(empty\)` should be 0"):
        _check_empty([1], None)

    with pytest.raises(TypeError, match=r"`empty` should implement `__len__`"):
        _check_empty(123, None)

    with pytest.raises(
        ValueError,
        match=r"Can't provide both empty=\[\] and empty_factory=",
    ):
        _check_empty([], list)

    with pytest.raises(TypeError, match=r"`len\(empty\)` should be 0"):
        _check_empty(None, lambda: [1])

    with pytest.raises(TypeError, match=r"empty_value=123 should implement `__len__`"):
        _check_empty(None, lambda: 123)

    with pytest.raises(TypeError, match=r"empty_factory=123 should be a callable"):
        _check_empty(None, 123)

    with pytest.raises(TypeError, match=r"should not return None"):
        _check_empty(empty=None, empty_factory=lambda: None)

    assert _check_empty(empty=None, empty_factory=list) is list


def _modify(values: list[int], right: list[int]) -> list[int]:
    values.extend(right)
    return values


def test_mutate_inputs():
    dag = Dag()
    source = dag.source_stream()
    right = dag.source_stream()
    modifier = dag.stream(_modify).map(source, right)
    passthrough = dag.stream(lambda x, _: x).map(source, right)

    source.set_stream([1, 2, 3])
    right.set_stream([4])
    dag.execute()
    assert modifier.get_value() == [1, 2, 3, 4]
    assert passthrough.get_value() == [1, 2, 3, 4]

    dag.execute()
    assert modifier.get_value() == []  # Not notified
    assert passthrough.get_value() == []

    right.set_stream([1])
    dag.execute()
    assert modifier.get_value() == [1]
    assert passthrough.get_value() == []  # Notified but got the factory list
    assert passthrough.get_cycle_id() != dag.get_cycle_id()  # considered not updated


def test_prune_simple():
    dag = Dag()
    source = dag.source_stream([], name="source")
    node = dag.stream(lambda x: x, []).map(source)
    assert node in dag._nodes
    assert source in dag._nodes
    assert dag.get_sources() == {"source": source}
    dag.prune()
    assert node not in dag._nodes
    assert source not in dag._nodes
    assert dag.get_sources() == {}
    dag.execute()


def test_prune_nothing():
    dag = Dag()
    source = dag.source_stream([], name="source")
    node = dag.stream(lambda x, _: x, []).map(source, dag.now())
    dag.sink("sink", node)

    assert node in dag._nodes
    assert source in dag._nodes
    assert dag.get_sources() == {"source": source}
    assert dag.prune() == []


def test_prune_sinks():
    dag = Dag()
    source_a = dag.source_stream([], name="source_a")
    source_b = dag.source_stream([], name="source_b")
    node_a = dag.stream(lambda x: x, []).map(source_a)
    node_b = dag.stream(lambda x: x, []).map(source_b)
    sink_b = dag.sink("sink_b", node_b)

    dag.prune()
    assert source_a not in dag._nodes
    assert node_a not in dag._nodes
    assert source_b in dag._nodes
    assert node_b in dag._nodes
    assert sink_b in dag._nodes
    assert dag.get_sources() == {"source_b": source_b}
    dag.execute()

    source_a.set_stream(["a", "b"])
    source_b.set_stream(["a", "b"])
    dag.execute()
    assert node_a.get_value() == []  # Not updated
    assert node_b.get_value() == ["a", "b"]
