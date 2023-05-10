from __future__ import annotations

import collections.abc
import dataclasses
import operator
import typing
from collections import defaultdict

import pandas as pd

P = typing.ParamSpec("P")

T = typing.TypeVar("T")
I = typing.TypeVar("I")  # noqa E741
O = typing.TypeVar("O")  # noqa E741

_STATE_EMPTY = object()
_STATE_UNCHANGED = object()

UTC_EPOCH = pd.to_datetime(0, utc=True)
UTC_MAX = pd.Timestamp.max.tz_localize("UTC")


class SourceStreamFunction(typing.Generic[T]):
    def __init__(self, empty: T, name: str):
        self._empty = empty
        self._name = name
        self._value = empty

    def set(self, value: T):
        self._value = value

    def __call__(self) -> T:
        result = self._value
        self._value = self._empty
        return result


class SinkFunction(typing.Generic[T]):
    def __init__(self, name: str):
        self._name = name
        self._value: typing.Optional[T] = None

    def get(self) -> typing.Optional[T]:
        return self._value

    def __call__(self, value: T) -> None:
        self._value = value
        return None


class ValueCutOff(typing.Generic[T]):
    def __init__(self, comparator: typing.Callable[[T, T], T] = operator.eq):
        self._value = _STATE_EMPTY
        self._comparator = comparator

    def __call__(self, value: T) -> T:
        if self._value == _STATE_EMPTY or not self._comparator(value, self._value):
            self._value = value
            return value
        else:
            return _STATE_UNCHANGED


class TimerManager:
    def __init__(self):
        self._next_timer: pd.Timestamp = UTC_MAX
        self._just_triggered: bool = False

    def has_next_timer(self) -> bool:
        return self._next_timer != UTC_MAX

    def just_triggered(self) -> bool:
        return self._just_triggered

    def get_next_timer(self) -> pd.Timestamp:
        return self._next_timer

    def set_next_timer(self, timer: pd.Timestamp):
        self._next_timer = timer

    def clear_next_timer(self):
        self._next_timer = UTC_MAX

    def _flush(self, now: pd.Timestamp) -> bool:
        if self._next_timer <= now:
            self.clear_next_timer()
            self._just_triggered = True
            return True
        else:
            self._just_triggered = False
            return False


class TimerManagerFunction:
    def __init__(self):
        self.timer_manager: TimerManager = TimerManager()

    def __call__(self) -> TimerManager:
        return self.timer_manager


@dataclasses.dataclass(frozen=True)
class SilentUpdate(typing.Generic[T]):
    value: T


class SourceState(typing.Generic[T]):
    def __init__(self, value: T):
        self._value = value

    def set_value(self, value: T):
        self._value = value

    def __call__(self) -> T:
        return self._value


@dataclasses.dataclass(frozen=True)
class NodeInputs:
    positional: typing.Tuple[Node]
    key_word: typing.Dict[str, Node]
    nodes: typing.Tuple[Node]

    @staticmethod
    def create(
        positional: typing.Sequence[Node], key_word: typing.Dict[str, Node]
    ) -> "NodeInputs":
        all_nodes = []
        for node in positional:
            _check_input(node)
            if node not in all_nodes:
                all_nodes.append(node)
        for key, node in key_word.items():
            if not isinstance(key, str):
                raise TypeError(type(key))
            _check_input(node)
            if node not in all_nodes:
                all_nodes.append(node)

        return NodeInputs(
            positional=tuple(positional),
            key_word=dict(key_word),
            nodes=tuple(all_nodes),
        )

    def input_nodes(self) -> typing.Tuple[Node]:
        return self.nodes


NO_INPUTS = NodeInputs.create([], {})


@dataclasses.dataclass
class RuntimeNodeData(typing.Generic[T]):
    value: typing.Optional[T]
    notifications: int
    cycle_id: int


@dataclasses.dataclass(frozen=True)
class Node(typing.Generic[T]):
    function: typing.Optional[typing.Callable[[...], T]]
    inputs: NodeInputs = dataclasses.field(repr=False)
    empty: typing.Any
    observers: list[Node] = dataclasses.field(repr=False)
    runtime_data: RuntimeNodeData

    @staticmethod
    def create(
        value: T = None,
        function: typing.Optional[typing.Callable[[...], T]] = None,
        inputs: NodeInputs = NO_INPUTS,
        empty: typing.Any = _STATE_EMPTY,
        notifications: int = 1,
    ) -> Node:
        return Node(
            function=function,
            inputs=inputs,
            empty=empty,
            runtime_data=RuntimeNodeData(value, notifications, 0),
            observers=[],
        )

    def get_value(self) -> T:
        return self.runtime_data.value

    def get_cycle_id(self) -> int:
        return self.runtime_data.cycle_id

    def set_stream(self, value: T):
        if not isinstance(self.function, SourceStreamFunction):
            raise TypeError(f"Only {SourceStreamFunction.__name__} can be set")
        self.function.set(value)
        self._stain()

    def get_sink_value(self) -> typing.Any:
        if not isinstance(self.function, SinkFunction):
            raise TypeError(f"Only {SinkFunction.__name__} can be read")
        return self.function.get()

    def _stain(self):
        self.runtime_data.notifications += 1

    def _clean(self, cycle_id: int):
        if self._should_recalculate():
            self._recalculate(cycle_id)
        else:
            if self._is_stream():
                self.runtime_data.value = self.empty
                self.runtime_data.notifications = 0

    def _is_stream(self) -> bool:
        return self.empty is not _STATE_EMPTY

    def _is_state(self) -> bool:
        return self.empty is _STATE_EMPTY

    def _should_recalculate(self) -> bool:
        return self.runtime_data.notifications != 0

    def _recalculate(self, cycle_id: int):
        if not self._should_recalculate():
            raise RuntimeError("Calling recalculate on un-notified node")

        positional_values = [node.get_value() for node in self.inputs.positional]
        key_word_values = {
            key: node.get_value() for key, node in self.inputs.key_word.items()
        }
        updated_value = self.function(*positional_values, **key_word_values)
        updated = self._process_updated_value(updated_value)

        if updated:
            self.runtime_data.cycle_id = cycle_id
            self._notify_observers()
        self.runtime_data.notifications = 0

    def _process_updated_value(self, updated_value) -> bool:
        if self._is_state():
            if isinstance(updated_value, SilentUpdate):
                self.runtime_data.value = updated_value.value
                return False
            if updated_value is not _STATE_UNCHANGED:
                self.runtime_data.value = updated_value
                return True
            else:
                return False
        else:
            self.runtime_data.value = updated_value
            return len(updated_value) > 0

    def _notify_observers(self):
        for observer in self.observers:
            observer._stain()


@dataclasses.dataclass(frozen=True)
class NodePrototype(typing.Generic[T]):
    _add_to_dag: typing.Callable[[NodeInputs], Node[T]]

    def map(self, *args: Node, **kwargs: Node) -> Node[T]:
        return self._add_to_dag(NodeInputs.create(positional=args, key_word=kwargs))


class Dag:
    """
    Main class used for building and executing dags
    """

    def __init__(self):
        self._nodes: list[Node] = []
        self._sources: dict[str, Node] = {}
        self._sinks: dict[str, Node] = {}
        self._now_node: Node[pd.Timestamp] = self._add_node(
            Node.create(UTC_EPOCH, SourceState(UTC_EPOCH))
        )
        self._silent_now_node: Node[pd.Timestamp] = self.silence(self._now_node)
        self._timer_manager_nodes: list[Node[TimerManager]] = []
        self._cycle_id: int = 0

    def const(self, value: T) -> Node[T]:
        return self._add_node(
            Node.create(
                function=lambda x: _STATE_UNCHANGED,
                inputs=NO_INPUTS,
                value=value,
                notifications=0,
            )
        )

    def source_stream(self, empty: T, name: str = None) -> Node[T]:
        _check_empty(empty)
        existing = self._sources.get(name) if name else None
        if existing is not None:
            if existing.empty != empty:
                raise ValueError(f"Duplicate source: {name}")
            else:
                return existing
        else:
            node = self._add_stream(
                function=SourceStreamFunction(empty, name),
                empty=empty,
                inputs=NO_INPUTS,
            )
            if name:
                self._sources[name] = node
            return node

    def stream(self, function: typing.Callable[P, T], empty: T) -> NodePrototype:
        _check_empty(empty)
        _check_function(function)

        def add_to_dag(inputs: NodeInputs) -> Node:
            return self._add_stream(function, empty, inputs)

        return NodePrototype(add_to_dag)

    def state(self, function: typing.Callable[P, T]) -> NodePrototype[T]:
        _check_function(function)

        def add_to_dag(inputs: NodeInputs) -> Node:
            return self._add_state(function, inputs)

        return NodePrototype(add_to_dag)

    def sink(self, name: str, input_node: Node[T]) -> Node[None]:
        return self._add_node(
            Node.create(
                function=SinkFunction(name),
                inputs=NodeInputs.create([input_node], {}),
                notifications=0,
            )
        )

    def now(self) -> Node[pd.Timestamp]:
        return self._silent_now_node

    def timer_manager(self) -> Node[TimerManager]:
        function = TimerManagerFunction()
        node = self._add_node(
            Node.create(
                value=function.timer_manager,
                function=function,
                inputs=NO_INPUTS,
                notifications=1,
            )
        )
        self._timer_manager_nodes.append(node)
        return node

    def cutoff(
        self, node: Node[T], comparator: typing.Callable[[T, T], T] = operator.eq
    ) -> Node[T]:
        _check_input(node)
        if not callable(comparator):
            raise TypeError("`comparator` should be callable")
        return self._add_node(
            Node.create(
                function=ValueCutOff(comparator), inputs=NodeInputs.create([node], {})
            )
        )

    def silence(self, node: Node[T]) -> Node[T]:
        _check_input(node)
        return self._add_node(
            Node.create(
                function=SilentUpdate,
                inputs=NodeInputs.create([node], {}),
                value=node.get_value(),
                empty=node.empty,
            )
        )

    def get_sources(self) -> dict[str, Node]:
        return self._sources

    def get_sinks(self) -> dict[str, list[Node]]:
        results = defaultdict(list)
        for node in self._nodes:
            if isinstance(node.function, SinkFunction):
                results[node.function._name].append(node)
        return dict(results)

    def get_next_timer(self) -> pd.Timestamp:
        next_timer = UTC_MAX
        for node in self._timer_manager_nodes:
            next_timer = min(next_timer, node.get_value().get_next_timer())
        return next_timer

    def _add_stream(
        self, function: typing.Callable[[...], T], empty: T, inputs: NodeInputs
    ) -> Node[T]:
        _check_function(function)
        _check_empty(empty)
        return self._add_node(
            Node.create(value=empty, function=function, inputs=inputs, empty=empty)
        )

    def get_cycle_id(self) -> int:
        return self._cycle_id

    def stabilize(self, timestamp: typing.Optional[pd.Timestamp] = None):
        self._cycle_id += 1
        if timestamp is not None:
            self._now_node.function.set_value(timestamp)
            self._now_node._stain()
            self._flush_timers(timestamp)

        for node in self._nodes:
            node._clean(self._cycle_id)

    def _flush_timers(self, now: pd.Timestamp) -> int:
        count = 0
        for node in self._timer_manager_nodes:
            timer_manager: TimerManager = node.get_value()
            if timer_manager._flush(now):
                node._stain()
                count += 1

        return count

    def _add_state(
        self, function: typing.Callable[[...], T], inputs: NodeInputs
    ) -> Node[T]:
        _check_function(function)
        return self._add_node(Node.create(function=function, inputs=inputs))

    def _add_node(self, node: Node) -> Node:
        if len(node.observers) > 0:
            raise ValueError(f"New {Node.__name__} can't have observers")
        if node in self._nodes:
            raise ValueError(f"{Node.__name__} already in dag")
        for input_node in node.inputs.input_nodes():
            if input_node not in self._nodes:
                raise ValueError(f"Input {Node.__name__} not in dag")
            input_node.observers.append(node)
        self._nodes.append(node)
        return node


def _check_empty(empty: T) -> T:
    if not isinstance(empty, collections.abc.Sized):
        raise TypeError("`empty` should implement `__len__`")
    elif len(empty) != 0:
        raise TypeError("`len(empty)` should be 0")
    else:
        return empty


def _check_input(node: Node) -> Node:
    if not isinstance(node, Node):
        raise TypeError(f"Inputs should be `{Node.__name__}`, got {type(node)}")
    else:
        return node


def _check_function(
    function: typing.Callable[typing.ParamSpec, T]
) -> typing.Callable[typing.ParamSpec, T]:
    if not callable(function):
        raise TypeError("`function` should be a callable")
    else:
        return function
