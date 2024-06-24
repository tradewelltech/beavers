"""Module for building and representing dags and nodes."""

from __future__ import annotations

import collections.abc
import dataclasses
import operator
from collections import defaultdict
from functools import cached_property
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Generic,
    Optional,
    ParamSpec,
    Sequence,
    TypeVar,
)

if TYPE_CHECKING:
    try:
        from beavers.pyarrow_wrapper import ArrowDagWrapper
    except ImportError:
        ArrowDagWrapper = None
    try:
        from beavers.pandas_wrapper import PandasWrapper
    except ImportError:
        PandasWrapper = None


import pandas as pd

P = ParamSpec("P")

T = TypeVar("T")
I = TypeVar("I")  # noqa E741
O = TypeVar("O")  # noqa E741

_STATE_EMPTY = object()
_VALUE_EMPTY = object()
_STATE_UNCHANGED = object()

UTC_EPOCH = pd.to_datetime(0, utc=True)
UTC_MAX = pd.Timestamp.max.tz_localize("UTC")


class _SourceStreamFunction(Generic[T]):
    def __init__(self, empty_factory: Callable[[], T], name: str):
        self._empty_factory = empty_factory
        self._name = name
        self._value = empty_factory()

    def set(self, value: T):
        self._value = value

    def __call__(self) -> T:
        result = self._value
        self._value = self._empty_factory()
        return result


class _SinkFunction(Generic[T]):
    def __init__(self, name: str):
        self._name = name
        self._value: Optional[T] = None

    def get(self) -> Optional[T]:
        return self._value

    def __call__(self, value: T) -> None:
        self._value = value
        return None


class _ValueCutOff(Generic[T]):
    def __init__(self, comparator: Callable[[T, T], bool] = operator.eq):
        self._value = _STATE_EMPTY
        self._comparator = comparator

    def __call__(self, value: T) -> T:
        if self._value == _STATE_EMPTY or not self._comparator(value, self._value):
            self._value = value
            return value
        else:
            return _STATE_UNCHANGED


class TimerManager:
    """
    API for setting and accessing timer for a given `Node`.

    - Timers are represented as `pd.Timestamp` with UTC timestamp.
    - A timer of `UTC_MAX` means no timer.
    - Each `TimerManager` is associated to only one `Node`.
    - Each `Node` can only set one upcoming timer.

    This class only stores data.
    It is accessed by the framework to decide when the next timer.
    """

    def __init__(self):
        """Initialize with default values."""
        self._next_timer: pd.Timestamp = UTC_MAX
        self._just_triggered: bool = False

    def has_next_timer(self) -> bool:
        """Return `True` if there is an upcoming timer."""
        return self._next_timer != UTC_MAX

    def just_triggered(self) -> bool:
        """Return `True` if the timer triggered for the current cycle."""
        return self._just_triggered

    def get_next_timer(self) -> pd.Timestamp:
        """Return the next triggered for this Node."""
        return self._next_timer

    def set_next_timer(self, timer: pd.Timestamp):
        """Set the next timer, cancelling any existing upcoming timer."""
        self._next_timer = timer

    def clear_next_timer(self):
        """Cancel the next timer."""
        self._next_timer = UTC_MAX

    def _flush(self, now: pd.Timestamp) -> bool:
        if self._next_timer <= now:
            self.clear_next_timer()
            self._just_triggered = True
            return True
        else:
            self._just_triggered = False
            return False


class _TimerManagerFunction:
    """
    Function for `TimerManager` nodes.

    Used by the framework to trigger timers.
    """

    def __init__(self):
        self.timer_manager: TimerManager = TimerManager()

    def __call__(self) -> TimerManager:
        return self.timer_manager


@dataclasses.dataclass(frozen=True)
class SilentUpdate(Generic[T]):
    """
    Wrap a value to make the update silent.

    A silent updates means the value changed but downstream nodes don't get notified.
    """

    value: T


class _SourceState(Generic[T]):
    def __init__(self, value: T):
        self._value = value

    def set_value(self, value: T):
        self._value = value

    def __call__(self) -> T:
        return self._value


@dataclasses.dataclass(frozen=True)
class _NodeInputs:
    """Internal representation of a node inputs."""

    positional: tuple[Node, ...]
    key_word: dict[str, Node]
    nodes: tuple[Node, ...]

    @staticmethod
    def create(positional: Sequence[Node], key_word: dict[str, Node]) -> "_NodeInputs":
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

        return _NodeInputs(
            positional=tuple(positional),
            key_word=dict(key_word),
            nodes=tuple(all_nodes),
        )

    def input_nodes(self) -> tuple[Node]:
        return self.nodes


_NO_INPUTS = _NodeInputs.create([], {})


@dataclasses.dataclass
class _RuntimeNodeData(Generic[T]):
    """Stores mutable information about a node state."""

    value: Optional[T]
    notifications: int
    cycle_id: int


@dataclasses.dataclass(frozen=True, eq=False)
class Node(Generic[T]):
    """
    Represent an element in a `Dag`.

    Stores all the runtime information about the node. This includes:

     - the underlying processing function
     - the node inputs (upstream nodes)
     - the node observers (downstream nodes)
     - the state of the node (last value, last update cycle id)

    `Node` should only be used to add to the dag.
    You shouldn't use them directly to read values (use sink for this)
    """

    _function: Optional[Callable[[...], T]]
    _inputs: _NodeInputs = dataclasses.field(repr=False)
    _empty_factory: Any
    _observers: list[Node] = dataclasses.field(repr=False)
    _runtime_data: _RuntimeNodeData

    @staticmethod
    def _create(
        value: T = None,
        function: Optional[Callable[[...], T]] = None,
        inputs: _NodeInputs = _NO_INPUTS,
        empty_factory: Any = _STATE_EMPTY,
        notifications: int = 1,
    ) -> Node:
        return Node(
            _function=function,
            _inputs=inputs,
            _empty_factory=empty_factory,
            _runtime_data=_RuntimeNodeData(value, notifications, 0),
            _observers=[],
        )

    def get_value(self) -> T:
        """Return the value of the output for the last update."""
        if self._runtime_data.value is _VALUE_EMPTY:
            return self._empty_factory()
        else:
            return self._runtime_data.value

    def get_cycle_id(self) -> int:
        """Return id of the cycle at which this node last updated."""
        return self._runtime_data.cycle_id

    def set_stream(self, value: T):
        """Set the value of a `_SourceStream`."""
        if not isinstance(self._function, _SourceStreamFunction):
            raise TypeError(f"Only {_SourceStreamFunction.__name__} can be set")
        self._function.set(value)
        self._stain()

    def get_sink_value(self) -> Any:
        """Return the value of a `_SinkFunction`."""
        if not isinstance(self._function, _SinkFunction):
            raise TypeError(f"Only {_SinkFunction.__name__} can be read")
        return self._function.get()

    def _stain(self):
        self._runtime_data.notifications += 1

    def _clean(self, cycle_id: int) -> bool:
        if self._should_recalculate():
            self._recalculate(cycle_id)
            return True
        else:
            if self._is_stream():
                self._runtime_data.value = _VALUE_EMPTY
                self._runtime_data.notifications = 0
            return False

    def _is_stream(self) -> bool:
        return self._empty_factory is not _STATE_EMPTY

    def _is_state(self) -> bool:
        return self._empty_factory is _STATE_EMPTY

    def _should_recalculate(self) -> bool:
        return self._runtime_data.notifications != 0

    def _recalculate(self, cycle_id: int):
        if not self._should_recalculate():
            raise RuntimeError("Calling recalculate on un-notified node")

        positional_values = [node.get_value() for node in self._inputs.positional]
        key_word_values = {
            key: node.get_value() for key, node in self._inputs.key_word.items()
        }
        updated_value = self._function(*positional_values, **key_word_values)
        updated = self._process_updated_value(updated_value)

        if updated:
            self._runtime_data.cycle_id = cycle_id
            self._notify_observers()
        self._runtime_data.notifications = 0

    def _process_updated_value(self, updated_value) -> bool:
        if self._is_state():
            if isinstance(updated_value, SilentUpdate):
                self._runtime_data.value = updated_value.value
                return False
            if updated_value is not _STATE_UNCHANGED:
                self._runtime_data.value = updated_value
                return True
            else:
                return False
        else:
            if isinstance(updated_value, SilentUpdate):
                self._runtime_data.value = updated_value.value
                return False
            else:
                self._runtime_data.value = updated_value
                return len(updated_value) > 0

    def _notify_observers(self):
        for observer in self._observers:
            observer._stain()

    def _get_empty(self) -> T:
        if not self._is_stream():
            raise TypeError(f"Argument should be a stream {Node.__name__}")
        else:
            return self._empty_factory()


@dataclasses.dataclass(frozen=True)
class NodePrototype(Generic[T]):
    """A `Node` that is yet to be added to the `Dag`."""

    _add_to_dag: Callable[[_NodeInputs], Node[T]]

    def map(self, *args: Node, **kwargs: Node) -> Node[T]:
        """Add the prototype to the dag and connect it to the given inputs."""
        return self._add_to_dag(_NodeInputs.create(positional=args, key_word=kwargs))


def _unchanged_callback():
    return _STATE_UNCHANGED


@dataclasses.dataclass
class DagMetrics:
    """Metrics for the execution of a dag."""

    notification_count: int = 0
    updated_node_count: int = 0
    cycle_count: int = 0
    node_count: int = 0


class Dag:
    """Main class used for building and executing a dag."""

    def __init__(self):
        """Create an empty `Dag`."""
        self._nodes: list[Node] = []
        self._sources: dict[str, Node] = {}
        self._sinks: dict[str, Node] = {}
        self._now_node: Node[pd.Timestamp] = self._add_node(
            Node._create(UTC_EPOCH, _SourceState(UTC_EPOCH))
        )
        self._silent_now_node: Node[pd.Timestamp] = self.silence(self._now_node)
        self._timer_manager_nodes: list[Node[TimerManager]] = []
        self._cycle_id: int = 0
        self._metrics = DagMetrics(node_count=len(self._nodes))

    def const(self, value: T) -> Node[T]:
        """
        Add a `Node` of constant value to the `Dag`.

        Parameters
        ----------
        value:
            The value of the constant node. Should be Immutable.

        """
        return self._add_node(
            Node._create(
                function=_unchanged_callback,
                inputs=_NO_INPUTS,
                value=value,
                notifications=0,
            )
        )

    def source_stream(
        self,
        empty: Optional[T] = None,
        empty_factory: Optional[Callable[[], T]] = None,
        name: Optional[str] = None,
    ) -> Node[T]:
        """
        Add a source stream `Node`.

        Parameters
        ----------
        empty:
            The value to which the stream reset to when there are no update.
             Must implement `__len__` and be empty
        empty_factory:
            A provider for the empty value
             A callable returning an object that implements `__len__` and is empty
        name:
            The name of the source

        """
        empty_factory = _check_empty(empty, empty_factory)
        existing = self._sources.get(name) if name else None
        if existing is not None:
            if existing._empty_factory != empty_factory:
                raise ValueError(f"Duplicate source: {name}")
            else:
                return existing
        else:
            node = self._add_stream(
                function=_SourceStreamFunction(empty_factory, name),
                empty_factory=empty_factory,
                inputs=_NO_INPUTS,
            )
            if name:
                self._sources[name] = node
            return node

    def stream(
        self,
        function: Callable[P, T],
        empty: Optional[T] = None,
        empty_factory: Optional[Callable[[], T]] = None,
    ) -> NodePrototype:
        """
        Add a stream `NodePrototype`.

        Stream nodes are reset to their empty value after each cycle.
        Therefore, the user must provide an `empty` value or an `empty_factory`

        The default is to use `list` as the `empty_factory`.

        Parameters
        ----------
        function:
            The processing function of the `Node`.
        empty:
            The value to which the stream reset to when there are no update.
             Must implement `__len__` and be empty
        empty_factory:
            A provider for the empty value
             A callable returning an object that implements `__len__` and is empty

        """
        empty_factory = _check_empty(empty, empty_factory)
        _check_function(function)

        def add_to_dag(inputs: _NodeInputs) -> Node:
            return self._add_stream(function, empty_factory, inputs)

        return NodePrototype(add_to_dag)

    def state(self, function: Callable[P, T]) -> NodePrototype[T]:
        """
        Add a state `NodePrototype`.

        Parameters
        ----------
        function:
            The processing function of the `Node`

        """
        _check_function(function)

        def add_to_dag(inputs: _NodeInputs) -> Node:
            return self._add_state(function, inputs)

        return NodePrototype(add_to_dag)

    def sink(self, name: str, input_node: Node[T]) -> Node[None]:
        """
        Add a sink.

        Parameters
        ----------
        name:
            The name the sink
        input_node:
            The input node to be connected to the sink

        """
        return self._add_node(
            Node._create(
                function=_SinkFunction(name),
                inputs=_NodeInputs.create([input_node], {}),
                notifications=0,
            )
        )

    def now(self) -> Node[pd.Timestamp]:
        """
        Return a `Node` whose value is the current time.

        The now `Node`:

        - won't trigger update every time the time change.
        - is unique for the `Dag`
        """
        return self._silent_now_node

    def timer_manager(self) -> Node[TimerManager]:
        """
        Create a new `TimerManager` to be connected to a `Node`.

        Any node that must wake up on a timer should be connected
         to its own `TimerManager`.

        """
        function = _TimerManagerFunction()
        node = self._add_node(
            Node._create(
                value=function.timer_manager,
                function=function,
                inputs=_NO_INPUTS,
                notifications=1,
            )
        )
        self._timer_manager_nodes.append(node)
        return node

    def cutoff(
        self, node: Node[T], comparator: Callable[[T, T], bool] = operator.eq
    ) -> Node[T]:
        """
        Tame the update from a `Node`, given a "cutoff" policy.

        Parameters
        ----------
        node:
            The node whose value needs cutoff
        comparator:
            The policy for the cutoff.
             When the `comparator` returns True, no updates get propagated.

        """
        _check_input(node)
        if not callable(comparator):
            raise TypeError("`comparator` should be callable")
        return self._add_node(
            Node._create(
                function=_ValueCutOff(comparator), inputs=_NodeInputs.create([node], {})
            )
        )

    def silence(self, node: Node[T]) -> Node[T]:
        """Silence a node, making it's update not trigger downstream."""
        _check_input(node)
        return self._add_node(
            Node._create(
                function=SilentUpdate,
                inputs=_NodeInputs.create([node], {}),
                value=node.get_value(),
                empty_factory=node._empty_factory,
            )
        )

    def prune(self) -> list[Node]:
        """Remove any parts of the dag that are not connected to a sink."""
        to_remove = []
        for node in self._nodes[::-1]:
            if (
                not isinstance(node._function, _SinkFunction)
                and node is not self._now_node
                and node is not self._silent_now_node
            ):
                observers = [
                    observer
                    for observer in node._observers
                    if observer not in to_remove
                ]
                if len(observers) == 0:
                    to_remove.append(node)
                else:
                    node._observers.clear()
                    node._observers.extend(observers)

        if to_remove:
            self._nodes = [node for node in self._nodes if node not in to_remove]
            self._sources = {
                name: node
                for name, node in self._sources.items()
                if node not in to_remove
            }
            self._timer_manager_nodes = [
                node for node in self._timer_manager_nodes if node not in to_remove
            ]
        return to_remove

    def get_sources(self) -> dict[str, Node]:
        """Return the source `Node`s."""
        return self._sources

    def get_sinks(self) -> dict[str, list[Node]]:
        """Return the sink `Node`s."""
        results = defaultdict(list)
        for node in self._nodes:
            if isinstance(node._function, _SinkFunction):
                results[node._function._name].append(node)
        return dict(results)

    def get_next_timer(self) -> pd.Timestamp:
        """Return the nest timer that needs to be triggered."""
        next_timer = UTC_MAX
        for node in self._timer_manager_nodes:
            next_timer = min(next_timer, node.get_value().get_next_timer())
        return next_timer

    def get_cycle_id(self) -> int:
        """Return the last cycle id."""
        return self._cycle_id

    def execute(self, timestamp: Optional[pd.Timestamp] = None):
        """Run the dag for a given timestamp."""
        self._cycle_id += 1
        if timestamp is not None:
            self._now_node._function.set_value(timestamp)
            self._now_node._stain()
            self._flush_timers(timestamp)

        for node in self._nodes:
            self._metrics.notification_count += node._runtime_data.notifications
            cleaned = node._clean(self._cycle_id)
            if cleaned:
                self._metrics.updated_node_count += 1
        self._metrics.cycle_count += 1
        self._metrics.node_count = len(self._nodes)

    def flush_metrics(self) -> DagMetrics:
        results = self._metrics
        self._metrics = DagMetrics(node_count=len(self._nodes))
        return results

    @cached_property
    def pa(self) -> "ArrowDagWrapper":
        """Returns the ArrowDagWrapper."""
        # Import dynamically because of circular dependency
        from beavers.pyarrow_wrapper import ArrowDagWrapper

        return ArrowDagWrapper(self)

    @cached_property
    def pd(self) -> "PandasWrapper":
        """Returns the PandasWrapper."""
        # Import dynamically because of circular dependency
        from beavers.pandas_wrapper import PandasWrapper

        return PandasWrapper(self)

    def _add_stream(
        self,
        function: Callable[[...], T],
        empty_factory: Callable[[], T],
        inputs: _NodeInputs,
    ) -> Node[T]:
        _check_function(function)
        return self._add_node(
            Node._create(
                value=empty_factory(),
                function=function,
                inputs=inputs,
                empty_factory=empty_factory,
            )
        )

    def _flush_timers(self, now: pd.Timestamp) -> int:
        count = 0
        for node in self._timer_manager_nodes:
            timer_manager: TimerManager = node.get_value()
            if timer_manager._flush(now):
                node._stain()
                count += 1

        return count

    def _add_state(self, function: Callable[[...], T], inputs: _NodeInputs) -> Node[T]:
        _check_function(function)
        return self._add_node(Node._create(function=function, inputs=inputs))

    def _add_node(self, node: Node) -> Node:
        if len(node._observers) > 0:
            raise ValueError(f"New {Node.__name__} can't have observers")
        if node in self._nodes:
            raise ValueError(f"{Node.__name__} already in dag")
        for input_node in node._inputs.input_nodes():
            if input_node not in self._nodes:
                raise ValueError(f"Input {Node.__name__} not in dag")
            input_node._observers.append(node)
        self._nodes.append(node)
        return node


def _check_empty(
    empty: Optional[T], empty_factory: Optional[Callable[[], T]]
) -> Callable[[], T]:
    if empty is not None and empty_factory is not None:
        raise ValueError(f"Can't provide both {empty=} and {empty_factory=}")
    elif empty is None and empty_factory is None:
        return list
    elif empty is not None:
        if not isinstance(empty, collections.abc.Sized):
            raise TypeError("`empty` should implement `__len__`")
        elif len(empty) != 0:
            raise TypeError("`len(empty)` should be 0")
        else:
            return lambda: empty
    else:
        assert empty is None
        if not callable(empty_factory):
            raise TypeError(f"{empty_factory=} should be a callable")

        empty_value = empty_factory()
        if empty_value is None:
            raise TypeError(f"{empty_factory=} should not return None")
        elif not isinstance(empty_value, collections.abc.Sized):
            raise TypeError(f"{empty_value=} should implement `__len__`")
        elif len(empty_value) != 0:
            raise TypeError("`len(empty)` should be 0")
        else:
            return empty_factory


def _check_input(node: Node) -> Node:
    if not isinstance(node, Node):
        raise TypeError(f"Inputs should be `{Node.__name__}`, got {type(node)}")
    else:
        return node


def _check_function(function: Callable[ParamSpec, T]) -> Callable[ParamSpec, T]:
    if not callable(function):
        raise TypeError("`function` should be a callable")
    else:
        return function
