# isort: skip_file

# --8<-- [start:source_stream]
from beavers import Dag

dag = Dag()

source_stream = dag.source_stream()

source_stream.set_stream([1, 2, 3])
dag.execute()
assert source_stream.get_value() == [1, 2, 3]
# --8<-- [end:source_stream]


# --8<-- [start:source_stream_again]
dag.execute()
assert source_stream.get_value() == []
# --8<-- [end:source_stream_again]

# --8<-- [start:source_stream_name]
my_source_stream = dag.source_stream(name="my_source")
dag.get_sources()["my_source"].set_stream([4, 5, 6])
dag.execute()
assert my_source_stream.get_value() == [4, 5, 6]
# --8<-- [end:source_stream_name]

# --8<-- [start:source_stream_empty]
dict_source_stream = dag.source_stream(empty_factory=dict)
dict_source_stream.set_stream({"hello": "world"})
dag.execute()
assert dict_source_stream.get_value() == {"hello": "world"}
dag.execute()
assert dict_source_stream.get_value() == {}
# --8<-- [end:source_stream_empty]


# --8<-- [start:stream_node]
def multiply_by_2(values: list[int]) -> list[int]:
    return [v * 2 for v in values]


stream_node = dag.stream(multiply_by_2).map(source_stream)

source_stream.set_stream([1, 2, 3])
dag.execute()
assert stream_node.get_value() == [2, 4, 6]
# --8<-- [end:stream_node]


# --8<-- [start:stream_node_again]
dag.execute()
assert stream_node.get_value() == []
# --8<-- [end:stream_node_again]


# --8<-- [start:stream_node_empty]
set_stream_node = dag.stream(set, empty_factory=set).map(source_stream)
source_stream.set_stream([1, 2, 3, 1, 2, 3])
dag.execute()
assert set_stream_node.get_value() == {1, 2, 3}
dag.execute()
assert set_stream_node.get_value() == set()
# --8<-- [end:stream_node_empty]


# --8<-- [start:stream_node_lambda]
lambda_stream_node = dag.stream(lambda x: x[:-1]).map(source_stream)
source_stream.set_stream([1, 2, 3])
dag.execute()
assert lambda_stream_node.get_value() == [1, 2]
# --8<-- [end:stream_node_lambda]


# --8<-- [start:stream_node_callable]
class MultiplyBy:
    def __init__(self, by: int):
        self.by = by

    def __call__(self, values: list[int]) -> list[int]:
        return [v * self.by for v in values]


callable_stream_node = dag.stream(MultiplyBy(3)).map(source_stream)
source_stream.set_stream([1, 2, 3])
dag.execute()
assert callable_stream_node.get_value() == [3, 6, 9]
# --8<-- [end:stream_node_callable]


# --8<-- [start:state_node]
class Accumulator:
    def __init__(self):
        self._count = 0

    def __call__(self, values: list[int]) -> int:
        self._count += sum(values)
        return self._count


state_node = dag.state(Accumulator()).map(source_stream)
source_stream.set_stream([1, 2, 3])
dag.execute()
assert state_node.get_value() == 6
dag.execute()
assert state_node.get_value() == 6
# --8<-- [end:state_node]


# --8<-- [start:const_node]
const_node = dag.const(2)
assert const_node.get_value() == 2
# --8<-- [end:const_node]


# --8<-- [start:map_positional]
to_append = dag.const([3])
positional_stream = dag.stream(lambda x, y: x + y).map(source_stream, to_append)
source_stream.set_stream([1, 2])
dag.execute()
assert positional_stream.get_value() == [1, 2, 3]
# --8<-- [end:map_positional]


# --8<-- [start:map_key_word]
key_word = dag.stream(lambda x, y: x + y).map(x=source_stream, y=to_append)
# --8<-- [end:map_key_word]
