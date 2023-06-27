# --8<-- [start:source_stream]
from beavers import Dag

dag = Dag()

source_stream = dag.source_stream([], "my_source")
source_stream.set_stream([1, 2, 3])

dag.execute()
assert source_stream.get_value() == [1, 2, 3]
# --8<-- [end:source_stream]


# --8<-- [start:source_stream_again]
dag.execute()
assert source_stream.get_value() == []
# --8<-- [end:source_stream_again]


stream_node = dag.stream(lambda x: x, []).map(source_stream)
state_node = dag.state(lambda x: x).map(source_stream)
