# ruff: noqa: E402
# isort: skip_file
import pandas as pd

from beavers import Dag

dag = Dag()

# --8<-- [start:propagate_any]
source_1 = dag.source_stream()
source_2 = dag.source_stream()
node = dag.stream(lambda x, y: x + y).map(source_1, source_2)

source_1.set_stream([1, 2, 3])
dag.execute()
assert node.get_value() == [1, 2, 3]  # source_1 updated

source_2.set_stream([4, 5, 6])
dag.execute()
assert node.get_value() == [4, 5, 6]  # source_2 updated

dag.execute()
assert node.get_value() == []  # no updates, reset to empty
# --8<-- [end:propagate_any]

# --8<-- [start:propagate_cycle_id]
source_1.set_stream([1, 2, 3])
dag.execute()
assert node.get_value() == [1, 2, 3]
assert node.get_cycle_id() == dag.get_cycle_id()

dag.execute()
assert node.get_value() == []
assert node.get_cycle_id() == dag.get_cycle_id() - 1
# --8<-- [end:propagate_cycle_id]


# --8<-- [start:propagate_both]
source_1.set_stream([1, 2, 3])
source_2.set_stream([4, 5, 6])
dag.execute()
assert node.get_value() == [1, 2, 3, 4, 5, 6]
assert node.get_cycle_id() == dag.get_cycle_id()
# --8<-- [end:propagate_both]


# --8<-- [start:propagate_empty]
def even_only(values: list[int]) -> list[int]:
    return [v for v in values if (v % 2) == 0]


even = dag.stream(even_only).map(source_1)

source_1.set_stream([1, 2, 3])
dag.execute()
assert even.get_value() == [2]
assert even.get_cycle_id() == dag.get_cycle_id()

source_1.set_stream([1, 3])
dag.execute()
assert even.get_value() == []
assert even.get_cycle_id() == dag.get_cycle_id() - 1
# --8<-- [end:propagate_empty]


# --8<-- [start:now_node]
def get_delay(timestamps: list[pd.Timestamp], now: pd.Timestamp) -> list[pd.Timedelta]:
    return [now - timestamp for timestamp in timestamps]


timestamp_stream = dag.source_stream()
delay = dag.stream(get_delay).map(timestamp_stream, dag.now())

timestamp_stream.set_stream(
    [
        pd.to_datetime("2022-01-01", utc=True),
        pd.to_datetime("2022-01-02", utc=True),
        pd.to_datetime("2022-01-03", utc=True),
    ]
)
dag.execute(timestamp=pd.to_datetime("2022-01-04", utc=True))
assert delay.get_value() == [
    pd.to_timedelta("3d"),
    pd.to_timedelta("2d"),
    pd.to_timedelta("1d"),
]

# --8<-- [end:now_node]

# --8<-- [start:timer_manager]
from beavers import TimerManager


def get_year(now: pd.Timestamp, timer_manager: TimerManager):
    if not timer_manager.has_next_timer():
        timer_manager.set_next_timer(
            pd.Timestamp(year=now.year + 1, day=1, month=1, tzinfo=now.tzinfo)
        )

    return now.year


year = dag.state(get_year).map(dag.now(), dag.timer_manager())

dag.execute(pd.to_datetime("2022-01-01", utc=True))
assert year.get_value() == 2022
assert year.get_cycle_id() == dag.get_cycle_id()

dag.execute(pd.to_datetime("2022-01-02", utc=True))
assert year.get_value() == 2022
assert year.get_cycle_id() == dag.get_cycle_id() - 1

dag.execute(pd.to_datetime("2023-01-02", utc=True))
assert year.get_value() == 2023
assert year.get_cycle_id() == dag.get_cycle_id()
# --8<-- [end:timer_manager]


# --8<-- [start:silence]
source_1 = dag.source_stream()
source_1_silence = dag.silence(source_1)
source_2 = dag.source_stream()

both = dag.stream(lambda x, y: x + y).map(source_1_silence, source_2)

source_1.set_stream([1, 2, 3])
source_2.set_stream([4, 5, 6])
dag.execute()
assert both.get_value() == [1, 2, 3, 4, 5, 6]
assert both.get_cycle_id() == dag.get_cycle_id()

source_1.set_stream([1, 2, 3])
dag.execute()
assert both.get_value() == []
assert (
    both.get_cycle_id() == dag.get_cycle_id() - 1
)  # No update because source_1 is silent

# --8<-- [end:silence]


# --8<-- [start:cutoff]
class GetMax:
    def __init__(self):
        self._max = 0.0

    def __call__(self, values: list[float]) -> float:
        self._max = max(self._max, *values)
        return self._max


source = dag.source_stream()
get_max = dag.state(GetMax()).map(source)
get_max_cutoff = dag.cutoff(get_max)

source.set_stream([1.0, 2.0])
dag.execute()
assert get_max.get_value() == 2.0
assert get_max.get_cycle_id() == dag.get_cycle_id()
assert get_max_cutoff.get_cycle_id() == dag.get_cycle_id()

source.set_stream([1.0])
dag.execute()
assert get_max.get_value() == 2.0
assert get_max.get_cycle_id() == dag.get_cycle_id()
assert get_max_cutoff.get_cycle_id() == dag.get_cycle_id() - 1

source.set_stream([3.0])
dag.execute()
assert get_max.get_value() == 3.0
assert get_max.get_cycle_id() == dag.get_cycle_id()
assert get_max_cutoff.get_cycle_id() == dag.get_cycle_id()
# --8<-- [end:cutoff]

# --8<-- [start:cutoff_custom]
get_max_cutoff_custom = dag.cutoff(get_max, lambda x, y: abs(x - y) < 0.1)

source.set_stream([4.0])
dag.execute()
assert get_max.get_value() == 4.0
assert get_max.get_cycle_id() == dag.get_cycle_id()
assert get_max_cutoff_custom.get_cycle_id() == dag.get_cycle_id()


source.set_stream([4.05])
dag.execute()
assert get_max.get_value() == 4.05
assert get_max.get_cycle_id() == dag.get_cycle_id()
assert get_max_cutoff_custom.get_value() == 4.0
assert get_max_cutoff_custom.get_cycle_id() == dag.get_cycle_id() - 1


source.set_stream([4.11])
dag.execute()
assert get_max.get_value() == 4.11
assert get_max.get_cycle_id() == dag.get_cycle_id()
assert get_max_cutoff_custom.get_value() == 4.11
assert get_max_cutoff_custom.get_cycle_id() == dag.get_cycle_id()
# --8<-- [end:cutoff_custom]
