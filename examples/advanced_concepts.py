# --8<-- [start:now_node]
import pandas as pd

from beavers import Dag


def get_delay(timestamps: list[pd.Timestamp], now: pd.Timestamp) -> list[pd.Timedelta]:
    return [now - timestamp for timestamp in timestamps]


dag = Dag()
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
assert both.get_value() == []  # No update because source_1 is silent
assert both.get_cycle_id() == dag.get_cycle_id() - 1

# --8<-- [end:silence]
