# isort: skip_file
# ruff: noqa: E402

import beavers

# --8<-- [start:simple_dag]
dag = beavers.Dag()
my_source = dag.source_stream(name="my_source")
my_sink = dag.sink("my_sink", my_source)
# --8<-- [end:simple_dag]

# --8<-- [start:simple_data_class]
import dataclasses
import pandas as pd


@dataclasses.dataclass(frozen=True)
class Message:
    timestamp: pd.Timestamp
    message: str


# --8<-- [end:simple_data_class]

# --8<-- [start:manual_replay]
my_source.set_stream(
    [
        Message(pd.Timestamp("2023-01-01T00:00:00Z"), "hello"),
        Message(pd.Timestamp("2023-01-01T00:00:30Z"), "How are you"),
    ]
)
dag.execute(pd.Timestamp("2023-01-01T00:01:00Z"))
assert my_sink.get_sink_value() == [
    Message(pd.Timestamp("2023-01-01T00:00:00Z"), "hello"),
    Message(pd.Timestamp("2023-01-01T00:00:30Z"), "How are you"),
]
# --8<-- [end:manual_replay]


# --8<-- [start:data_source]
import beavers.replay


@dataclasses.dataclass(frozen=True)
class MessageDataSource:
    messages: list[Message]

    def read_to(self, timestamp: pd.Timestamp) -> list[Message]:
        results = []
        while self.messages and self.messages[0].timestamp <= timestamp:
            results.append(self.messages.pop(0))
        return results

    def get_next(self) -> pd.Timestamp:
        if self.messages:
            return self.messages[0].timestamp
        else:
            return beavers.replay.UTC_MAX


# --8<-- [end:data_source]


# --8<-- [start:replay_context]
replay_context = beavers.replay.ReplayContext(
    start=pd.to_datetime("2023-01-01T00:00:00Z"),
    end=pd.to_datetime("2023-01-02T00:00:00Z"),
    frequency=pd.to_timedelta("1h"),
)
# --8<-- [end:replay_context]


# --8<-- [start:data_source_provider]
@dataclasses.dataclass(frozen=True)
class CsvDataSourceProvider:
    file_name: str

    def __call__(
        self, replay_context: beavers.replay.ReplayContext
    ) -> beavers.replay.DataSource[list[Message]]:
        df = pd.read_csv(self.file_name, parse_dates=["timestamp"])
        messages = [Message(*row) for _, row in df.iterrows()]
        messages.sort(key=lambda x: x.timestamp)
        return MessageDataSource(messages)


# --8<-- [end:data_source_provider]


# --8<-- [start:data_sink]
@dataclasses.dataclass(frozen=True)
class CsvDataSink:
    destination: str
    data: list[pd.DataFrame] = dataclasses.field(default_factory=list)

    def append(self, timestamp: pd.Timestamp, data: pd.DataFrame):
        self.data.append(data)

    def close(self):
        pd.concat(self.data).to_csv(self.destination)


# --8<-- [end:data_sink]


# --8<-- [start:data_sink_provider]
class CsvDataSinkProvider:
    destination: str

    def __call__(self, replay_context: beavers.replay.ReplayContext) -> CsvDataSink:
        return CsvDataSink(self.destination)


# --8<-- [end:data_sink_provider]

# --8<-- [start:replay_driver]
replay_driver = beavers.replay.ReplayDriver.create(
    dag=dag,
    replay_context=replay_context,
    data_source_providers={"my_source": CsvDataSourceProvider()},
    data_sink_providers={"my_sink": CsvDataSinkProvider()},
)
replay_driver.run()
# --8<-- [end:replay_driver]

# This is just to print the csv file:
file = "data.csv"
df = pd.DataFrame(
    {
        "timestamp": [
            pd.Timestamp("2023-01-01T01:00:00Z"),
            pd.Timestamp("2023-01-01T01:01:00Z"),
        ],
        "message": ["Hello", "How are you"],
    }
)
df.to_csv("data.csv", index=False)

df_after = pd.read_csv("data.csv", parse_dates=["timestamp"])
pd.testing.assert_frame_equal(df, df_after)

messages = [Message(*row) for _, row in df_after.iterrows()]
print(messages)
