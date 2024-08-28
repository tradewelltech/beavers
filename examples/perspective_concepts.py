# ruff: noqa: E402
# isort: skip_file

from typing import Sequence, Optional

# --8<-- [start:schema]
import pyarrow as pa


KEY_VALUE_SCHEMA = pa.schema(
    [
        pa.field("timestamp", pa.timestamp("ms", "UTC")),
        pa.field("key", pa.string()),
        pa.field("value", pa.string()),
    ]
)
# --8<-- [end:schema]

# --8<-- [start:converter]
import confluent_kafka


def kafka_messages_to_pyarrow(
    messages: Sequence[confluent_kafka.Message],
) -> pa.Table:
    return pa.table(
        [
            [m.timestamp()[1] for m in messages],
            [None if m.key() is None else m.key().decode("utf-8") for m in messages],
            [
                None if m.value() is None else m.value().decode("utf-8")
                for m in messages
            ],
        ],
        schema=KEY_VALUE_SCHEMA,
    )


# --8<-- [end:converter]

# --8<-- [start:dag]
from beavers import Dag
from beavers.perspective_wrapper import PerspectiveTableDefinition


def create_test_dag() -> Dag:
    dag = Dag()
    stream = dag.pa.source_table(
        name="key_value",
        schema=KEY_VALUE_SCHEMA,
    )
    dag.psp.to_perspective(
        stream,
        PerspectiveTableDefinition(
            name="key_value",
            index_column="key",
        ),
    )
    return dag


# --8<-- [end:dag]

# --8<-- [start:run]
from beavers.kafka import KafkaDriver, SourceTopic
from beavers.perspective_wrapper import run_web_application


def run_dashboard(
    topic: str = "key-value",
    port: int = 8082,
    consumer_config: Optional[dict] = None,
):
    if consumer_config is None:
        consumer_config = {"bootstrap.servers": "localhost:9092", "group.id": "beavers"}

    dag = create_test_dag()

    kafka_driver = KafkaDriver.create(
        dag=dag,
        producer_config={},
        consumer_config=consumer_config,
        source_topics={
            "key_value": SourceTopic.from_earliest(topic, kafka_messages_to_pyarrow)
        },
        sink_topics={},
    )

    run_web_application(kafka_driver=kafka_driver, port=port)


# --8<-- [end:run]
