import functools
import json
import logging
from operator import itemgetter
from typing import Any, Callable, Sequence

import click
import confluent_kafka
import pandas as pd

from beavers import Dag
from beavers.kafka import KafkaDriver, KafkaProducerMessage, SourceTopic


def create_test_dag() -> "Dag":
    dag = Dag()
    left_stream = dag.source_stream(name="left")
    right_stream = dag.source_stream(name="right")
    both_stream = dag.stream(
        lambda left, right: sorted(left + right, key=itemgetter("timestamp"))
    ).map(left_stream, right_stream)
    dag.sink("both", both_stream)
    return dag


def kafka_messages_to_json(
    messages: Sequence[confluent_kafka.Message],
) -> list[dict[str, Any]]:
    return [
        {
            "topic": message.topic(),
            "partition": message.partition(),
            "offset": message.offset(),
            "timestamp": str(
                pd.to_datetime(message.timestamp()[1], unit="ms", utc=True)
            ),
            "key": message.key().encode("utf-8") if message.key() else None,
            "value": message.value().decode("utf-8"),
        }
        for message in messages
    ]


def kafka_message_serializer(
    payloads: list[dict[str, Any]], topic: str
) -> list[KafkaProducerMessage]:
    return [
        KafkaProducerMessage(topic, key=None, value=json.dumps(payload))
        for payload in payloads
    ]


SOURCE_TOPIC_CREATORS: dict[str, Callable[[str], SourceTopic]] = {
    "latest": functools.partial(
        SourceTopic.from_latest, message_deserializer=kafka_messages_to_json
    ),
    "earliest": functools.partial(
        SourceTopic.from_earliest, message_deserializer=kafka_messages_to_json
    ),
    "15min": functools.partial(
        SourceTopic.from_relative_time,
        message_deserializer=kafka_messages_to_json,
        relative_time=pd.to_timedelta("15min"),
    ),
    "start-of-day": functools.partial(
        SourceTopic.from_start_of_day,
        message_deserializer=kafka_messages_to_json,
        start_of_day_time=pd.to_timedelta("00:00:00"),
        start_of_day_timezone="UTC",
    ),
    "absolute-time": functools.partial(
        SourceTopic.from_absolute_time,
        message_deserializer=kafka_messages_to_json,
        absolute_time=pd.Timestamp.utcnow().normalize(),
    ),
    "committed": functools.partial(
        SourceTopic.from_committed,
        message_deserializer=kafka_messages_to_json,
    ),
}


@click.command()
@click.option("--left-topic", type=click.STRING, default="left")
@click.option(
    "--left-offset", type=click.Choice(SOURCE_TOPIC_CREATORS.keys()), default="earliest"
)
@click.option("--right-topic", type=click.STRING, default="right")
@click.option(
    "--right-offset",
    type=click.Choice(SOURCE_TOPIC_CREATORS.keys()),
    default="earliest",
)
@click.option("--both-topic", type=click.STRING, default="both")
@click.option(
    "--consumer-config",
    type=json.loads,
    default='{"bootstrap.servers": "localhost:9092", "group.id": "beavers"}',
)
@click.option(
    "--producer-config",
    type=json.loads,
    default='{"bootstrap.servers": "localhost:9092"}',
)
@click.option("--batch-size", type=click.INT, default="2")
def kafka_test_bench(
    left_topic: str,
    left_offset: str,
    right_topic: str,
    right_offset: str,
    both_topic: str,
    consumer_config: dict,
    producer_config: dict,
    batch_size: int,
):
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.DEBUG,
    )

    dag = create_test_dag()

    driver = KafkaDriver.create(
        dag=dag,
        producer_config=producer_config,
        consumer_config=consumer_config,
        source_topics={
            "left": SOURCE_TOPIC_CREATORS[left_offset](left_topic),
            "right": SOURCE_TOPIC_CREATORS[right_offset](right_topic),
        },
        sink_topics={
            "both": functools.partial(kafka_message_serializer, topic=both_topic)
        },
        batch_size=batch_size,
    )
    while True:
        driver.run_cycle()


if __name__ == "__main__":
    kafka_test_bench()
