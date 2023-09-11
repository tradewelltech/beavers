# ruff: noqa: E402
# isort: skip_file


import confluent_kafka

# --8<-- [start:dag]
from beavers import Dag


class CountWords:
    state = {}

    def __call__(self, new_words: list[str]) -> dict[str, int]:
        for word in new_words:
            self.state[word] = self.state.get(word, 0) + 1
        return self.state


def update_stream(
    state: dict[str, int], updated_words: list[str]
) -> list[tuple[str, int]]:
    return [(word, state[word]) for word in set(updated_words)]


dag = Dag()
word_source = dag.source_stream(name="words")
count_state = dag.state(CountWords()).map(word_source)
count_stream = dag.stream(update_stream, []).map(count_state, word_source)
dag.sink("counts", count_stream)
# --8<-- [end:dag]


# --8<-- [start:deserializer]
def deserialize_messages(messages: list[confluent_kafka.Message]) -> list[str]:
    return [message.value() for message in messages]


# --8<-- [end:deserializer]

# --8<-- [start:kafka_source]
from beavers.kafka import SourceTopic, KafkaDriver

source_topic = SourceTopic.from_latest("words", deserialize_messages)
# --8<-- [end:kafka_source]


# --8<-- [start:serializer]
from beavers.kafka import KafkaProducerMessage


def serialize_counts(values: list[tuple[str, int]]) -> list[KafkaProducerMessage]:
    return [
        KafkaProducerMessage(
            topic="counts",
            key=word,
            value=str(count),
        )
        for word, count in values
    ]


# --8<-- [end:serializer]


# --8<-- [start:kafka_driver]
kafka_driver = KafkaDriver.create(
    dag=dag,
    consumer_config={
        "enable.partition.eof": True,
        "group.id": "beavers",
        "bootstrap.servers": "localhost:9092",
    },
    producer_config={"bootstrap.servers": "localhost:9092"},
    source_topics={"words": source_topic},
    sink_topics={"counts": serialize_counts},
)
while True:
    kafka_driver.run_cycle()
# --8<-- [end:kafka_driver]


# Note: you can test it with
# kafka-console-producer --topic words --bootstrap-server=localhost:9092
# And:
# kafka-console-consumer
#   --topic=counts \
#   --bootstrap-server=localhost:9092 \
#   --property print.key=true
