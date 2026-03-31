"""Kafka sources and sinks for PyArrow tables."""

import dataclasses
import io
import json

import confluent_kafka
import pyarrow as pa
import pyarrow.json

from beavers.kafka import (
    KafkaMessageDeserializer,
    KafkaMessageSerializer,
    KafkaProducerMessage,
)


@dataclasses.dataclass(frozen=True)
class JsonDeserializer(KafkaMessageDeserializer[pa.Table]):
    """Deserialize Kafka messages from JSON into a PyArrow table."""

    schema: pa.Schema

    def __call__(self, messages: confluent_kafka.Message) -> pa.Table:
        """Deserialize messages into a PyArrow table."""
        if messages:
            with io.BytesIO() as buffer:
                for message in messages:
                    buffer.write(message.value())
                    buffer.write(b"\n")
                buffer.seek(0)
                return pyarrow.json.read_json(
                    buffer,
                    parse_options=pyarrow.json.ParseOptions(
                        explicit_schema=self.schema
                    ),
                )
        else:
            return self.schema.empty_table()


@dataclasses.dataclass(frozen=True)
class JsonSerializer(KafkaMessageSerializer[pa.Table]):
    """Serialize a PyArrow table into JSON Kafka messages."""

    topic: str

    def __call__(self, table: pa.Table):
        """Serialize a PyArrow table into Kafka producer messages."""
        return [
            KafkaProducerMessage(
                self.topic,
                key=None,
                value=json.dumps(message, default=str).encode("utf-8"),
            )
            for message in table.to_pylist()
        ]
