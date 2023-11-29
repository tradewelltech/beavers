from beavers.pyarrow_kafka import JsonDeserializer, JsonSerializer
from tests.test_kafka import mock_kafka_message
from tests.test_util import TEST_TABLE


def test_json_deserializer_empty():
    deserializer = JsonDeserializer(TEST_TABLE.schema)
    assert deserializer([]) == TEST_TABLE.schema.empty_table()


def test_end_to_end():
    deserializer = JsonDeserializer(TEST_TABLE.schema)
    serializer = JsonSerializer("topic-1")
    out_messages = serializer(TEST_TABLE)
    in_messages = [
        mock_kafka_message(topic=m.topic, value=m.value) for m in out_messages
    ]
    assert deserializer(in_messages) == TEST_TABLE
