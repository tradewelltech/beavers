"""
Unit tests for tradewell.beavers.kafka
"""
import dataclasses
import io
import logging
import queue
from typing import AnyStr, Callable, Optional, Tuple, Union

import confluent_kafka
import mock
import pandas as pd
import pytest
from confluent_kafka import Message, TopicPartition
from confluent_kafka.admin import ClusterMetadata, PartitionMetadata, TopicMetadata

from beavers.engine import UTC_EPOCH, UTC_MAX, Dag
from beavers.kafka import (
    ConsumerMetrics,
    KafkaDriver,
    KafkaProducerMessage,
    OffsetPolicy,
    ProducerMetrics,
    SourceTopic,
    _ConsumerManager,
    _get_message_ns,
    _get_previous_start_of_day,
    _poll_all,
    _ProducerManager,
    _resolve_topic_offsets,
    _resolve_topics_offsets,
    _RuntimeSinkTopic,
    _RuntimeSourceTopic,
)
from beavers.kafka import logger as beavers_logger
from tests.test_util import SetATimer, TimerEntry, create_word_count_dag


def mock_kafka_message(
    topic: str = "",
    partition: int = 0,
    timestamp: Union[None, int, pd.Timestamp] = None,
    value: bytes = b"",
    error: Optional[confluent_kafka.KafkaError] = None,
    offset: int = 0,
    key: bytes = b"",
) -> Message:
    msg = mock.Mock()
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    if timestamp is None:
        msg.timestamp.return_value = (confluent_kafka.TIMESTAMP_NOT_AVAILABLE, None)
    else:
        msg.timestamp.return_value = (
            confluent_kafka.TIMESTAMP_LOG_APPEND_TIME,
            timestamp if isinstance(timestamp, int) else timestamp.value // 1_000_000,
        )
    msg.value.return_value = value
    msg.error.return_value = error
    msg.offset.return_value = offset
    msg.key.return_value = key
    return msg


class MockConsumer:
    def __init__(self):
        self._queue = queue.Queue()
        self._paused: list[TopicPartition] = []
        self._topics: dict[str, TopicMetadata] = {}
        self._offsets_for_time: dict[Tuple[TopicPartition, int], TopicPartition] = {}
        self._watermark_offsets: dict[TopicPartition, tuple[int, int]] = {}
        self._committed: dict[TopicPartition:int] = {}

    def append(self, message: Message):
        self._queue.put(message)

    def extend(self, messages: list[Message]):
        for message in messages:
            self.append(message)

    def poll(self, timeout: Optional[float]) -> Optional[Message]:
        try:
            return self._queue.get(block=True, timeout=timeout)
        except queue.Empty:
            return None

    def pause(self, topic_partitions: list[TopicPartition]):
        for topic_partition in topic_partitions:
            assert topic_partition not in self._paused
        self._paused.extend(topic_partitions)

    def resume(self, topic_partitions: list[TopicPartition]):
        for topic_partition in topic_partitions:
            assert topic_partition in self._paused
            self._paused.remove(topic_partition)

    def list_topics(self, topic_name, timeout: Optional[float]) -> ClusterMetadata:
        results = ClusterMetadata()
        try:
            results.topics[topic_name] = self._topics[topic_name]
        except KeyError:
            pass
        return results

    def offsets_for_times(
        self, partitions: list[TopicPartition], timeout: Optional[float] = None
    ) -> list[TopicPartition]:
        return [self._offsets_for_time[(p, p.offset)] for p in partitions]

    def mock_offset_for_time(
        self, topic: str, partition: int, timestamp: pd.Timestamp, offset: int
    ):
        self._offsets_for_time[
            TopicPartition(topic, partition), timestamp.value // 1_000_000
        ] = TopicPartition(topic, partition, offset)

    def get_watermark_offsets(self, partition: TopicPartition) -> tuple[int, int]:
        return self._watermark_offsets[partition]

    def committed(
        self, partitions: list[TopicPartition], timeout: Optional[float] = None
    ):
        return [
            TopicPartition(tp.topic, tp.partition, self._committed[tp])
            for tp in partitions
        ]


class MockProducer:
    def __init__(self):
        self.polls = []
        self.produced = []

    def produce(self, topic: str, key: AnyStr, value: AnyStr, on_delivery: Callable):
        self.produced.append((topic, key, value, on_delivery))

    def poll(self, time: float):
        self.polls.append(time)

    def ack_all(self):
        for topic, key, value, call_back in self.produced:
            call_back(None, mock_kafka_message(topic=topic, value=value, key=key))
        self.produced.clear()

    def fail_all(self):
        for topic, key, value, call_back in self.produced:
            call_back("BAD!", mock_kafka_message(topic=topic, value=value, key=key))
        self.produced.clear()


def test_get_previous_start_of_day_utc():
    assert pd.to_datetime(
        "2022-09-18 08:00:00", utc=True
    ) == _get_previous_start_of_day(
        pd.to_datetime("2022-09-18 16:55:16", utc=True), pd.Timedelta("08:00:00"), "UTC"
    )

    assert pd.to_datetime(
        "2022-09-17 17:00:00", utc=True
    ) == _get_previous_start_of_day(
        pd.to_datetime("2022-09-18 16:55:16", utc=True), pd.Timedelta("17:00:00"), "UTC"
    )


def test_get_previous_start_of_day_nyc():
    tz = "America/New_York"
    assert pd.to_datetime(
        "2022-09-18 12:00:00", utc=True
    ) == _get_previous_start_of_day(
        pd.to_datetime("2022-09-18 16:55:16", utc=True), pd.Timedelta("08:00:00"), tz
    )

    assert pd.to_datetime(
        "2022-09-17 21:00:00", utc=True
    ) == _get_previous_start_of_day(
        pd.to_datetime("2022-09-18 16:55:16", utc=True), pd.Timedelta("17:00:00"), tz
    )


def test_consumer_manager_priming():
    tp0 = TopicPartition("topic-a", 0, offset=0)
    tp1 = TopicPartition("topic-a", 1, offset=10)
    tp2 = TopicPartition("topic-b", 0, offset=100)
    tp3 = TopicPartition("topic-c", 0, offset=1000)

    watermark_offsets = {tp0: (10, 11), tp1: (20, 22), tp2: (30, 32), tp3: (40, 43)}

    cutoff = pd.to_datetime("2022-10-19 01:00:00", utc=True)

    cutoff_ms = cutoff.value // 1_000_000

    mock_consumer = MockConsumer()
    consumer_manager = _ConsumerManager(
        cutoff=pd.to_datetime("2022-10-19 01:00:00", utc=True),
        partitions=watermark_offsets,
        consumer=mock_consumer,
        batch_size=100,
        max_held_messages=200,
    )

    # 0. No messages in, no message out
    assert consumer_manager.poll(0.0) == []
    assert consumer_manager._get_priming_watermark() == UTC_EPOCH

    mock_consumer.append(
        mock_kafka_message(
            tp0.topic,
            tp0.partition,
            cutoff_ms - 100,
            b"M1",
            None,
            10,
        )
    )
    assert consumer_manager.poll(0.0) == []
    assert len(consumer_manager._held_messages) == 1
    assert consumer_manager._get_priming_watermark() == UTC_EPOCH

    # 1. Messages on all partitions, can start releasing messages
    mock_consumer.append(
        mock_kafka_message(tp1.topic, tp1.partition, cutoff_ms - 100, b"M2", None, 20)
    )
    mock_consumer.append(
        mock_kafka_message(tp2.topic, tp2.partition, cutoff_ms - 100, b"M3", None, 30)
    )
    mock_consumer.append(
        mock_kafka_message(tp3.topic, tp3.partition, cutoff_ms - 50, b"M4", None, 40)
    )
    messages = consumer_manager.poll(0.0)
    assert [m.value() for m in messages] == [b"M1", b"M2", b"M3"]
    assert len(consumer_manager._held_messages) == 1  # Holding on to M4

    # 2. Messages come out of order
    mock_consumer.append(
        mock_kafka_message(tp0.topic, tp0.partition, cutoff_ms - 90, b"M5", None, 11)
    )
    mock_consumer.append(
        mock_kafka_message(tp1.topic, tp1.partition, cutoff_ms - 90, b"M6", None, 21)
    )
    mock_consumer.append(
        mock_kafka_message(tp2.topic, tp2.partition, cutoff_ms - 91, b"M7", None, 31)
    )
    mock_consumer.append(
        mock_kafka_message(tp2.topic, tp2.partition, cutoff_ms - 90, b"M8", None, 32)
    )
    messages = consumer_manager.poll(0.0)
    assert [m.value() for m in messages] == [b"M7", b"M5", b"M6", b"M8"]
    assert len(consumer_manager._held_messages) == 1  # still holding on to M4
    assert consumer_manager._get_priming_watermark() == pd.to_datetime(
        cutoff_ms - 90, unit="ms", utc=True
    )

    mock_consumer.append(
        mock_kafka_message(tp1.topic, tp1.partition, cutoff_ms - 90, b"M9", None, 22)
    )
    messages = consumer_manager.poll(0.0)
    assert [m.value() for m in messages] == [b"M9", b"M4"]
    assert len(consumer_manager._held_messages) == 0
    assert consumer_manager._get_priming_watermark() == pd.to_datetime(
        cutoff_ms - 50, unit="ms", utc=True
    )

    # Last partitions move, messages are released freely
    mock_consumer.append(
        mock_kafka_message(tp3.topic, tp3.partition, cutoff_ms + 10, b"M9", None, 41)
    )
    messages = consumer_manager.poll(0.0)
    assert [m.value() for m in messages] == [b"M9"]
    assert consumer_manager._held_messages == []
    assert consumer_manager._get_priming_watermark() is None


def test_consumer_manager_update_partition_info():
    topic_partition = TopicPartition("topic-a", 0, offset=100)
    partitions = {topic_partition: (1, 2)}
    cutoff = pd.to_datetime("2022-10-19 01:00:00", utc=True)

    mock_consumer = MockConsumer()
    consumer_manager = _ConsumerManager(
        cutoff=cutoff,
        partitions=partitions,
        consumer=mock_consumer,
        batch_size=100,
        max_held_messages=200,
    )
    assert consumer_manager._low_water_mark_ns == 0

    consumer_manager._update_partition_info(
        [
            mock_kafka_message(
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                value=b"HELLO",
                timestamp=cutoff,
                offset=1,
            )
        ]
    )

    assert consumer_manager._low_water_mark_ns == cutoff.value

    with mock.patch(
        "pandas.Timestamp.utcnow",
        return_value=cutoff + pd.to_timedelta("20s"),
    ):
        consumer_manager._update_partition_info(
            [
                mock_kafka_message(
                    topic=topic_partition.topic,
                    partition=topic_partition.partition,
                    value=b"WPR:D",
                    offset=2,
                )
            ]
        )
    assert (
        consumer_manager._low_water_mark_ns == (cutoff + pd.to_timedelta("20s")).value
    )
    assert consumer_manager._get_priming_watermark() is None


def test_consumer_manager_batching():
    tp1 = TopicPartition("topic-a", 0, offset=0)
    tp2 = TopicPartition("topic-a", 1, offset=0)
    cutoff = pd.to_datetime("2022-10-19 01:00:00", utc=True)
    cutoff_ms = cutoff.value // 1_000_000

    mock_consumer = MockConsumer()
    consumer_manager = _ConsumerManager(
        cutoff=pd.to_datetime("2022-10-19 01:00:00", utc=True),
        partitions={
            tp1: (0, 2),
            tp2: (0, 2),
        },
        consumer=mock_consumer,
        batch_size=2,
        max_held_messages=200,
    )
    assert len(consumer_manager.poll(0.0)) == 0
    assert consumer_manager._get_priming_watermark() == UTC_EPOCH

    mock_consumer.append(
        mock_kafka_message(tp1.topic, tp1.partition, cutoff_ms + 1, b"MSG1.1", offset=0)
    )

    mock_consumer.append(
        mock_kafka_message(tp2.topic, tp2.partition, cutoff_ms + 1, b"MSG2.1", offset=0)
    )
    mock_consumer.append(
        mock_kafka_message(tp2.topic, tp2.partition, cutoff_ms + 2, b"MSG2.2", offset=1)
    )
    mock_consumer.append(
        mock_kafka_message(tp2.topic, tp2.partition, cutoff_ms + 3, b"MSG2.3", offset=2)
    )
    mock_consumer.append(
        mock_kafka_message(tp2.topic, tp2.partition, cutoff_ms + 4, b"MSG2.4", offset=3)
    )

    messages = consumer_manager.poll(0.0)
    assert len(messages) == 2
    assert [m.value() for m in messages] == [b"MSG1.1", b"MSG2.1"]

    mock_consumer.append(
        mock_kafka_message(tp1.topic, tp1.partition, cutoff_ms + 2, b"MSG1.2", offset=1)
    )
    messages = consumer_manager.poll(0.0)
    assert len(messages) == 2
    assert [m.value() for m in messages] == [b"MSG2.2", b"MSG1.2"]

    messages = consumer_manager.poll(0.0)
    assert len(messages) == 2
    assert [m.value() for m in messages] == [b"MSG2.3", b"MSG2.4"]

    messages = consumer_manager.poll(0.0)
    assert messages == []

    mock_consumer.append(
        mock_kafka_message(tp2.topic, tp2.partition, cutoff_ms + 5, b"MSG2.5", offset=4)
    )
    mock_consumer.append(
        mock_kafka_message(tp1.topic, tp1.partition, cutoff_ms + 3, b"MSG1.3", offset=2)
    )
    mock_consumer.append(
        mock_kafka_message(tp1.topic, tp1.partition, cutoff_ms + 4, b"MSG1.4", offset=3)
    )

    messages = consumer_manager.poll(0.0)
    assert len(messages) == 2
    assert [m.value() for m in messages] == [b"MSG1.3", b"MSG1.4"]

    messages = consumer_manager.poll(0.0)
    assert len(messages) == 1
    assert [m.value() for m in messages] == [b"MSG2.5"]

    messages = consumer_manager.poll(0.0)
    assert len(messages) == 0


class PlainKafkaDeserializer:
    def __call__(self, messages: list[Message]) -> list[str]:
        return [message.value().decode("utf-8") for message in messages]


class WorldCountSerializer:
    def __init__(self, topic: str):
        self._topic = topic
        self.values = []

    def __call__(self, values: dict[str, int]) -> list[KafkaProducerMessage]:
        return [
            KafkaProducerMessage(
                self._topic, key.encode("utf-8"), str(value).encode("utf-8")
            )
            for key, value in values.items()
        ]


class MockProducerManager:
    def __init__(self):
        self.messages = []
        self.polls = 0

    def produce_one(self, topic: str, key: bytes, value: bytes):
        self.messages.append(KafkaProducerMessage(topic, key, value))

    def flush(self):
        results, self.messages = self.messages, []
        return results

    def poll(self):
        self.polls += 1


class LogHelper:
    def __init__(self, output: io.StringIO):
        self.output = output

    def flush(self) -> list[str]:
        self.output.seek(0)
        data = self.output.read()
        results = [r for r in data.split("\n") if r]
        self.output.seek(0)
        self.output.truncate(0)
        return results


@pytest.fixture()
def log_helper() -> LogHelper:
    level = beavers_logger.level
    beavers_logger.setLevel(logging.DEBUG)
    with io.StringIO() as output:
        helper = LogHelper(output)
        handler = logging.StreamHandler(output)
        beavers_logger.addHandler(handler)
        yield helper
        beavers_logger.removeHandler(handler)
        beavers_logger.setLevel(level)


def test_kafka_driver_word_count(log_helper: LogHelper):
    beavers_logger.setLevel(logging.DEBUG)
    output = io.StringIO()
    beavers_logger.addHandler(logging.StreamHandler(output))

    dag, word_count = create_word_count_dag()

    cutoff = pd.to_datetime("2022-10-19 01:00:00", utc=True)
    cutoff_ms = cutoff.value // 1_000_000

    mock_consumer = MockConsumer()
    consumer_manager = _ConsumerManager(
        cutoff=cutoff,
        partitions={TopicPartition("topic-a", 0, offset=0): (0, 4)},
        consumer=mock_consumer,
        batch_size=2,
        max_held_messages=200,
    )
    mock_producer_manager = MockProducerManager()

    kafka_driver = KafkaDriver(
        dag=dag,
        runtime_source_topics=[
            _RuntimeSourceTopic(
                topic_name="topic-a",
                node=dag.get_sources()["messages"],
                deserializer=PlainKafkaDeserializer(),
            )
        ],
        runtime_sink_topics=[
            _RuntimeSinkTopic(
                nodes=dag.get_sinks()["results"],
                serializer=WorldCountSerializer("topic-out"),
            )
        ],
        consumer_manager=consumer_manager,
        producer_manager=mock_producer_manager,
    )

    assert kafka_driver.run_cycle(0.0) is False
    mock_consumer.extend(
        [
            mock_kafka_message("topic-a", 0, cutoff_ms - 10, b"FOO", offset=0),
            mock_kafka_message("topic-a", 0, cutoff_ms - 9, b"BAR", offset=1),
            mock_kafka_message("topic-a", 0, cutoff_ms - 8, b"FOO", offset=2),
            mock_kafka_message("topic-a", 0, cutoff_ms - 7, b"BARZ", offset=3),
        ]
    )
    assert kafka_driver.run_cycle(0.0) is True
    assert word_count._counts == {"BAR": 1, "FOO": 1}
    assert mock_producer_manager.flush() == [
        KafkaProducerMessage("topic-out", b"BAR", b"1"),
        KafkaProducerMessage("topic-out", b"FOO", b"1"),
    ]
    assert log_helper.flush() == [
        "Ran cycle cycle_id=1, messages=2, time=2022-10-19 00:59:59.993000+00:00, "
        "next_timer=2262-04-11 23:47:16.854775807+00:00",
    ]

    assert kafka_driver.run_cycle(0.0) is True
    assert word_count._counts == {"BAR": 1, "FOO": 2, "BARZ": 1}
    assert mock_producer_manager.flush() == [
        KafkaProducerMessage("topic-out", b"BARZ", b"1"),
        KafkaProducerMessage("topic-out", b"FOO", b"2"),
    ]
    assert log_helper.flush() == [
        "Ran cycle cycle_id=2, messages=2, time=2022-10-19 00:59:59.993000+00:00, "
        "next_timer=2262-04-11 23:47:16.854775807+00:00"
    ]

    assert kafka_driver.run_cycle(0.0) is False
    assert mock_producer_manager.flush() == []
    assert log_helper.flush() == []

    mock_consumer.extend(
        [mock_kafka_message("topic-a", 0, cutoff_ms + 10, b"FOO", offset=4)]
    )
    assert kafka_driver.run_cycle(0.0) is True
    assert mock_producer_manager.flush() == [
        KafkaProducerMessage("topic-out", b"FOO", b"3")
    ]
    assert kafka_driver.run_cycle(0.0) is False
    assert mock_producer_manager.flush() == []
    assert len(log_helper.flush()) == 1

    metrics = kafka_driver.flush_metrics()
    assert metrics.deserialization_ns > 0
    assert metrics.deserialization_count == 6
    assert metrics.serialization_ns > 0
    assert metrics.serialization_count == 3
    assert metrics.execution_ns > 0
    assert metrics.execution_count == 3

    mock_consumer.extend(
        [
            mock_kafka_message(
                "topic-a",
                0,
                error=confluent_kafka.KafkaError(
                    confluent_kafka.KafkaError.BROKER_NOT_AVAILABLE
                ),
            ),
        ]
    )
    assert kafka_driver.run_cycle(0.0) is False
    assert kafka_driver._consumer_manager.flush_metrics().error_message_count == 1


def _timestamp_to_bytes(timestamp: pd.Timestamp) -> bytes:
    return str(timestamp).encode("utf-8")


def test_kafka_driver_timer():
    dag = Dag()
    messages_stream = dag.source_stream([], name="messages")
    timestamp_stream = dag.stream(
        lambda x: [TimerEntry(pd.to_datetime(v), [1, 2, 3]) for v in x if v], []
    ).map(messages_stream)
    timer_setter = SetATimer()
    dag.state(timer_setter).map(timestamp_stream, dag.now(), dag.timer_manager())

    cutoff = pd.to_datetime("2022-10-19 01:00:00+0000")
    cutoff_ms = cutoff.value // 1_000_000

    mock_consumer = MockConsumer()
    consumer_manager = _ConsumerManager(
        cutoff=cutoff,
        partitions={TopicPartition("topic-a", 0, offset=0): (0, 3)},
        consumer=mock_consumer,
        batch_size=2,
        max_held_messages=200,
    )
    mock_producer_manager = MockProducerManager()

    kafka_driver = KafkaDriver(
        dag=dag,
        runtime_source_topics=[
            _RuntimeSourceTopic(
                topic_name="topic-a",
                node=dag.get_sources()["messages"],
                deserializer=PlainKafkaDeserializer(),
            )
        ],
        runtime_sink_topics=[],
        consumer_manager=consumer_manager,
        producer_manager=mock_producer_manager,
    )

    # No message, do nothing:
    assert kafka_driver.run_cycle(0.0) is False

    # Set a timer in a bit
    mock_consumer.append(
        mock_kafka_message(
            "topic-a",
            0,
            cutoff_ms - 10,
            _timestamp_to_bytes(cutoff + pd.to_timedelta("10s")),
            offset=1,
        )
    )
    assert kafka_driver.run_cycle(0.0) is True
    assert dag.get_next_timer() == cutoff + pd.to_timedelta("10s")

    # Set a timer a bit later
    mock_consumer.append(
        mock_kafka_message(
            "topic-a",
            0,
            cutoff_ms - 5,
            _timestamp_to_bytes(cutoff + pd.to_timedelta("20s")),
            offset=2,
        )
    )
    assert kafka_driver.run_cycle(0.0) is True
    assert dag.get_next_timer() == cutoff + pd.to_timedelta("20s")

    with mock.patch(
        "pandas.Timestamp.utcnow",
        return_value=cutoff + pd.to_timedelta("20s"),
    ):
        # This should trigger the timer, but we're not pass the cutoff
        assert kafka_driver.run_cycle(0.0) is False
        assert (
            kafka_driver._consumer_manager._get_priming_watermark()
            == cutoff - pd.to_timedelta("5ms")
        )
        assert dag.get_next_timer() == cutoff + pd.to_timedelta("20s")

    # Publish EOF message
    mock_consumer.append(mock_kafka_message("topic-a", 0, None, b"", offset=3))
    with mock.patch(
        "pandas.Timestamp.utcnow",
        # Cheating a bit and go back in time:
        return_value=cutoff + pd.to_timedelta("10s"),
    ):
        assert kafka_driver.run_cycle(0.0) is True  # Ran because of new message
        assert kafka_driver._consumer_manager._get_priming_watermark() is None
        assert dag.get_next_timer() == cutoff + pd.to_timedelta("20s")

    with mock.patch(
        "pandas.Timestamp.utcnow",
        return_value=cutoff + pd.to_timedelta("20s"),
    ):
        assert kafka_driver.run_cycle(0.0) is True
        assert dag.get_next_timer() == UTC_MAX


def test_consumer_manager_pause_unpause():
    cutoff = pd.to_datetime("2022-10-19 01:00:00", utc=True)

    mock_consumer = MockConsumer()
    consumer_manager = _ConsumerManager(
        cutoff=cutoff,
        partitions={
            TopicPartition("topic-a", 0, offset=0): (0, 4),
            TopicPartition("topic-a", 1, offset=0): (0, 1),
            TopicPartition("topic-a", 2, offset=0): (0, 6),
        },
        consumer=mock_consumer,
        batch_size=2,
        max_held_messages=3,
    )

    # Start with a lot of message, they will be held and some partitions paused
    mock_consumer.extend(
        [
            mock_kafka_message(
                "topic-a", 0, cutoff - pd.to_timedelta("10s"), b"1", offset=0
            ),
            mock_kafka_message(
                "topic-a", 0, cutoff - pd.to_timedelta("10s"), b"2", offset=1
            ),
            mock_kafka_message(
                "topic-a", 1, cutoff - pd.to_timedelta("5s"), b"3", offset=0
            ),
            mock_kafka_message(
                "topic-a", 2, cutoff - pd.to_timedelta("3s"), b"4", offset=0
            ),
            mock_kafka_message(
                "topic-a", 2, cutoff - pd.to_timedelta("3s"), b"5", offset=1
            ),
            mock_kafka_message(
                "topic-a", 2, cutoff - pd.to_timedelta("3s"), b"6", offset=2
            ),
            mock_kafka_message(
                "topic-a", 2, cutoff - pd.to_timedelta("3s"), b"7", offset=3
            ),
            mock_kafka_message(
                "topic-a", 2, cutoff - pd.to_timedelta("3s"), b"8", offset=4
            ),
            mock_kafka_message(
                "topic-a", 2, cutoff - pd.to_timedelta("3s"), b"9", offset=5
            ),
        ]
    )
    assert [m.value() for m in consumer_manager.poll(0.0)] == []
    assert [m.value() for m in consumer_manager.poll(0.0)] == [b"1", b"2"]
    assert consumer_manager._get_priming_watermark() == cutoff - pd.to_timedelta("10s")
    assert mock_consumer._paused == [
        TopicPartition("topic-a", 1),
        TopicPartition("topic-a", 2),
    ]

    # Move the most lagging partition (0) ahead, partition 1 is resumed
    mock_consumer.extend(
        [
            mock_kafka_message(
                "topic-a", 0, cutoff - pd.to_timedelta("5s"), b"10", offset=2
            ),
        ]
    )
    assert [m.value() for m in consumer_manager.poll(0.0)] == []
    assert [m.value() for m in consumer_manager.poll(0.0)] == []
    assert [m.value() for m in consumer_manager.poll(0.0)] == [b"3", b"10"]
    assert consumer_manager._get_priming_watermark() == cutoff - pd.to_timedelta("5s")
    assert mock_consumer._paused == [TopicPartition("topic-a", 2)]

    # Partition 0 and 1 go ahead of partition 2, 1 and 2 are paused
    mock_consumer.extend(
        [
            mock_kafka_message(
                "topic-a", 0, cutoff - pd.to_timedelta("2s"), b"11", offset=3
            ),
            mock_kafka_message(
                "topic-a", 1, cutoff - pd.to_timedelta("2s"), b"12", offset=1
            ),
        ]
    )
    assert [m.value() for m in consumer_manager.poll(0.0)] == [b"4", b"5"]
    assert consumer_manager._get_priming_watermark() == cutoff - pd.to_timedelta("3s")
    assert mock_consumer._paused == [
        TopicPartition("topic-a", 0),
        TopicPartition("topic-a", 1),
    ]

    # Some messages a read, not enough to move the dial
    assert [m.value() for m in consumer_manager.poll(0.0)] == [b"6", b"7"]
    assert consumer_manager._get_priming_watermark() == cutoff - pd.to_timedelta("3s")
    assert mock_consumer._paused == [
        TopicPartition("topic-a", 0),
        TopicPartition("topic-a", 1),
    ]

    # Some messages a read, not enough to move the dial
    assert [m.value() for m in consumer_manager.poll(0.0)] == [b"8", b"9"]
    assert consumer_manager._get_priming_watermark() == cutoff - pd.to_timedelta("3s")
    assert mock_consumer._paused == [
        TopicPartition("topic-a", 0),
        TopicPartition("topic-a", 1),
    ]

    # Some message a read, not enough to move the dial
    assert [m.value() for m in consumer_manager.poll(0.0)] == []
    assert consumer_manager._get_priming_watermark() == cutoff - pd.to_timedelta("3s")
    assert mock_consumer._paused == [
        TopicPartition("topic-a", 0),
        TopicPartition("topic-a", 1),
    ]

    # Mark partition 2 as EOF and poll. We unpause because only 1 (<3) messages are held
    mock_consumer.extend(
        [
            mock_kafka_message(
                "topic-a", 2, cutoff - pd.to_timedelta("1s"), b"", b"EOF", offset=6
            )
        ]
    )
    assert [m.value() for m in consumer_manager.poll(0.0)] == [b"11", b"12"]
    assert consumer_manager._get_priming_watermark() == cutoff - pd.to_timedelta("2s")
    assert len(consumer_manager._held_messages) == 1
    assert mock_consumer._paused == []

    # Nothing to read because partitions are holding us back
    assert [m.value() for m in consumer_manager.poll(0.0)] == []
    assert consumer_manager._get_priming_watermark() == cutoff - pd.to_timedelta("2s")
    assert mock_consumer._paused == []

    # Mark all partitions as eof
    mock_consumer.extend(
        [
            mock_kafka_message("topic-a", 0, cutoff, b"", offset=4),
            mock_kafka_message("topic-a", 1, cutoff, b"", offset=2),
        ]
    )
    assert [m.value() for m in consumer_manager.poll(0.0)] == [b"", b""]
    assert consumer_manager._get_priming_watermark() is None
    assert mock_consumer._paused == []

    assert [m.value() for m in consumer_manager.poll(0.0)] == [b""]
    assert consumer_manager._get_priming_watermark() is None
    assert mock_consumer._paused == []


def test_consumer_manager_pause_unpause_eof():
    """In this example topic a publishes frequently but topic-b doesn't"""
    cutoff = pd.to_datetime("2022-10-19 01:00:00", utc=True)
    tpa = TopicPartition("topic-a", 0, offset=0)
    tpb = TopicPartition("topic-b", 0, offset=0)

    mock_consumer = MockConsumer()
    consumer_manager = _ConsumerManager(
        cutoff=cutoff,
        partitions={
            tpa: (0, 9),
            tpb: (10, 12),
        },
        consumer=mock_consumer,
        batch_size=2,
        max_held_messages=3,
    )

    # Start with a lot of message, they will be held and some partitions paused
    mock_consumer.extend(
        [
            mock_kafka_message(
                "topic-a", 0, cutoff - pd.to_timedelta("10s"), b"1", offset=0
            ),
            mock_kafka_message(
                "topic-a", 0, cutoff - pd.to_timedelta("10s"), b"2", offset=1
            ),
            mock_kafka_message(
                "topic-a", 0, cutoff - pd.to_timedelta("5s"), b"3", offset=2
            ),
            mock_kafka_message(
                "topic-a", 0, cutoff - pd.to_timedelta("3s"), b"4", offset=3
            ),
            # B messages
            mock_kafka_message(
                "topic-b", 0, cutoff - pd.to_timedelta("30s"), b"A", offset=10
            ),
            mock_kafka_message(
                "topic-b", 0, cutoff - pd.to_timedelta("1s"), b"B", offset=11
            ),
            mock_kafka_message(
                "topic-b", 0, cutoff - pd.to_timedelta("1s"), value=b"EOF", offset=12
            ),
        ]
    )

    assert [m.value() for m in consumer_manager.poll(0.0)] == []
    assert len(consumer_manager._held_messages) == 3
    assert mock_consumer._paused == [tpa]

    assert [m.value() for m in consumer_manager.poll(0.0)] == [b"A"]
    assert mock_consumer._paused == [tpa]

    assert [m.value() for m in consumer_manager.poll(0.0)] == [b"1", b"2"]
    assert len(consumer_manager._held_messages) == 4
    assert mock_consumer._paused == [tpb]

    assert [m.value() for m in consumer_manager.poll(0.0)] == [b"3", b"4"]
    assert mock_consumer._paused == [tpb]

    assert [m.value() for m in consumer_manager.poll(0.0)] == []


def test_all_partitions_eof():
    """Every topic is already live"""
    cutoff = pd.to_datetime("2022-10-19 01:00:00", utc=True)

    mock_consumer = MockConsumer()
    consumer_manager = _ConsumerManager(
        cutoff=cutoff,
        partitions={
            TopicPartition("topic-a", 0, offset=0): (0, 0),
            TopicPartition("topic-b", 0, offset=0): (10, 10),
        },
        consumer=mock_consumer,
        batch_size=2,
        max_held_messages=3,
    )

    # Start with a lot of message, they will be held and some partitions paused
    mock_consumer.extend([])
    with mock.patch(
        "pandas.Timestamp.utcnow",
        return_value=cutoff + pd.to_timedelta("20s"),
    ):
        assert [m.value() for m in consumer_manager.poll(0.0)] == []
    assert mock_consumer._paused == []  # Not pausing EOF partitions
    assert consumer_manager._get_priming_watermark() is None
    assert consumer_manager.flush_metrics() == ConsumerMetrics(
        consumed_message_size=0,
        consumed_message_count=0,
        released_message_count=0,
    )


def test_get_message_ns():
    assert _get_message_ns(mock_kafka_message(timestamp=None)) == 9223372036854775807
    assert _get_message_ns(mock_kafka_message(timestamp=0)) == 0
    assert _get_message_ns(mock_kafka_message(timestamp=123)) == 123_000_000


def test_poll_all():
    consumer = MockConsumer()
    assert _poll_all(consumer, 0.0, 100) == []
    consumer.append(mock_kafka_message())
    assert len(_poll_all(consumer, 0.0, 100)) == 1
    consumer.extend([mock_kafka_message()] * 1000)
    assert len(_poll_all(consumer, 0.0, 100)) == 100


@dataclasses.dataclass(frozen=True)
class PassThroughKafkaMessageDeserializer:
    messages: list[confluent_kafka.Message] = dataclasses.field(default_factory=list)

    def append_message(self, message: confluent_kafka.Message):
        self.messages.append(message)

    def flush(self) -> list[confluent_kafka.Message]:
        """Convert queued messages to data"""
        results = self.messages.copy()
        self.messages.clear()
        return results


def test_from_xxx():
    deserializer = PassThroughKafkaMessageDeserializer()
    assert SourceTopic.from_latest("topic-1", deserializer) == SourceTopic(
        "topic-1", deserializer, OffsetPolicy.LATEST
    )

    assert SourceTopic.from_start_of_day(
        "topic-1", deserializer, pd.Timedelta("00:15:00"), "UTC"
    ) == SourceTopic(
        "topic-1",
        deserializer,
        OffsetPolicy.START_OF_DAY,
        start_of_day_time=pd.Timedelta("00:15:00"),
        start_of_day_timezone="UTC",
    )

    assert SourceTopic.from_relative_time(
        "topic-1", deserializer, relative_time=pd.Timedelta("00:20:00")
    ) == SourceTopic(
        "topic-1",
        deserializer,
        OffsetPolicy.RELATIVE_TIME,
        relative_time=pd.Timedelta("00:20:00"),
    )

    assert SourceTopic.from_earliest("topic-1", deserializer) == SourceTopic(
        "topic-1", deserializer, OffsetPolicy.EARLIEST
    )

    assert SourceTopic.from_committed("topic-1", deserializer) == SourceTopic(
        "topic-1", deserializer, OffsetPolicy.COMMITTED
    )

    assert SourceTopic.from_absolute_time(
        "topic-1", deserializer, pd.to_datetime("2022-01-01", utc=True)
    ) == SourceTopic(
        "topic-1",
        deserializer,
        OffsetPolicy.ABSOLUTE_TIME,
        absolute_time=pd.to_datetime("2022-01-01", utc=True),
    )


def partition_metadata(id: int) -> PartitionMetadata:
    result = PartitionMetadata()
    result.id = id
    return result


def topic_metadata(topic: str, partitions: list[PartitionMetadata]):
    results = TopicMetadata()
    results.topic = topic
    results.partitions = {p.id: p for p in partitions}
    return results


def assert_topic_offsets(actual: list[TopicPartition], expected: list[TopicPartition]):
    """TopicPartition doesn't take offset in consideration in its value semantic"""
    assert actual == expected
    for a, e in zip(actual, expected):
        assert a.offset == e.offset


def test_resolve_topic_offsets_latest():
    consumer = MockConsumer()
    now = pd.to_datetime("2022-01-01", utc=True)
    deserializer = PassThroughKafkaMessageDeserializer()

    from_latest = SourceTopic.from_latest("topic-1", deserializer)

    with pytest.raises(KeyError, match="topic-1"):
        _resolve_topic_offsets(consumer, from_latest, now)

    consumer._topics["topic-1"] = topic_metadata(
        "topic-1",
        [
            partition_metadata(0),
            partition_metadata(1),
        ],
    )

    with pytest.raises(KeyError, match="topic-1"):
        _resolve_topic_offsets(consumer, from_latest, now)

    consumer._watermark_offsets = {
        TopicPartition("topic-1", 0): (10, 10),
        TopicPartition("topic-1", 1): (20, 20),
    }

    assert _resolve_topic_offsets(consumer, from_latest, now) == {
        TopicPartition("topic-1", 0): (10, 9),
        TopicPartition("topic-1", 1): (20, 19),
    }


def test_from_start_of_day():
    consumer = MockConsumer()
    now = pd.to_datetime("2022-01-01", utc=True)
    deserializer = PassThroughKafkaMessageDeserializer()

    from_start_of_day = SourceTopic.from_start_of_day(
        "topic-1",
        deserializer,
        pd.Timedelta("00:15:00"),
        "UTC",
    )

    with pytest.raises(KeyError, match="topic-1"):
        _resolve_topic_offsets(consumer, from_start_of_day, now)

    consumer._topics["topic-1"] = topic_metadata(
        "topic-1",
        [
            partition_metadata(0),
            partition_metadata(1),
        ],
    )

    with pytest.raises(KeyError, match=r"TopicPartition\{topic=topic-1"):
        _resolve_topic_offsets(consumer, from_start_of_day, now)

    start_of_day = pd.to_datetime("2021-12-31T00:15:00Z")
    consumer.mock_offset_for_time("topic-1", 0, start_of_day, 123)
    consumer.mock_offset_for_time("topic-1", 1, start_of_day, 124)

    consumer._watermark_offsets = {
        TopicPartition("topic-1", 0): (0, 130),
        TopicPartition("topic-1", 1): (0, 140),
    }

    assert _resolve_topic_offsets(consumer, from_start_of_day, now) == {
        TopicPartition("topic-1", 0): (123, 129),
        TopicPartition("topic-1", 1): (124, 139),
    }


def test_resolve_topics_offsets():
    consumer = MockConsumer()

    consumer._topics["topic-1"] = topic_metadata("topic-1", [partition_metadata(0)])

    start_of_day = pd.to_datetime("2021-12-31T00:15:00Z")
    consumer.mock_offset_for_time("topic-1", 0, start_of_day, 123)
    consumer._watermark_offsets = {TopicPartition("topic-1", 0): (0, 130)}

    results = _resolve_topics_offsets(
        consumer,
        [
            SourceTopic.from_start_of_day(
                "topic-1",
                PassThroughKafkaMessageDeserializer(),
                pd.Timedelta("00:15:00"),
                "UTC",
            )
        ],
        pd.to_datetime("2022-01-01", utc=True),
    )
    assert len(results) == 1
    assert results == {TopicPartition(topic="topic-1", partition=0): (123, 129)}


def test_from_relative_time():
    consumer = MockConsumer()
    now = pd.to_datetime("2022-01-01", utc=True)
    deserializer = PassThroughKafkaMessageDeserializer()

    source_topic = SourceTopic.from_relative_time(
        "topic-1",
        deserializer,
        pd.Timedelta("00:15:00"),
    )

    with pytest.raises(KeyError, match="topic-1"):
        _resolve_topic_offsets(consumer, source_topic, now)

    consumer._topics["topic-1"] = topic_metadata(
        "topic-1",
        [
            partition_metadata(0),
            partition_metadata(1),
        ],
    )

    with pytest.raises(KeyError, match=r"TopicPartition\{topic=topic-1"):
        _resolve_topic_offsets(consumer, source_topic, now)

    actual_time = pd.to_datetime("2021-12-31T23:45:00Z")
    consumer.mock_offset_for_time("topic-1", 0, actual_time, 123)
    consumer.mock_offset_for_time("topic-1", 1, actual_time, 124)

    consumer._watermark_offsets = {
        TopicPartition("topic-1", 0): (0, 130),
        TopicPartition("topic-1", 1): (0, 140),
    }

    assert _resolve_topic_offsets(consumer, source_topic, now) == {
        TopicPartition("topic-1", 0): (123, 129),
        TopicPartition("topic-1", 1): (124, 139),
    }


def test_from_absolute_time():
    consumer = MockConsumer()
    now = pd.to_datetime("2022-01-01", utc=True)
    start_time = pd.to_datetime("2021-12-31", utc=True)
    deserializer = PassThroughKafkaMessageDeserializer()

    source_topic = SourceTopic.from_absolute_time(
        "topic-1",
        deserializer,
        start_time,
    )

    with pytest.raises(KeyError, match="topic-1"):
        _resolve_topic_offsets(consumer, source_topic, now)

    consumer._topics["topic-1"] = topic_metadata(
        "topic-1",
        [
            partition_metadata(0),
            partition_metadata(1),
        ],
    )

    with pytest.raises(KeyError, match=r"TopicPartition\{topic=topic-1"):
        _resolve_topic_offsets(consumer, source_topic, now)

    consumer.mock_offset_for_time("topic-1", 0, start_time, 123)
    consumer.mock_offset_for_time("topic-1", 1, start_time, 124)

    consumer._watermark_offsets = {
        TopicPartition("topic-1", 0): (0, 130),
        TopicPartition("topic-1", 1): (10, 140),
    }

    assert _resolve_topic_offsets(consumer, source_topic, now) == {
        TopicPartition("topic-1", 0): (123, 129),
        TopicPartition("topic-1", 1): (124, 139),
    }


def test_from_earliest():
    consumer = MockConsumer()
    now = pd.to_datetime("2022-01-01", utc=True)
    deserializer = PassThroughKafkaMessageDeserializer()

    source_topic = SourceTopic.from_earliest("topic-1", deserializer)

    with pytest.raises(KeyError, match="topic-1"):
        _resolve_topic_offsets(consumer, source_topic, now)

    consumer._topics["topic-1"] = topic_metadata(
        "topic-1",
        [
            partition_metadata(0),
            partition_metadata(1),
        ],
    )
    consumer._watermark_offsets = {
        TopicPartition("topic-1", 0): (10, 130),
        TopicPartition("topic-1", 1): (20, 140),
    }

    assert _resolve_topic_offsets(consumer, source_topic, now) == {
        TopicPartition("topic-1", 0): (10, 129),
        TopicPartition("topic-1", 1): (20, 139),
    }


def test_from_committed():
    consumer = MockConsumer()
    now = pd.to_datetime("2022-01-01", utc=True)
    deserializer = PassThroughKafkaMessageDeserializer()

    source_topic = SourceTopic.from_committed("topic-1", deserializer)

    with pytest.raises(KeyError, match="topic-1"):
        _resolve_topic_offsets(consumer, source_topic, now)

    consumer._topics["topic-1"] = topic_metadata(
        "topic-1",
        [
            partition_metadata(0),
            partition_metadata(1),
        ],
    )
    consumer._watermark_offsets = {
        TopicPartition("topic-1", 0): (0, 130),
        TopicPartition("topic-1", 1): (10, 140),
    }

    consumer._committed = {
        TopicPartition("topic-1", 0): 100,
        TopicPartition("topic-1", 1): 110,
    }

    assert _resolve_topic_offsets(consumer, source_topic, now) == {
        TopicPartition("topic-1", 0): (100, 129),
        TopicPartition("topic-1", 1): (110, 139),
    }


def test_no_topic():
    consumer = MockConsumer()
    now = pd.to_datetime("2022-01-01", utc=True)
    deserializer = PassThroughKafkaMessageDeserializer()

    source_topic = SourceTopic.from_committed("topic-1", deserializer)
    consumer._topics["topic-1"] = topic_metadata("topic-1", [])

    with pytest.raises(ValueError, match="Topic topic-1 does not exist"):
        _resolve_topic_offsets(consumer, source_topic, now)


def test_no_policy():
    consumer = MockConsumer()
    now = pd.to_datetime("2022-01-01", utc=True)

    source_topic = SourceTopic(
        "topic-1",
        PassThroughKafkaMessageDeserializer(),
        "BAD!",  # type: ignore
    )
    consumer._topics["topic-1"] = topic_metadata("topic-1", [partition_metadata(0)])
    consumer._watermark_offsets[TopicPartition("topic-1", 0)] = (0, 1)

    with pytest.raises(ValueError, match="OffsetPolicy BAD! not supported for topic-1"):
        _resolve_topic_offsets(consumer, source_topic, now)


def test_producer_manager_ok():
    mock_producer = MockProducer()
    producer_manager = _ProducerManager(mock_producer)
    producer_manager.poll()
    assert mock_producer.polls == [0.0]

    producer_manager.produce_one("topic-1", "key-1", "value-1")
    assert mock_producer.produced == [
        ("topic-1", "key-1", "value-1", producer_manager.on_delivery)
    ]

    assert producer_manager.flush_metrics() == ProducerMetrics(
        produced_count=1,
        produced_size=7,
    )

    mock_producer.ack_all()
    assert producer_manager.flush_metrics() == ProducerMetrics(confirmed_count=1)


def test_producer_manager_produce_error():
    mock_producer = MockProducer()
    producer_manager = _ProducerManager(mock_producer)
    mock_producer.produced = None

    producer_manager.produce_one("topic-1", "key-1", "value-1")
    assert producer_manager.flush_metrics() == ProducerMetrics(produced_error_count=1)
    assert producer_manager._errors == 1

    producer_manager.produce_one("topic-1", "key-1", "value-1")
    assert producer_manager.flush_metrics() == ProducerMetrics(produced_error_count=1)
    assert producer_manager._errors == 2


def test_producer_manager_not_delivered():
    mock_producer = MockProducer()
    producer_manager = _ProducerManager(mock_producer)
    producer_manager.produce_one("topic-1", "key-1", "value-1")
    producer_manager.flush_metrics()
    mock_producer.fail_all()
    assert producer_manager.flush_metrics() == ProducerMetrics(delivery_error_count=1)
    assert producer_manager._errors == 1

    producer_manager.produce_one("topic-1", "key-1", "value-1")
    producer_manager.flush_metrics()
    mock_producer.fail_all()
    assert producer_manager.flush_metrics() == ProducerMetrics(delivery_error_count=1)
    assert producer_manager._errors == 2


def test_producer_manager_create():
    with mock.patch("confluent_kafka.Producer", autospec=True):
        producer_manager = _ProducerManager.create({})
        assert producer_manager._producer is not None


def test_consumer_manager_create():
    with mock.patch("confluent_kafka.Consumer", autospec=True):
        consumer_manager = _ConsumerManager.create(
            {"enable.partition.eof": True}, [], 500, timeout=None
        )
        assert consumer_manager._consumer is not None


def test_consumer_manager_create_partition_eof():
    with mock.patch("confluent_kafka.Consumer", autospec=True):
        _ConsumerManager.create({}, [], 500, timeout=None)
    with mock.patch("confluent_kafka.Consumer", autospec=True):
        _ConsumerManager.create({"enable.partition.eof": True}, [], 500, timeout=None)
    with mock.patch("confluent_kafka.Consumer", autospec=True):
        _ConsumerManager.create({"enable.partition.eof": False}, [], 500, timeout=None)
    with mock.patch("confluent_kafka.Consumer", autospec=True):
        _ConsumerManager.create({"enable.partition.eof": None}, [], 500, timeout=None)


def test_runtime_sink_topic():
    dag = Dag()
    node = dag.source_stream(empty={})
    sink = dag.sink("sink", node)
    runtime_sink_topic = _RuntimeSinkTopic([sink], WorldCountSerializer("topic-1"))

    dag.execute()
    assert runtime_sink_topic.serialize(dag.get_cycle_id()) == []

    node.set_stream({"foo": "bar"})
    dag.execute()
    assert runtime_sink_topic.serialize(dag.get_cycle_id()) == [
        KafkaProducerMessage(topic="topic-1", key=b"foo", value=b"bar")
    ]

    dag.execute()
    assert runtime_sink_topic.serialize(dag.get_cycle_id()) == []


def test_coverage():
    with mock.patch("confluent_kafka.Consumer", autospec=True):
        KafkaDriver.create(Dag(), {}, {}, {}, {}, 1_000)
