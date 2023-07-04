import contextlib
import dataclasses
import logging
import time
from enum import Enum
from typing import Any, AnyStr, Generic, Optional, Protocol, TypeVar

import confluent_kafka
import confluent_kafka.admin
import pandas as pd

from beavers.engine import UTC_EPOCH, UTC_MAX, Dag, Node

logger = logging.getLogger(__name__)

T = TypeVar("T")

KAFKA_EOF_CODE = -191


class KafkaMessageDeserializer(Protocol[T]):
    """Interface for converting incoming kafka messages to custom data"""

    def __call__(self, messages: list[confluent_kafka.Message]) -> T:
        """Convert batch of messages to data"""


@dataclasses.dataclass(frozen=True)
class KafkaProducerMessage:
    topic: str
    key: AnyStr
    value: AnyStr


class KafkaMessageSerializer(Protocol[T]):
    """Interface for converting custom data to outgoing kafka messages"""

    def __call__(self, value: T) -> list[KafkaProducerMessage]:
        """Convert batch of custom data to `KafkaProducerMessage`"""


class OffsetPolicy(Enum):
    LATEST = 1
    EARLIEST = 2
    START_OF_DAY = 3
    RELATIVE_TIME = 4
    ABSOLUTE_TIME = 5
    COMMITTED = 6


@dataclasses.dataclass(frozen=True)
class SourceTopic(Generic[T]):
    name: str
    message_deserializer: KafkaMessageDeserializer[T]
    offset_policy: OffsetPolicy
    start_of_day_time: Optional[pd.Timedelta] = None
    start_of_day_timezone: Optional[str] = None
    relative_time: Optional[pd.Timedelta] = None
    absolute_time: Optional[pd.Timestamp] = None

    @staticmethod
    def from_latest(
        name: str, message_deserializer: KafkaMessageDeserializer[T]
    ) -> "SourceTopic[T]":
        return SourceTopic(
            name=name,
            message_deserializer=message_deserializer,
            offset_policy=OffsetPolicy.LATEST,
        )

    @staticmethod
    def from_earliest(
        name: str, message_deserializer: KafkaMessageDeserializer[T]
    ) -> "SourceTopic[T]":
        return SourceTopic(
            name=name,
            message_deserializer=message_deserializer,
            offset_policy=OffsetPolicy.EARLIEST,
        )

    @staticmethod
    def from_relative_time(
        name: str,
        message_deserializer: KafkaMessageDeserializer[T],
        relative_time: pd.Timedelta,
    ) -> "SourceTopic[T]":
        return SourceTopic(
            name=name,
            message_deserializer=message_deserializer,
            offset_policy=OffsetPolicy.RELATIVE_TIME,
            relative_time=relative_time,
        )

    @staticmethod
    def from_start_of_day(
        name: str,
        message_deserializer: KafkaMessageDeserializer[T],
        start_of_day_time: pd.Timedelta,
        start_of_day_timezone: str,
    ) -> "SourceTopic[T]":
        return SourceTopic(
            name=name,
            message_deserializer=message_deserializer,
            offset_policy=OffsetPolicy.START_OF_DAY,
            start_of_day_time=start_of_day_time,
            start_of_day_timezone=start_of_day_timezone,
        )

    @staticmethod
    def from_absolute_time(
        name: str,
        message_deserializer: KafkaMessageDeserializer[T],
        absolute_time: pd.Timestamp,
    ) -> "SourceTopic[T]":
        return SourceTopic(
            name=name,
            message_deserializer=message_deserializer,
            offset_policy=OffsetPolicy.ABSOLUTE_TIME,
            absolute_time=absolute_time,
        )

    @staticmethod
    def from_committed(
        name: str, message_deserializer: KafkaMessageDeserializer[T]
    ) -> "SourceTopic[T]":
        return SourceTopic(
            name=name,
            message_deserializer=message_deserializer,
            offset_policy=OffsetPolicy.COMMITTED,
        )


class _RuntimeSourceTopic(Generic[T]):
    def __init__(
        self,
        topic_name: str,
        node: Node[T],
        deserializer: KafkaMessageDeserializer[T],
    ):
        self._topic_name = topic_name
        self._node = node
        self._deserializer = deserializer
        self._errors = 0
        self._messages: list[confluent_kafka.Message] = []

    def append(self, message: confluent_kafka.Message):
        self._messages.append(message)

    def flush(self) -> bool:
        results: T = self._deserializer(self._messages)
        self._messages = []
        if results:
            self._node.set_stream(results)
            return True
        else:
            return False


@dataclasses.dataclass
class ProducerMetrics:
    produced_count: int = 0
    produced_size: int = 0
    produced_error_count: int = 0
    delivery_error_count: int = 0
    confirmed_count: int = 0


class _ProducerManager:
    def __init__(
        self,
        producer_config: dict[str, Any],
    ):
        self._producer = confluent_kafka.Producer(producer_config)
        self._errors = 0
        self._metrics: ProducerMetrics = ProducerMetrics()

    def poll(self):
        self._producer.poll(0.0)

    def produce_one(self, topic: str, key: AnyStr, value: bytes) -> bool:
        try:
            self._producer.produce(
                topic=topic, key=key, value=value, on_delivery=self.on_delivery
            )
            self._metrics.produced_size += len(value)
            self._metrics.produced_count += 1
            return True
        except Exception as err:
            if self._errors == 0:
                logger.error("Error producing message on %s", topic, exc_info=err)
                self._errors += 1
                return False

    def on_delivery(self, err, msg: confluent_kafka.Message):
        if err:
            if self._errors == 0:
                logger.error("Error delivering message on %s: %s", msg.topic(), err)
            self._errors += 1
            self._metrics.delivery_error_count += 1
        else:
            self._metrics.confirmed_count += 1

    def flush_metrics(self) -> ProducerMetrics:
        results = self._metrics
        self._metrics = ProducerMetrics()
        return results


@dataclasses.dataclass
class _PartitionInfo:
    current_offset: int
    timestamp_ns: int = UTC_EPOCH.value
    paused: bool = False
    primed: bool = False
    eof: bool = False


@dataclasses.dataclass
class ConsumerMetrics:
    consumed_message_size: int = 0
    consumed_message_count: int = 0
    paused_partitions: int = 0
    released_message_count: int = 0
    held_message_count: int = 0


class _ConsumerManager:
    def __init__(
        self,
        cutoff: pd.Timestamp,
        partitions: list[confluent_kafka.TopicPartition],
        consumer: confluent_kafka.Consumer,
        batch_size: int,
        max_held_messages: int,
    ):
        self._cutoff_ns: int = cutoff.value
        self._consumer: confluent_kafka.Consumer = consumer
        self._partition_info: dict[confluent_kafka.TopicPartition, _PartitionInfo] = {
            partition: _PartitionInfo(partition.offset) for partition in partitions
        }
        self._held_messages: list[confluent_kafka.Message] = []
        self._batch_size: int = batch_size
        self._max_held_messages: int = max_held_messages
        self._low_water_mark_ns: int = UTC_EPOCH.value
        self._paused: int = 0
        self._metrics: ConsumerMetrics = ConsumerMetrics()

    @staticmethod
    def create(
        consumer_config: dict[str, Any],
        source_topics: list[SourceTopic],
        batch_size: int,
        timeout: Optional[float],
    ) -> "_ConsumerManager":
        if not consumer_config.get("enable.partition.eof"):
            raise ValueError("'enable.partition.eof' should be set to true")
        consumer = confluent_kafka.Consumer(consumer_config)
        cutoff = pd.Timestamp.utcnow()
        offsets = _resolve_topics_offsets(consumer, source_topics, cutoff, timeout)
        consumer.assign(offsets)
        return _ConsumerManager(cutoff, offsets, consumer, batch_size, batch_size * 5)

    def poll(self, timeout: float) -> list[confluent_kafka.Message]:
        new_messages = _poll_all(
            self._consumer,
            timeout,
            max(self._batch_size, self._max_held_messages - len(self._held_messages)),
        )
        self._metrics.consumed_message_count += len(new_messages)
        self._metrics.consumed_message_size += sum(len(m.value()) for m in new_messages)

        self._held_messages.extend(new_messages)
        self._held_messages.sort(key=_get_message_ns)

        self._update_partition_info(new_messages)

        limit = (
            self._batch_size
            if self._low_water_mark_ns >= self._cutoff_ns and self._paused == 0
            else self._get_limit(self._low_water_mark_ns)
        )
        self._held_messages, released_messages = (
            self._held_messages[limit:],
            self._held_messages[:limit],
        )
        self._managed_paused_partitions()

        self._metrics.released_message_count += len(released_messages)
        self._metrics.held_message_count = len(self._held_messages)
        return released_messages

    def flush_metrics(self) -> ConsumerMetrics:
        results = self._metrics
        self._metrics = ConsumerMetrics()
        return results

    def _get_limit(self, watermark: int) -> int:
        # TODO: do a binary search / use bisect once it supports key
        for i, message in enumerate(self._held_messages):
            timestamp = _get_message_ns(message)
            if timestamp > watermark or i >= self._batch_size:
                return i
        return self._batch_size

    def _get_priming_watermark(self) -> Optional[pd.Timestamp]:
        if self._low_water_mark_ns < self._cutoff_ns:
            return pd.to_datetime(self._low_water_mark_ns, utc=True)
        else:
            return None

    def _managed_paused_partitions(self):
        to_pause = []
        to_resume = []

        for tp, info in self._partition_info.items():
            if (
                len(self._held_messages) >= self._max_held_messages
                and not info.paused
                and info.timestamp_ns > self._low_water_mark_ns
            ):
                logger.debug(
                    "Pausing %s:%d %s",
                    tp.topic,
                    tp.partition,
                    pd.to_datetime(info.timestamp_ns, utc=True),
                )
                to_pause.append(tp)
                info.paused = True
            elif info.paused and info.timestamp_ns <= self._low_water_mark_ns:
                logger.debug(
                    "Resuming %s:%d %s",
                    tp.topic,
                    tp.partition,
                    pd.to_datetime(info.timestamp_ns, utc=True),
                )
                to_resume.append(tp)
                info.paused = False
        if to_pause:
            self._consumer.pause(to_pause)
        if to_resume:
            self._consumer.resume(to_resume)
        self._paused = sum(v.paused for v in self._partition_info.values())
        self._metrics.paused_partitions = self._paused

    def _update_partition_info(self, new_messages: list[confluent_kafka.Message]):
        for message in new_messages:
            topic_partition = confluent_kafka.TopicPartition(
                message.topic(), message.partition()
            )
            partition_info: _PartitionInfo = self._partition_info[topic_partition]
            timestamp_type, timestamp = message.timestamp()
            if timestamp_type == confluent_kafka.TIMESTAMP_NOT_AVAILABLE:
                if (
                    message.error() is not None
                    and message.error().code() == KAFKA_EOF_CODE
                ):
                    partition_info.eof = True
            else:
                partition_info.timestamp_ns = timestamp * 1_000_000
                partition_info.eof = False
            partition_info.current_offset = message.offset()
            if partition_info.timestamp_ns >= self._cutoff_ns:
                partition_info.primed = True
        self._low_water_mark_ns = min(
            (v.timestamp_ns for v in self._partition_info.values() if not v.eof),
            default=pd.Timestamp.utcnow().value,
        )


@dataclasses.dataclass(frozen=True)
class _RuntimeSinkTopic:
    nodes: list[Node]
    serializer: KafkaMessageSerializer

    def flush(self, cycle_id: int, producer_manger: _ProducerManager):
        for node in self.nodes:
            if node.get_cycle_id() == cycle_id:
                node_value = node.get_sink_value()
                # TODO: capture serialization time in metrics
                messages = self.serializer(node_value)
                for message in messages:
                    producer_manger.produce_one(
                        message.topic, message.key, message.value
                    )


@dataclasses.dataclass
class ExecutionMetrics:
    serialization_ns: int = 0
    serialization_count: int = 0
    execution_ns: int = 0
    execution_count: int = 0

    @contextlib.contextmanager
    def measure_serialization_time(self):
        before = time.time_ns()
        try:
            yield
        finally:
            self.serialization_ns += time.time_ns() - before
            self.serialization_count += 1

    @contextlib.contextmanager
    def measure_execution_time(self):
        before = time.time_ns()
        try:
            yield
        finally:
            self.execution_ns += time.time_ns() - before
            self.execution_count += 1


class KafkaDriver:
    def __init__(
        self,
        dag: Dag,
        runtime_source_topics: list[_RuntimeSourceTopic],
        runtime_sink_topics: list[_RuntimeSinkTopic],
        consumer_manager: _ConsumerManager,
        producer_manager: _ProducerManager,
    ):
        self._dag = dag
        self._source_topics: dict[str, _RuntimeSourceTopic] = {
            runtime_source_topic._topic_name: runtime_source_topic
            for runtime_source_topic in runtime_source_topics
        }
        self._consumer_manager = consumer_manager
        self._sink_topics = runtime_sink_topics
        self._producer_manager = producer_manager
        self._metrics = ExecutionMetrics()

    @staticmethod
    def create(
        dag: Dag,
        producer_config: dict[str, Any],
        consumer_config: dict[str, Any],
        source_topics: dict[str, SourceTopic],
        sink_topics: dict[str, KafkaMessageSerializer],
        batch_size: int = 5_000,
    ) -> "KafkaDriver":
        source_nodes = dag.get_sources()
        assert sorted(source_nodes.keys()) == sorted(source_topics.keys()), (
            source_nodes.keys(),
            source_topics.keys(),
        )
        runtime_source_topics = [
            _RuntimeSourceTopic(
                source_topic.name,
                source_nodes[name],
                source_topic.message_deserializer,
            )
            for name, source_topic in source_topics.items()
        ]
        consumer_manager = _ConsumerManager.create(
            consumer_config=consumer_config,
            source_topics=list(source_topics.values()),
            batch_size=batch_size,
            timeout=10.0,
        )

        dag_sinks = dag.get_sinks()
        runtime_sink_topics = [
            _RuntimeSinkTopic(dag_sinks[key], value)
            for key, value in sink_topics.items()
        ]
        producer_manager = _ProducerManager(producer_config)
        return KafkaDriver(
            dag=dag,
            runtime_source_topics=runtime_source_topics,
            runtime_sink_topics=runtime_sink_topics,
            consumer_manager=consumer_manager,
            producer_manager=producer_manager,
        )

    def run(self):
        while True:
            self.run_cycle()

    def flush_metrics(self) -> ExecutionMetrics:
        results = self._metrics
        self._metrics = ExecutionMetrics()
        return results

    def run_cycle(self, poll_for_seconds: float = 1.0) -> bool:
        messages = self._consumer_manager.poll(poll_for_seconds)

        if self._run_cycle(messages):
            self._produce_records(self._dag.get_cycle_id())
            self._producer_manager.poll()
            return True
        else:
            self._producer_manager.poll()
            return False

    def _process_messages(self, messages: list[confluent_kafka.Message]):
        for message in messages:
            self._process_message(message)

    def _process_message(self, message: confluent_kafka.Message):
        if message.error() is None:
            self._source_topics[message.topic()].append(message)

    def _produce_records(self, cycle_id: int):
        for sink_topic in self._sink_topics:
            sink_topic.flush(cycle_id, self._producer_manager)

    def _run_cycle(self, messages: list[confluent_kafka.Message]) -> bool:
        has_messages = False
        with self._metrics.measure_serialization_time():
            self._process_messages(messages)
            for handler in self._source_topics.values():
                has_messages = handler.flush() or has_messages
        cycle_time = (
            self._consumer_manager._get_priming_watermark() or pd.Timestamp.utcnow()
        )

        if has_messages or self._dag.get_next_timer() <= cycle_time:
            with self._metrics.measure_execution_time():
                self._dag.execute(cycle_time)
                logger.debug(
                    "Ran cycle cycle_id=%d, messages=%d, time=%s, next_timer=%s",
                    self._dag.get_cycle_id(),
                    len(messages),
                    cycle_time,
                    self._dag.get_next_timer(),
                )
            return True
        else:
            return False


def _resolve_topics_offsets(
    consumer: confluent_kafka.Consumer,
    source_topics: list[SourceTopic],
    now: pd.Timestamp,
    timeout: Optional[float] = None,
) -> list[confluent_kafka.TopicPartition]:
    assignments: list[confluent_kafka.TopicPartition] = []
    for source_topic in source_topics:
        assignments.extend(_resolve_topic_offsets(consumer, source_topic, now, timeout))
    return assignments


def _resolve_topic_offsets(
    consumer: confluent_kafka.Consumer,
    source_topic: SourceTopic,
    now: pd.Timestamp,
    timeout: Optional[float] = None,
) -> list[confluent_kafka.TopicPartition]:
    cluster_metadata: confluent_kafka.admin.ClusterMetadata = consumer.list_topics(
        source_topic.name, timeout
    )
    topic_meta_data: confluent_kafka.admin.TopicMetadata = cluster_metadata.topics[
        source_topic.name
    ]
    if len(topic_meta_data.partitions) == 0:
        raise ValueError(f"Topic {source_topic.name} does not exist")

    if source_topic.offset_policy == OffsetPolicy.LATEST:
        return [
            confluent_kafka.TopicPartition(
                topic=source_topic.name,
                partition=p.id,
                offset=confluent_kafka.OFFSET_END,
            )
            for p in topic_meta_data.partitions.values()
        ]
    elif source_topic.offset_policy == OffsetPolicy.EARLIEST:
        return [
            confluent_kafka.TopicPartition(
                topic=source_topic.name,
                partition=p.id,
                offset=confluent_kafka.OFFSET_BEGINNING,
            )
            for p in topic_meta_data.partitions.values()
        ]
    elif source_topic.offset_policy == OffsetPolicy.RELATIVE_TIME:
        offset_timestamp = now - source_topic.relative_time
        offset_ms = offset_timestamp.value // 1_000_000
        return consumer.offsets_for_times(
            [
                confluent_kafka.TopicPartition(
                    topic=source_topic.name, partition=p.id, offset=offset_ms
                )
                for p in topic_meta_data.partitions.values()
            ],
            timeout,
        )
    elif source_topic.offset_policy == OffsetPolicy.START_OF_DAY:
        offset_timestamp = _get_previous_start_of_day(
            now, source_topic.start_of_day_time, source_topic.start_of_day_timezone
        )
        offset_ms = offset_timestamp.value // 1_000_000
        return consumer.offsets_for_times(
            [
                confluent_kafka.TopicPartition(
                    topic=source_topic.name, partition=p.id, offset=offset_ms
                )
                for p in topic_meta_data.partitions.values()
            ],
            timeout,
        )
    elif source_topic.offset_policy == OffsetPolicy.ABSOLUTE_TIME:
        offset_ms = source_topic.absolute_time.value // 1_000_000
        return consumer.offsets_for_times(
            [
                confluent_kafka.TopicPartition(
                    topic=source_topic.name, partition=p.id, offset=offset_ms
                )
                for p in topic_meta_data.partitions.values()
            ],
            timeout,
        )
    elif source_topic.offset_policy == OffsetPolicy.COMMITTED:
        return [
            confluent_kafka.TopicPartition(
                topic=source_topic.name,
                partition=p.id,
                offset=confluent_kafka.OFFSET_STORED,
            )
            for p in topic_meta_data.partitions.values()
        ]
    else:
        raise ValueError(
            f"{OffsetPolicy.__name__} {source_topic.offset_policy}"
            f" not supported for {source_topic.name}"
        )


def _get_previous_start_of_day(
    now: pd.Timestamp, start_of_day_time: pd.Timedelta, start_of_day_timezone: str
) -> pd.Timestamp:
    local_now = now.tz_convert(start_of_day_timezone)
    if (local_now - local_now.normalize()) > start_of_day_time:
        return (local_now.normalize() + start_of_day_time).tz_convert("UTC")
    else:
        # TODO: consider adding calendar?
        return (
            local_now.normalize() - pd.to_timedelta("1d") + start_of_day_time
        ).tz_convert("UTC")


def _poll_all(
    consumer: confluent_kafka.Consumer, timeout_second: float, limit: int
) -> list[confluent_kafka.Message]:
    messages = []
    first_message = consumer.poll(timeout_second)
    if first_message is not None:
        messages.append(first_message)
        while len(messages) < limit:
            message = consumer.poll(0.0)
            if message is None:
                break
            else:
                messages.append(message)
    return messages


def _get_message_ns(message: confluent_kafka.Message) -> int:
    timestamp_type, timestamp = message.timestamp()
    if timestamp_type == confluent_kafka.TIMESTAMP_NOT_AVAILABLE:
        return UTC_MAX.value
    else:
        return timestamp * 1_000_000
