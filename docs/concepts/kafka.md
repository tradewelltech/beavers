# Live with Kafka

This section explains how to run a beavers application in real time using kafka.

## Count Word Example

Starting with a simple "count word" dag with one source going to one sink:

```python
--8<-- "examples/kafka_concepts.py:dag"
```

This dag has got a source node called `words` and a sink node called `counts`

## Defining Kafka Source

We will be receiving data from kafka, on a topic called `words`.

First we need to define how we deserialize messages coming from kafka:

```python
--8<-- "examples/kafka_concepts.py:deserializer"
```

Then, we put together the `SourceTopic` with its:

- topic (`words`)
- deserializer (`deserialize_messages`)
- replay policy (`from_latest`) 

```python
--8<-- "examples/kafka_concepts.py:kafka_source"
```

There are multiple kafka replay policy available, see the api doc for the full list.

## Defining Kafka Sink

We will be sending the results to the `counts` topic. 
The key will be the word.T The value will be the latest count.

First we need to define a serializer, which converts each count to a `KafkaProducerMessage`

```python
--8<-- "examples/kafka_concepts.py:serializer"
```

The serializer is responsible for providing the topic for each outgoing message. 

## Putting it together with KafkaDriver

The `KafkaDriver` takes care of creating the kafka producer and consumer, and passing the message through:

```python
--8<-- "examples/kafka_concepts.py:kafka_driver"
```

## Beavers Kafka Features

- One consumer: There is only one consumer (rather than one consumer for each topic)
- One producer: There is only one producer (rather than one producer for each topic)
- When polling messages, beavers tries to read all available messages, up to a limit of `batch_size=5000` (which is configurable in the KafkaDriver)
- When replaying past data, beavers orchestrate topic/partition so data is replayed in order, across topics, based on each message timestamp.
- When replaying past data, some newer messages have to be held. 
  To avoid memory issue, the number of held messages is capped to `batch_size*5`.
  Once the number of held messages get to high, partitions that are ahead of the watermark are paused.
  These partitions are un-paused once the application catches up


## Beavers Kafka Limitations

- One beavers application consumes every partition for requested topics (no load balancing/scaling) 
