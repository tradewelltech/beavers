# Scripts

These script are helpful for testing beavers with simple real time applications

## Set up

Use kafka-kraft in docker for kafka:

```shell
docker run --name=simple_kafka -p 9092:9092 -d bashj79/kafka-kraft
```

If it needs to be started again:

```shell
docker start simple_kafka
```

## `kafka_test_bench`

Tests a simple application with kafka, making sure it replays in order.
The "timestamp" of the output messages should be in order across topics when replaying.

### Create Topics

```shell
docker exec -it simple_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --create --topic=left --partitions=1 --replication-factor=1 
docker exec -it simple_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --create --topic=right --partitions=1 --replication-factor=1 
docker exec -it simple_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --create --topic=both --partitions=1 --replication-factor=1 
```

### Run the Beavers job

```shell
python -m scripts.kafka_test_bench --batch-size=2
```

### Publish data

```shell
docker exec -it simple_kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server=localhost:9092 --topic=left
docker exec -it simple_kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server=localhost:9092 --topic=right
```

### See out put data

```shell
docker exec -it simple_kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server=localhost:9092 \
  --topic=both \
  --property print.key=true \
  --from-beginning
```

## `perpective_test_bench.py`

### Create the topic

```shell
docker exec -it simple_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server=localhost:9092 --create --topic=key-value --partitions=1 --replication-factor=1 
```

### Publish data

```shell
docker exec -it simple_kafka /opt/kafka/bin/kafka-console-producer.sh \
  --topic=key-value \
  --bootstrap-server=localhost:9092 \
   --property parse.key=true \
   --property key.separator=, 
```
