# Scripts

## `kafka_test_bench`

Tests a simple application with kafka, making sure it replays in order.
The "timestamp" of the output messages should be in order across topics when replaying.


Helpful commands:

```shell
docker run -p 9092:9092 -d bashj79/kafka-kraft
kafka-topics --create --topic left --bootstrap-server=localhost:9092
kafka-topics --create --topic right --bootstrap-server=localhost:9092
kafka-topics --create --topic both --bootstrap-server=localhost:9092
kafka-console-producer --topic left --bootstrap-server=localhost:9092
kafka-console-producer --topic right --bootstrap-server=localhost:9092
kafka-console-consumer \
    --topic=both \
    --bootstrap-server=localhost:9092 \
    --property print.key=true
python -m scripts.kafka_test_bench --batch-size=2
```
