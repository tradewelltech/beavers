import json

import click
from examples.perspective_concepts import run_dashboard


@click.command()
@click.option("--topic", type=click.STRING, default="key-value")
@click.option("--port", type=click.INT, default=8082)
@click.option(
    "--consumer-config",
    type=json.loads,
    default='{"bootstrap.servers": "localhost:9092", "group.id": "beavers"}',
)
def perspective_test_bench(
    topic: str,
    port: int,
    consumer_config: dict,
):
    run_dashboard(topic=topic, port=port, consumer_config=consumer_config)


if __name__ == "__main__":
    perspective_test_bench()
