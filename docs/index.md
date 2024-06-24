![Beavers Logo][5]

# Beavers

[Documentation][6] / [Installation][7] / [Repository][1] / [PyPI][8]

[Beavers][1] is a python library for stream processing, optimized for analytics.

It is used at [Tradewell Technologies][2],
to calculate analytics and serve model predictions,
for both realtime and batch jobs.

## Key Features

- Works in **real time** (eg: reading from Kafka) and **replay mode** (eg: reading from Parquet files).
- Optimized for analytics, using micro-batches (instead of processing records one by one).
- Similar to [incremental][3], it updates nodes in a dag incrementally.
- Taking inspiration from [kafka streams][4], there are two types of nodes in the dag:
    - **Stream**: ephemeral micro-batches of events (cleared after every cycle).
    - **State**: durable state derived from streams.
- Clear separation between the business logic and the IO.
  So the same dag can be used in real time mode, replay mode or can be easily tested.
- Functional interface: no inheritance or decorator required.
- Support for complicated joins, not just "linear" data flow.

## Limitations

- No concurrency support.
  To speed up calculation use libraries like pandas, pyarrow or polars.
- No async code.
  To speed up IO use kafka driver native thread or parquet IO thread pool.
- No support for persistent state.
  Instead of saving state, replay historic data from kafka to prime stateful nodes.

## Talks

- [Unified batch and stream processing in python | PyData Global 2023][9]

[1]: https://github.com/tradewelltech/beavers
[2]: https://www.tradewelltech.co/
[3]: https://github.com/janestreet/incremental
[4]: https://www.confluent.io/blog/kafka-streams-tables-part-1-event-streaming/
[5]: https://raw.githubusercontent.com/tradewelltech/beavers/master/docs/static/icons/beavers/logo.svg
[6]: https://beavers.readthedocs.io/en/latest/
[7]: https://beavers.readthedocs.io/en/latest/install/
[8]: https://pypi.org/project/beavers/
[9]: https://www.youtube.com/watch?v=8pUwsGA8SQM
