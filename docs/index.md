![Beavers Logo][5]

# Beavers

[Beavers][1] is a python library for stream processing, optimized for analytics. 

It is used at [Tradewell Technologies][2], 
to calculate analytics and serve model predictions,
in both realtime and batch jobs.

## Key Features

- Works in **real time** (eg: reading from kafka) and **replay mode** (eg: reading from parquet)
- Optimized for analytics, it uses micro-batching (instead of processing records one by one)
- Similar to [incremental][3], it  updates nodes in a dag incrementally
- Taking inspiration from [kafka streams][4], there are two types of nodes in the dag:
    * **Stream**: ephemeral micro-batches of events (cleared after every cycle)
    * **State**: durable state derived from streams
- Clear separation between the business logic and the IO. 
  So the same dag can be used in real time mode, replay mode or can be easily tested.
- Functional interface: no inheritance or decorator required

[1]: https://github.com/tradewelltech/beavers
[2]: https://www.tradewelltech.co/
[3]: https://github.com/janestreet/incremental
[4]: https://www.confluent.io/blog/kafka-streams-tables-part-1-event-streaming/
[5]: ./static/icons/beavers/logo.svg
