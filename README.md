
[![PyPI Version][pypi-image]][pypi-url]
[![Python Version][versions-image]][versions-url]
[![Github Stars][stars-image]][stars-url]
[![codecov][codecov-image]][codecov-url]
[![Build Status][build-image]][build-url]
[![Documentation][doc-image]][doc-url]
[![License][license-image]][license-url]
[![Downloads][downloads-image]][downloads-url]
[![Downloads][downloads-month-image]][downloads-month-url]
[![pydocstyle][pydocstyle-image]][pydocstyle-url]
[![Checked with mypy][mypy-image]][mypy-url]
[![Ruff][ruff-image]][ruff-url]

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

[pypi-image]: https://img.shields.io/pypi/v/beavers
[pypi-url]: https://pypi.org/project/beavers/
[build-image]: https://github.com/tradewelltech/beavers/actions/workflows/ci.yaml/badge.svg
[build-url]: https://github.com/tradewelltech/beavers/actions/workflows/ci.yaml
[stars-image]: https://img.shields.io/github/stars/tradewelltech/beavers
[stars-url]: https://github.com/tradewelltech/beavers
[versions-image]: https://img.shields.io/pypi/pyversions/beavers
[versions-url]: https://pypi.org/project/beavers/
[doc-image]: https://readthedocs.org/projects/beavers/badge/?version=latest
[doc-url]: https://beavers.readthedocs.io/en/latest/?badge=latest
[license-image]: http://img.shields.io/:license-Apache%202-blue.svg
[license-url]: https://github.com/tradewelltech/beavers/blob/main/LICENSE
[codecov-image]: https://codecov.io/gh/tradewelltech/beavers/branch/main/graph/badge.svg?token=GY6KL7NT1Q
[codecov-url]: https://codecov.io/gh/tradewelltech/beavers
[downloads-image]: https://pepy.tech/badge/beavers
[downloads-url]: https://static.pepy.tech/badge/beavers
[downloads-month-image]: https://pepy.tech/badge/beavers/month
[downloads-month-url]: https://static.pepy.tech/badge/beavers/month
[codestyle-image]: https://img.shields.io/badge/code%20style-black-000000.svg
[codestyle-url]: https://github.com/ambv/black
[snyk-image]: https://snyk.io/advisor/python/beavers/badge.svg
[snyk-url]: https://snyk.io/advisor/python/beavers
[pydocstyle-image]: https://img.shields.io/badge/pydocstyle-enabled-AD4CD3
[pydocstyle-url]: http://www.pydocstyle.org/en/stable/
[mypy-image]: http://www.mypy-lang.org/static/mypy_badge.svg
[mypy-url]: http://mypy-lang.org/
[ruff-image]: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json
[ruff-url]: https://github.com/astral-sh/ruff
