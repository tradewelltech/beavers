
[![PyPI Version][pypi-image]][pypi-url]
[![Python Version][versions-image]][versions-url]
[![Github Stars][stars-image]][stars-url]
[![codecov][codecov-image]][codecov-url]
[![Build Status][build-image]][build-url]
[![Documentation][doc-image]][doc-url]
[![License][license-image]][license-url]
[![Downloads][downloads-image]][downloads-url]
[![Downloads][downloads-month-image]][downloads-month-url]
[![Code style: black][codestyle-image]][codestyle-url]

# Beavers
_____

**Beavers** is a python library for stream processing, optimize for analytics. 


It is used at [Tradewell Technologies](https://www.tradewelltech.co/), 
to calculate analytics and serve model predictions,
in both realtime and batch jobs.

# Key Features


* Works in **real time** (eg: reading from kafka) and **replay mode** (eg: reading from parquet)
* Optimized for analytics, it uses micro-batching (instead of processing records one by one)
* Similar to [incremental](https://github.com/janestreet/incremental), it  updates nodes in a dag incrementally
* Taking inspiration from [kafka streams](https://www.confluent.io/blog/kafka-streams-tables-part-1-event-streaming/), there are two types of nodes in the dag:
  * Stream: ephemeral micro-batches of events (cleared after every cycle)
  * State: durable state derived from streams
* Clear separation between the business logic and the IO. 
  So the same dag can be used in real time mode, replay mode or can be easily tested.


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
