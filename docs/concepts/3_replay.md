# Replay

This section explains how you can run a dag using historical data, typically stored in files or databases.

## Manual Replay

Starting with a simple data with one source going to one sink:

```python
--8<-- "examples/replay_concepts.py:simple_dag"
```

Assuming your data has got this shape:
```python
--8<-- "examples/replay_concepts.py:simple_data_class"
```

You could replay the data manually your self and run the dag for regular interval:
```python
--8<-- "examples/replay_concepts.py:manual_replay"
```

But this requires a lot of boilerplate code and becomes cumbersome very quickly. 

## Replay Framework

The replay framework uses a few key abstraction in order to define how the data is loaded and injected in the dag.

### `DataSource`

A `DataSource` provides a way of streaming data. 
```python
--8<-- "examples/replay_concepts.py:data_source"
```

By convention, `DataSource`s:

- return `UTC_MAX` when there is no more data
- are stateful and need to remember what has already been read. 

### `ReplayContext`

The `ReplayContext` contains timing information:
```python
--8<-- "examples/replay_concepts.py:replay_context"
```

:warning: By convention all timestamps are UTC


### `DataSourceProvider`

A `DataSourceProvider` provides a way of creating `DataSource`.

Assuming the data is stored in a csv file:

```csv
timestamp,message
2023-01-01 01:00:00+00:00,Hello
2023-01-01 01:01:00+00:00,How are you
```

Provided with the `ReplayContext`, it will load the and return a `DataSource`
 
```python
--8<-- "examples/replay_concepts.py:data_source_provider"
```


### `DataSink`

A `DataSink` provides a way of capturing the output of nodes and saving the data:

 
```python
--8<-- "examples/replay_concepts.py:data_sink"
```

### `DataSinkProvider`

A `DataSinkProvider` provides a way of creating `DataSink`.

In this example we save the data to csv:

 
```python
--8<-- "examples/replay_concepts.py:data_sink_provider"
```


### `ReplayDriver`

The replay driver is responsible for putting the dag, context, sources and sinks together, and orchestrate the replay.

```python
--8<-- "examples/replay_concepts.py:replay_driver"
```


## Reading Files Partitioned By Time

Assuming:

- you want to replay a dag for a long period of time.
- all that historic data doesn't fit into time
- the data is partitioned by time period. For example one file per day, `input_2023-01-01.csv`.

It's then possible, with the `IteratorDataSourceAdapter` to load each file one by one as they are needed.

In this example, csv files are stored under . We need to provide:

- a generator that will yield a `DataSource` for each file, in order
- a way to concatenate the output of 2 `DataSource`. In this case we'll use `+` to merge two lists
- an empty value for the case there is no more data, or we reach the last file.

```python
--8<-- "examples/replay_concepts.py:iterator_data_source_adapter"
```
