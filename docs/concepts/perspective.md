# Perspective Integration

This section explains how to build a live web dashboard with [Perspective](https://github.com/finos/perspective) and Beavers.

In Beavers, you can connect any node of type `pyarrow.Table` to a perspective table.
All you need to do is call `dag.psp.to_perspecive`, and provide a `PerspectiveTableDefinition`.


## Key Value Example

We'll write a super simple key-value store application.
It listens to a topic, and displays the value of kafka messages by key, with their timestamp

## Install

```shell
pip install beavers[pyarrow, perpective-python]
```

## Defining the schema of incoming message

First we define a schema for the incoming "key value" messages:

- a timestamp, in millis
- a key (string)
- a value (string)

```python
--8<-- "examples/perspective_concepts.py:schema"
```

## Convert kafka messages to arrow Table

Then we write a function that converts kafka messages to an apache arrow table of "key value" messages:

```python
--8<-- "examples/perspective_concepts.py:converter"
```


## Create a dag

We create a super simple dag. 
It has a source, called `key_value`, which is a table of "key value" messages.
The source is plugged into a perspective table, called... `key_value`, whose index is the `key` column

```python
--8<-- "examples/perspective_concepts.py:dag"
```

## Run the dashboard

Lastly, we put everything together in an application
```python
--8<-- "examples/perspective_concepts.py:run"
```

You should be able to see it in http://localhost:8082/key_value
