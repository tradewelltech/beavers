
# Source Streams 

A source stream is a node whose value can be set externally.

When `Dag.execute` is called, the updated value is propagated in the dag

```python
--8<-- "examples/dag_concepts.py:source_stream"
```

If the dag is executed again, the value of the source stream will be reset to it's empty value.

```python
--8<-- "examples/dag_concepts.py:source_stream_again"
```
