
# DAG

At its core, `beavers` executes a Directed Acyclic Graph (DAG), where each node is a python function.   
This section discuss the different type of nodes in the DAG.

## Stream Source

A stream source is a node whose value can be set externally.

When `Dag.execute` is called, the updated value is propagated in the DAG

```python
--8<-- "examples/dag_concepts.py:source_stream"
```

If the DAG is executed again, the value of the source stream will be reset to its empty value.

```python
--8<-- "examples/dag_concepts.py:source_stream_again"
```

The default empty value is set to `[]`, but it can be customized:

```python
--8<-- "examples/dag_concepts.py:source_stream_empty"
```

A source stream can be given a name, so they can be retrieved (and their value set):

```python
--8<-- "examples/dag_concepts.py:source_stream_name"
```

## Stream Node

A stream node uses the output of other nodes to calculate its updated value. 

```python
--8<-- "examples/dag_concepts.py:stream_node"
```

If the DAG is executed again, the value of the stream node will be reset to its empty value.

```python
--8<-- "examples/dag_concepts.py:stream_node_again"
```

Again, the default empty value is set to `[]`, but it can be customized:
```python
--8<-- "examples/dag_concepts.py:stream_node_empty"
```

The function provided to the node can be any callable, like a lambda:
```python
--8<-- "examples/dag_concepts.py:stream_node_lambda"
```

Or a class defining `__call__`:
```python
--8<-- "examples/dag_concepts.py:stream_node_callable"
```

## State Node

A state node retains its value from one DAG execution to the next, even if it didn't update:
```python
--8<-- "examples/dag_concepts.py:state_node"
```

## Const Node

A const node is a node whose value doesn't change.
```python
--8<-- "examples/dag_concepts.py:const_node"
```

## Connecting Nodes (aka `map`)

Nodes are connected by calling the `map` function. 
Stream nodes can be connected to state nodes, stream nodes or const nodes, and vice versa.

> :warning: The `map` function doesn't execute the underlying node. 
> Instead it adds a node to the DAG

The map function can use positional arguments:

```python
--8<-- "examples/dag_concepts.py:map_positional"
```
Or key word arguments:

```python
--8<-- "examples/dag_concepts.py:map_key_word"
```
