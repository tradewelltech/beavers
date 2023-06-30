
# Dag

## Stream Source

A stream source is a node whose value can be set externally.

When `Dag.execute` is called, the updated value is propagated in the dag

```python
--8<-- "examples/dag_concepts.py:source_stream"
```

If the dag is executed again, the value of the source stream will be reset to its empty value.

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

If the dag is executed again, the value of the stream node will be reset to its empty value.

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

Or a callable:
```python
--8<-- "examples/dag_concepts.py:stream_node_callable"
```

## State Node

A state node retain its value from one dag execution to the next, even if it didn't update:
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
Stream node can be connected to state node, and vice versa.
Same thing applies to const nodes.

> :warning: The `map` function doesn't execute the underlying node. 
> Instead it adds a node to the dag

The map function can use positional arguments:

```python
--8<-- "examples/dag_concepts.py:map_positional"
```
Or key word arguments:

```python
--8<-- "examples/dag_concepts.py:map_key_word"
```

## Update Propagation

- Nodes are notified if any of their input node was updated during the current execution cycle
```python
--8<-- "examples/dag_concepts.py:propagate_any"
```
- You can check if a node updated by looking at it "cycle_id"
```python
--8<-- "examples/dag_concepts.py:propagate_cycle_id"
```
- If several inputs of a node get updated during the same cycle, the node will be executed once (and not once per input)
```python
--8<-- "examples/dag_concepts.py:propagate_both"
```
- Stream nodes (and sources) are not considered updated if their output is empty
```python
--8<-- "examples/dag_concepts.py:propagate_empty"
```
