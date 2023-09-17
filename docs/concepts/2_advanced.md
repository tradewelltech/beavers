# Advanced 

This section discuss advanced features that control how updates propagate in the DAG.

## How updates propagate in the DAG

- Nodes are notified if any of their input node was updated during the current execution cycle
```python
--8<-- "examples/advanced_concepts.py:propagate_any"
```
- You can check if a node updated by looking at its `cycle_id`
```python
--8<-- "examples/advanced_concepts.py:propagate_cycle_id"
```
- If several inputs of a node get updated during the same cycle, the node will be executed once (and not once per input)
```python
--8<-- "examples/advanced_concepts.py:propagate_both"
```
- Stream nodes (and sources) are not considered updated if their output is empty
```python
--8<-- "examples/advanced_concepts.py:propagate_empty"
```


## Now node

Beavers can be used in both `live` and `replay` mode. 
In `replay` mode, the wall clock isn't relevant. 
To access the current time of the replay, you should use the now node:

```python
--8<-- "examples/advanced_concepts.py:now_node"
```

The now node is shared for the whole DAG.
Its value gets updated silently. 

## TimerManager

To be notified when time passes, nodes can subscribe to a `TimerManager` node.

```python
--8<-- "examples/advanced_concepts.py:timer_manager"
```

## Silent updates

Some node may update too often, or their updates may not be relevant to other nodes.
In this case it's possible to silence them:

```python
--8<-- "examples/advanced_concepts.py:silence"
```

`silence` returns a new silenced node (rather than modify the existing node)

## Value Cutoff

By default, state nodes will update everytime they are notified. 
The framework doesn't check that their value has changed.

You can add a cutoff, to prevent updates when the value hasn't changed:

```python
--8<-- "examples/advanced_concepts.py:cutoff"
```

You can also provide a custom comparator to allow some tolerance when deciding if a value has changed:

```python
--8<-- "examples/advanced_concepts.py:cutoff_custom"
```
