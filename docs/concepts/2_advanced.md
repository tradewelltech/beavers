# Advanced 

## Now node

Beavers can be used in both live and replay mode. 
In replay mode, the wall clock isn't relevant. 
To access the current time of the replay, you should use the now node:

```python
--8<-- "examples/advanced_concepts.py:now_node"
```

The now node is shared for the whole dag.
Its value gets updated silently. 

## TimerManager

To be notified when a time passes, nodes can subscribe to a `TimerManager` node.

```python
--8<-- "examples/advanced_concepts.py:timer_manager"
```

## Silent update

Some node myy update too often, or their updates may not be relevant to other nodes.
In this case it's possible to silence them:

```python
--8<-- "examples/advanced_concepts.py:silence"
```
