# Pyarrow integration

This section explains how to a beavers with pyarrow.

## ETF value calculation example

In this example we want to calculate the value of an ETF.
If you are not familiar with ETFs, think about them as just a basket of shares.

Starting with a table of prices:
```python
--8<-- "examples/pyarrow_concepts.py:business_logic_price"
```

| ticker   |   price |
|:---------|--------:|
| AAPL     |  174.79 |
| GOOGL    |  130.25 |
| MSFT     |  317.01 |
| F        |   12.43 |
| GM       |   35.28 |

And another table containing the composition of each ETF:
```python
--8<-- "examples/pyarrow_concepts.py:business_logic_composition"
```

| etf   | ticker   |   quantity |
|:------|:---------|-----------:|
| TECH  | AAPL     |        2.0 |
| TECH  | GOOGL    |        2.0 |
| TECH  | MSFT     |        1.0 |
| CARS  | F        |        3.0 |
| CARS  | GM       |        1.0 |

In a few line of `pyarrow` we can derive the value of each ETF:
```python
--8<-- "examples/pyarrow_concepts.py:business_logic_calculation"
```

| etf  |   value |
|:-----|--------:|
| TECH |  927.09 |
| CARS |   72.57 |

## ETF value calculation DAG

Once the business logic of the calculation is writen and tested it can be added into a Dag.
We'll be using the Dag `pa` helper which makes it easier to deal with `pyarrow` table in beavers.

First we define two source streams, made of `pyarrow.Table`:
```python
--8<-- "examples/pyarrow_concepts.py:dag_source"
```

Then we keep track of the latest value for each source stream:
```python
--8<-- "examples/pyarrow_concepts.py:dag_state"
```

Lastly we put together the prices and etf composition state:
```python
--8<-- "examples/pyarrow_concepts.py:dag_calculation"
```

And that's it:

```python
--8<-- "examples/pyarrow_concepts.py:dag_test"
```


## Taming updates

This dag does the job of calculating the etf value in real time.
But there is one issue.
The value of every ETFs would update every time either the `price` or the `etf_composition` source update.
Even if the updates comes on a ticker that is not relevant to the ETFs we are tracking. 

In the example below, when the price of GameStop updates, we recalculate the value of every ETFs.
Even though their value hasn't changed:
```python
--8<-- "examples/pyarrow_concepts.py:spurious_update"
```

To tame update we need to identify which ETF needs updating.

ETF values can update because their composition has changed:
```python
--8<-- "examples/pyarrow_concepts.py:updated_because_of_composition"
```

Or because one of their component has updated: 
```python
--8<-- "examples/pyarrow_concepts.py:updated_because_of_price"
```

We can then put it back together and only calculate updates for relevant symbols:
```python
--8<-- "examples/pyarrow_concepts.py:update_all"
```


And see that only the value TECH ETF updates when a tech stock update:
```python
--8<-- "examples/pyarrow_concepts.py:update_all_test"
```

| etf   |   value |
|:------|--------:|
| TECH  |  927.13 |
