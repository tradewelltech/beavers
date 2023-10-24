# Pandas integration

This section explains how to use beavers with pandas.

## ETF value calculation example

In this example we want to calculate the value of ETFs.
If you are not familiar with ETFs, think about them as just a basket of shares.

Starting with a table of individual share prices:
```python
--8<-- "examples/pandas_concepts.py:business_logic_price"
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
--8<-- "examples/pandas_concepts.py:business_logic_composition"
```

| etf   | ticker   |   quantity |
|:------|:---------|-----------:|
| TECH  | AAPL     |        2.0 |
| TECH  | GOOGL    |        2.0 |
| TECH  | MSFT     |        1.0 |
| CARS  | F        |        3.0 |
| CARS  | GM       |        1.0 |

In a few line of `pandas` we can derive the value of each ETF:
```python
--8<-- "examples/pandas_concepts.py:business_logic_calculation"
```

| etf  |   value |
|:-----|--------:|
| TECH |  927.09 |
| CARS |   72.57 |

## ETF value calculation DAG

Once the business logic of the calculation is writen and tested it can be added into a Dag.
We'll be using the Dag `pd` helper which makes it easier to deal with `pandas` table in beavers.

First we define two source streams, made of `pandas.DataFrame`:
```python
--8<-- "examples/pandas_concepts.py:dag_source"
```

Then we keep track of the latest value for each source stream:
```python
--8<-- "examples/pandas_concepts.py:dag_state"
```

Lastly we put together the share prices and ETF composition:
```python
--8<-- "examples/pandas_concepts.py:dag_calculation"
```

And that's it:

```python
--8<-- "examples/pandas_concepts.py:dag_test"
```
