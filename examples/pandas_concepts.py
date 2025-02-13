# ruff: noqa: E402
# isort: skip_file

# --8<-- [start:business_logic_price]
import pandas as pd

price_table = pd.DataFrame.from_records(
    [
        {"ticker": "AAPL", "price": 174.79},
        {"ticker": "GOOGL", "price": 130.25},
        {"ticker": "MSFT", "price": 317.01},
        {"ticker": "F", "price": 12.43},
        {"ticker": "GM", "price": 35.28},
    ],
)

price_dtypes = price_table.dtypes

# --8<-- [end:business_logic_price]

# print(price_table.to_pandas().to_markdown(index=False))

# --8<-- [start:business_logic_composition]
etf_composition_table = pd.DataFrame.from_records(
    [
        {"etf": "TECH", "ticker": "AAPL", "quantity": 2.0},
        {"etf": "TECH", "ticker": "GOOGL", "quantity": 2.0},
        {"etf": "TECH", "ticker": "MSFT", "quantity": 1.0},
        {"etf": "CARS", "ticker": "F", "quantity": 3.0},
        {"etf": "CARS", "ticker": "GM", "quantity": 2.0},
    ],
)

etf_composition_dtypes = etf_composition_table.dtypes
# --8<-- [end:business_logic_composition]

# print(etf_composition_table.to_pandas().to_markdown(index=False, ffmt=".1f"))


# --8<-- [start:business_logic_calculation]
def calculate_etf_value(
    etf_composition: pd.DataFrame, price: pd.DataFrame
) -> pd.DataFrame:
    return (
        etf_composition.merge(price, left_on="ticker", right_on="ticker", how="left")
        .assign(values=lambda x: x["price"] * x["quantity"])
        .groupby("etf")
        .aggregate([("value", "sum")])
    )


etf_value_table = calculate_etf_value(
    etf_composition=etf_composition_table, price=price_table
)
# --8<-- [end:business_logic_calculation]


# print(etf_value_table.to_pandas().to_markdown(index=False, floatfmt=".2f"))

# --8<-- [start:dag_source]
from beavers import Dag

dag = Dag()
price_source = dag.pd.source_df(dtypes=price_dtypes, name="price")
etf_composition_source = dag.pd.source_df(
    dtypes=etf_composition_dtypes, name="etf_composition"
)
# --8<-- [end:dag_source]

# --8<-- [start:dag_state]
price_state = dag.pd.last_by_keys(price_source, ["ticker"])
etf_composition_state = dag.pd.last_by_keys(
    etf_composition_source,
    ["etf", "ticker"],
)
# --8<-- [end:dag_state]


# --8<-- [start:dag_calculation]
etf_value_state = dag.state(calculate_etf_value).map(
    etf_composition_state,
    price_state,
)
# --8<-- [end:dag_calculation]


# --8<-- [start:dag_test]
price_source.set_stream(price_table)
etf_composition_source.set_stream(etf_composition_table)
dag.execute()
pd.testing.assert_frame_equal(etf_value_state.get_value(), etf_value_table)
# --8<-- [end:dag_test]
