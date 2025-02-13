# ruff: noqa: E402
# isort: skip_file

import polars.testing


# --8<-- [start:business_logic_price]
import polars as pl

PRICE_SCHEMA = pl.Schema(
    [
        ("ticker", pl.String()),
        ("price", pl.Float64()),
    ]
)

price_table = pl.DataFrame(
    [
        {"ticker": "AAPL", "price": 174.79},
        {"ticker": "GOOGL", "price": 130.25},
        {"ticker": "MSFT", "price": 317.01},
        {"ticker": "F", "price": 12.43},
        {"ticker": "GM", "price": 35.28},
    ],
    schema=PRICE_SCHEMA,
)
# --8<-- [end:business_logic_price]

# print(price_table.to_pandas().to_markdown(index=False))

# --8<-- [start:business_logic_composition]
ETF_COMPOSITION_SCHEMA = pl.Schema(
    [
        ("etf", pl.String()),
        ("ticker", pl.String()),
        ("quantity", pl.Float64()),
    ]
)


etf_composition_table = pl.DataFrame(
    [
        {"etf": "TECH", "ticker": "AAPL", "quantity": 2.0},
        {"etf": "TECH", "ticker": "GOOGL", "quantity": 2.0},
        {"etf": "TECH", "ticker": "MSFT", "quantity": 1.0},
        {"etf": "CARS", "ticker": "F", "quantity": 3.0},
        {"etf": "CARS", "ticker": "GM", "quantity": 2.0},
    ],
    schema=ETF_COMPOSITION_SCHEMA,
)
# --8<-- [end:business_logic_composition]

# print(etf_composition_table.to_pandas().to_markdown(index=False, floatfmt=".1f"))


# --8<-- [start:business_logic_calculation]
ETF_VALUE_SCHEMA = pl.Schema(
    [
        ("etf", pl.String()),
        ("value", pl.Float64()),
    ]
)


def calculate_etf_value(
    etf_composition: pl.DataFrame, price: pl.DataFrame
) -> pl.DataFrame:
    return (
        etf_composition.join(price, on=["ticker"])
        .select(pl.col("etf"), (pl.col("price") * pl.col("quantity")).alias("value"))
        .group_by("etf", maintain_order=True)
        .agg(pl.col("value").sum())
        .cast(ETF_VALUE_SCHEMA)
    )


etf_value_table = calculate_etf_value(
    etf_composition=etf_composition_table, price=price_table
)
# --8<-- [end:business_logic_calculation]


# print(etf_value_table.to_pandas().to_markdown(index=False, floatfmt=".2f"))

# --8<-- [start:dag_source]
from beavers import Dag

dag = Dag()
price_source = dag.pl.source_table(schema=PRICE_SCHEMA, name="price")
etf_composition_source = dag.pl.source_table(
    schema=ETF_COMPOSITION_SCHEMA, name="etf_composition"
)
# --8<-- [end:dag_source]

# --8<-- [start:dag_state]
price_state = dag.pl.last_by_keys(price_source, ["ticker"])
etf_composition_state = dag.pl.last_by_keys(
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
polars.testing.assert_frame_equal(etf_value_state.get_value(), etf_value_table)
# --8<-- [end:dag_test]


# --8<-- [start:spurious_update]
new_price_updates = pl.DataFrame(
    [{"ticker": "GME", "price": 123.0}],
    PRICE_SCHEMA,
)
price_source.set_stream(new_price_updates)
dag.execute()
assert len(etf_value_state.get_value()) == 2
assert etf_value_state.get_cycle_id() == dag.get_cycle_id()
# --8<-- [end:spurious_update]

# --8<-- [start:updated_because_of_composition]
updated_because_of_composition = dag.pl.get_series(
    etf_composition_source,
    "etf",
)
# --8<-- [end:updated_because_of_composition]


# --8<-- [start:updated_because_of_price]
def get_etf_to_update_because_of_price(
    etf_composition_state: pl.DataFrame, price_update: pl.DataFrame
) -> pl.Series:
    updated_tickers = price_update["ticker"].unique()
    return etf_composition_state.filter(pl.col("ticker").is_in(updated_tickers))[
        "etf"
    ].unique()


updated_because_of_price = dag.stream(
    get_etf_to_update_because_of_price, empty=pl.Series(name="etf", dtype=pl.String())
).map(etf_composition_state, price_source)
# --8<-- [end:updated_because_of_price]

# --8<-- [start:update_all]
stale_etfs = dag.pl.concat_series(
    updated_because_of_price, updated_because_of_composition
)


def get_composition_for_etfs(
    etf_composition_state: pl.DataFrame,
    etfs: pl.Series,
) -> pl.DataFrame:
    return etf_composition_state.filter(pl.col("etf").is_in(etfs))


stale_etf_compositions = dag.pl.table_stream(
    get_composition_for_etfs, ETF_COMPOSITION_SCHEMA
).map(etf_composition_state, stale_etfs)

updated_etf = dag.pl.table_stream(calculate_etf_value, ETF_VALUE_SCHEMA).map(
    stale_etf_compositions, price_state
)
# --8<-- [end:update_all]

# --8<-- [start:update_all_test]
price_source.set_stream(
    pl.DataFrame(
        [{"ticker": "MSFT", "price": 317.05}],
        schema=PRICE_SCHEMA,
    )
)
dag.execute()
assert len(updated_etf.get_value()) == 1
# --8<-- [end:update_all_test]

# print(updated_etf.get_value().to_pandas().to_markdown(index=False))
