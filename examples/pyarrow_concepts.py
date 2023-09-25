# ruff: noqa: E402
# isort: skip_file

# --8<-- [start:business_logic_price]
import pyarrow as pa

PRICE_SCHEMA = pa.schema(
    [
        pa.field("ticker", pa.string()),
        pa.field("price", pa.float64()),
    ]
)

price_table = pa.Table.from_pylist(
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

print(price_table.to_pandas().to_markdown(index=False))

# --8<-- [start:business_logic_composition]
ETF_COMPOSITION_SCHEMA = pa.schema(
    [
        pa.field("etf", pa.string()),
        pa.field("ticker", pa.string()),
        pa.field("quantity", pa.float64()),
    ]
)


etf_composition_table = pa.Table.from_pylist(
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

print(etf_composition_table.to_pandas().to_markdown(index=False, floatfmt=".1f"))


# --8<-- [start:business_logic_calculation]
import pyarrow.compute as pc

ETF_VALUE_SCHEMA = pa.schema(
    [
        pa.field("etf", pa.string()),
        pa.field("value", pa.float64()),
    ]
)


def calculate_etf_value(etf_composition: pa.Table, price: pa.Table) -> pa.Table:
    positions_with_prices = etf_composition.join(price, keys=["ticker"])
    values = pc.multiply(
        positions_with_prices["price"], positions_with_prices["quantity"]
    )
    positions_with_prices = positions_with_prices.append_column("value", values)
    return (
        positions_with_prices.group_by("etf")
        .aggregate([("value", "sum")])
        .rename_columns(ETF_VALUE_SCHEMA.names)
    )


etf_value_table = calculate_etf_value(
    etf_composition=etf_composition_table, price=price_table
)
# --8<-- [end:business_logic_calculation]


print(etf_value_table.to_pandas().to_markdown(index=False, floatfmt=".2f"))

# --8<-- [start:dag_source]
from beavers import Dag

dag = Dag()
price_source = dag.pa.source_stream(schema=PRICE_SCHEMA, name="price")
etf_composition_source = dag.pa.source_stream(
    schema=ETF_COMPOSITION_SCHEMA, name="etf_composition"
)
# --8<-- [end:dag_source]

# --8<-- [start:dag_state]
price_state = dag.pa.latest_by_keys(price_source, ["ticker"])
etf_composition_state = dag.pa.latest_by_keys(
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
assert etf_value_state.get_value() == etf_value_table
# --8<-- [end:dag_test]


# --8<-- [start:spurious_update]
new_price_updates = pa.Table.from_pylist(
    [{"ticker": "GME", "price": 123.0}],
    PRICE_SCHEMA,
)
price_source.set_stream(new_price_updates)
dag.execute()
assert len(etf_value_state.get_value()) == 2
assert etf_value_state.get_cycle_id() == dag.get_cycle_id()
# --8<-- [end:spurious_update]

# --8<-- [start:updated_because_of_composition]
updated_because_of_composition = dag.pa.get_column(
    etf_composition_source,
    "etf",
)
# --8<-- [end:updated_because_of_composition]


# --8<-- [start:updated_because_of_price]
def get_etf_to_update_because_of_price(
    etf_composition_state: pa.Table, price_update: pa.Table
) -> pa.Array:
    updated_tickers = pc.unique(price_update["ticker"])
    return pc.unique(
        etf_composition_state.filter(
            pc.is_in(etf_composition_state["ticker"], updated_tickers)
        )["etf"]
    )


updated_because_of_price = dag.stream(
    get_etf_to_update_because_of_price, pa.array([], pa.string())
).map(etf_composition_state, price_source)
# --8<-- [end:updated_because_of_price]

# --8<-- [start:update_all]
stale_etfs = dag.pa.concat_arrays(
    updated_because_of_price, updated_because_of_composition
)


def get_composition_for_etfs(
    etf_composition_state: pa.Table, etfs: pa.Array
) -> pa.Table:
    return etf_composition_state.filter(
        pc.is_in(
            etf_composition_state["etf"],
            etfs,
        )
    )


stale_etf_compositions = dag.pa.table_stream(
    get_composition_for_etfs, ETF_COMPOSITION_SCHEMA
).map(etf_composition_state, stale_etfs)

updated_etf = dag.pa.table_stream(calculate_etf_value, ETF_VALUE_SCHEMA).map(
    stale_etf_compositions, price_state
)
# --8<-- [end:update_all]

# --8<-- [start:update_all_test]
price_source.set_stream(
    pa.Table.from_pylist(
        [{"ticker": "MSFT", "price": 317.05}],
        PRICE_SCHEMA,
    )
)
dag.execute()
assert len(updated_etf.get_value()) == 1
# --8<-- [end:update_all_test]

print(updated_etf.get_value().to_pandas().to_markdown(index=False))
