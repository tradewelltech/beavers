import pandas as pd
from examples import etfs

from beavers.testing import DagTestBench


def test_run_dag():
    dag = etfs.create_dag()
    bench = DagTestBench(dag)

    # Price and ETF come in:
    timestamp_0 = pd.to_datetime("2023-06-10 12:00:00+0000")
    (
        bench.set_source(
            "price",
            [
                etfs.PriceRecord(timestamp_0, "AAPL", 180.0),
                etfs.PriceRecord(timestamp_0, "GOOG", 120.0),
            ],
        )
        .set_source(
            "etf_composition",
            [etfs.EtfComposition(timestamp_0, "TECH", {"AAPL": 1.0, "GOOG": 1.5})],
        )
        .run(timestamp_0)
        .assert_sink_list("etf_price", [etfs.PriceRecord(timestamp_0, "TECH", 144.0)])
    )

    # AAPL price update:
    timestamp_1 = timestamp_0 + pd.to_timedelta("1s")
    (
        bench.set_source(
            "price",
            [
                etfs.PriceRecord(timestamp_1, "AAPL", 200.0),
            ],
        )
        .run(timestamp_1)
        .assert_sink_list("etf_price", [etfs.PriceRecord(timestamp_1, "TECH", 152.0)])
    )

    # Unrelated price updates:
    timestamp_2 = timestamp_0 + pd.to_timedelta("2s")
    (
        bench.set_source(
            "price",
            [
                etfs.PriceRecord(timestamp_2, "MSFT", 330.0),
            ],
        )
        .run(timestamp_2)
        .assert_sink_not_updated("etf_price")
    )

    # New ETF comes in
    timestamp_3 = timestamp_0 + pd.to_timedelta("4s")
    (
        bench.set_source(
            "etf_composition",
            [etfs.EtfComposition(timestamp_3, "SOFT", {"MSFT": 0.5, "GOOG": 1.0})],
        )
        .run(timestamp_3)
        .assert_sink_list("etf_price", [etfs.PriceRecord(timestamp_3, "SOFT", 190.0)])
    )

    # ETF extends with missing price:
    timestamp_4 = timestamp_0 + pd.to_timedelta("4s")
    (
        bench.set_source(
            "etf_composition",
            [
                etfs.EtfComposition(
                    timestamp_4, "SOFT", {"MSFT": 0.5, "GOOG": 1.0, "ORCL": 0.5}
                )
            ],
        )
        .run(timestamp_4)
        .assert_sink_list("etf_price", [etfs.PriceRecord(timestamp_4, "SOFT", None)])
    )
