# pip install tardis-dev
# requires Python >=3.6

from tardis_dev import datasets


download_dir_path = '/home/tardis-data'


datasets.download(
    download_dir=download_dir_path,
    exchange="deribit",
    data_types=[
        "options_chain",
        # "trades",
        # "quotes",
        # "derivative_ticker",
        # "book_snapshot_25",
        # "liquidations"
    ],
    from_date="2021-07-01",
    to_date="2021-07-02",
    # symbols=["BTC-PERPETUAL", "ETH-PERPETUAL"],
    symbols=["OPTIONS"],
    api_key="TD.qB7sCicxbZeufIW1.dJevxJdkxgC3-QX.5-kIISombI-mV4f.wJoHLovYU-zdcd5.U36-jXE6V9g7pGw.WEsr",
)
