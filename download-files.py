# pip install tardis-dev
# requires Python >=3.6

from tardis_dev import datasets


download_dir_path = '/home/tardis-data'


for d in range(1, 17):
    if d < 9:
        from_date = f'0{d}'
        to_date = f'0{d+1}'
        continue
    elif d == 9:
        from_date = f'09'
        to_date = f'10'
        continue
    elif d < 31:
        from_date = f'{d}'
        to_date = f'{d+1}'
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
        from_date=f"2021-11-{from_date}",
        to_date=f"2021-11-{to_date}",
        # symbols=["BTC-PERPETUAL", "ETH-PERPETUAL"],
        symbols=["OPTIONS"],
        api_key="TD.qB7sCicxbZeufIW1.dJevxJdkxgC3-QX.5-kIISombI-mV4f.wJoHLovYU-zdcd5.U36-jXE6V9g7pGw.WEsr",
    )

