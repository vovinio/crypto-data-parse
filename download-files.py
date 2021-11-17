# pip install tardis-dev
# requires Python >=3.6
import argparse

from tardis_dev import datasets
from datetime import datetime, timedelta


def dl_month(month, days_range=31):
    start_month = datetime(year=2021, month=month, day=1)
    end_month = datetime(year=2021, month=month+1, day=1)
    dates = []
    for i in range(days_range):
        from_date = start_month + timedelta(days=i)
        to_date = start_month + timedelta(days=i+1)
        if to_date > end_month:
            break
        dates.append((from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")))
    return dates


def dl_files_in_month(dir_path, month, days_range=31):
    dates = dl_month(month, days_range=days_range)
    for from_date, to_date in dates:
        datasets.download(
            download_dir=dir_path,
            exchange="deribit",
            data_types=[
                "options_chain",
                # "trades",
                # "quotes",
                # "derivative_ticker",
                # "book_snapshot_25",
                # "liquidations"
            ],
            from_date=from_date,
            to_date=to_date,
            symbols=["OPTIONS"],
            api_key="TD.qB7sCicxbZeufIW1.dJevxJdkxgC3-QX.5-kIISombI-mV4f.wJoHLovYU-zdcd5.U36-jXE6V9g7pGw.WEsr",
        )


parser = argparse.ArgumentParser()
parser.add_argument("--month", "-m")
parser.add_argument("--days", "-d")
args = parser.parse_args()
month = args.month
days = args.days

download_dir_path = f'/home/tardis-data/{month}'
dl_files_in_month(download_dir_path, month, days)
