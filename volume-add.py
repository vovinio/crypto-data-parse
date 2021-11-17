import asyncio
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError, AutoReconnect
from tardis_client import TardisClient, Channel


tardis_client = TardisClient(api_key="TD.qB7sCicxbZeufIW1.dJevxJdkxgC3-QX.5-kIISombI-mV4f.wJoHLovYU-zdcd5.U36-jXE6V9g7pGw.WEsr")


async def replay():
  # replay method returns Async Generator
  messages = tardis_client.replay(
    exchange="deribit",
    from_date="2021-11-01",
    to_date="2021-11-02",
    filters=[Channel(name="ticker", symbols=["BTC-26NOV21-52000-P"])]
  )
  # messages as provided by Deribit real-time stream
  async for local_timestamp, message in messages:
    print(message)


# asyncio.run(replay())



# mongo_client_data = MongoClient('mongodb://{}:{}@88.99.252.172:27017'.format('moonrock', '!8478xhgaxBiz35'))
# db_options_chain = mongo_client_data['knight-fund']['options-chain']
#
#
# docs = db_options_chain.find({
#   'event_time': {'$lte': datetime.fromtimestamp(1635724800042000/1000000) + timedelta(seconds=5), '$gt': datetime.fromtimestamp(1635724800042000/1000000) - timedelta(seconds=5)}, 'symbol': 'BTC-26NOV21-52000-P'
# })
# for doc in docs:
#   print(doc)

print(datetime.fromtimestamp(1635724803544/1000))
print(datetime.fromtimestamp(1635724803544/1000, tz=timezone.utc))
print(datetime.fromtimestamp(1635724803554000/1000000))