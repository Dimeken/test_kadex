import utils
import tracemalloc
import time
from time import sleep
import pydantic


class Price(pydantic.BaseModel):
    symbol: str
    price: float


# vars
tablename = 'expDB.dbo.prices'
url_price = 'https://api.binance.com/api/v3/ticker/price'

# connecting to db
confdb_file = 'confDB.json'
confdb = utils.read_json(confdb_file)
conn = utils.connect(confdb)

# service
tracemalloc.start()
while True:
    print('*' * 20)
    print('updating...')

    start_time = time.time()
    prices = utils.get_latest_prices(url_price)
    if prices is None:
        sleep(10)
        continue

    # json validation
    prices = pydantic.parse_obj_as(list[Price], prices)
    prices = [(price.symbol, price.price) for price in prices]

    # inserting
    utils.truncate_table(conn, tablename, to_commit=False)
    utils.insert(conn, prices, tablename)

    end_time = time.time()
    current, peak = tracemalloc.get_traced_memory()
    print(f'Time taken: {end_time - start_time}')
    print(f'Current memory usage is {current / 10 ** 6} MB \nPeak was {peak / 10 ** 6} MB')
