import requests
import utils
import tracemalloc
import time

# vars
tablename = 'expDB.dbo.prices'
url_ping = 'https://api.binance.com/api/v1/ping'
url_price = 'https://api.binance.com/api/v3/ticker/price'

resp = requests.get(url_ping)
print(resp.status_code)

# connecting to db
confdb_file = 'confDB.json'
confdb = utils.read_json(confdb_file)
conn = utils.connect(confdb)

# service
tracemalloc.start()
while True:
    print('*'*20)
    print('updating...')

    start_time = time.time()
    prices = utils.get_latest_prices(url_price)
    prices = [(js['symbol'], js['price']) for js in prices]
    utils.truncate_table(conn, tablename, to_commit=False)
    utils.insert(conn, prices, tablename)

    end_time = time.time()
    current, peak = tracemalloc.get_traced_memory()
    print(f'Time taken: {end_time - start_time}')
    print(f'Current memory usage is {current / 10**6} MB \nPeak was {peak / 10**6} MB')


