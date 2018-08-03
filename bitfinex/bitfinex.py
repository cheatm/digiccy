from datetime import datetime, timedelta
import requests
import pandas as pd
import json
from itertools import chain
import logging
import click
from pymongo import MongoClient
from pymongo.collection import Collection
from datautils.mongodb import append, read, insert
from itertools import product


VARIABLES = ["GAP", "LIMIT", "TARGETS", "START", "END", "RETRY", "LOG", "BAR", "MONGODB", "PROXIES", "req_args"]


logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

URL = "https://api.bitfinex.com/v2/candles/trade:1m:%s/hist"
GAP = 12*60*60*1000
COLUMNS = ["timestamp", "open", "close", "high", "low", "volume"]
LIMIT = 60*12 + 1

TARGETS = list(map(lambda s: "t"+s, "BTCUSD,ETHUSD,EOSUSD,BCHUSD,NEOUSD,ETCUSD,IOTUSD,LTCUSD,XRPUSD,XMRUSD".split(",")))
START = 20180101
END = None

RETRY = 3

PROXIES = None

req_args = {}
# ---------------------------- 入库相关配置 ----------------------------

LOG = "logs.bitfinex"
BAR = "VnTrader_1Min_Db"
MONGODB = "localhost"


client = MongoClient(MONGODB)
db = client[BAR]
log_db, log_col = LOG.split(".")
log = client[log_db][log_col]

# ---------------------------- 入库相关配置 ----------------------------


def init(filename=""):
    import json
    with open(filename) as f:
        conf = json.load(f)
    globals().update(conf)
    init_db()
    if PROXIES:
        req_args["proxies"] = PROXIES


def init_db():
    client = MongoClient(MONGODB)
    db = client[BAR]
    log_db, log_col = LOG.split(".")
    log = client[log_db][log_col]
    globals().update(
        {"client": client, "db": db, "log": log}
    )


def initializer(variables):
    globals().update(variables)
    init_db()


# 获取url
def get_url(contract, **kwargs):
    query = "&".join(map(lambda item: "%s=%s" % item, kwargs.items()))
    url = URL % contract
    if query:
        return "%s/?%s" % (url, query)
    else:
        return url


# 获取分钟线原始数据(stream字符流)
def get_hist_1min_content(contract, **kwargs):
    url = get_url(contract, **kwargs)
    # logging.warning("requests.get | %s | %s", url, req_args)
    response = requests.get(url, **req_args)
    if response.status_code == 200:
        return response.content
    else:
        raise requests.ConnectionError(response.status_code)


# 获取分钟原始数据(json格式)
def get_hist_1min_docs(contract, **kwargs):
    content = get_hist_1min_content(contract, **kwargs)
    return json.loads(content)


# 分批获取数据(接口有最大长度限制)
def iter_hist_1min(contract, start, end, **kwargs):
    kwargs["limit"] = LIMIT
    for s, e in gap_range(start, end):
        docs = get_hist_1min_docs(contract, start=s, end=e, **kwargs)
        yield docs


# 计算分段
def gap_range(start, end):
    for s in range(int(start), int(end), GAP):
        e = s + GAP
        if e < end:
            yield s, e - 1
        else:
            yield s, end


# 获取一分钟数据，整合成DataFrame
def get_hist_1min(contract, start, end, **kwargs):
    if end - start <= GAP:
        docs = get_hist_1min_docs(contract, start=start, end=end, **kwargs)
    else:
        docs = list(chain(*iter_hist_1min(contract, start, end, **kwargs)))
    
    return pd.DataFrame(docs, columns=COLUMNS)


# 时间类型转换
def mts2datetime(mts):
    return datetime.fromtimestamp(mts/1000)

# 时间类型转换
def mts2date(mts):
    return mts2datetime(mts).strftime("%Y-%m-%d")

# 时间类型转换
def dt2time(t):
    return t.strftime("%H:%M:%S.%f")

# 时间类型转换
def dt2date(t):
    return t.strftime("%Y%m%d")

# 时间类型转换
def date2mts(t):
    return int(datetime.strptime(str(t), "%Y%m%d").timestamp()*1000)


def latest_record(collection):
    assert isinstance(collection, Collection)
    doc = collection.find_one(sort=[("end", -1)])
    if doc:
        date = doc["date"]
        return int(date.replace("-", ""))
    return None


# 创建获取数据索引并入库
def create_index(collection, contracts, start, end):
    assert isinstance(collection, Collection)
    collection.create_index([("contract", 1), ("start", 1), ("end", 1)])
    index = create_index_frame(contracts, start, end)
    r = append(collection, index)
    print(r)

# 创建获取数据索(DataFrame)
def create_index_frame(contracts, start, end):
    keys = list(map(lambda item: (item[0], item[1][0], item[1][1]), product(contracts, gap_range(start, end))))
    index = pd.DataFrame(keys, columns=["contract", "start", "end"])
    index["date"] = index["start"].apply(mts2date)
    index["vtSymbol"] = index["contract"].apply(lambda s: "%s:bitfinex" % s)
    index["count"] = 0
    return index.set_index(["contract", "start", "end"])


# 将原始DataFrame修改成符合vnpy格式
def vnpy_format(frame, contract, exchange, vtSymbol=None):
    assert isinstance(frame, pd.DataFrame)
    frame["datetime"] = frame.pop("timestamp").apply(mts2datetime)
    frame["time"] = frame["datetime"].apply(dt2time)
    frame["date"] = frame["datetime"].apply(dt2date)
    frame["symbol"] = contract
    frame["exchange"] = exchange
    frame["vtSymbol"] = vtSymbol if vtSymbol else "%s:%s" % (contract, exchange)
    frame["gatewayName"] = ""
    frame["rawData"] = None
    frame["openInterest"] = 0
    return frame


def main():
    # create_temp()
    insert_from_log(-1)

def test_start(contract, date):
    data = get_hist_1min(contract, date.timestamp()*1000, date.timestamp()*1000+24*60*60*1000-1, sort=1)
    return vnpy_format(data, contract, "bitfinex")

def on_error(e):
    logging.error(e)


# 通过数据库中的索引下载数据入库
def insert_from_log(retry=3, docs=None):
    """
    retry: 循环次数，一次循环中如果有失败的任务则会开启下一次循环，循环数-1，直到循环数为0。循环数设为负数表示循环直到所有任务完成。
    """
    from multiprocessing.pool import Pool
    import multiprocessing
    retry -= 1
    if docs is None:
        docs = find()

    pool = Pool(None, initializer, ({name: globals()[name] for name in VARIABLES},))

    for doc in docs:
        pool.apply_async(handle_doc, kwds=doc, error_callback=on_error)
    
    pool.close()
    try:
        pool.join()
    except KeyboardInterrupt:
        pool.terminate()
        return 

    docs = list(find())
    logging.warning("Unfilled: %s", len(docs))

    if retry:
        logging.warning("retry | %s", retry)
        if docs:
            insert_from_log(retry, docs)


from time import sleep


def handle_doc(contract, start, end, **kwargs):
    sleep(1)
    exchange = "bitfinex"
    vtSymbol = "%s:%s" % (contract, exchange)
    frame = get_hist_1min(contract, start, end, limit=LIMIT)
    count = len(frame.index)
    if count:
        frame = vnpy_format(frame, contract, exchange, vtSymbol)
        col = db[vtSymbol]
        inserted = _insert(col, frame)
    else:
        inserted = -1
        count = -1
    flt = {"contract": contract, "start": start, "end": end} 
    logging.warning("insert | %s |  %s", flt, inserted)
    to_set = {"count": count, "inserted": inserted}
    log.update_one(
        flt, 
        {"$set": {"count": count},
         "$inc": {"inserted": inserted}}
    )
    logging.warning("update log | %s | %s", flt, to_set)
    return flt


def _insert(collection, frame):
    assert isinstance(collection, Collection)
    assert isinstance(frame, pd.DataFrame)
    if frame.index.name is not None:
        frame = frame.reset_index()
    count = 0
    for doc in frame.to_dict("record"):
        try:
            collection.insert_one(doc)
        except DuplicateKeyError:
            pass
        else:
            count += 1
    return count


def drop_duplicates(name):
    collection = db[name]
    dates = set()
    dups = 0
    for doc in collection.find(None, {"datetime": 1}):
        dt = doc["datetime"]
        if dt in dates:
            _id = doc["_id"]
            collection.delete_one({"_id": _id})
            logging.warning("drop | %s | %s | %s", name, dt, _id)
            dups += 1
        else:
            dates.add(dt)
    return dups


def is_bitfinex(s):
    return "bitfinex" in s


def create_collection_index():
    for name in filter(is_bitfinex, db.collection_names()):
        print(name)
        db[name].create_index("datetime", background=True)
        db[name].create_index("date", background=True)
        print("finish")


def find():
    yield from log.find({"count": 0}, {"_id": 0})


def yesterday():
    date = datetime.now() - timedelta(days=1)
    return date.year * 10000 + date.month*100 + date.day


import click
import os

@click.command()
@click.option("-f", "--filename", default="./conf.json")
@click.option("-l", "--log", default=None)
@click.option("-c", "--contracts", default=None)
@click.option("-s", "--start", default=None, type=click.INT)
@click.option("-e", "--end", default=None, type=click.INT)
def create(log=None, contracts=None, start=None, end=None, filename="./conf.json"):
    if filename and os.path.isfile(filename):
        init(filename)
    if log:
        globals()["LOG"] = log
        init_db()
    if contracts:
        globals()["TARGETS"] = contracts.split(",")
    if not start:
        latest = latest_record(globals()["log"])
        if latest:
            start = latest
        else:
            start = START
    if not end:
        if END:
            end = END
        else:
            end = yesterday()
    create_index(globals()["log"], TARGETS, date2mts(start), date2mts(end)-1)
    logging.warning("create index | %s ~ %s", start, end)


@click.command()
@click.option("-f", "--filename", default="./conf.json")
@click.option("-l", "--log", default=None)
@click.option("-r", "--retry", default=0, type=click.INT)
def download(log=None, retry=0, filename=None):
    if filename and os.path.isfile(filename):
        init(filename)
    if log:
        globals()["LOG"] = log
        init_db()
    
    if retry != 0:
        retry = RETRY
    
    insert_from_log(retry)


group = click.Group(
    "bitfinex", 
    {"create": create,
     "download": download}
)


if __name__ == '__main__':
    group()