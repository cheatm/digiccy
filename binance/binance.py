from datetime import datetime, timedelta
import requests
import pandas as pd
import json
from itertools import chain
import logging
import click
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError
from datautils.mongodb import append, read, insert
from itertools import product


VARIABLES = ["GAP", "LIMIT", "TARGETS", "START", "END", "RETRY", "LOG", "BAR", "MONGODB", "req_args"]


logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

URL = "https://api.binance.com/api/v1/klines"
GAP = 60*12*60*1000
COLUMNS = ["timestamp", "open", "high", "low", "close", "volume","closetime","quote_volume","number_of_trades","buy_base_volume","buy_quote_volume","Ignore"]
LIMIT = 60*12
# 把当前的时间转化成最近的整数分钟时间的日期

def now2startdate(t):
    return datetime.fromtimestamp(int(t.timestamp()*1000)//60000*60000/1000)


START=20180202
END=None
TARGETS=["SCBNB", "QLCBTC", "SALTBTC", "DLTBNB", "BCNBNB", "AIONBTC", "CDTBTC", "INSBTC", "REQBTC", "KEYBTC", "DLTBTC", "MFTBTC", "TRIGBTC", "SUBBTC", "DENTBTC", "NASBNB", "ARDRBNB", "STEEMBNB", "ARKBTC", "LUNBTC", "QSPBTC", "REPBTC", "ADABTC", "APPCBTC", "XZCBTC", "AEBTC", "IOTXBTC", "WAVESBTC", "WANBNB", "AMBBTC", "SKYBNB", "ONTBTC", "CNDBNB", "LTCBTC", "BTSBNB", "HOTBTC", "GASBTC", "KNCBTC", "IOSTBTC", "AMBBNB", "ZECBTC", "MTLBTC", "THETABNB", "NXSBNB", "ARDRBTC", "NEOBNB", "BATBNB", "LINKBTC", "WABIBTC", "ZRXBTC", "IOTABNB", "PIVXBTC", "XRPBTC", "QLCBNB", "OMGBTC", "QTUMBNB", "RCNBTC", "MTHBTC", "GXSBTC", "AGIBNB", "SNMBTC", "SNTBTC", "AGIBTC", "OSTBTC", "MDABTC", "VIABNB", "EOSBTC", "XLMBTC", "POABTC", "POEBTC", "WANBTC", "QKCBTC", "LOOMBTC", "POABNB", "ZENBNB", "ARNBTC", "XEMBNB", "TUSDBTC", "LSKBTC", "ENJBNB", "QTUMBTC", "BRDBNB", "ICNBTC", "GNTBNB", "CLOAKBTC", "SCBTC", "XZCBNB", "ETCBNB", "CVCBNB", "THETABTC", "NCASHBNB", "GVTBTC", "PPTBTC", "SNGLSBTC", "ENJBTC", "ONTBNB", "SYSBTC", "BTSBTC", "MFTBNB", "CNDBTC", "ICXBNB", "BCNBTC", "ZENBTC", "BNBBTC", "OSTBNB", "BLZBTC", "RCNBNB", "VIBEBTC", "STORMBNB", "WPRBTC", "STEEMBTC", "BATBTC", "STRATBTC", "SYSBNB", "EOSBNB", "SKYBTC", "LOOMBNB", "QSPBNB", "EVXBTC", "ZILBNB", "ETHBTC", "NEOBTC", "STORMBTC", "YOYOBTC", "APPCBNB", "POWRBTC", "ELFBTC", "LENDBTC", "RLCBTC", "NANOBTC", "RLCBNB", "CMTBNB", "NAVBTC", "NAVBNB", "ADXBNB", "MANABTC", "BTGBTC", "LRCBTC", "MCOBNB", "LSKBNB", "XMRBTC", "ASTBTC", "WTCBNB", "NEBLBNB", "NASBTC", "XRPBNB", "XVGBTC", "MCOBTC", "CMTBTC", "FUNBTC", "TNBBTC", "BNTBTC", "TRIGBNB", "ZILBTC", "VIABTC", "RPXBTC", "RDNBTC", "AEBNB", "DGDBTC", "WINGSBTC", "ICXBTC", "FUELBTC", "PIVXBNB", "GTOBNB", "BCCBTC", "KMDBTC", "NCASHBTC", "VETBNB", "REPBNB", "BCPTBTC", "NANOBNB", "BCDBTC", "TRXBTC", "RPXBNB", "IOTABTC", "XLMBNB", "CVCBTC", "VIBBTC", "MODBTC", "LTCBNB", "DASHBTC", "DOCKBTC", "WABIBNB", "BCCBNB", "NULSBTC", "DATABTC", "POWRBNB", "GTOBTC", "CHATBTC", "NEBLBTC", "NPXSBTC", "WTCBTC", "ADABNB", "WAVESBNB", "HSRBTC", "YOYOBNB", "TUSDBNB", "VETBTC", "ADXBTC", "NXSBTC", "AIONBNB", "ETCBTC", "BQXBTC", "BCPTBNB", "XEMBTC", "EDOBTC", "GRSBTC", "BLZBNB", "OAXBTC", "NULSBNB", "BRDBTC", "STORJBTC", "DNTBTC", "GNTBTC", "TNTBTC", "RDNBNB", "ENGBTC"] 
RETRY = 3

PROXIES = None
req_args = {}


# ---------------------------- 入库相关配置 ----------------------------

LOG = "logs.binance"
BAR = "VnTrader_1Min_Db"
MONGODB = "localhost:37017"


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


# # 获取url
def get_url(**kwargs):
    query = "&".join(map(lambda item: "%s=%s" % item, kwargs.items()))
    url = URL
    if query:
        return "%s?%s" % (url, query)
    else:
        return url


# 获取分钟线原始数据(stream字符流)
def get_hist_1min_content(**kwargs):
    url = get_url(**kwargs)
    logging.debug("request url | %s", url)
    response = requests.get(url, **req_args)
    if response.status_code == 200:
        return response.content
    else:
        raise requests.ConnectionError(response.status_code, response.content)


# 获取分钟原始数据(json格式)
def get_hist_1min_docs(**kwargs):
    content = get_hist_1min_content(**kwargs)
    return json.loads(content)


# 分批获取数据(接口有最大长度限制)
def iter_hist_1min(symbol, interval,startTime, endTime, **kwargs):
    kwargs["limit"] = LIMIT
    for s, e in gap_range(startTime, endTime):
        docs = get_hist_1min_docs(symbol=symbol,interval=interval, startTime=s, endTime=e, **kwargs)
        yield docs


# 计算分段
def gap_range(startTime, endTime):
    for s in range(int(startTime), int(endTime), GAP):
        e = s + GAP
        if e < endTime:
            yield s, e - 60*1000
        else:
            yield s, endTime


# 获取一分钟数据，整合成DataFrame
def get_hist_1min(symbol,interval,startTime=None, endTime=None, **kwargs):
    if startTime and endTime:
        if endTime - startTime <= GAP:
            docs = get_hist_1min_docs(symbol=symbol, interval=interval,startTime=startTime, endTime=endTime, **kwargs)
        else:
            docs = list(chain(*iter_hist_1min(symbol,interval, startTime, endTime, **kwargs)))
    else:
        if startTime:
            kwargs["startTime"] = startTime
        if endTime:
            kwargs["endTime"] = endTime
        docs = get_hist_1min_docs(symbol=symbol, interval=interval, **kwargs)
    
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


# 创建获取数据索引并入库
def create_index(collection, symbols, start, end):
    assert isinstance(collection, Collection)
    collection.create_index([("symbol", 1), ("start", 1), ("end", 1)])
    index = create_index_frame(symbols, start, end)
    if len(index.index):
        r = append(collection, index)

# 创建获取数据索(DataFrame)
def create_index_frame(symbols, start, end):
    keys = list(map(lambda item: (item[0], item[1][0], item[1][1]), product(symbols, gap_range(start, end))))
    print(keys)
    index = pd.DataFrame(keys, columns=["symbol", "start", "end"])
    index["date"] = index["start"].apply(mts2date)
    index["vtSymbol"] = index["symbol"].apply(lambda s: "%s:binance" % s)
    index["count"] = 0
    return index.set_index(["symbol", "start", "end"])


BAR_COLUMN = ["vtSymbol", "symbol", "exchange", "open", "high", "low", "close", "date", "time", "datetime", "volume", "openInterest"]


# 将原始DataFrame修改成符合vnpy格式
def vnpy_format(frame, symbol, exchange, vtSymbol=None):
    assert isinstance(frame, pd.DataFrame)
    frame["datetime"] = frame.pop("timestamp").apply(mts2datetime)
    frame["time"] = frame["datetime"].apply(dt2time)
    frame["date"] = frame["datetime"].apply(dt2date)
    frame["symbol"] = symbol
    frame["exchange"] = exchange
    frame["vtSymbol"] = vtSymbol if vtSymbol else vt_symbol(symbol, exchange)
    frame["gatewayName"] = ""
    frame["rawData"] = None
    frame["openInterest"] = 0
    for key in ["open", "high", "low", "close", "volume"]:
        frame[key] = frame[key].apply(float)
    return frame[BAR_COLUMN]


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
    
    try:
        pool.close()
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


def vt_symbol(symbol, exchange):
    return "%s:%s" % (symbol, exchange)


from time import sleep


def handle_doc(symbol, start, end, **kwargs):
    sleep(1)
    exchange = "binance"
    vtSymbol = vt_symbol(symbol, exchange)
    try:
        frame = get_hist_1min(symbol, "1m",start, end, limit=LIMIT)
    except Exception as e:
        logging.error("handle | %s | %s | %s | %s", symbol, start, end, e)
        raise e
    count = len(frame.index)
    if count:
        frame = vnpy_format(frame, symbol, exchange, vtSymbol)
        col = db[vtSymbol]
        inserted = _insert(col, frame)
    else:
        inserted = -1
        count = -1
    flt = {"symbol": symbol, "start": start, "end": end} 
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


def is_binance(s):
    return "binance" in s


def create_collection_index(*names):
    if not names:
        names = db.collection_names()
    for name in filter(is_binance, names):
        try:
            db[name].create_index("datetime", background=True, unique=True)
            db[name].create_index("date", background=True)
        except Exception as e:
            logging.error("create collection index | %s | %s", name, e)
        else:
            logging.warning("create collection index | %s | ok", name)


def find():
    yield from log.find({"count": 0}, {"_id": 0})


def latest_record(collection):
    assert isinstance(collection, Collection)
    doc = collection.find_one(sort=[("end", -1)])
    if doc:
        date = doc["date"]
        return int(date.replace("-", ""))
    return None


def today():
    date = datetime.now()
    return date.year * 10000 + date.month*100 + date.day


class StreamBars(object):

    def __init__(self, symbol, limit=LIMIT):
        self.symbol = symbol
        self.limit = limit

    def get_last(self):
        raise NotImplementedError()
    
    def handle(self, bars):
        raise NotImplementedError()
    
    def next_bars(self, retry=RETRY, recursion=100):
        if recursion == 0:
            logging.warning("request next bars | left retry chance 0 | exit")
            return 
        try:
            last = self.get_last()
        except Exception as e:
            return 

        try:
            bars = get_hist_1min(self.symbol, "1m", last, limit=self.limit)
            self.handle(bars)
        except Exception as e:
            logging.error("request next bars | %s | %s | %s | %s", self.symbol, last, self.limit, e)
            if retry == 0:
                logging.error("request next bars | left retry chance 0 | exit")
            else:
                self.next_bars(retry-1, recursion-1)
        else:
            logging.warning("request next bars | %s | %s | %s | %s", self.symbol, last, self.limit, len(bars))
            if len(bars) >= self.limit:
                self.next_bars(retry, recursion-1)


from pymongo.collection import Collection


class MongodbStreamBars(StreamBars):

    def __init__(self, collection, symbol, limit=LIMIT, default_start=None):
        assert isinstance(collection, Collection)
        self.collection = collection
        super(MongodbStreamBars, self).__init__(symbol, limit)
        self._default = default_start if default_start else int(datetime.now().timestamp()*1000 - 7*24*60*60*1000) 

    def get_last(self):
        doc = self.collection.find_one(sort=[("datetime", -1)])
        if doc:
            return int(doc["datetime"].timestamp()*1000)
        else:
            return self._default
    
    def handle(self, bars):
        if len(bars) == 0:
            return
        data = vnpy_format(bars, self.symbol, "binance")
        for doc in data.to_dict("record"):
            try:
                self.collection.update_one({"datetime": doc["datetime"]}, {"$set": doc}, upsert=True)
            except Exception as e:
                logging.error("write bar | %s | %s | %s", self.symbol, doc, e)



import click
import os

@click.command()
@click.option("-f", "--filename", default="./conf.json")
@click.option("-l", "--log", default=None)
@click.option("-c", "--symbols", default=None)
@click.option("-s", "--start", default=None, type=click.INT)
@click.option("-e", "--end", default=None, type=click.INT)
def create(log=None, symbols=None, start=None, end=None, filename="./conf.json"):
    if filename and os.path.isfile(filename):
        init(filename)
        
    if log:
        globals()["LOG"] = log
        init_db()
    if symbols:
        globals()["TARGETS"] = symbols.split(",")
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
            end = today()
    create_index(globals()["log"], TARGETS, date2mts(start), date2mts(end)-1)
    create_collection_index()
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
    
    if not retry:
        retry = RETRY
    
    insert_from_log(retry)


@click.command()
@click.argument("filename", nargs=1, default="conf.json")
def update(filename):
    init(filename)
    init_db()
    tables = db.collection_names()
    for symbol in TARGETS:
        name = vt_symbol(symbol, "binance")
        if name not in tables:
            col = db.create_collection(name, capped=True, size=2**25)
            col.create_index("datetime", unique=True, background=True)
            col.create_index("date", background=True)
        else:
            col = db[name]
        msb = MongodbStreamBars(col, symbol)
        msb.next_bars()


group = click.Group(
    "binance", 
    {"create": create,
     "download": download,
     "update": update}
)


def clean(collection):
    assert isinstance(collection, Collection)
    price = ["open", "high", "low", "close", "volume"]
    cursor = collection.find({}, list(price))
    count = cursor.count()
    _id = None
    while True:
        try:
            doc = next(cursor)
        except StopIteration:
            logging.warning("%s | finish", collection)
            break
        except Exception as e:
            logging.error("%s | %s", e)
            cursor = collection.find({"_id": {"$gt": _id}}, list(price))
        else:
            upd = {}
            for key in price:
                upd[key] = float(doc[key])
            collection.update_one({"_id": doc["_id"]}, {"$set": upd})
            _id = doc["_id"]
            count -= 1
            if count % 1000 == 0:
                logging.warning("%s | left %d", collection.name, count)
        

if __name__ == '__main__':
    group()