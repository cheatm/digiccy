from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
import re
import os
import logging


URI = os.environ.get("MONGODB_URI", "localhost")
BAR = os.environ.get("BAR", "VnTrader_1Min_Db")
MATCH = os.environ.get("BAR_MATCH", "")

logging.basicConfig(level=logging.DEBUG)


def find_collections(db, match=None):
    assert isinstance(db, Database)
    names = db.collection_names()
    if match:
        names =  list(filter(lambda s: re.search(match, s), names))
    
    for name in names:
        yield db[name]


def ensure_index(collection):
    assert isinstance(collection, Collection)
    indexes = collection.index_information()
    for name in ["date", "datetime"]:
        if "%s_1" % name not in indexes:
            collection.create_index(name, background=True)
            logging.warning("create index %s for %s", name, collection.name)
    

def drop_dups(collection):
    assert isinstance(collection, Collection)
    KEY = "datetime"
    indexes = collection.index_information()
    if indexes.get("datetime_1", {}).get("unique", False):
        logging.warning("%s of %s is unique", KEY, collection.name)
        return
    else:
        keys = set()
        for doc in collection.find(projection={"_id": 1, KEY: 1}):
            dt = doc.get(KEY, None)
            if dt in keys:
                collection.delete_one({"_id": doc["_id"]})
                logging.debug("drop dups | %s", doc)
            else:
                keys.add(dt) 
        collection.create_index("datetime", unique=True, background=True)
        logging.warning("drop dups & create unique index %s for %s", KEY, collection.name)
    

def create(uri, db, match=None):
    database = MongoClient(uri)[db]
    for collection in find_collections(database, match):
        try:
            ensure_index(collection)
        except Exception as e:
            logging.error("create index | %s | %s", collection.name, e)


def unique(uri, db, match=None):
    database = MongoClient(uri)[db]
    for collection in find_collections(database, match):
        try:
            drop_dups(collection)
        except Exception as e:
            logging.error("drop dups | %s | %s", collection.name, e)


METHODS = {
    "unique": drop_dups,
    "create": ensure_index
}


import click


@click.command()
@click.option("--uri", "-u", default=URI)
@click.option("--db", "-d", default=BAR)
@click.option("--match", "-m", default=MATCH)
@click.argument("methods", nargs=-1)
def command(uri, db, match, methods):
    if not methods:
        methods = ["unique"]
    
    database = MongoClient(uri)[db]
    for collection in find_collections(database, match):
        for name in methods:
            try:
                method = METHODS[name]
                method(collection)
            except Exception as e:
                logging.error("%s | %s | %s", name, collection.name, e)
            

if __name__ == '__main__':
    command()