#! /bin/bash
source /etc/profile

cd $BITFINEX

python bitfinex.py create -f conf.json
python bitfinex.py download -f conf.json