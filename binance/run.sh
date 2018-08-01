#! /bin/bash
source /etc/profile

cd $BINANCE

python binance.py create -f conf.json
python binance.py download -f conf.json