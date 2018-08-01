#! /bin/bash
source /etc/profile

if [ $BITFINEX ]
then
    cd $BITFINEX
fi

python bitfinex.py create -f conf.json
python bitfinex.py download -f conf.json