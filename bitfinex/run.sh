#! /bin/bash
source /etc/profile

if [ $BITFINEX ]
then
    cd $BITFINEX
fi

python bitfinex.py create -e `date +%Y%m%d` -f conf.json
python bitfinex.py download -f conf.json