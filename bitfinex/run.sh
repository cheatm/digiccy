#! /bin/bash
source /etc/profile

if [ $BITFINEX ]
then
    cd $BITFINEX
fi

python bitfinex.py create -e `date -d "1 day ago" +%Y%m%d` -f conf.json
python bitfinex.py download -f conf.json