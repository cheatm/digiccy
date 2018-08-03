#! /bin/bash
source /etc/profile

if [ $BINANCE ]
then
    cd $BINANCE
fi

python binance.py create -e `date +%Y%m%d` -f conf.json
python binance.py download -f conf.json