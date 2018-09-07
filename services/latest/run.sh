#! /bin/bash

python binance/binance.py update conf.json

while true
do
    now=`date +%M`
    if [ `expr $now % 15` == 0 ]
    then
        echo "start update at `date`"
        python binance/binance.py update conf.json
        echo "finish update at `date`"
    fi

    seconds=`date +%S`
    sleep `expr 60 - $seconds`
done
