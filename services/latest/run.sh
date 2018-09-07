#! /bin/bash

while true
do
    if [ `expr $now % 15` == 0 ]
    then
        echo "start update at `date`"
        python binance/binance.py update binance/conf-udp.json
        echo "finish update at `date`"
    fi

    seconds=`date +%S`
    sleep `expr 60 - $seconds`
done
