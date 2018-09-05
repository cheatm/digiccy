#! /bin/bash
source /etc/profile

if [ $BITFINEX ]
then
    cd $BITFINEX
fi


if [ $CREATE == YES ]
then
    echo "------------------------- Download start at `date` -------------------------"
    python bitfinex.py create -f conf.json
    python bitfinex.py download -f conf.json
    echo "------------------------- Download finish at `date` -------------------------"
fi


point=(03:00 09:00)

while true
do
    now = `date +%H:%M`
    for t in ${point[*]} 
    do
        if [ $now == $t ]
        then
            python bitfinex.py create -f conf.json
            python bitfinex.py download -f conf.json
        fi
    done

    if [ `date +%M` == "00" ]
    then
        echo binance alive `date +%Y-%m-%dT%H:%M:%S`
    fi

    sleep 60
done