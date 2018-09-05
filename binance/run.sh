#! /bin/bash
source /etc/profile

if [ $BINANCE ]
then
    cd $BINANCE
fi

if [ $CREATE == YES ]
then
    python binance.py create -f conf.json
    python binance.py download -f conf.json
fi

point=(00:00 06:00)

while true
do
    now = `date +%H:%M`
    for t in ${point[*]} 
    do
        if [ $now == $t ]
        then
            python binance.py create -f conf.json
            python binance.py download -f conf.json
        fi
    done

    if [ `date +%M` == "00" ]
    then
        echo binance alive `date +%Y-%m-%dT%H:%M:%S`
    fi

    sleep 60
done