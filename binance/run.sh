#! /bin/bash
source /etc/profile

if [ $BINANCE ]
then
    cd $BINANCE
fi

if [ $CREATE ]
then
    echo "------------------------- Download start at `date` -------------------------"
    python binance.py create -f conf.json
    python binance.py download -f conf.json
    echo "------------------------- Download start at `date` -------------------------"
fi

point=(00:00 06:00)

while true
do
    now=`date +%H:%M`
    for t in ${point[*]} 
    do
        if [ $now == $t ]
        then
            echo "------------------------- Download start at `date` -------------------------"
            python binance.py create -f conf.json
            python binance.py download -f conf.json
            echo "------------------------- Download start at `date` -------------------------"
        fi
    done

    if [ `date +%M` == "00" ]
    then
        echo binance alive `date +%Y-%m-%dT%H:%M:%S`
    fi

    sleep 60
done