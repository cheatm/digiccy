#! /bin/bash

python export.py > /etc/profile.d/env.sh
crontab $PWD/routing/schedule
/usr/sbin/cron -f