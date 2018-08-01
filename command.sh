#! /bin/bash

python export.py > /etc/profile.d/env.sh
/usr/sbin/cron -f