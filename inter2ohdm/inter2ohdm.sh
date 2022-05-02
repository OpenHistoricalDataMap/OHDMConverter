#! /bin/bash
path=$(pwd)
t1=$(date +%s)
sudo -iu postgres psql -d ohdm -f $path'/inter2ohdm.sql'
sleep 3
t2=$(date +%s)
duration=$((t2 - t1))
echo "Duration Time(H:M:S): $(date -d @${duration} -u +%H:%M:%S)"