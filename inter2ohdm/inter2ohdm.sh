#! /bin/bash
full_file_name=$(realpath -e $0)
path=${full_file_name%/*} 
t1=$(date +%s)
psql -d ohdm -f $path'/inter2ohdm.sql'
sleep 3
t2=$(date +%s)
duration=$((t2 - t1))
echo "Duration Time(H:M:S): $(date -d @${duration} -u +%H:%M:%S)"