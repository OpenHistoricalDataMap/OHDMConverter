#! /bin/bash
full_file_name=$(realpath -e $0)
path=${full_file_name%/*} 
t1=$(date +%s)
psql -d ohdm -f $path'/preprocess.sql'
sleep 3

classification_file=$path"/classification.csv"
psql -d ohdm -c "\copy inter.classification TO "$classification_file" WITH DELIMITER ',';" 
printf "Saved mapfeatures into %s\n\n" "$classification_file"


osm2pgsql -d ohdm -x -O flex -S $path'/osm2inter.lua' -c $path'/map.osm'
printf "Converted osm2inter\n"
sleep 3
psql -d ohdm -f $path'/postprocess.sql'
printf "Postprocess done\n"
t2=$(date +%s)
duration=$((t2 - t1))
printf "Duration Time(H:M:S): %s" "$(date -d @${duration} -u +%H:%M:%S)"