#! /bin/bash
full_file_name=$(realpath -e $0)
path=${full_file_name%/*} 
t1=$(date +%s)
psql -d ohdm -f $path'/01preprocess.sql'
sleep 3

classification_file=$path"/02classification.csv"
psql -d ohdm -c "\copy updatedb.classification TO "$classification_file" WITH DELIMITER ',';" 
printf "Saved mapfeatures into %s\n\n" "$classification_file"


osm2pgsql -d ohdm -x -O flex -S $path'/02osm2updatedb.lua' -c $path'/02testupdate.osm'
# osm2pgsql -d ohdm -x -O flex -S $path'/02osm2updatedb.lua' -c $path'/berlin.osm.pbf'
printf "Converted osm2updatedb\n"
sleep 3
psql -d ohdm -f $path'/03postprocess.sql'
t2=$(date +%s)
duration=$((t2 - t1))
printf "Convert osm2updatedb, lasted(H:M:S)= %s\n" "$(date -d @${duration} -u +%H:%M:%S)"