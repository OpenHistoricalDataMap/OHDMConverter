#! /bin/bash
full_file_name=$(realpath -e $0)
path=${full_file_name%/*} 
parentpath=$(builtin cd $path; cd ..; cd ..; pwd)
t1=$(date +%s)
psql -d ohdm -f $path'/01preprocess.sql'
sleep 3

classification_file=$path"/02classification.csv"
psql -d ohdm -c "\copy updatedb.classification TO "$classification_file" WITH DELIMITER ',';" 
printf "Saved classifications into %s\n\n" "$classification_file"


osm2pgsql --slim -d ohdm -x -O flex -S $path'/02osm2updatedb.lua' -c $path'/02testupdate.osm'
# osm2pgsql --slim -d ohdm -x -O flex -S $path'/02osm2updatedb.lua' -c $parentpath'/osm-files/berlin_2022-06-09.osm.pbf'
# osm2pgsql --slim -d ohdm -x -O flex -S $path'/02osm2updatedb.lua' -c $parentpath'/osm-files/germany_2022-06-12.osm.pbf'
printf "Converted osm2updatedb\n"
sleep 3
psql -d ohdm -f $path'/03postprocess.sql'
t2=$(date +%s)
duration=$((t2 - t1))
printf "Convert osm2updatedb, lasted(H:M:S)= %s\n" "$(date -d @${duration} -u +%H:%M:%S)"