#! /bin/bash
full_file_name=$(realpath -e $0)
path=${full_file_name%/*} 
t1=$(date +%s)
sudo -iu postgres psql -d ohdm -f $path'/preprocess.sql'
sleep 3

classification_file=$path'/classification.csv'
classifiaction_csv=$(sudo -iu postgres psql -d ohdm -f $path'/extract_classification_to_csv.sql')
sudo echo "$classifiaction_csv" | sudo tee "$classification_file" > /dev/null
printf "Saved mapfeatures into %s\n\n" "$classification_file"


sudo -iu postgres osm2pgsql -d ohdm -x -O flex -S $path'/osm2inter.lua' -c $path'/littlemap.osm'
printf "Converted osm2inter\n"
sleep 3
sudo -iu postgres psql -d ohdm -f $path'/postprocess.sql'
printf "Postprocess done\n"
t2=$(date +%s)
duration=$((t2 - t1))
printf "Duration Time(H:M:S): %s" "$(date -d @${duration} -u +%H:%M:%S)"