#! /bin/bash
full_file_name=$(realpath -e $0)
path=${full_file_name%/*} 
$path'/osm2inter/osm2inter.sh'

$path'/inter2ohdm/inter2ohdm.sh'

t1=$(date +%s)
$path'/update/osm2updatedb.sh'

psql -d ohdm -f $path'/update/04updateprocess.sql'

# psql -d ohdm -f $path'/update/05test_insert-in-ohdm.sql'

# psql -d ohdm -f $path'/update/06alter-inter-schema.sql'
t2=$(date +%s)
duration=$((t2 - t1))
printf "Import updatedb, update entries in updatedb and update ohdm\n, lasted(H:M:S)= %s\n" "$(date -d @${duration} -u +%H:%M:%S)"