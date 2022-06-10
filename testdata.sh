#! /bin/bash
full_file_name=$(realpath -e $0)
path=${full_file_name%/*} 
$path'/osm2inter/osm2inter.sh'

$path'/inter2ohdm/inter2ohdm.sh'

$path'/update/osm2updatedb.sh'

psql -d ohdm -f $path'/update/04updateprocess.sql'

psql -d ohdm -f $path'/update/05test_insert-in-ohdm.sql'

# psql -d ohdm -f $path'/update/06alter-inter-schema.sql'