#! /bin/bash
full_file_name=$(realpath -e $0)
path=${full_file_name%/*} 
psql -d ohdm -f $path'/inter2ohdm.sql'