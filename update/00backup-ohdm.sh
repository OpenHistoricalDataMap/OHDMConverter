#! /bin/bash
full_file_name=$(realpath -e $0)
path=${full_file_name%/*} 
t1=$(date +%s)
backuppath=$path/backup_$(date +\%Y-\%m-\%d-\%H-\%M)/

pg_dump --dbname=ohdm --format=directory --jobs=10 --file=$backuppath

t2=$(date +%s)
duration=$((t2 - t1))
printf "Saved dump from ohdm database on $backuppath \n"
printf "Duration Time(H:M:S): %s\n" "$(date -d @${duration} -u +%H:%M:%S)"

### restore database with: ###
# pg_restore --dbname=ohdm --format=directory --clean --create --jobs=10 --file=$backuppath

### for backup in one sql file use: ###
# mkdir $path'/backup/'
# backupfile=$path'/backup/backup-ohdm_'$(date +\%Y-\%m-\%d-\%H-\%M)'.sql'
# pg_dump --dbname=ohdm --file=$backupfile
# printf "Saved dump from ohdm database on $backupfile \n"
# printf "Duration Time(H:M:S): %s\n" "$(date -d @${duration} -u +%H:%M:%S)"

