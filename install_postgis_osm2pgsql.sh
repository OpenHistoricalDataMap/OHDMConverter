#! /bin/bash
[[ $EUID -ne 0 ]] && echo "This script must be run as root." && exit 1

# install postgresql
sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
apt-get update
apt-get -y install postgresql-14
apt-get install postgresql-14-postgis-3

echo "Installed PostgreSQL and PostGIS"

# install osm2pgsql

git clone https://github.com/openstreetmap/osm2pgsql.git

apt-get install make cmake g++ libboost-dev libboost-system-dev \
  libboost-filesystem-dev libexpat1-dev zlib1g-dev \
  libbz2-dev libpq-dev libproj-dev lua5.3 liblua5.3-dev pandoc

cd osm2pgsql

mkdir build && cd build
cmake ..

make

make man

make install

printf "use osm2pgsql like\n"
printf "sudo -iu <database-user> osm2pgsql -d <database-name> \n\t -x -O flex \n\t -S <your-defined-lua-script \n\t -c <your-osm-file>\n"
printf "NOTE: all paths must be full absolute paths\n"
printf "Example:\n"
printf "sudo -iu postgres osm2pgsql -d ohdm -x -O flex\\ \n -S /home/postgres/OHDMConverter/osm2inter.lua\\ \n -c /home/postgres/OHDMConverter/berlin.osm\n"