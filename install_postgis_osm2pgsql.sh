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

printf "Installed osm2pgsql\n"