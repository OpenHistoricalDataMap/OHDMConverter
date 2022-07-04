This guide is intended as an instruction for the installation of a PostgreSQL database management system and the use of the other components: osm2pgsql and psql. 
osm2pgsql is a command line program written in portable C++ and serves as a tool to import geometric data in the form of osm files into the PostgreSQL database.
In order to be able to use osm2pgsql and psql completely, several tools are required, the installation of which is shown in the following.

>**NOTE**: Successfully tested on Ubuntu 20.04

# Installation database management system (DBMS)
First we need the dbms itself, in this case we use PostgreSQL as the database management system.

You can install postgis and osm2pgsql in one step, when you use the [installation script](https://github.com/OpenHistoricalDataMap/OHDMConverter/blob/SteSad/install_postgis_osm2pgsql.sh)
  
Create the file repository configuration:

```
sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
```

Import the repository signing key:

```
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
```

Update the package lists:
```
sudo apt-get update
```

Install the PostgreSQL.

```
sudo apt-get -y install postgresql-14
```

A server is also initialised within the installation. This server has the following values:
* Name of the server: main
* Port number: 5432
* Name of the database: postgres
* Name of the database owner: postgres

In addition you have to install [PostGIS](https://postgis.net/): 
```
sudo apt-get install postgresql-14-postgis-3
```
PostGIS is a spatial database extender for PostgreSQL object-relational database. It adds support for geographic objects allowing location queries to be run in SQL.

<br><br>

# Installation osm2pgsql
You have to use osm2pgsql up to version 1.5.1, therefor we do not use the standard installation.

If you have used the [installation script](https://github.com/OpenHistoricalDataMap/OHDMConverter/blob/SteSad/install_postgis_osm2pgsql.sh), you can skip this section.

Download a Version from the [Releases](https://github.com/openstreetmap/osm2pgsql/releases) up to 1.5.1<br>
and extract the tar container or use the following script
```
cd /opt
sudo wget https://github.com/openstreetmap/osm2pgsql/archive/refs/tags/1.6.0.tar.gz
sudo tar -xvf 1.6.0.tar.gz && sudo rm 1.6.0.tar.gz
cd osm2pqsql-1.6.0
```

Install neccessary packages to install osqm2psql as a C++ project
```
sudo apt-get install make cmake g++ libboost-dev libboost-system-dev \
  libboost-filesystem-dev libexpat1-dev zlib1g-dev \
  libbz2-dev libpq-dev libproj-dev lua5.3 liblua5.3-dev pandoc
```

Once dependencies are installed, use CMake to build the Makefiles in a separate folder:
```
sudo mkdir build && cd build
sudo cmake ..
```

When the Makefiles have been successfully built, compile with
```
sudo make
```

The man page can be rebuilt with:
```
sudo make man
```

The compiled files can be installed with
```
sudo make install
```

The usage og osm2pgsql will be described later.

<br><br>

# Create database user
To ensure that all scripts can be executed, you must create a database user who has full access rights to the database.
```
createuser --interactive --pwprompt
```
<br><br>

# Preparation for the conversions
In order to import an osm file into the OHDM database, several intermediate steps have to be processed. In the first conversion step, osm2pgsql and psql is used to fill an intermediate database with osm data. Several components are needed for this.

All necessary scripts are in the GitHub repository [OHDMConverter Branch SteSad](https://github.com/OpenHistoricalDataMap/OHDMConverter/tree/SteSad)

This repository can be clone with:
```bash
git clone -b SteSad https://github.com/OpenHistoricalDataMap/OHDMConverter.git
```

## osm file
One little example file of an osm file is also in the cloned repository [02testmap.osm](https://github.com/OpenHistoricalDataMap/OHDMConverter/blob/SteSad/osm2inter/02testmap.osm)<br>
This file contains the area of the HTW Berlin Campus Wilhelminenhof.

Other osm files can be downloaded from:<br>
https://www.openstreetmap.org/ ,but you have to pay attention to the size.

The osm file from the hole planet can be downloaded from:<br>
https://planet.openstreetmap.org/

## lua script
To use osm2pgsql with the Flex Output option you need a lua script, which describes the tables in the database and specifies the conversion of each osm object.<br>
More information: https://osm2pgsql.org/doc/manual.html#the-flex-output

## postgresql scripts
All scripts expect the ohdm database to be present.
```bash
psql -c "CREATE DATABASE ohdm;"
```

### add neccessary extension
- PostGIS is a spatial database extender for 		PostgreSQL object-relational database. It adds support for geographic objects allowing location queries to be run in SQL.
	```bash
	psql -d ohdm -c "CREATE EXTENSION postgis;"
	```

- This module implements the hstore data type for 	storing sets of key/value pairs within a single PostgreSQL value.
	```bash
	psql -d ohdm -c "CREATE EXTENSION hstore;"
	```
- The pg_trgm module provides functions and operators for determining the similarity of alphanumeric text based on trigram matching, as well as index operator classes that support fast searching for similar strings.
	```bash
	psql -d ohdm -c "CREATE EXTENSION pg_trgm;"
	```

> **NOTE** <...> contained in the code blocks must be replaced with your own parameters.

<br><br>

# Import OSM file to the intermediate database
<details><summary>Use of individual scripts</summary>

## Preprocess
The 2 databases need the [map features](https://wiki.openstreetmap.org/wiki/Map_features) as a reference table. The [01preprocess.sql](https://github.com/OpenHistoricalDataMap/OHDMConverter/blob/SteSad/osm2inter/01preprocess.sql) realizes this and also creates the schema "inter".

You can find the 01preprocess.sql script in the osm2inter folder after clone this repository and run with:
```
psql \
--host=<servername> \
--port=<port> \
--username=<username> \
--password \
--dbname=<database> \
--file=01preprocess.sql
```
<br>

## Extract Map Features as CSV File
The lua script in the next step need a csv file with all map features to refer the created classcodes in the nodes, ways and relation entries. <br>
To realizes that run:
```
psql \
--host=<servername> \
--port=<port> \
--username=<username> \
--password \
--dbname=<database> \
--command="\copy inter.classification TO <file_you_want_copy_to> WITH DELIMITER ',';"
```
<br>

## Import OSM File with osm2pgsql
The previous steps ensure that a database schema with the name "inter" exists and that all map features are entered as tables in this database schema. In addition, a csv file is created that reflects these entries. 

Now the actual import can be started.
```
osm2pgsql \
--host=<servername> \
--port=<port> \
--user=<username> \
--password \
--database=<database> \
--log-level=info \
--extra-attributes \
--output=flex \
--style=02osm2inter.lua \
--create 02testmap.osm
```
<br>

## Postprocess
The osm2pgsql tool creates columns with unique non "Null" values, but these values are not recognized as primary keys in the PostgreSQL database. Therefore, after using the osm2pgsql tool, make sure that all tables with generated ids become primary keys. This is done with the [03postprocess.sql](https://github.com/OpenHistoricalDataMap/OHDMConverter/blob/SteSad/osm2inter/03postprocess.sql) script.

```
psql \
--host=<servername> \
--port=<port> \
--username=<username> \
--password \
--dbname=<database> \
--file=03postprocess.sql
```

</details><br><br>

<details><summary>Import as one process</summary>

In addition, there is a Python file which executes all the individual processes together. For this Python script, a JSON file is needed that contains all database information:<br>
[database-parameter.json](https://github.com/OpenHistoricalDataMap/OHDMConverter/blob/SteSad/osm2inter/database-parameter.json)
```json
{
	"servername":"localhost",
	"port":"5432",
	"username":"postgres",
	"password":"my_password",
	"database":"ohdm"
}
```

Afterwards, this Python script can be executed as follows:
```
python3 osm2inter.py osm2inter
```
The additional argument "osm2inter" describes the conversion method to be used.
</details><br><br>
