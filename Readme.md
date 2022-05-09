This guide is intended as an instruction for the installation of a PostgreSQL database management system and the use of the other components: osm2pgsql and psql. 
osm2pgsql is a command line program written in portable C++ and serves as a tool to import geometric data in the form of osm files into the PostgreSQL database.
In order to be able to use osm2pgsql and psql completely, several tools are required, the installation of which is shown in the following.

>**NOTE**: Only Ubuntu 20.04 and Windows 10 have been successfully tested. 

# 1. Installation database management system (DBMS)
First we need the dbms itself, in this case we use PostgreSQL as the database management system.

You can install postgis and osm2pgsql in one step, when you use the [installation script](TODO: add link to script])
<details>
  <summary>Ubuntu 20.04</summary>
  
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

</details>

<br><br>

<details>
  <summary>Windows 10</summary>

  Download PostgreSQL 14.2 &rarr; https://www.enterprisedb.com/downloads/postgres-postgresql-downloads

  Start the installation of PostgresSQL.<br>
  First you have to set the Installation Directory, (default): `C:\Program Files\PostgreSQL\14`<br>
  Be sure the you have all Components selected:
  * PostgreSQL Server &rarr; this is the database management server itself
  * pgAdmin 4 &rarr; This GUI-based tool serves as an additional administrative interface for managing PostgreSQL
  * Stack Builder &rarr; This is used to install extensions for PostgreSQL.
  * Command Line Tools &rarr; We need this tool for the later setup of the database.

  Next, we need to define a directory where the data will be stored by the server,<br> (default): `C:\Program Files\PostgreSQL\14\data`<br>
  After that we have to assign a password for the superuser (postgres) of the database, i.e: `my_password`<br>
  We have to select the port number the server should listen on (default): `5432`<br>
  In addition, we can select the locale, but also leave it default.

  Now the installation of PostgreSQL should start. Once this is complete, the Stack Builder can be started directly. The Stack Builder can also be started manually afterwards.

  In the Stack Builder, the running PostgreSQL server must be selected. Here PostgreSQL 14 on port 5432.

  [PostGIS](https://postgis.net/) must be installed as an extension, this can be found under Spatial Extensions and the latest version should be selected:<br>
  _PostGIS 3.2 Bundle for PostgreSQL 14 (64bit) v3.2.0_ <br>
  Before the actual installation of PostGIS begins, the path for the downloaded files can be defined.<br>
  During the installation the path of the installation is requested, this can be the same as for PostgreSQL itself:<br>
  `C:\Program Files\PostgreSQL\14`.

  It also asks about setting up several environment variables, which should all be activated for the OHDM project.

  Afterwards the system should be restarted.
</details>

<br><br>

# 2. Installation osm2pgsql
You have to use osm2pgsql up to version 1.5.1, therefor we do not use the standard installation.

<details>
<summary>Ubuntu 20.04</summary>

If you have used the [installation script](TODO: add link to script]), you can skip this section.

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
</details>

<br><br>

<details>
<summary>Windows</summary>

## Installing osm2psql
You can [download prebuild binaries](https://osm2pgsql.org/download/windows/). Unpack the ZIP file and you can immediately use osm2pgsql.

The usage og osm2pgsql will be described later.
</details>

<br><br>

# 3. Preparation for the conversions
In order to import an osm file into the OHDM database, several intermediate steps have to be processed. In the first conversion step, osm2pgsql and psql is used to fill an intermediate database with osm data. Several components are needed for this.

All necessary scripts are in the GitHub repository [OHDMConverter Branch SteSad](https://github.com/OpenHistoricalDataMap/OHDMConverter/tree/SteSad)

This repository can be clone with:
```bash
git clone -b SteSad https://github.com/OpenHistoricalDataMap/OHDMConverter.git
```

## osm file
One little example file of an osm file is also in the cloned repository `littlemap.osm`<br>
This file contains the area of the HTW Berlin Campus Wilhelminenhof.

Other osm files can be downloaded from:<br>
https://www.openstreetmap.org/ ,but you have to pay attention to the size.

The osm file from the hole planet can be downloaded from:<br>
https://planet.openstreetmap.org/

## lua script
To use osm2pgsql with the Flex Output option you need a lua script, which describes the tables in the database and specifies the conversion of each osm object.<br>
More information: https://osm2pgsql.org/doc/manual.html#the-flex-output

## postgresql scripts
```bash
psql -c "CREATE DATABASE ohdm;"
```
The preparation of the database can be realized with the [preprocess.sql](TODO: add link). In this file there is also the complete insert statement for the necessary osm map features.

# Usage

To ensure that the target schema corresponds to what the OHDM converter understands, a lua script is included when using the osm2pgsql tool.<br>
> [osm2inter.lua](https://github.com/OpenHistoricalDataMap/OHDMConverter/blob/SteSad/osm2inter/osm2inter.lua)


```
sudo -iu <databaseuser> osm2pgsql -d <databasename> -W \
  --extra-attributes --output=flex --style=<your_defined_lua_script> \
  -c <your_osm_file>
```
* `sudo -iu <database_user>` &rarr; to run followed commands as specified user
* `-d <database_name>` &rarr; specifies the database in which to work
* `-W` &rarr; will promt a password input for the database connection
* `-x` &rarr; needed for using timestamp, uid and username from osm tags
* `-O flex` &rarr; activate using spezified style files
* `-S <your_defined_lua_script>` &rarr; specify lua file wich will use
* `-c <your_osm_file>` &rarr; specify the osm file wich will use

i.e.
```
sudo -iu postgres osm2pgsql -d ohdm -W -x -O flex -S osm2inter.lua -c berlin-latest.osm
```

</details><br><br>


<details><summary>Windows</summary>

### Installing osm2psql
```
C:\Program Files\osm2pgsql\.\osm2pgsql.exe -d ohdm -U postgres -W -x -O flex -S osm2inter.lua -c berlin-latest.osm
```

</details>