# OHDMConverter with osm2pgsql tool

## osm2pgsql
To convert OSM data files with the suffix .osm, .osm.gz, .osm.bz2, or .osm.pbf, sometimes just .pbf into an intermediate database, the osm2pgslq tool is needed.

To ensure that the target schema corresponds to what the OHDM converter understands, a lua script is included when using the osm2pgsql tool.<br>
> [osm2inter.lua](https://github.com/OpenHistoricalDataMap/OHDMConverter/blob/SteSad/osm2inter/osm2inter.lua)

<details><summary>Ubuntu 20.04</summary>

### Installing osm2pgsql
> **NOTE** Detailed instructions can be found at the following link. https://github.com/openstreetmap/osm2pgsql

The latest source code is available in the osm2pgsql git repository on GitHub and can be downloaded as follows:
```
git clone git://github.com/openstreetmap/osm2pgsql.git
```
Osm2pgsql uses the cross-platform CMake build system to configure and build itself.

There are some libaries required, you have to install it with
```
sudo apt-get install make cmake g++ libboost-dev libboost-system-dev \
  libboost-filesystem-dev libexpat1-dev zlib1g-dev \
  libbz2-dev libpq-dev libproj-dev lua5.3 liblua5.3-dev pandoc
```

Once dependencies are installed, use CMake to build the Makefiles in a separate folder:
```
mkdir build && cd build
cmake ..
```

When the Makefiles have been successfully built, compile with
```
make
```

The man page can be rebuilt with:
```
make man
```

The compiled files can be installed with
```
sudo make install
```
After that steps, osm2pgsql successfully installed and be used like<br>
```
sudo -iu <databaseuser> osm2pgsql -d <databasename> -W \
  --extra-attributes --output=flex --style=<your_defined_lua_script> \
  -c <your_osm_file>
```
* `sudo -iu <database_user>` &rarr; to run followed commands as specified user
* `-d <database_name>` &rarr; specifies the database in which to work
* `-W` &rarr; will promt a password input for the database connection
* `--extra-attributes` &rarr; needed for using timestamp, uid and username from osm tags
* `--output=flex` &rarr; activate using spezified style files
* `--style=<your_defined_lua_script>` &rarr; specify lua file wich will use
* `-c <your_osm_file>` &rarr; specify the osm file wich will use

i.e.
```
sudo -iu postgres osm2pgsql -d ohdm -W --extra-attributes --output=flex --style=osm2inter.lua -c berlin-latest.osm
```

</details><br><br>


<details><summary>Windows</summary>

### Installing osm2psql
You can [download prebuilt binaries](https://osm2pgsql.org/download/windows/). Unpack the ZIP file and you can immediately use osm2pgsql.

```
C:\Program Files\osm2pgsql\.\osm2pgsql.exe -d ohdm -U postgres -W --output=flex --style=osm2inter.lua -c berlin-latest.osm
```

</details>