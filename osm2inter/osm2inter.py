import os
import pathlib
import time
import subprocess

def get_absulte_path():
    return pathlib.Path(__file__).parent.absolute()

def get_current_time():
    return time.time()

def run(name: str):
    if name == 'nt':
        run_nt()
    if name == 'posix':
        run_posix()

def run_nt():
    absolute_path = get_absulte_path()

def run_posix():
    absolute_path = get_absulte_path()
    t_start = get_current_time()
    os.system('sudo -iu postgres psql -d ohdm -f '+str(absolute_path)+'/preprocess.sql')
    classification_file = open(str(absolute_path) + '/classification.csv', 'w')
    result = subprocess.run(["sudo", "-iu", "postgres", "psql", "-d", "ohdm", "-f", str(absolute_path)+"/extract_classification_to_csv.sql"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    classification_file.write(result.stdout)
    classification_file.close()
    print(result.stderr)
    os.system('sudo -iu postgres osm2pgsql -d ohdm -x -O flex -S '+str(absolute_path)+'/osm2inter.lua -c '+str(absolute_path)+'/berlin.osm')
    print('Converted osm2inter\n')
    os.system('sudo -iu postgres psql -d ohdm -f '+str(absolute_path)+'/postprocess.sql')
    print('Postprocess done\n')
    t_end = get_current_time()
    print('Duration Time(H:M:S): ' + time.strftime('%H:%M:%S', time.gmtime(t_end - t_start)))


run(os.name)