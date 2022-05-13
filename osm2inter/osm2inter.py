import os
import pathlib
from re import I
import time
import pexpect
import json
from datetime import datetime
import sys

def read_form_json(file):
    properties = json.load(open(file))
    servername = properties['servername'] 
    port = properties['port'] 
    username = properties['username'] 
    password = properties['password'] 
    database = properties['database']
    return servername, port, username, password, database

def get_absulte_path():
    return pathlib.Path(__file__).parent.absolute()

def get_current_time():
    return time.time()

def osm2inter():
    absolute_path = get_absulte_path()
    # logfile = str(absolute_path)+'/py-osm2inter-'+datetime.now().strftime("%Y-%m-%d-%H-%M-%S")+'.log'
    # log = open(logfile, 'w')
    # log.write('### Convert osm File to intermediate database ###')
    print('### Convert osm File to intermediate database ###')
    servername, port, username, password, database = read_form_json(str(absolute_path)+'/database-parameter.json')
    t_start = get_current_time()

    # preprocess psql
    preprocess_args = [
        'psql',
        ' --host='+str(servername),
        ' --port='+str(port),
        ' --username='+str(username),
        ' --password',
        ' --dbname='+str(database),
        ' --quiet',
        ' --file='+str(absolute_path)+'/preprocess.sql'        
        ]
    preprocess = pexpect.spawn(''.join(preprocess_args), encoding='utf-8')
    preprocess.expect('Password:\s?')
    preprocess.sendline(password)
    preprocess.expect(pexpect.EOF, timeout=None)
    # log.write(preprocess.before)
    # print(preprocess.before)
    
    
    
    # mapfeatures 
    copy_to_csv_args = [
        'psql',
        ' --host='+str(servername),
        ' --port='+str(port),
        ' --username='+str(username),
        ' --password',
        ' --dbname='+str(database),
        ' --quiet',
        ' -c "\copy inter.classification TO "',
        str(absolute_path),
        '"/classification.csv WITH DELIMITER \',\'";'
        ]
    copy_to_csv = pexpect.spawn(''.join(copy_to_csv_args), encoding='utf-8')
    copy_to_csv.expect('Password:\s?')
    copy_to_csv.sendline(password)
    copy_to_csv.expect(pexpect.EOF, timeout=None)
    # log.write(copy_to_csv.before)
    # print(copy_to_csv.before)
    print('Recreate database and saved new csv file')
    
    
    # osm2pgsql
    print("osm2pgsql process ...")
    osm2pgsql_args = [
        'osm2pgsql',
        ' --host='+str(servername),
        ' --port='+str(port),
        ' --user='+str(username),
        ' --password',
        ' --database='+str(database),
        ' --log-level=info'
        ' --extra-attributes',
        ' --output=flex'
        ' --style='+str(absolute_path)+'/osm2inter.lua',
        ' --create '+str(absolute_path)+'/littlemap.osm'
        ]
    osm2pgsql = pexpect.spawn(''.join(osm2pgsql_args), encoding='utf-8')
    osm2pgsql.expect('Password:\s?')
    osm2pgsql.sendline(password)
    osm2pgsql.expect(pexpect.EOF, timeout=None)
    # log.write(osm2pgsql.before)
    print(osm2pgsql.before)
    print('imported osm file in intermediate database')
    
    
    # postprocess
    postprocess_args = [
        'psql',
        ' --host='+str(servername),
        ' --port='+str(port),
        ' --username='+str(username),
        ' --password',
        ' --dbname='+str(database),
        ' --file='+str(absolute_path)+'/postprocess.sql'        
        ]
    postprocess = pexpect.spawn(''.join(postprocess_args), encoding='utf-8')
    postprocess.expect('Password:\s?')
    postprocess.sendline(password)
    postprocess.expect(pexpect.EOF, timeout=None)
    # log.write(postprocess.before)
    # print(postprocess.before)
    print('full import process done')

    t_end = get_current_time()
    duration = 'Duration Time(H:M:S): ' + time.strftime('%H:%M:%S', time.gmtime(t_end - t_start))
    # log.write(duration)
    print(duration)

def inter2ohdm():
    absolute_path = get_absulte_path()
    # logfile = str(absolute_path)+'/py-osm2inter-'+datetime.now().strftime("%Y-%m-%d-%H-%M-%S")+'.log'
    # log = open(logfile, 'w')
    # log.write('### Convert osm File to intermediate database ###')
    print('### Convert osm File to intermediate database ###')
    servername, port, username, password, database = read_form_json(str(absolute_path)+'/database-parameter.json')
    t_start = get_current_time()
    pass
    

def run(name: str, args):
    if len(args) > 1:
        if name == 'nt':
            run_nt(args[1])
        if name == 'posix':
            run_posix(args[1])
    else:
        get_help_message(args[1:])


def run_nt(methodename):
    absolute_path = get_absulte_path()

def run_posix(methodname):
    if 'osm2inter' == methodname:
        osm2inter()
        return
    elif 'inter2ohdm' == methodname:
        inter2ohdm()
        return
    else:
        get_help_message(methodname)
    

def get_help_message(args):
    message = f'To use this script choose args:\n'
    message += f'\t osm2inter -> to import an osm file in the intermediate database\n'
    message += f'\t inter2ohdm -> to convert intermediate data to the ohdm database\n'
    if len(args) == 0:
        print(message)
    else:
        message += f'\nyou wrote: '+str(args)
        print(message)
    

run(os.name, sys.argv)