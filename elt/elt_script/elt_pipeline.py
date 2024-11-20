import subprocess
import time


def waiting_for_postgres(host,delay_time = 5, max_retries = 5):
    retries = 0
    print('Connecting to Postgres .......')
    while retries < max_retries:
        try:
            result = subprocess.run(['pg_isready','-h',host],check = True, capture_output = True, text = True)
            if 'accepting connections' in result.stdout:
                print(f'Connection successfully')
                return True
        except subprocess.CalledProcessError as e:
            print(f'Connection failed: {e}')
            retries += 1
            print(f'Connection retrying.....Please wait for {delay_time} seconds . Attempt: {retries}/{max_retries}')
            time.sleep(delay_time)
    return False

if not waiting_for_postgres(host = 'source_postgres'):
    print('Failing to connect')
    exit(1)


source_config = {'host':'source_postgres',
                 'database':'source_db',
                 'user':'postgres',
                 'password':'secret'
                 }

destination_config = {'host':'destination_postgres',
                      'database':'destination_db',
                      'user':'postgres',
                      'password':'secret'
                 }
dump_command = ['pg_dump',
                '-h',source_config['host'],
                '-d',source_config['database'],
                '-U',source_config['user'],
                '-f','data_dump.sql',
                '-w'
                ]
subprocess_env = dict(PGPASSWORD = source_config['password'])

subprocess.run(dump_command,env = subprocess_env,check = True)

import_command = ['psql',
                '-h',destination_config['host'],
                '-d',destination_config['database'],
                '-U',destination_config['user'],
                '-a','-f','data_dump.sql'
                ]
subprocess_env = dict(PGPASSWORD = destination_config['password'])

subprocess.run(import_command,env = subprocess_env,check = True)

print('Ending ELT Script')
