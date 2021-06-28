import subprocess
import time


def start_pg_container():
    output = subprocess.run('docker ps | grep "cbc_dbmanager_test_postgres"',
                            shell=True,
                            capture_output=True,
                            text=True,
                            check=False)
    print(f'Looking for pg container: {output.stdout}')

    if len(output.stdout) < 3:

        # Check to make sure there's a DB dir. Must be in the same dir as this class.
        output = subprocess.run('ls db | grep "No such file or directory"',
                                shell=True,
                                capture_output=True,
                                text=True,
                                check=False)
        print(f'Looking for db directory {output.stdout}')

        if len(output.stdout) > 3:
            # DB doesn't exist. Make the DIR
            print('Creating database directory.')
            subprocess.run('mkdir db',
                           shell=True,
                           capture_output=False,
                           text=True,
                           check=False)

        # Start the container
        print('Container not found')
        subprocess.run('docker compose up -d',
                       shell=True,
                       capture_output=False,
                       text=True,
                       check=False)
        for x in range(10):
            print(f'Continue in {10 - x}s')
            time.sleep(1)

    print('Container is running.')


def shutdown_pg_container():
    # Start the container
    print('Shutting down postgres container')
    subprocess.run('docker compose down',
                   shell=True,
                   capture_output=False,
                   text=True,
                   check=False)