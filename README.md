# cbc_redshift_db_connect




## .env Setup for testing

Example:
```
DEBUG_MODE=true
# Postgres Test DB
POSTGRES_USER=test
POSTGRES_PASSWORD=test
POSTGRES_DB=test
PGDATA=/var/lib/postgresql/data/pgdata

# Path to the database on the local file system. 
LOCALDATABASE=~/Code/dbmanager/tests/db

# SSH
USE_SSH=False

# DB Config
DB_NAME=test
DB_USER=test
DB_PORT=5434
DB_PASSWORD=test
DB_SCHEMA=public
DB_HOST=localhost
```


## Build cheat 

python setup.py sdist
twine upload dist/*
