# Custom ETL Pipeline


An ETL tool for your MongoDB and Postgres.

## Setup
Install requirements locally: `pip install -r requirements.txt`

Install purr: `python setup.py install`

## Usage
`purr [option]`

optional arguments:
`-h, --help` show this help message and exit

`-c MDB_COLLS [MDB_COLLS ...], --colls [MDB_COLLS ...]`
MongoDB collections which will be transfered. Case
sensitive!
`-sf, --setup-file` Transfer all collections from MongoDB to PG

`-cf, --collection-file` 

**Examples**

`purr -sf setup.yml -cf collections.yml`
* transfers collections defined in setup.yml with attribute names and types from collections.yml

