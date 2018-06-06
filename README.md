# Custom ETL Pipeline


An ETL tool for your MongoDB and Postgres.

## Setup
Install requirements locally: `pip install -r requirements.txt`

Install purr: `python setup.py install`

## Usage
`purr [option]`

optional arguments:
`-h, --help` show this help message and exit
`-sf, --setup-file` - define path to setup.yml file which should look like the following:   
```
postgres: 
  db_name: my_pg_database
  user: my_pg_user
  schema_name: death_to_mosql
  schema_reset: false
  table_truncate: false
  table_drop: false
mongo:
  db_name: my_mongo_database
  repl_set_members: mongo_conn_string
  collections:
    - Company
    - Customer
    - User
tailing: true
typecheck_auto: false
```
It is not necessary to add all the collections in setup.yml. These collection must be in collections.yml, otherwise they cannot be transferred to the Postgres database.

`-cf, --collection-file` - define path to collection.yml file which contains information about the collections and its fields which will be transfered.
```
my_mongo_database:
  Company:
    :columns:
      - id:
        :source: _id
        :type: TEXT
      - name:
        :source: name
        :type: TEXT
      - active:
        :source: active
        :type: BOOLEAN
      - domains:
        :source: domains
        :type: JSONB
      - created_at:
        :source: createdAt
        :type: TIMESTAMP
    :meta:
      :table: company
      :extra_props: JSONB
```

**Examples**

`purr -sf setup.yml -cf collections.yml`
* transfers collections defined in setup.yml with attribute names and types from collections.yml

