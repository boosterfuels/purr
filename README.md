# Custom ETL Pipeline

An ETL tool for your MongoDB and Postgres.

![WatchingYou](https://i.pinimg.com/736x/46/ab/1c/46ab1c8f2dc96d112ba7304782d59599--funny-animals-funny-cats.jpg)

## Support
MongoDB 3.4.2
PostgreSQL 9.6.8

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
  connection: mongo_connection_string
tailing: true
typecheck_auto: false
```
It is not necessary to add all the collections in setup.yml. These collection must be in collections.yml, otherwise they cannot be transferred to the Postgres database.

`-cf, --collection-file` - define path to collections.yml file which contains information about the collections and its fields which will be transfered.
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
* start transfering collections
