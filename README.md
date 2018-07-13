# Custom ETL Pipeline

An ETL tool to transfer data from MongoDB to Postgres.

## Support

MongoDB 3.4.2

PostgreSQL 9.6.8


![WatchingYou](https://i.pinimg.com/736x/46/ab/1c/46ab1c8f2dc96d112ba7304782d59599--funny-animals-funny-cats.jpg)

## Setup
Install requirements locally: `pip install -r requirements.txt`

Install purr: `python setup.py install`

## Usage
`purr -sf path/to/setup.yml -cf path/to/collections.yml`

`-h, --help` show help message

`-sf, --setup-file` 

You can create a setup file to organize your settings.

- path to YAML file which contains settings
  - settings for Postgres: 
    - `db_name`: name of database
    - `connection`: connection string to database 
    - `schema_name`: name of schema where the collections will be transfered to 
    - `schema_reset`: 
      - `true`: reset existing schema
      - `false`: keep existing schema
    - `table_truncate`: truncate table before starting data transfer
    - `table_drop`: drop table before starting data transfer
  - settings for MongoDB
    - `db_name`: name of database
    - `connection`: connection string to database 
  - general settings 
    - `tailing`: keep tailing the oplog after collection transfer is finished
    - `typecheck_auto`: 
      - `true`: let Purr decide the data type for each field
      - `false`: use a YAML file to describe collection types (`-cf`) 
  

You can also set these variables using the command line.
- `-sf or --setup-file`: path to the setup file if exists
- `-cf or --collection-file`: path to the collection file if exists
- `-td or --table-drop`: defaults to `false`
- `-tt or --table-truncate`: defaults to `false`
- `-sr or --schema-reset`: defaults to `false`
- `-sn or --schema-name`: defaults to `public`
- `-pg or --pg-connection`: connection string to PG database (*)
- `-mdb or --mongo-connection`: connection string to Mongo database (*)
- `-n or --mongo-db-name`: equivalent of `db_name` for MongoDB (*)
- `-t or --tail`: equivalent of `tailing`; defaults to `false`
- `-ta or --typecheck-auto`: defaults to `false`

Variables followed by (*) are mandatory. 

**Example setup.yml**
 
```
postgres: 
  db_name: my_pg_database
  connection: postgres://127.0.0.1:5432/postgres
  schema_name: maine_coon
  schema_reset: false
  table_truncate: false
  table_drop: false
mongo:
  db_name: my_mongo_database
  connection: mongodb://localhost:27017
tailing: true
typecheck_auto: false
include_extra_props: true
```


`-cf, --collection-file` 
- path to YAML file which contains information about the collections and its fields
- all the collections described here will be transferred 

**Example collections.yml**

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

**Example: start Purr using a setup file and collections described in collections.yml**

`purr -sf setup.yml -cf collections.yml`


**Example: start Purr without setup file (tailing mode)**

`purr -cf collections.yml -pg postgres://127.0.0.1:5432/postgres -mdb mongodb://localhost:27017 -n db_name -t`