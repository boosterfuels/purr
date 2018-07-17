# Custom ETL Pipeline

Purr is an ETL tool which transfers your collections from MongoDB to Postgres. 


![WatchingYou](https://i.pinimg.com/736x/46/ab/1c/46ab1c8f2dc96d112ba7304782d59599--funny-animals-funny-cats.jpg)

## Setup
Install requirements locally: `pip install -r requirements.txt`

Install Purr: `python setup.py install`


## Support

MongoDB 3.4.2

PostgreSQL 9.6.8


## Quickstart:
1. Install requirements and [Purr](#setup)
2. Create a YAML file which contains the [collection map](#collection-map).
3. Make sure you started Mongo (as a [replica set](https://docs.mongodb.com/manual/replication/)) and PG
4. On the command line type:
`purr -cf path/to/collections.yml -pg postgres://127.0.0.1:5432/postgres -mdb mongodb://localhost:27017 -n db_name -t`

With this quickstart, the following happens:
After transfering all collections to your public schema, Purr starts tailing the oplog. Make changes to any of your documents and check what happens in your Postgres database.


## Usage

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
- `-ex or --typecheck-auto`: defaults to `false`

Variables followed by (*) are mandatory. 

Start Purr using a setup file:
`purr -sf path/to/setup.yml -cf path/to/collections.yml`


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

## Collection map

`-cf, --collection-file` 

The collection map is a YAML file which contains information about the database and the collections you want to transfer. Only the collections that are described here will be transfered to your Postgres database. A collection map should have the following structure:

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

*Explanation:*

When connecting to the Mongo instance, Purr looks for the database name in the collection map (`my_mongo_database`).
Collection `Company` will be transferred to table `company` (described under `meta` -> `table`). This table will have 6 columns with the following types:
```
id: text, 
name: text, 
active: boolean, 
domains: jsonb, 
created_at: text,
extra_props: jsonb
```

### Data types
Purr defines 5 different data types:
- boolean
- double precision
- text
- timestamp
- jsonb

### Extra properties

Extra properties are properties of a document in MongoDB which do not have their own defined name, source and type in the collection map.
Having extra properties means that values which are not defined in the collection map will be part of a column named `extra_props` with type defined based on 
the collection map.
Leaving out extra properties from the collection map will make `extra_props` type default to `JSONB`.

In case that you want to include extra properties you have to start Purr with option `-ex`. 

If you already have an extra_props column but you restarted Purr without this option, all columns named `extra_props` one by one.
- drop column `extra_props` for `Coll1`
- drop columns that are not in the collection map
- start transfer
... and so on for the other collections

Starting Purr without extra properties can be significantly faster.

**Example: start Purr using a setup file and collections described in collections.yml**

`purr -sf setup.yml -cf collections.yml`


**Example: start Purr without setup file (tailing mode)**

`purr -cf collections.yml -pg postgres://127.0.0.1:5432/postgres -mdb mongodb://localhost:27017 -n db_name -t`

## Tailing
If `-t` was set, Purr starts tailing the `oplog` after transferring all the collections described in the collection map. The oplog is a capped collection that records all write operation that happened in your Mongo instance. Tailing is started from the timestamp Purr saved in the beginning (before it created `purr_info`). Tailing has to happen after all the tables were created since there may be write operations on a collection that was not yet transferred and therefore it's corresponding relation does not yet exist.

## Output

Purr will log a warning if it could not transfer a document.

## Connectivity issues
In the begging, Purr creates a table called `purr_info` which contains the timestamp which is refreshed every x minutes if there was a successful transfer. If Purr is disconnected from the database it waits a couple of seconds before attempting to reconnect. If succeeded, Purr first checks `purr_info` for the latest timestamp it managed to save and continues tailing from there.

## Contribute
Purr was built using Python3. If you would like to contribute, check out our guidelines

## Guidelines