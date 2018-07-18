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

You can start Purr using command line options or you can use a setup file to organize your settings.

### Setup file
`-h, --help` show help message
`-sf, --setup-file` 

Your setup has to be in a YAML file which must have the following form:

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
    - `include_extra_props`: include properties which are not described

You can start Purr using a setup file like this:
`purr -sf path/to/setup.yml -cf path/to/collections.yml`

### Command line options

You can set all the variables from the previous section using the command line. Passing connection strings to MongoDB and Postgres is mandatory.

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
- `-ex or --include-extra-properties`: defaults to `false`

You can start Purr using a setup file like this:
`purr -cf path/to/collections.yml -pg postgres://127.0.0.1:5432/postgres -mdb mongodb://localhost:27017 -n mongo_db_name -t`

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

## Collection map

`-cf, --collection-file` 

The collection map is a YAML file which contains information about the database and the collections you want to transfer. Only the collections that are described here will be transfered to your Postgres database.

A collection map should have the following structure:

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
id: text
name: text
active: boolean
domains: jsonb
created_at: text
extra_props: jsonb
```

### Data types
Purr uses 5 different data types when creating rows for a table:
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
- drop column `extra_props` for `Collection1`
- drop columns that are not in the collection map
- start transfer
... and so on for the other collections

Starting Purr without extra properties can be significantly faster.

**Example: start Purr using a setup file and collections described in collections.yml**

`purr -sf setup.yml -cf collections.yml`


**Example: start Purr without setup file (tailing mode)**

`purr -cf collections.yml -pg postgres://127.0.0.1:5432/postgres -mdb mongodb://localhost:27017 -n db_name -t`
If `-t` is set, Purr starts tailing the `oplog` after transferring all the collections described in the collection map. The oplog is a capped collection that records all write operations that happened in your Mongo instance. Tailing is started from the timestamp Purr saved in the beginning (before it created `purr_info`). Tailing has to happen after all the tables were created since there may be write operations on a collection that was not yet transferred and therefore it's corresponding relation does not yet exist.Start Mongo as a replicaset.

## Restarting Purr with different collection map

Sooner or later you will need to change your collection map. Restarting Purr will do the following if you did not delete your tables previously:
Adding a new attribute will make Purr update your entire collection.
Removing a attribute drops the entire column.
Changing a column name will drop the old column and create the new one. 
Changing a column type will result with an attempt to ALTER the column. If the attempt was unsuccessful, Purr will try to convert your data to JSONB.

## Tailing

You can start tailing from a specific timestamp by passing `-ts` when starting Purr which skips collection transfer.
If no value is added, Purr will do the following:
- `purr_info` will be created if it does not exist
    - the current timestamp will be inserted 
- if `purr_info` exists, it will read the latest timestamp and check if the oplog has any new entries
  - if the timestamp is "too old", Purr transfers all collections

If timestamp is added, Purr will do the following:
- `purr_info` will be created if it does not exist
    - the given timestamp will be inserted 
- if `purr_info` exists, the latest timestamp will be compared to the given timestamp and the oldest wins

## Output

Purr will log a warning if it could not transfer a document.

## Connectivity issues
In the begging, Purr creates a table called `purr_info`. This table contains a timestamp which is refreshed every x minutes in case of a successful transfer. If Purr is disconnected from the database, it waits a couple of seconds before attempting to reconnect. If succeeded, Purr first checks `purr_info` for the latest timestamp it managed to save and continues tailing from there.

## Contribute
Purr is an open-source project that was built using Python3. If you would like to contribute, check out our [guidelines](##guidelines)

### Guidelines
Contributing to Purr is a great way to learn more about 
- MongoDB
- PostgreSQL
- Python3