# Purr

Purr is an open source ETL tool which transfers your collections from MongoDB to PostgreSQL.

![WatchingYou](https://i.pinimg.com/736x/46/ab/1c/46ab1c8f2dc96d112ba7304782d59599--funny-animals-funny-cats.jpg)

## Features

- quick setup
- generate schema based on your MongoDB collections and documents
- create your own schema
- change the schema without stopping Purr

## Support

MongoDB 3.4
PostgreSQL 9.6

## Documentation

Check out our [Documentation](https://boosterfuels.github.io/purr/docs)

## Installation

Check out an [example]() or set things up on your own:

- install Purr
  `pip install purr-etl`
- start MongoDB as a replica set
- create a database in MongoDB and add a collection with a couple of documents
- create a database in Postgres to connect to
- generate a collection map
  `purr -m -mdb mongodb://localhost:27017 -n db_name`
- start Purr
  `purr -cf collections.yml -pg postgres://127.0.0.1:5432/postgres -mdb mongodb://localhost:27017 -n db_name -t`

### Docker

`...`

## Contribute

Take a look at our contribution [Guidelines](https://boosterfuels.github.io/purr/docs#contribute).

## FAQ
