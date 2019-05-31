# Purrito

Purrito is an open source ETL tool which transfers collections from MongoDB to PostgreSQL.

## Features

- quick setup
- generate schema based on existing MongoDB collections or use a custom schema
- keep syncing with MongoDB real-time 
- update the schema while syncing without stopping anything
- handle connectivity issues
- view collection transfer statistics

## Support

MongoDB 3.4

PostgreSQL 9.6

## Installation

Check out an [example]() or set things up on your own:

- install Purrito by typing `pip install purrito`
- start MongoDB as a replica set
- create a source database in MongoDB and add a collection with a couple of documents
- create a destination database in Postgres
- generate the schema (`collections.yml`): 

  `purrito -m -mdb mongodb://localhost:27017 -n source_db`
- start Purrito:

  `purrito -cf collections.yml -pg postgres://localhost:5432/destination_db -mdb mongodb://localhost:27017 -n source_db -t`


## Documentation

For more details about Purrito check out our [Documentation](https://boosterfuels.github.io/purr/docs).


## Contribute

Want to help us and the community with perfecting Purrito? Take a look at our contribution [Guidelines](https://boosterfuels.github.io/purr/docs#contribute) and submit a PR.