# Custom ETL Pipeline

An ETL tool for your MongoDB and Postgres.

optional arguments:
`-h, --help` show this help message and exit
`-c MDB_COLLS [MDB_COLLS ...], --colls [MDB_COLLS ...]`
MongoDB collections which will be transfered. Case
sensitive!
`-a, --all` Transfer all collections from MongoDB to PG
`-t, --tail` Start tailing the oplog
`-tr, --truncate` Clear existing tables
`-dr, --drop` Drop existing tables
`-s START_DATE_TIME, --start START_DATE_TIME`
The Start Date - format YY-MM-DDTHH:MM:SS. Example
21/1/06T16:30:00

**Examples**

`python3 etl/main.py -c FuelRequest -dr -t -s 18-4-18T10:16:00`

* drops FuelRequest in PG
* transfers collection FuelRequest
* starts tailing from specific datetime YY-MM-DDTHH:MM:SS.

`python3 etl/main.py -c Supplier,Feedback -dr`

* drops tables FuelRequest and Audience in PG
* transfers collection FuelRequest and Audience from MongoDB

`python3 etl/main.py -c Tank`

* transfers only collection Tank

Entry point: main.py

`python3 etl/main.py -c Supplier -dr`
