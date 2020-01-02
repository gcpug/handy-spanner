# An emulator for Cloud Spanner

## Install

```
go get github.com/gcpug/handy-spanner/cmd/handy-spanner
```

The Spanner emulator uses sqlite3 internally. You may need to build go-sqlite3 explicitly.
It also requires cgo to use sqlite3.

```
go get -u github.com/mattn/go-sqlite3
go install github.com/mattn/go-sqlite3
```

## Usage

### Run as an independent process

```
./handy-spanner
```

or

```
docker run --rm -it -p 9999:9999 handy-spanner
```

It runs a fake spanner server as a process. It serves spanner gRPC server by port 9999 by default.

#### Access to the server

The google-cloud-go, the official Spanner SDK, supports to access an emulator server.
Set the address to an emulator server to environment variable `SPANNER_EMULATOR_HOST`, then google-cloud-go transparently use the server in the client.

So if you want to replace spanner server with the fake server you run, just do:

```
export SPANNER_EMULATOR_HOST=localhost:9999
```

Note that the fake spanner server has no databases nor tables by default. You need to create them by yourself.

### Run as a buillt-in server in Go

If you use a fake spanner server in tests in Go, it's easier to run it in a process.
See an [example](https://github.com/gcpug/handy-spanner/blob/master/fake/example_test.go) for the details.

Note that it becomes specific implementations for a fake server, which means you cannot switch the backend depending on the situation. If you want to test on both Cloud Spanner and fake, it's better to use a fake server as an independent process.

## Can and Cannot

### Supported features

* Read
   * Keys and KeyRange as KeySet
   * Secondary index
   * STORING columns for secondary index
   * Respect column orders for index
* Query
   * Select result set by column name and *
   * Most operators in WHERE clause: IN, BETWEEN, IS NULL
   * Conditions in WHERE clause: =, !=, >, <, AND, OR
   * Order By keyword with ASC, DESC
   * Group By and Having statement
   * LIMIT OFFSET
   * SELECT alias
   * Query Parameters
   * Literals (except STRUCT)
   * JOINs
   * Subquery
   * SET operations: UNION, INTERSECT, EXCEPT
   * UNNEST
   * Functions (partially)
   * Arithmetic operations
* Mutation
   * All mutation types: Insert, Update, InsertOrUpdate, Replace, Delete
   * Commit timestamp
* Transaction
   * Isolation level: SERIALIZABLE
* DML
   * Insert, Update, Delete
* DDL
   * CreateTable, CreateIndex only
* Data Types
   * Int, Float, String, Bool, Byte, Date, Timestamp, Array<Any>

### Not supported features

* Transaction
   * Timestamp bound read
* Query
   * Strict type checking
   * More functions
   * Partionan Query
   * EXCEPT ALL and INTERSECT ALL
   * Merging INT64 and FLOAT64 in SET operations
   * Array operations
   * Struct
* DDL
   * Alter Table, Drop Table, Drop Index
   * Database management
   * Long running operations
* Replace
   * wrong behavior on conflict

## Implementation

### Transaction simulation

handy-spanner uses sqlite3 in [Shared-Cache Mode](https://www.sqlite.org/sharedcache.html). There is a characteristic in the trasactions.

* Only one transaction can hold write lock per database to write database tables.
    * Other transactions still can hold read lock.
* Write transaction holds write lock against database tables while writing the tables.
    * Other read transactions cannot read the table while locked
* Read transaction holds read lock against database tables while reading the tables.
    * Other read transactions can read the table by holding read lock
    * Write transaction cannot write the table while read-locked

If we simply use the transactions, dead lock should happen in read and write locks. To simulate spanner transactions correctly as possible, handy-spanner manages sqlite3 transactions inside.

* Each spanner transaction starts own sqlite3 transaction.
* Only one spanner transaction can hold write lock per database.
* While a transaction holds write lock, other spanner transactions cannot newly get read or write lock.
* When write transaction tries to write a table, it forces transactions that hold read lock to the table to release the lock.
   * The transactions become "aborted"
* The aborted transactions are expected to be retried by the client.

![abort](img/abort1.png) ![abort](img/abort2.png)

### DML

Because of transaction limitations, DML also has limitations.

When a transaction(A) updates a table, other transactions cannot read/write the table until the transaction(A) commits. This limitation may become an inconsistency to the Cloud Spanner. Other limitations are same to mutations with commit.

![block](img/read_block.png)

## Copyright

* Author: Masahiro Sano ([@kazegusuri](https://github.com/kazegusuri))
* Copyright: 2019 Masahiro Sano
* License: Apache License, Version 2.0
