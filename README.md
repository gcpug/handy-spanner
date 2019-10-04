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

Note that it becomes specific implementations for a fake server, which means you cannot switch the backend depending on the situation. If you want to test on both real spanner and fake, it's better to use a fake server as an independent process.

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
   * JOINs (partially)
   * Subquery (partially)
   * UNNEST (partially)
   * Functions (partially)
* Mutation
   * All mutation types: Insert, Update, InsertOrUpdate, Replace, Delete
   * Commit timestamp
* DML
   * fully not yet supported
* DDL
   * CreateTable, CreateIndex only
* Data Types
   * Int, Float, String, Bool, Byte, Date, Timestamp

### Not supported features

* Transaction
   * Applying mutations is not transactional
   * Optimistic lock
   * No check for transaction type RO/RW
* Data Types
   * Array and Struct type
* Query
   * Arithmetic operations
   * Strict type checking
   * More functions
   * UNION, INTERSECT, EXCEPT
   * Partionan Query
* DML
   * not yet
* DDL
   * Alter Table, Drop Table, Drop Index
   * Database management
   * Long running operations
* Replace
   * wrong behavior on conflict

## Copyright

* Author: Masahiro Sano ([@kazegusuri](https://github.com/kazegusuri))
* Copyright: 2019 Masahiro Sano
* License: Apache License, Version 2.0
