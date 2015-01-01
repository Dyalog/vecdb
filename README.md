# README #

`vecdb`
Current version: 0.2.0

**Version 0.2** adds support for SHARDING, and introduces changes to the database format which are not upwards compatible.

### What is this repository for? ###
`vecdb` is a simple "columnar database": each column in the database is stored in a single memory-mapped files. It is written in and for Dyalog APL as a tool on which to base new applications which need to generate and query very large amounts of data and do a large number of high performance reads, but do not need a full set of RDBMS features. In particuler, there is no "transactional" storage mechanism, and no ability to join tables built-in to the database.

### Features

The current version supports the following data types:

* 1, 2 and 4 byte integers
* 8-byte IEEE double-precision floats
* Boolean
* Char (via a "symbol table" of up to 32,767 unique strings indexed by 2-byte integers)

Database modification can only be done using Append and Update operations (no Delete).

The `Query` function takes a constraint in the form of a list of (column_name values) pair. Each one represents the relation which can be expressed in APL as (column_data∊values); if there is more than one constraint they are AND-ed together. Query also accepts a list of column names to be retrieve for records which match the constraint; if no columns are requested, row indices are returned.

A `Read` function takes a list of column names and row indices and returns the requested data.

### Goals

The intention is to extend `vecdb` with the following functionality. Much of this is still half-baked, discussion is welcome. owever, the one application that is being built upon `vecdb` and is driving the initial development requires the following items.

1. "Sharding": This idea needs to be developed, but the current thinking is that one or more key fields are identified, and a function is defined to map distinct key tuples to a "shard". A list of folder names points to the folders that will contain the mapped columns for each shard. The result of Query (and argument to Read) will become a 2-row (2-column?) matrix containing shard numbers and record offsets within the shard.
1. Parallel database queries: For a sharded database, an isolate process will be spun up to perform queries and updates on one or more shards (each shard only being handles by a single process).
1. A front-end server will allow RESTful database access (this item is perhaps optional). As it stands, `vecdb` is effectively an embedded database engine which does not support data sharing between processes on the same or on separate machines.

### Longer Term (Dreams)

There are ideas to add support for timeseries and versioning. This would include:

1. Add a single-byte indexed Char type (perhaps denoted lowercase "c"), indexing up to 127 unique strings
1. Support for deleting records
1. Performing all updates without overwriting data, and tagging old data with the timestamps defining its lifetime, allowing efficient queries on the database as it appeared at any given time in the past.
1. Built-in support for the computation of aggregate values as part of the parallel query mechanism, based on timeseries or other key values.

### How do I get set up? ###

Clone/Fork the repo, and

```apl
    ]load vecdb.dyalog
```

### Tests ###

The full system test creates a database containing all supported data types, inserts and updates records, performs queries, and finally deletes the database.

```apl
    ]load TestVecdb.dyalog
    #.TestVecdb.RunAll
```

See doc\Usage.md for more information on usage.

### Contribution guidelines ###

At this early stage, until the project acquires a bit more direction, we ask you to contact one of the key collaborators to discuss your ideas.

Please read doc\Implementation.md before continuing.

### Key Collaborators ###

* mkrom@dyalog.com
* stf@apl.it

