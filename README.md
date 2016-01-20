# README #

`vecdb`
Current version: 0.2.3

### What is this repository for? ###
`vecdb` is a simple "columnar database": each column in the database is stored in a single memory-mapped files. It is written in and for Dyalog APL as a tool on which to base new applications which need to generate and query very large amounts of data and do a large number of high performance reads, but do not need a full set of RDBMS features. In particuler, there is no "transactional" storage mechanism, and no ability to join tables built-in to the database.

### Features

#### Supported data types: ####

* 1, 2 and 4 byte integers
* 8-byte IEEE double-precision floats
* Boolean
* Char (via a "symbol table" of up to 32,767 unique strings indexed by 2-byte integers)

#### Sharding ####

`vecdb` databases can be *sharded*, or *horizontally partitioned*. Each shard is a separate folder, named when the database is created (by default, there is a single shard). Each folder contains a file for each database column - which is memory mapped to an APL vector when the database is opened. A list of *sharding columns* is defined when the db is created; the values of these columns are passed as the argument to a user-defined *sharding function*, which has to return an origin-1 index into the list of shards, for each record.

#### Supported Operations ####

**Query**: At the moment, the `Query` function takes a constraint in the form of a list of (column_name values) pairs. Each one represents the relation which can be expressed in APL as (column_data∊values). If more than constraint is provided, they are AND-ed together. 
Query also takes a list of column names to be retrieved for records which match the constraint.

Query results are returned as a vector with one element per database column, each item containing a vector of values for that column.

**Search** If the `Query`function is called with an empty list of columns, record identifiers are returned as a 2-column matrix of (shard) (record index) pairs.

**Read**: The `Read` function accepts a matrix in the format returned by a search query and a list of column names, and returns a vector per column.

**Update**: The `Update` function also takes as input a search query result, a list of columns, and a vector of vectors containing new data values.

**Append**: Takes a list of column names and a vector of data vectors, one per named column. The columns involved in the Shard selection must always be included.

**Delete**: Deletion is not currently supported.

### Short-Term Goals ###

1. Enhance the query function to accept enhanced queries consisting of column names, comparison functions and values - and support AND/OR. If possible, optimise queries to be sensitive to sharding.
1. Parallel database queries: For a sharded database: Spin a number of isolate processes up and distribute the shards between them, so that each shard is handled by a single process. Enhance the database API functions to use these processes to perform searches, reads and writes in parallel.
1. Add a front-end server with a RESTful database API. As it stands, `vecdb` is effectively an embedded database engine which does not support data sharing between processes on the same or on separate machines.

### Longer Term (Dreams) ###

There are ideas to add support for timeseries and versioning. This would include:

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
* nicolas@dyalog.com
* stf@apl.it
