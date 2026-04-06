# DocumentDB Extended B-tree (exbtree) — Prototype

## Overview

`pg_documentdb_extended_btree` is a prototype extension that adapts the
`exbtree` access method (a fork of nbtree) to serve as a B-tree index for
DocumentDB BSON documents.  The index stores **composite index terms**
(`bsonindexterm`) while the column type remains `bson`.

## Problem 1: Storage

A B-tree index needs a compact, comparable representation for its keys.  Raw
`bson` is not a good fit for this as it not concise.

The existing `bsonindexterm` format was designed to solve exactly this: it
is a concise, order-preserving serialization that strips the document
envelope and encodes type + value + path metadata into a compact byte
sequence suitable for index storage and comparison. 

Two options were considered:

1. **New unified datatype** — create a single type that can represent both
   `bson` and `bsonindexterm`, so nbtree sees a consistent type throughout.
   This is the cleaner solution but requires a larger refactor across the
   storage and operator layers.

2. **Hack btree to store a different type than it declares** — teach the AM
   to silently convert `bson` → `bsonindexterm` at insert time and have the
   query path produce `bsonindexterm` quals directly.  This avoids the type
   system refactor at the cost of a type-safety gap inside the index.

This prototype takes **option 2**, mostly since the technical risks around
option 1 are much lower. 

## How it works

### Insert path — `extractValue` (proc 100)

We added an `extractValue` support function slot (proc 100) to exbtree,
During index builds and inserts, the AM calls `documentdb_xbt_extract_value` 
which creates bsonindexterms from the input document.

The comparator (`documentdb_xbt_compare`) and the five btree operators
(`@@=`, `@@<`, `@@<=`, `@@>`, `@@>=`) all delegate to
`CompareSerializedBsonIndexTermWithCollation`, so they operate on the stored
index terms correctly.

## Problem 2: Querying 

exbtree has no `extractQuery` callback.  The planner must emit quals that
already reference the stored type.  To achieve this, `dollar_support` (the
SupportRequestIndexCondition handler shared by all dollar operators) was
extended:

- When the target index uses the `exbtree` AM, `HandleSupportRequestCondition`
  branches into `HandleExBtreeSupportRequestCondition`.
- That function takes the query `bson` value (e.g. `{"a": 5}`), reconstructs
  a document keyed by the index path, and runs it through
  `GenerateCompositeTermsFromOptions` — the same term generator used at insert
  time.
- The resulting `bsonindexterm` datum replaces the original `bson` operand in
  the qual, and the qual is rewritten to use the `@@=` / `@@<` / `@@>` family
  of operators so the planner can push it into a btree index scan.
- The scan is marked `lossy = false` for the time being since rechecks are not needed.

This means the dollar operators (`$eq`, `$gt`, `$gte`, `$lt`, `$lte`)
convert from `bson` into index terms at plan time, and the index scan
operates entirely in `bsonindexterm` space.

## Opclass definition

```sql
CREATE OPERATOR CLASS documentdb_xbt_opclass
    FOR TYPE bson USING exbtree AS
        OPERATOR 1 @@<,
        OPERATOR 2 @@<=,
        OPERATOR 3 @@=,
        OPERATOR 4 @@>=,
        OPERATOR 5 @@>,
        FUNCTION 1 documentdb_xbt_compare(bson, bson),
        FUNCTION 5 gin_bson_composite_path_options(internal),  -- reused from GIN
        FUNCTION 100 documentdb_xbt_extract_value(bson);       -- exbtree extension
```

Index creation example:

```sql
CREATE INDEX idx ON tbl USING exbtree
  (document documentdb_api_catalog.documentdb_xbt_opclass
           (pathspec='[ "foo" ]', tl=2147483647));
```

## License

See [LICENSE](LICENSE).
