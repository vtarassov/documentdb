# DocumentDB Extended B-tree (exbtree) Adapter

## Overview

`pg_documentdb_extended_btree` adapts the `exbtree` access method (a fork of
nbtree with GIN-like extension slots) to index DocumentDB BSON documents using
the same composite path infrastructure that powers the RUM-based indexes.

The index stores serialized **composite index terms** while the column type
remains `bson`.  The adapter delegates term generation, query decomposition,
and match evaluation to the existing composite path functions in
`pg_documentdb`, so the index semantics are consistent with the RUM path.

## Query Path

The query path currently deviates significantly from how EX-RUM operates. 
EX-RUM calls ModifyScanKeysForCompositeScan to combine all scan keys into one. 
It does this during the first call to amgettuple, likely because earlier AM
functions do not know the scan direction. 

In this initial phase we've opted not to copy this pattern into exbtree and we'll
need to discuss the best way to do that. For the time being, exbtree calls 
extractQuery for each ScanKey individually. This is enough for simple scans but
will fail on more complex scenarios.

### How RUM composite indexes work

In the RUM path, the flow is:

1. **Planner**: `dollar_support` matches each `bson_dollar_<op>` function to
   the index and produces an `OpExpr` index condition (e.g. `document @> val`).
2. **Rescan**: `ModifyScanKeysForCompositeScan` (in `rum.c`) combines all N
   scan keys into a **single** composite query spec:
   ```json
   { "q": [{"op": 2, "foo": 1}, {"op": 4, "foo": 5}], "m": true, ... }
   ```
3. **extractQuery**: parses the composite spec, merges operators on the same
   path into unified bounds (e.g. `$gt 1 AND $lt 5` -> one entry with lower=1
   exclusive, upper=5 exclusive), and returns extracted entries with
   `partialmatch` flags.
4. **comparePartial**: called per-entry during the GIN posting-list scan;
   checks both lower and upper bounds in a single call.
5. **consistent**: called per-TID with a `check[]` array indicating which
   entries' posting lists contain that TID. For ordered scans with composite ops,
   this shortcuts to true.

### How the exbtree adapter works

The exbtree path reuses the same composite path functions but with some adjustments:

1. **Planner**: identical to RUM - `dollar_support` still has separate handling for 
    exbtree, but it's currently equivalent and kept in for debugging.
2. **Rescan**: the exbtree AM receives N separate scan keys from Postgres
   (one per WHERE clause).  **Unlike RUM, there is currently no
   `ModifyScanKeysForCompositeScan` step.**  Each scan key is processed
   independently:
   - `documentdb_xbt_extract_query` wraps the single operator into a
     composite query spec and delegates to
     `gin_bson_composite_path_extract_query`.
   - `exbt_build_scan_keys` converts extracted entries into btree-native
     positioning keys (`>=` for partial match, `=` for exact match).
3. **Scan**: the btree positions using the native keys, then for each
   candidate tuple calls `exbt_consistent_check` which:
   - Calls `comparePartial` for each partial-match key
   - Populates `keyMatches[]` with the results
   - Calls `consistent` with `keyMatches` as the `check[]` array
4. **consistent**: `documentdb_xbt_consistent` checks that all `check[]`
   entries are true before delegating to
   `gin_bson_composite_path_consistent`.  This is necessary because the GIN
   consistent function has early-return optimizations that skip `check[]`
   inspection (safe in GIN where entries are pre-filtered, but not in exbtree
   where boundary values from btree positioning can reach consistent with
   `check[i]=false`).

## Insert path - extractValue (proc 100)

`documentdb_xbt_extract_value` creates composite index terms from the input
document using `GenerateCompositeTermsFromOptions` (same term format as the
RUM composite path).

The comparator (`documentdb_xbt_compare`) and the five btree-native operators
(`@@=`, `@@<`, `@@<=`, `@@>`, `@@>=`) delegate to
`CompareSerializedBsonIndexTermWithCollation`.


## Opclass definition

```sql
CREATE OPERATOR CLASS documentdb_xbt_opclass
    FOR TYPE bson USING exbtree AS
        OPERATOR 1 @@<,  OPERATOR 2 @@<=,  OPERATOR 3 @@=,
        OPERATOR 4 @@>=, OPERATOR 5 @@>,
        OPERATOR  6 @=,  OPERATOR  7 @>,   OPERATOR  8 @>=,
        OPERATOR  9 @<,  OPERATOR 10 @<=,  ...
        FUNCTION 1   documentdb_xbt_compare(bson, bson),
        FUNCTION 5   gin_bson_composite_path_options(internal),
        FUNCTION 100 documentdb_xbt_extract_value(bson),
        FUNCTION 101 documentdb_xbt_extract_query(...),
        FUNCTION 102 documentdb_xbt_consistent(...),
        FUNCTION 103 documentdb_xbt_compare_partial(...);
```

## Known limitations

- **No scan key combination**: range queries (`$gt AND $lt`) produce separate
  scan keys instead of a merged composite query.  Correctness is maintained by
  the `check[]` guard in `documentdb_xbt_consistent`, but this is less
  efficient than the single-entry approach used by RUM.
- **No multi-key (array) support**: `DocumentDBExtendedBtreeGetMultiKeyStatus`
  always returns false.
- **No index-only scans**: `exbt_canreturn` returns false for key columns with
  `extractValue` since the stored value is a transformed index term, not the
  original document.

## License

See [LICENSE](LICENSE).
