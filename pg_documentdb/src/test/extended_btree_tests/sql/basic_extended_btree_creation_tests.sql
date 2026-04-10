SET search_path TO documentdb_api, documentdb_core;
SET documentdb_core.bsonUseEJson TO true;

SELECT documentdb_api.create_collection('testdb', 'testcoll');

DO $$
DECLARE
    tbl_name text;
BEGIN
    SELECT 'documentdb_data.documents_' || collection_id
    INTO tbl_name
    FROM documentdb_api_catalog.collections
    WHERE database_name = 'testdb' AND collection_name = 'testcoll';

    EXECUTE format(
        'CREATE INDEX idx_foo_xbt ON %s
         USING exbtree (document documentdb_api_catalog.documentdb_xbt_opclass
                        (pathspec=''[ "foo" ]'', tl=2147483647))',
        tbl_name
    );
END
$$;

SELECT documentdb_api.insert_one('testdb', 'testcoll', '{"_id": 1, "foo": 1}');
SELECT documentdb_api.insert_one('testdb', 'testcoll', '{"_id": 2, "foo": 3}');
SELECT documentdb_api.insert_one('testdb', 'testcoll', '{"_id": 3, "foo": 2}');
SELECT documentdb_api.insert_one('testdb', 'testcoll', '{"_id": 4, "bar": 4}');
SELECT documentdb_api.insert_one('testdb', 'testcoll', '{"_id": 5, "foo": 5}');
SELECT documentdb_api.insert_one('testdb', 'testcoll', '{"_id": 6, "foo": 10}');

SET enable_seqscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = off;

-- Test 1: $eq via extractQuery pipeline
EXPLAIN (FORMAT JSON, COSTS OFF)
SELECT document FROM documentdb_data.documents_2
WHERE documentdb_api_catalog.bson_dollar_eq(document, '{"foo": 3}'::documentdb_core.bsonquery);

SELECT document FROM documentdb_data.documents_2
WHERE documentdb_api_catalog.bson_dollar_eq(document, '{"foo": 3}'::documentdb_core.bsonquery);

-- Test 2: $gt via extractQuery/comparePartial pipeline
EXPLAIN (FORMAT JSON, COSTS OFF)
SELECT document FROM documentdb_data.documents_2
WHERE documentdb_api_catalog.bson_dollar_gt(document, '{"foo": 2}'::documentdb_core.bsonquery);

SELECT document FROM documentdb_data.documents_2
WHERE documentdb_api_catalog.bson_dollar_gt(document, '{"foo": 2}'::documentdb_core.bsonquery);

-- Test 3: $gte via extractQuery/comparePartial pipeline
SELECT document FROM documentdb_data.documents_2
WHERE documentdb_api_catalog.bson_dollar_gte(document, '{"foo": 3}'::documentdb_core.bsonquery);

-- Test 4: $lt via extractQuery/comparePartial pipeline
SELECT document FROM documentdb_data.documents_2
WHERE documentdb_api_catalog.bson_dollar_lt(document, '{"foo": 3}'::documentdb_core.bsonquery);

-- Test 5: $lte via extractQuery/comparePartial pipeline
SELECT document FROM documentdb_data.documents_2
WHERE documentdb_api_catalog.bson_dollar_lte(document, '{"foo": 3}'::documentdb_core.bsonquery);

-- Test 6: Range query ($gt + $lt) via extractQuery/comparePartial
EXPLAIN (FORMAT JSON, COSTS OFF)
SELECT document FROM documentdb_data.documents_2
WHERE documentdb_api_catalog.bson_dollar_gt(document, '{"foo": 1}'::documentdb_core.bsonquery)
  AND documentdb_api_catalog.bson_dollar_lt(document, '{"foo": 5}'::documentdb_core.bsonquery);

SELECT document FROM documentdb_data.documents_2
WHERE documentdb_api_catalog.bson_dollar_gt(document, '{"foo": 1}'::documentdb_core.bsonquery)
  AND documentdb_api_catalog.bson_dollar_lt(document, '{"foo": 5}'::documentdb_core.bsonquery);
