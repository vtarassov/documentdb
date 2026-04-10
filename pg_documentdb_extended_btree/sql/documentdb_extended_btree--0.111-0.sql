-- ============================================================
-- DocumentDB Extended B-tree prototype using exbtree AM
-- ============================================================
--
-- Uses BsonGinCompositePathOptions for opclass options (same as
-- the composite GIN index) and GenerateCompositeTermsFromOptions
-- for term generation.
--
-- extractQuery / comparePartial / consistent delegate to the
-- existing composite path implementations, enabling RUM-like
-- query decomposition within the exbtree AM.
--
-- Dollar operators are registered at strategy (BsonIndexStrategy + 5)
-- so they don't collide with btree-native strategies 1-5.  The
-- adapter subtracts the offset before delegating.
--
-- Usage:
--   CREATE INDEX idx ON tbl USING exbtree
--     (document documentdb_api_catalog.documentdb_xbt_opclass
--              (pathspec='[ "foo" ]', tl=2147483647));
-- ============================================================

-- Schema and grants
CREATE SCHEMA IF NOT EXISTS documentdb_extended_btree_catalog;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'documentdb_admin_role') THEN
    EXECUTE 'GRANT USAGE ON SCHEMA documentdb_extended_btree_catalog TO documentdb_admin_role';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'documentdb_readonly_role') THEN
    EXECUTE 'GRANT USAGE ON SCHEMA documentdb_extended_btree_catalog TO documentdb_readonly_role';
  END IF;
END
$$;

-- extractValue: called by exbtree AM during insert.
CREATE OR REPLACE FUNCTION __API_CATALOG_SCHEMA__.documentdb_xbt_extract_value(
    __CORE_SCHEMA__.bson
)
RETURNS __CORE_SCHEMA__.bson
LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$documentdb_xbt_extract_value$function$;

-- B-tree comparator
CREATE OR REPLACE FUNCTION __API_CATALOG_SCHEMA__.documentdb_xbt_compare(
    __CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson
)
RETURNS int4
LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$documentdb_xbt_compare$function$;

-- extractQuery (proc 101): subtracts EXBT_STRATEGY_OFFSET before delegating.
CREATE OR REPLACE FUNCTION __API_CATALOG_SCHEMA__.documentdb_xbt_extract_query(
    __CORE_SCHEMA__.bson, internal, int2, internal, internal, internal, internal
)
RETURNS internal
LANGUAGE c PARALLEL SAFE STABLE
AS 'MODULE_PATHNAME', $function$documentdb_xbt_extract_query$function$;

-- comparePartial (proc 103): forces strategy to BSON_INDEX_STRATEGY_COMPOSITE_QUERY before delegating.
CREATE OR REPLACE FUNCTION __API_CATALOG_SCHEMA__.documentdb_xbt_compare_partial(
    bytea, bytea, int2, internal
)
RETURNS int4
LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$documentdb_xbt_compare_partial$function$;

-- consistent (proc 102): guards check[] entries and forces strategy to BSON_INDEX_STRATEGY_COMPOSITE_QUERY before delegating.
CREATE OR REPLACE FUNCTION __API_CATALOG_SCHEMA__.documentdb_xbt_consistent(
    internal, smallint, anyelement, integer, internal, internal
)
RETURNS boolean
LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$documentdb_xbt_consistent$function$;

-- Btree-native operator procedures
CREATE OR REPLACE FUNCTION __API_CATALOG_SCHEMA__.documentdb_xbt_eq(
    __CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson
) RETURNS bool LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$documentdb_xbt_eq$function$;

CREATE OR REPLACE FUNCTION __API_CATALOG_SCHEMA__.documentdb_xbt_lt(
    __CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson
) RETURNS bool LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$documentdb_xbt_lt$function$;

CREATE OR REPLACE FUNCTION __API_CATALOG_SCHEMA__.documentdb_xbt_lte(
    __CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson
) RETURNS bool LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$documentdb_xbt_lte$function$;

CREATE OR REPLACE FUNCTION __API_CATALOG_SCHEMA__.documentdb_xbt_gt(
    __CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson
) RETURNS bool LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$documentdb_xbt_gt$function$;

CREATE OR REPLACE FUNCTION __API_CATALOG_SCHEMA__.documentdb_xbt_gte(
    __CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson
) RETURNS bool LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$documentdb_xbt_gte$function$;

-- Btree-native operators (strategies 1-5, used for internal positioning)
CREATE OPERATOR __API_CATALOG_SCHEMA__.@@= (
    LEFTARG = __CORE_SCHEMA__.bson, RIGHTARG = __CORE_SCHEMA__.bson,
    PROCEDURE = __API_CATALOG_SCHEMA__.documentdb_xbt_eq,
    COMMUTATOR = OPERATOR(__API_CATALOG_SCHEMA__.@@=),
    NEGATOR = OPERATOR(__API_CATALOG_SCHEMA__.@@!=)
);

CREATE OPERATOR __API_CATALOG_SCHEMA__.@@< (
    LEFTARG = __CORE_SCHEMA__.bson, RIGHTARG = __CORE_SCHEMA__.bson,
    PROCEDURE = __API_CATALOG_SCHEMA__.documentdb_xbt_lt,
    NEGATOR = OPERATOR(__API_CATALOG_SCHEMA__.@@>=)
);

CREATE OPERATOR __API_CATALOG_SCHEMA__.@@<= (
    LEFTARG = __CORE_SCHEMA__.bson, RIGHTARG = __CORE_SCHEMA__.bson,
    PROCEDURE = __API_CATALOG_SCHEMA__.documentdb_xbt_lte,
    NEGATOR = OPERATOR(__API_CATALOG_SCHEMA__.@@>)
);

CREATE OPERATOR __API_CATALOG_SCHEMA__.@@> (
    LEFTARG = __CORE_SCHEMA__.bson, RIGHTARG = __CORE_SCHEMA__.bson,
    PROCEDURE = __API_CATALOG_SCHEMA__.documentdb_xbt_gt,
    NEGATOR = OPERATOR(__API_CATALOG_SCHEMA__.@@<=)
);

CREATE OPERATOR __API_CATALOG_SCHEMA__.@@>= (
    LEFTARG = __CORE_SCHEMA__.bson, RIGHTARG = __CORE_SCHEMA__.bson,
    PROCEDURE = __API_CATALOG_SCHEMA__.documentdb_xbt_gte,
    NEGATOR = OPERATOR(__API_CATALOG_SCHEMA__.@@<)
);

-- Operator class using exbtree AM.
--
-- Strategies 1-5:  btree-native (@@<, @@<=, @@=, @@>=, @@>)
-- Strategies 6+:   dollar operators at (BsonIndexStrategy + 5)
--                  e.g. @= ($eq=1) -> 6, @> ($gt=2) -> 7, ...
-- Functions 1,5:   standard btree comparator + opclass options
-- Functions 100-103: exbtree extension slots (extractValue,
--                    extractQuery, consistent, comparePartial)
CREATE OPERATOR CLASS __API_CATALOG_SCHEMA__.documentdb_xbt_opclass
    FOR TYPE __CORE_SCHEMA__.bson USING exbtree AS
        -- btree-native strategies 1-5
        OPERATOR 1 __API_CATALOG_SCHEMA__.@@<,
        OPERATOR 2 __API_CATALOG_SCHEMA__.@@<=,
        OPERATOR 3 __API_CATALOG_SCHEMA__.@@=,
        OPERATOR 4 __API_CATALOG_SCHEMA__.@@>=,
        OPERATOR 5 __API_CATALOG_SCHEMA__.@@>,
        -- dollar operators at BsonIndexStrategy + 5
        OPERATOR  6 __API_CATALOG_SCHEMA__.@= (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),   -- $eq   (1+5)
        OPERATOR  7 __API_CATALOG_SCHEMA__.@> (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),   -- $gt   (2+5)
        OPERATOR  8 __API_CATALOG_SCHEMA__.@>= (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),  -- $gte  (3+5)
        OPERATOR  9 __API_CATALOG_SCHEMA__.@< (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),   -- $lt   (4+5)
        OPERATOR 10 __API_CATALOG_SCHEMA__.@<= (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),  -- $lte  (5+5)
        OPERATOR 11 __API_CATALOG_SCHEMA__.@*= (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),  -- $in   (6+5)
        OPERATOR 12 __API_CATALOG_SCHEMA__.@!= (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),  -- $ne   (7+5)
        OPERATOR 13 __API_CATALOG_SCHEMA__.@!*= (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson), -- $nin  (8+5)
        OPERATOR 14 __API_CATALOG_SCHEMA__.@~ (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),   -- $regex(9+5)
        OPERATOR 15 __API_CATALOG_SCHEMA__.@? (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),   -- $exists(10+5)
        OPERATOR 16 __API_CATALOG_SCHEMA__.@@# (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),  -- $size (11+5)
        OPERATOR 17 __API_CATALOG_SCHEMA__.@# (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),   -- $type (12+5)
        OPERATOR 18 __API_CATALOG_SCHEMA__.@&= (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),  -- $all  (13+5)
        OPERATOR 19 __API_CATALOG_SCHEMA__.=?= (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),  -- unique(14+5)
        OPERATOR 20 __API_CATALOG_SCHEMA__.@!& (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),  -- $bitsAllClear(15+5)
        OPERATOR 21 __API_CATALOG_SCHEMA__.@!| (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),  -- $bitsAnyClear(16+5)
        OPERATOR 22 __API_CATALOG_SCHEMA__.@#? (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),  -- $elemMatch(17+5)
        OPERATOR 23 __API_CATALOG_SCHEMA__.@& (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),   -- $bitsAllSet(18+5)
        OPERATOR 24 __API_CATALOG_SCHEMA__.@| (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),   -- $bitsAnySet(19+5)
        OPERATOR 25 __API_CATALOG_SCHEMA__.@% (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),   -- $mod  (20+5)
        OPERATOR 30 __API_CATALOG_SCHEMA__.@<> (__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),  -- $range(25+5)
        -- support functions
        FUNCTION 1 __API_CATALOG_SCHEMA__.documentdb_xbt_compare(__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),
        FUNCTION 5 __API_SCHEMA_INTERNAL_V2__.gin_bson_composite_path_options(internal),
        FUNCTION 100 __API_CATALOG_SCHEMA__.documentdb_xbt_extract_value(__CORE_SCHEMA__.bson),
        FUNCTION 101 __API_CATALOG_SCHEMA__.documentdb_xbt_extract_query(__CORE_SCHEMA__.bson, internal, int2, internal, internal, internal, internal),
        FUNCTION 102 __API_CATALOG_SCHEMA__.documentdb_xbt_consistent(internal, smallint, anyelement, integer, internal, internal),
        FUNCTION 103 __API_CATALOG_SCHEMA__.documentdb_xbt_compare_partial(bytea, bytea, int2, internal);
