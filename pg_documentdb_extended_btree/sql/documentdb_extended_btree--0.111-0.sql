-- ============================================================
-- DocumentDB Extended B-tree prototype using exbtree AM
-- ============================================================
--
-- Uses BsonGinCompositePathOptions for opclass options (same as
-- the composite GIN index) and GenerateCompositeTermsFromIndexSpec
-- for term generation.
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
-- Transforms bson document -> serialized composite index term.
CREATE OR REPLACE FUNCTION __API_CATALOG_SCHEMA__.documentdb_xbt_extract_value(
    __CORE_SCHEMA__.bson
)
RETURNS __CORE_SCHEMA__.bson
LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$documentdb_xbt_extract_value$function$;

-- B-tree comparator: interprets both bson args as serialized index terms.
CREATE OR REPLACE FUNCTION __API_CATALOG_SCHEMA__.documentdb_xbt_compare(
    __CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson
)
RETURNS int4
LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$documentdb_xbt_compare$function$;

-- Operator procedures
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

-- Operators
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
-- FUNCTION 1  = comparator (standard btree slot)
-- FUNCTION 5  = opclass options (reuses composite path options from pg_documentdb)
-- FUNCTION 100 = extractValue (exbtree extension slot)
CREATE OPERATOR CLASS __API_CATALOG_SCHEMA__.documentdb_xbt_opclass
    FOR TYPE __CORE_SCHEMA__.bson USING exbtree AS
        OPERATOR 1 __API_CATALOG_SCHEMA__.@@<,
        OPERATOR 2 __API_CATALOG_SCHEMA__.@@<=,
        OPERATOR 3 __API_CATALOG_SCHEMA__.@@=,
        OPERATOR 4 __API_CATALOG_SCHEMA__.@@>=,
        OPERATOR 5 __API_CATALOG_SCHEMA__.@@>,
        FUNCTION 1 __API_CATALOG_SCHEMA__.documentdb_xbt_compare(__CORE_SCHEMA__.bson, __CORE_SCHEMA__.bson),
        FUNCTION 5 __API_SCHEMA_INTERNAL_V2__.gin_bson_composite_path_options(internal),
        FUNCTION 100 __API_CATALOG_SCHEMA__.documentdb_xbt_extract_value(__CORE_SCHEMA__.bson);

