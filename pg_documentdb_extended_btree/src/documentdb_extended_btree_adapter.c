/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * src/documentdb_extended_btree_adapter.c
 *
 * DocumentDB Extended B-tree prototype using the exbtree AM.
 *
 * Uses BsonGinCompositePathOptions for opclass options (same as
 * the composite GIN index) so that ValidateIndexForQualifierValue
 * and the planner infrastructure recognize the index.
 *
 * The extractValue function (proc 100) uses
 * GenerateCompositeTermsFromOptions to produce index terms
 * in the same format as the composite GIN path.
 *
 * extractQuery (proc 101), consistent (proc 102), and
 * comparePartial (proc 103) delegate to the existing composite
 * path implementations from pg_documentdb, enabling RUM-like
 * query decomposition and partial-match scanning within the
 * exbtree AM.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <miscadmin.h>
#include <catalog/pg_am.h>
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "commands/defrem.h"
#include "access/reloptions.h"

#include "index_am/index_am_exports.h"
#include "io/bson_core.h"
#include "io/pgbsonelement.h"
#include "opclass/bson_gin_index_term.h"
#include "opclass/bson_gin_index_mgmt.h"
#include "opclass/bson_gin_composite.h"
#include "opclass/bson_gin_private.h"

PG_MODULE_MAGIC;

void _PG_init(void);

/* Forward declarations */
PG_FUNCTION_INFO_V1(documentdb_xbt_extract_value);
PG_FUNCTION_INFO_V1(documentdb_xbt_compare);
PG_FUNCTION_INFO_V1(documentdb_xbt_eq);
PG_FUNCTION_INFO_V1(documentdb_xbt_lt);
PG_FUNCTION_INFO_V1(documentdb_xbt_lte);
PG_FUNCTION_INFO_V1(documentdb_xbt_gt);
PG_FUNCTION_INFO_V1(documentdb_xbt_gte);

/* RUM-like support functions for composite path ops */
PG_FUNCTION_INFO_V1(documentdb_xbt_extract_query);
PG_FUNCTION_INFO_V1(documentdb_xbt_compare_partial);
PG_FUNCTION_INFO_V1(documentdb_xbt_consistent);

/* Imported composite path functions from pg_documentdb */
extern Datum gin_bson_composite_path_extract_query(PG_FUNCTION_ARGS);
extern Datum gin_bson_composite_path_compare_partial(PG_FUNCTION_ARGS);
extern Datum gin_bson_composite_path_consistent(PG_FUNCTION_ARGS);

/*
 * Strategy offset: the exbtree opclass registers the dollar operators at
 * (BsonIndexStrategy + EXBT_STRATEGY_OFFSET) so they don't collide with
 * btree's native strategies 1-5.  The adapter subtracts this offset before
 * delegating to the composite path functions which expect the original
 * BsonIndexStrategy values.
 */
#define EXBT_STRATEGY_OFFSET 5

/* --------------------------------------------------------- */
/* AM registration                                            */
/* --------------------------------------------------------- */

static Oid CachedAmOid = InvalidOid;
static Oid CachedOpFamilyOid = InvalidOid;

static const char *GetDocumentDBCatalogSchema(void) { return "documentdb_api_catalog"; }

static bool
DocumentDBExtendedBtreeGetMultiKeyStatus(Relation indexRelation)
{
	/* exbtree prototype: no multi-key support yet */
	return false;
}

static Oid
DocumentDBExtendedBtreeIndexAmId(void)
{
	if (CachedAmOid == InvalidOid)
		CachedAmOid = get_am_oid("exbtree", true);
	return CachedAmOid;
}

static Oid
DocumentDBExtendedBtreeOpFamilyOid(void)
{
	if (CachedOpFamilyOid == InvalidOid)
	{
		Oid amOid = DocumentDBExtendedBtreeIndexAmId();
		if (OidIsValid(amOid))
			CachedOpFamilyOid = get_opfamily_oid(amOid,
				list_make2(makeString("documentdb_api_catalog"),
						   makeString("documentdb_xbt_opclass")), true);
	}
	return CachedOpFamilyOid;
}

static BsonIndexAmEntry DocumentDBIndexAmEntry = {
	.is_single_path_index_supported = false,
	.is_wild_card_supported = false,
	.is_wild_card_projection_supported = false,
	.is_order_by_supported = false,
	.is_backwards_scan_supported = true,
	.is_index_only_scan_supported = false,
	.can_support_parallel_scans = true,
	.get_am_oid = DocumentDBExtendedBtreeIndexAmId,
	.get_single_path_op_family_oid = NULL,
	.get_composite_path_op_family_oid = DocumentDBExtendedBtreeOpFamilyOid,
	.get_text_path_op_family_oid = NULL,
	.get_hashed_path_op_family_oid = NULL,
	.get_unique_path_op_family_oid = NULL,
	.add_explain_output = NULL,
	.am_name = "extended_btree",
	.get_opclass_catalog_schema = GetDocumentDBCatalogSchema,
	.get_opclass_internal_catalog_schema = GetDocumentDBCatalogSchema,
	.get_multikey_status = DocumentDBExtendedBtreeGetMultiKeyStatus,
	.get_truncation_status = NULL,
	.can_order_in_index_scans = NULL,
	.supports_ordered_operator_scans = false,
	.create_indexes_support_funcs = NULL,
};

PGDLLEXPORT void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR, (errmsg(
			"pg_documentdb_extended_btree can only be loaded via shared_preload_libraries"),
			errdetail_log(
				"Add pg_documentdb_extended_btree to shared_preload_libraries.")));
	}
	RegisterIndexAm(DocumentDBIndexAmEntry);
}

/* --------------------------------------------------------- */
/* extractValue (proc 100 for exbtree AM)                     */
/*                                                            */
/* Reads the composite path options and uses                  */
/* GenerateCompositeTermsFromOptions to produce index terms.   */
/* --------------------------------------------------------- */

Datum
documentdb_xbt_extract_value(PG_FUNCTION_ARGS)
{
	pgbson *document = PG_GETARG_PGBSON(0);

	if (!PG_HAS_OPCLASS_OPTIONS())
		ereport(ERROR, (errmsg("documentdb_xbt_extract_value: no opclass options")));

	BsonGinCompositePathOptions *options =
		(BsonGinCompositePathOptions *) PG_GET_OPCLASS_OPTIONS();

	uint32_t numTerms = 0;
	Datum *terms = GenerateCompositeTermsFromOptions(document, options, &numTerms);

	if (numTerms > 0) {
		PG_RETURN_DATUM(terms[0]);
	}

	/* Fallback: return a null-valued term */
	pgbsonelement nullElem = { 0 };
	nullElem.path = "";
	nullElem.pathLength = 0;
	nullElem.bsonValue.value_type = BSON_TYPE_NULL;

	IndexTermCreateMetadata meta = { 0 };
	meta.indexTermSizeLimit = (INT32_MAX / 1) - 4;
	meta.pathPrefix.string = "$";
	meta.pathPrefix.length = 1;
	meta.allowValueOnly = true;

	BsonIndexTermSerialized serialized = SerializeBsonIndexTerm(&nullElem, &meta);
	PG_RETURN_POINTER(serialized.indexTermVal);
}

/* --------------------------------------------------------- */
/* Comparator and operator procedures                         */
/* --------------------------------------------------------- */

static int32_t
CompareIndexTermBytea(bytea *left, bytea *right)
{
	/*
	 * Delegate to the same comparison used by GIN's gin_bson_compare
	 * (function 1 of composite_path_ops).  This calls CompareBsonGinIndexTerms
	 * which handles both composite and single-path terms, decomposing
	 * composite terms field-by-field via CompareCompositeIndexTerms.
	 */
	return CompareSerializedBsonIndexTermWithCollation(PointerGetDatum(left),
													  PointerGetDatum(right),
													  NULL);
}

Datum
documentdb_xbt_compare(PG_FUNCTION_ARGS)
{
	bytea *left = PG_GETARG_BYTEA_PP(0);
	bytea *right = PG_GETARG_BYTEA_PP(1);
	int32_t cmp = CompareIndexTermBytea(left, right);
	PG_FREE_IF_COPY(left, 0);
	PG_FREE_IF_COPY(right, 1);
	PG_RETURN_INT32(cmp);
}

Datum documentdb_xbt_eq(PG_FUNCTION_ARGS)
{
	bytea *l = PG_GETARG_BYTEA_PP(0), *r = PG_GETARG_BYTEA_PP(1);
	int32_t c = CompareIndexTermBytea(l, r);
	PG_FREE_IF_COPY(l, 0); PG_FREE_IF_COPY(r, 1);
	PG_RETURN_BOOL(c == 0);
}

Datum documentdb_xbt_lt(PG_FUNCTION_ARGS)
{
	bytea *l = PG_GETARG_BYTEA_PP(0), *r = PG_GETARG_BYTEA_PP(1);
	int32_t c = CompareIndexTermBytea(l, r);
	PG_FREE_IF_COPY(l, 0); PG_FREE_IF_COPY(r, 1);
	PG_RETURN_BOOL(c < 0);
}

Datum documentdb_xbt_lte(PG_FUNCTION_ARGS)
{
	bytea *l = PG_GETARG_BYTEA_PP(0), *r = PG_GETARG_BYTEA_PP(1);
	int32_t c = CompareIndexTermBytea(l, r);
	PG_FREE_IF_COPY(l, 0); PG_FREE_IF_COPY(r, 1);
	PG_RETURN_BOOL(c <= 0);
}

Datum documentdb_xbt_gt(PG_FUNCTION_ARGS)
{
	bytea *l = PG_GETARG_BYTEA_PP(0), *r = PG_GETARG_BYTEA_PP(1);
	int32_t c = CompareIndexTermBytea(l, r);
	PG_FREE_IF_COPY(l, 0); PG_FREE_IF_COPY(r, 1);
	PG_RETURN_BOOL(c > 0);
}

Datum documentdb_xbt_gte(PG_FUNCTION_ARGS)
{
	bytea *l = PG_GETARG_BYTEA_PP(0), *r = PG_GETARG_BYTEA_PP(1);
	int32_t c = CompareIndexTermBytea(l, r);
	PG_FREE_IF_COPY(l, 0); PG_FREE_IF_COPY(r, 1);
	PG_RETURN_BOOL(c >= 0);
}

/* --------------------------------------------------------- */
/* extractQuery (proc 101 for exbtree AM)                     */
/*                                                            */
/* The planner passes through the original dollar operators   */
/* (e.g. @>) at offset strategy numbers.  We wrap the         */
/* individual dollar operator into the composite query spec   */
/* format that the composite path extract_query expects for   */
/* BSON_INDEX_STRATEGY_COMPOSITE_QUERY, then delegate.        */
/* --------------------------------------------------------- */

Datum
documentdb_xbt_extract_query(PG_FUNCTION_ARGS)
{
	/* arg 0 = query bson, arg 2 = strategy (int2) */
	int16 offsetStrategy = PG_GETARG_INT16(2);
	int16 realStrategy = offsetStrategy - EXBT_STRATEGY_OFFSET;

	pgbson *queryBson = PG_GETARG_PGBSON(0);

	/*
	 * Wrap the individual dollar operator into the composite query spec:
	 *   { "q": [ { "op": <strategy>, <path>: <value> } ], "m": true, "or": false, "db": false }
	 *
	 * This is the same format ModifyScanKeysForCompositeScan produces
	 * at rescan time in the RUM path.
	 */
	pgbson_writer querySpecWriter;
	PgbsonWriterInit(&querySpecWriter);

	pgbson_array_writer queryWriter;
	PgbsonWriterStartArray(&querySpecWriter, "q", 1, &queryWriter);

	pgbson_writer clauseWriter;
	PgbsonArrayWriterStartDocument(&queryWriter, &clauseWriter);
	PgbsonWriterAppendInt32(&clauseWriter, "op", 2, (int32_t) realStrategy);
	PgbsonWriterConcat(&clauseWriter, queryBson);
	PgbsonArrayWriterEndDocument(&queryWriter, &clauseWriter);

	PgbsonWriterEndArray(&querySpecWriter, &queryWriter);

	PgbsonWriterAppendBool(&querySpecWriter, "m", 1, true);
	PgbsonWriterAppendBool(&querySpecWriter, "or", 2, false);
	PgbsonWriterAppendBool(&querySpecWriter, "db", 2, false);

	pgbson *compositeQuery = PgbsonWriterGetPgbson(&querySpecWriter);

	/* Replace arg 0 with the composite spec and arg 2 with COMPOSITE_QUERY */
	fcinfo->args[0].value = PointerGetDatum(compositeQuery);
	fcinfo->args[2].value = Int16GetDatum(BSON_INDEX_STRATEGY_COMPOSITE_QUERY);

	return gin_bson_composite_path_extract_query(fcinfo);
}

/* --------------------------------------------------------- */
/* comparePartial (proc 103 for exbtree AM)                   */
/*                                                            */
/* extractQuery wrapped the dollar operator into COMPOSITE_   */
/* QUERY format, so comparePartial must see that strategy     */
/* regardless of the opclass strategy on the scan key.        */
/* --------------------------------------------------------- */

Datum
documentdb_xbt_compare_partial(PG_FUNCTION_ARGS)
{
	/* Force strategy to COMPOSITE_QUERY - extractQuery already wrapped it */
	fcinfo->args[2].value = Int16GetDatum(BSON_INDEX_STRATEGY_COMPOSITE_QUERY);

	return gin_bson_composite_path_compare_partial(fcinfo);
}

/* --------------------------------------------------------- */
/* consistent (proc 102 for exbtree AM)                       */
/*                                                            */
/* In GIN/RUM, consistent is called per-TID after             */
/* comparePartial has already filtered non-matching entries.   */
/* The GIN consistent has early-return optimizations that      */
/* skip checking the check[] array.  In exbtree, the btree    */
/* positions with >= on the extracted lower bound, so boundary */
/* values that comparePartial rejects (check[i]=false) still  */
/* reach consistent.  We must verify all check[] entries are   */
/* true before delegating.                                     */
/* --------------------------------------------------------- */

Datum
documentdb_xbt_consistent(PG_FUNCTION_ARGS)
{
	bool *check = (bool *) PG_GETARG_POINTER(0);
	int32_t numKeys = (int32_t) PG_GETARG_INT32(3);
	bool *recheck = (bool *) PG_GETARG_POINTER(5);

	for (int i = 0; i < numKeys; i++)
	{
		if (!check[i])
		{
			*recheck = false;
			PG_RETURN_BOOL(false);
		}
	}

	/* All keys matched - delegate for additional logic */
	fcinfo->args[1].value = Int16GetDatum(BSON_INDEX_STRATEGY_COMPOSITE_QUERY);
	return gin_bson_composite_path_consistent(fcinfo);
}
