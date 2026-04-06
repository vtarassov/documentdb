/*-------------------------------------------------------------------------
 * Copyright (c) Microsoft Corporation.  All rights reserved.
 *
 * include/opclass/bson_gin_composite.h
 *
 * Exports for the composite index term and index management.
 *
 *-------------------------------------------------------------------------
 */

#ifndef BSON_GIN_COMPOSITE_H
#define BSON_GIN_COMPOSITE_H

#include "opclass/bson_gin_index_mgmt.h"

Datum * GenerateCompositeTermsFromIndexSpec(pgbson *document, pgbson *keySpec,
											uint32_t *numTerms);

Datum * GenerateCompositeTermsFromOptions(pgbson *document,
										  BsonGinCompositePathOptions *options,
										  uint32_t *numTerms);

int32_t GetIndexPathsFromCompositeOptions(BsonGinCompositePathOptions *options,
										  const char **indexPaths,
										  int8_t *sortOrders);

#endif
