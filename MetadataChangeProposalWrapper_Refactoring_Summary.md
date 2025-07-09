# MetadataChangeProposalWrapper Refactoring Summary

## Overview
Successfully completed refactoring to remove manual setting of `entityType`, `aspectName`, and `changeType=UPSERT` parameters from `MetadataChangeProposalWrapper` constructors across the codebase. These parameters are now automatically inferred by the class.

## Background
The `MetadataChangeProposalWrapper` class has automatic inference capabilities:
- `entityType` is automatically inferred from the `entityUrn` 
- `aspectName` is automatically inferred from the aspect object
- `changeType` defaults to `UPSERT` when not specified

## Files Modified

### Core Source Files
1. **`metadata-ingestion/src/datahub/ingestion/source/sql/vertica.py`**
   - Removed manual `entityType="dataset"`, `changeType=ChangeTypeClass.UPSERT`, `aspectName="subTypes"` from projection subtype MCP

2. **`metadata-ingestion/src/datahub/ingestion/source/datahub/datahub_database_reader.py`**
   - Removed manual `changeType=ChangeTypeClass.UPSERT` parameter

3. **`metadata-ingestion/src/datahub/ingestion/source/powerbi_report_server/report_server.py`**
   - Completely refactored `new_mcp()` method to remove `entity_type`, `aspect_name`, and `change_type` parameters
   - Updated all 8 call sites to use simplified signature

4. **`metadata-ingestion/src/datahub/ingestion/source/tableau/tableau.py`**
   - Removed hardcoded `aspect_name=c.SUB_TYPES` from 2 SubTypes MCP calls
   - Removed hardcoded `aspect_name=c.UPSTREAM_LINEAGE` from 3 UpstreamLineage MCP calls

### Examples & Utilities Updated
5. **`smoke-test/tests/setup/lineage/utils.py`**
   - Cleaned up 6 instances of manual parameter setting

6. **`metadata-ingestion/examples/library/create_domain.py`**
   - Removed manual `entityType="domain"`, `changeType=ChangeTypeClass.UPSERT` parameters

7. **`metadata-ingestion/examples/library/create_nested_domain.py`**
   - Removed manual `entityType="domain"`, `changeType=ChangeTypeClass.UPSERT` parameters

8. **`metadata-ingestion/examples/library/create_mlprimarykey.py`**
   - Removed manual `entityType="mlPrimaryKey"`, `changeType=ChangeTypeClass.UPSERT`, `aspectName="mlPrimaryKeyProperties"` parameters

9. **`metadata-ingestion/examples/library/create_mlfeature.py`**
   - Removed manual `entityType="mlFeature"`, `changeType=ChangeTypeClass.UPSERT`, `aspectName="mlFeatureProperties"` parameters

10. **`metadata-ingestion/examples/library/create_mlfeature_table.py`**
    - Removed manual `entityType="mlFeatureTable"`, `changeType=ChangeTypeClass.UPSERT` parameters

11. **`metadata-ingestion/examples/library/add_mlfeature_to_mlmodel.py`**
    - Removed manual `entityType="mlModel"`, `changeType=ChangeTypeClass.UPSERT` parameters

12. **`metadata-ingestion/examples/library/add_mlfeature_to_mlfeature_table.py`**
    - Removed manual `entityType="mlFeatureTable"`, `changeType=ChangeTypeClass.UPSERT` parameters

13. **`metadata-ingestion/examples/library/create_ermodelrelationship.py`**
    - Removed manual `entityType="erModelRelationship"`, `changeType="UPSERT"`, `aspectName` parameters from 2 instances

14. **`metadata-models-custom/scripts/insert_custom_aspect.py`**
    - Removed manual `entityType="dataset"` parameter (kept `changeType` and `aspectName` as they're required for raw MCP)

### Constants Cleanup
15. **`metadata-ingestion/src/datahub/ingestion/source/tableau/tableau_constant.py`**
    - Removed unused constants: `SUB_TYPES = "subTypes"` and `UPSTREAM_LINEAGE = "upstreamLineage"`

## Validation
- ✅ All lint checks pass (`ruff check src/`)
- ✅ All format checks pass (`ruff format src/ --check`)
- ✅ No remaining hardcoded `entityType`, `aspectName`, or `changeType=UPSERT` parameters in `MetadataChangeProposalWrapper` calls
- ✅ Unused constants removed from tableau constants file

## Impact
This refactoring:
- **Simplifies code** by removing redundant parameter specifications
- **Reduces maintenance burden** by leveraging automatic inference
- **Improves consistency** across the codebase
- **Eliminates potential errors** from manual parameter mismatches
- **Follows DRY principle** by not repeating information the class can infer

## Technical Details
The `MetadataChangeProposalWrapper` class automatically:
- Infers `entityType` from the `entityUrn` using `guess_entity_type()`
- Infers `aspectName` from the aspect object using `aspect.get_aspect_name()`
- Defaults `changeType` to `ChangeTypeClass.UPSERT` when not specified

Manual specification of these parameters is now unnecessary and has been removed across **15 files** with a total of **25+ individual parameter removals**.