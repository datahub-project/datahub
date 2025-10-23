# Investigation Summary: OpenSearch Index Template Ingestion Issue

## Issue Description
User reported that only one index template is being ingested from OpenSearch v2.17 when using:
- `ingest_index_templates: true`
- `index_template_pattern: allow: - '.*'`

## Root Cause
The Elasticsearch source connector only supported **legacy index templates** (accessed via `indices.get_template()`), but modern versions of Elasticsearch (7.8+) and OpenSearch support **composable index templates** (accessed via `indices.get_index_template()`).

### Background
- **Legacy templates**: Deprecated in ES 7.8+, accessed via `get_template()`
- **Composable templates**: Introduced in ES 7.8+, accessed via `get_index_template()`
- OpenSearch v2.17 supports both types of templates

The user likely had:
- 1 legacy template (which was ingested)
- Multiple composable templates (which were NOT ingested)

## Solution
Modified the Elasticsearch source to support both template types:

### Changes Made

1. **Updated `get_workunits_internal()` method** (lines 409-430):
   - Now fetches both legacy and composable templates
   - Legacy templates: `client.indices.get_template()`
   - Composable templates: `client.indices.get_index_template()`
   - Gracefully handles clusters that don't support composable templates

2. **Enhanced `_extract_mcps()` method** (lines 448-570):
   - Added `is_composable_template` parameter
   - Handles different data structures for composable vs legacy templates:
     - **Composable**: `template.mappings`, `template.settings`, `template.aliases`
     - **Legacy**: `mappings`, `settings`, `aliases` (at root level)

3. **Added test coverage** (test_elasticsearch_source.py):
   - New test `test_composable_template_structure()` validates composable template parsing

### Technical Details

**Composable Template Response Structure:**
```json
{
  "index_templates": [
    {
      "name": "template-name",
      "index_template": {
        "index_patterns": ["pattern-*"],
        "template": {
          "mappings": {...},
          "settings": {...},
          "aliases": {...}
        }
      }
    }
  ]
}
```

**Legacy Template Response Structure:**
```json
{
  "template-name": {
    "index_patterns": ["pattern-*"],
    "mappings": {...},
    "settings": {...},
    "aliases": {...}
  }
}
```

## Impact
- **Fixed**: Users can now ingest ALL index templates from OpenSearch/Elasticsearch
- **Backward Compatible**: Still supports legacy templates
- **Forward Compatible**: Works with modern Elasticsearch 7.8+ and OpenSearch 2.x

## Testing
1. Syntax validation: ✅ Passed
2. Unit test added: ✅ `test_composable_template_structure()`
3. Backward compatibility: ✅ Legacy template support unchanged

## Files Modified
1. `/workspace/metadata-ingestion/src/datahub/ingestion/source/elastic_search.py`
2. `/workspace/metadata-ingestion/tests/unit/test_elasticsearch_source.py`

## Recommendation
This is a **bug fix** that resolves incomplete template ingestion for modern Elasticsearch/OpenSearch versions. Users should upgrade to get full template coverage.
