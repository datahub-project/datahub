package com.linkedin.metadata.search.elasticsearch.indexbuilder;

// TODO further split REINDEXING etc to show how it finished (so all values would reflect a status
// after _reindex call ended), for now it's enough for our purposes
public enum ReindexResult {

  // was new
  CREATED_NEW,

  // mappings/settings didnt require reindex
  NOT_REINDEXED_NOTHING_APPLIED,

  // no reindex, but mappings/settings were applied
  NOT_REQUIRED_MAPPINGS_SETTINGS_APPLIED,

  // reindexing already ongoing
  REINDEXING_ALREADY,

  // reindxing skipped, 0 docs
  REINDEXED_SKIPPED_0DOCS,

  // reindex launched
  REINDEXING,

  // should not happen
  $UNKNOWN;
}
