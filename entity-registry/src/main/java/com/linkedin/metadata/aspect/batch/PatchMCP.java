package com.linkedin.metadata.aspect.batch;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.AspectRetriever;
import jakarta.json.JsonPatch;

/**
 * A change proposal represented as a patch to an exiting stored object in the primary data store.
 */
public interface PatchMCP extends MCPItem {

  /**
   * Convert a Patch to an Upsert
   *
   * @param recordTemplate the current value record template
   * @return the upsert
   */
  ChangeMCP applyPatch(RecordTemplate recordTemplate, AspectRetriever aspectRetriever);

  JsonPatch getPatch();
}
