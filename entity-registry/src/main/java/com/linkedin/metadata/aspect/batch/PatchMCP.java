package com.linkedin.metadata.aspect.batch;

import com.github.fge.jsonpatch.Patch;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.AspectRetriever;

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

  Patch getPatch();
}
