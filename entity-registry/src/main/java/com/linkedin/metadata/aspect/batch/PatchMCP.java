package com.linkedin.metadata.aspect.batch;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import jakarta.json.JsonPatch;
import javax.annotation.Nullable;

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

  /** Parsed generic patch when the proposal carried one; otherwise null. */
  @Nullable
  default GenericJsonPatch getGenericJsonPatch() {
    return null;
  }
}
