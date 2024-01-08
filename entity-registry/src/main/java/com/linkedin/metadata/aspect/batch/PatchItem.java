package com.linkedin.metadata.aspect.batch;

import com.github.fge.jsonpatch.Patch;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.plugins.validation.AspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;

/**
 * A change proposal represented as a patch to an exiting stored object in the primary data store.
 */
public abstract class PatchItem extends MCPBatchItem {

  /**
   * Convert a Patch to an Upsert
   *
   * @param entityRegistry the entity registry
   * @param recordTemplate the current value record template
   * @return the upsert
   */
  public abstract UpsertItem applyPatch(
      EntityRegistry entityRegistry,
      RecordTemplate recordTemplate,
      AspectRetriever aspectRetriever);

  public abstract Patch getPatch();
}
