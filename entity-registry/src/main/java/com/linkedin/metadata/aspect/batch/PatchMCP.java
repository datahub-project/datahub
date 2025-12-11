/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
