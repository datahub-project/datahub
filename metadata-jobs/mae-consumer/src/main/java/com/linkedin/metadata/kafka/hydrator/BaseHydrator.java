/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.entity.EntityResponse;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BaseHydrator {

  /** Use values in the entity response to hydrate the document */
  protected abstract void hydrateFromEntityResponse(
      ObjectNode document, EntityResponse entityResponse);
}
