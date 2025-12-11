/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

/* SPDX-License-Identifier: Apache-2.0  */
/*
 * Originally developed by Acryl Data, Inc.; subsequently adapted, enhanced, and maintained by the National Digital Twin Programme.
 */
/*
/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Class to store all the vendors related context information. */
/*
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
public class VendorsContext {
  private final Map<String, Object> contextMap = new HashMap<>();

  public void register(String key, Object value) {
    contextMap.put(key, value);
  }

  public Optional<Object> fromVendorsContext(String key) {
    return Optional.ofNullable(contextMap.get(key));
  }

  public boolean contains(String key) {
    return contextMap.containsKey(key);
  }
  ;
}
