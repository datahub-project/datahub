/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Class to store all the vendors related context information. */
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
