/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.models;

import java.util.Comparator;
import org.apache.commons.lang3.tuple.Pair;

public class PropertyOverrideComparator implements Comparator<Pair<String, Object>> {
  public int compare(Pair<String, Object> o1, Pair<String, Object> o2) {
    return Integer.compare(o2.getKey().split("/").length, o1.getKey().split("/").length);
  }
}
