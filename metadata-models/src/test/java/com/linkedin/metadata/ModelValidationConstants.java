/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata;

import com.google.common.collect.ImmutableSet;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.UnionTemplate;
import java.util.Set;

public class ModelValidationConstants {

  private ModelValidationConstants() {
    // Util class
  }

  static final Set<Class<? extends UnionTemplate>> IGNORED_ASPECT_CLASSES = ImmutableSet.of();

  static final Set<Class<? extends RecordTemplate>> IGNORED_SNAPSHOT_CLASSES = ImmutableSet.of();

  static final Set<Class<? extends RecordTemplate>> IGNORED_DELTA_CLASSES = ImmutableSet.of();
}
