/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package io.datahubproject.openapi.models;

import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface GenericAspect {
  @Nonnull
  Map<String, Object> getValue();

  @Nullable
  Map<String, Object> getSystemMetadata();

  @Nullable
  Map<String, String> getHeaders();
}
