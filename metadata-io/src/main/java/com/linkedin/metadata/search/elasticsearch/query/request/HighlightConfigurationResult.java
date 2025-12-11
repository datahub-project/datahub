/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.search.elasticsearch.query.request;

import java.util.Set;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/**
 * Result of applying highlight field configuration. Contains both the fields to highlight and which
 * fields were explicitly configured.
 */
@Getter
@Builder
public class HighlightConfigurationResult {

  /** The final set of fields that should be highlighted */
  @NonNull private final Set<String> fieldsToHighlight;

  /**
   * Fields that were explicitly configured via 'add' or 'replace' operations. These fields should
   * not have field expansion applied.
   */
  @NonNull private final Set<String> explicitlyConfiguredFields;
}
