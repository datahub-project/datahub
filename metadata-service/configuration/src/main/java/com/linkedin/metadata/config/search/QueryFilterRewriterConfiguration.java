/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class QueryFilterRewriterConfiguration {

  private ExpansionRewriterConfiguration containerExpansion;
  private ExpansionRewriterConfiguration domainExpansion;

  @NoArgsConstructor
  @AllArgsConstructor
  @Data
  public static class ExpansionRewriterConfiguration {
    public static final ExpansionRewriterConfiguration DEFAULT =
        new ExpansionRewriterConfiguration(false, 100, 100);

    boolean enabled;
    private int pageSize;
    private int limit;
  }
}
