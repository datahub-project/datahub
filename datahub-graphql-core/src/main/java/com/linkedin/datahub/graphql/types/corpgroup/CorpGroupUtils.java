/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.corpgroup;

import com.linkedin.common.urn.CorpGroupUrn;
import java.net.URISyntaxException;

public class CorpGroupUtils {

  private CorpGroupUtils() {}

  public static CorpGroupUrn getCorpGroupUrn(final String urnStr) {
    if (urnStr == null) {
      return null;
    }
    try {
      return CorpGroupUrn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format("Failed to create CorpGroupUrn from string %s", urnStr), e);
    }
  }
}
