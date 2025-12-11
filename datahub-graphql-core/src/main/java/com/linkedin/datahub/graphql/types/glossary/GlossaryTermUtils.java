/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.glossary;

import com.linkedin.common.urn.GlossaryTermUrn;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

public class GlossaryTermUtils {

  private GlossaryTermUtils() {}

  static GlossaryTermUrn getGlossaryTermUrn(String urnStr) {
    try {
      return GlossaryTermUrn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format("Failed to retrieve glossary with urn %s, invalid urn", urnStr));
    }
  }

  public static String getGlossaryTermName(String hierarchicalName) {
    if (hierarchicalName.contains(".")) {
      String[] nodes = hierarchicalName.split(Pattern.quote("."));
      return nodes[nodes.length - 1];
    }
    return hierarchicalName;
  }
}
