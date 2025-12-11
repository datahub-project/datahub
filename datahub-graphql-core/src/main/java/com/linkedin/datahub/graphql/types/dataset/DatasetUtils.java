/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.dataset;

import com.linkedin.common.urn.DatasetUrn;
import java.net.URISyntaxException;

public class DatasetUtils {

  private DatasetUtils() {}

  static DatasetUrn getDatasetUrn(String urnStr) {
    try {
      return DatasetUrn.createFromString(urnStr);
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          String.format("Failed to retrieve dataset with urn %s, invalid urn", urnStr));
    }
  }
}
