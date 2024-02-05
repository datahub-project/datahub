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
