package com.linkedin.dataset.client;

import com.linkedin.dataset.Dataset;
import com.linkedin.dataset.DatasetKey;
import javax.annotation.Nonnull;


public class DatasetUtils {
  private DatasetUtils() {

  }

  /**
   * Get DatasetKey from Dataset
   * @param dataset Dataset
   * @return DatasetKey
   */
  @Nonnull
  public static DatasetKey getDatasetKey(@Nonnull Dataset dataset) {
    return new DatasetKey().setPlatform(dataset.getPlatform())
        .setName(dataset.getName())
        .setOrigin(dataset.getOrigin());
  }
}
