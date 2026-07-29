package com.linkedin.metadata.utils;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import java.util.Locale;

public class AliasesUtils {

  private AliasesUtils() {}

  /**
   * The case-insensitive resolution key for a dataset URN: the name is lowercased in full,
   * including any platform instance prefix; the platform and environment are unchanged. A
   * non-dataset URN throws {@link URISyntaxException}.
   */
  public static DatasetUrn lowercaseDatasetUrn(Urn urn) throws URISyntaxException {
    DatasetUrn dataset = DatasetUrn.createFromUrn(urn);
    return new DatasetUrn(
        dataset.getPlatformEntity(),
        dataset.getDatasetNameEntity().toLowerCase(Locale.ROOT),
        dataset.getOriginEntity());
  }
}
