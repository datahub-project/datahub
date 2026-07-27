package com.linkedin.metadata.utils;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import java.util.Locale;

public class AliasesUtils {

  private AliasesUtils() {}

  /**
   * The lowercased form of a dataset URN, used as the case-insensitive resolution key: the platform
   * and name are lowercased and the environment (FabricType) is preserved so the result stays a
   * valid URN. A non-dataset URN throws {@link URISyntaxException}.
   */
  public static DatasetUrn lowercaseDatasetUrn(Urn urn) throws URISyntaxException {
    DatasetUrn dataset = DatasetUrn.createFromUrn(urn);
    DataPlatformUrn platform =
        new DataPlatformUrn(
            dataset.getPlatformEntity().getPlatformNameEntity().toLowerCase(Locale.ROOT));
    return new DatasetUrn(
        platform,
        dataset.getDatasetNameEntity().toLowerCase(Locale.ROOT),
        dataset.getOriginEntity());
  }
}
