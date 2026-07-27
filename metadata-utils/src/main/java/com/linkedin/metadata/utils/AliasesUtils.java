package com.linkedin.metadata.utils;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import java.util.Locale;

public class AliasesUtils {

  private AliasesUtils() {}

  /**
   * The lowercased form of a dataset URN: platform and name lowercased, the environment
   * (FabricType) preserved so the result stays a valid URN.
   *
   * <p>This is the contract for case-insensitive URN resolution and MUST stay in sync with the
   * ingestion-side lowercasing (the DataHub Python {@code lowercase_dataset_urn}). Any change here
   * has to be mirrored there or resolution silently stops matching.
   *
   * <p>Dataset-only: the URN is parsed as a {@link DatasetUrn}, so a non-dataset URN throws {@link
   * URISyntaxException}. Extending the aspect to other entity types needs a per-type rule here.
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
