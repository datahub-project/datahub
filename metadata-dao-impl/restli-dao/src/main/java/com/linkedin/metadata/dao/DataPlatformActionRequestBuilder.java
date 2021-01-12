
package com.linkedin.metadata.dao;

import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.metadata.snapshot.DataPlatformSnapshot;


/**
 * An action request builder for data platform entities.
 */
public class DataPlatformActionRequestBuilder extends BaseActionRequestBuilder<DataPlatformSnapshot, DataPlatformUrn> {

  private static final String BASE_URI_TEMPLATE = "dataPlatforms";

  public DataPlatformActionRequestBuilder() {
    super(DataPlatformSnapshot.class, DataPlatformUrn.class, BASE_URI_TEMPLATE);
  }
}