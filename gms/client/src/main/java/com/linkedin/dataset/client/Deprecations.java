package com.linkedin.dataset.client;

import com.linkedin.common.client.DatasetsClient;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.DatasetDeprecation;
import com.linkedin.dataset.DeprecationRequestBuilders;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.CreateIdRequest;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.common.ComplexResourceKey;
import com.linkedin.restli.common.EmptyRecord;
import javax.annotation.Nonnull;

public class Deprecations extends DatasetsClient {
  private static final DeprecationRequestBuilders DEPRECATION_REQUEST_BUILDERS
      = new DeprecationRequestBuilders();

  public Deprecations(@Nonnull Client restliClient) {
    super(restliClient);
  }

  /**
   * Creates or Updates DatasetDeprecation aspect
   *
   * @param datasetUrn dataset urn
   * @param datasetDeprecation dataset deprecation
   */
  public void updateDatasetDeprecation(@Nonnull DatasetUrn datasetUrn,
      @Nonnull DatasetDeprecation datasetDeprecation)
      throws RemoteInvocationException {

    CreateIdRequest<Long, DatasetDeprecation> request = DEPRECATION_REQUEST_BUILDERS.create()
        .datasetKey(new ComplexResourceKey<>(toDatasetKey(datasetUrn), new EmptyRecord()))
        .input(datasetDeprecation)
        .build();

    _client.sendRequest(request).getResponse();
  }

  /**
   * Get DatasetDeprecation aspect
   *
   * @param datasetUrn dataset urn
   */
  @Nonnull
  public DatasetDeprecation getDatasetDeprecation(@Nonnull DatasetUrn datasetUrn)
      throws RemoteInvocationException {

    Request<DatasetDeprecation> request =
        DEPRECATION_REQUEST_BUILDERS.get()
            .datasetKey(new ComplexResourceKey<>(toDatasetKey(datasetUrn), new EmptyRecord()))
            .id(BaseLocalDAO.LATEST_VERSION)
            .build();

    return _client.sendRequest(request).getResponse().getEntity();
  }
}
