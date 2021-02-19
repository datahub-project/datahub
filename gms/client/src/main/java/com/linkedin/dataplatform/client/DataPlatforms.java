package com.linkedin.dataplatform.client;

import com.linkedin.data.template.StringArray;
import com.linkedin.dataPlatforms.DataPlatform;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.dataplatform.DataPlatformsRequestBuilders;
import com.linkedin.metadata.restli.BaseClient;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.GetAllRequest;
import com.linkedin.restli.client.GetRequest;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;


public class DataPlatforms extends BaseClient {

  private static final DataPlatformsRequestBuilders PLATFORMS_REQUEST_BUILDERS = new DataPlatformsRequestBuilders();

  public DataPlatforms(@Nonnull Client restliClient) {
    super(restliClient);
  }

  /**
   * Get data platform details by name
   * @param platformName String
   * @return DataPlatformInfo
   * @throws RemoteInvocationException
   */
  @Nonnull
  public DataPlatformInfo getPlatformByName(@Nonnull String platformName) throws RemoteInvocationException {
    final GetRequest<DataPlatform> req = PLATFORMS_REQUEST_BUILDERS.get()
        .id(platformName)
        .aspectsParam(new StringArray(DataPlatformInfo.class.getCanonicalName()))
        .build();
    return _client.sendRequest(req).getResponse().getEntity().getDataPlatformInfo();
  }

  /**
   * Get all data platforms
   * @return List<DataPlatformInfo>
   * @throws RemoteInvocationException
   */
  @Nonnull
  public List<DataPlatform> getAllPlatforms() throws RemoteInvocationException {
    final GetAllRequest<DataPlatform> req = PLATFORMS_REQUEST_BUILDERS.getAll().build();
    return new ArrayList<>(_client.sendRequest(req)
            .getResponse()
            .getEntity()
            .getElements());
  }
}
