package com.linkedin.dataplatform.client;

import com.linkedin.metadata.restli.BaseClient;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.dataplatform.DataPlatformsRequestBuilders;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.GetAllRequest;
import com.linkedin.restli.client.Request;
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
    Request<DataPlatformInfo> req = PLATFORMS_REQUEST_BUILDERS.get().id(platformName).build();
    return _client.sendRequest(req).getResponse().getEntity();
  }

  /**
   * Get all data platforms
   * @return List<DataPlatformInfo>
   * @throws RemoteInvocationException
   */
  @Nonnull
  public List<DataPlatformInfo> getAllPlatforms() throws RemoteInvocationException {
    GetAllRequest<DataPlatformInfo> req = PLATFORMS_REQUEST_BUILDERS.getAll().build();
    return _client.sendRequest(req).getResponse().getEntity().getElements();
  }
}
