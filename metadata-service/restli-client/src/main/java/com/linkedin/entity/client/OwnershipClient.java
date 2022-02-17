package com.linkedin.entity.client;

import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import java.net.URISyntaxException;
import java.util.Collections;
import javax.annotation.Nullable;

import static com.linkedin.metadata.Constants.*;


/**
 * Basic client that fetches {@link Ownership} aspects from the Metadata Service.
 */
public class OwnershipClient {

  private final EntityClient _entityClient;

  public OwnershipClient(final EntityClient entityClient) {
    _entityClient = entityClient;
  }

  /**
   * Retrieve the latest version of the standard {@link Ownership} aspect from the Metadata Service,
   * using a raw {@link EntityClient}.
   *
   * @param urn stringified urn associated with the entity to fetch Ownership for.
   * @return an instance of {@link Ownership} if one is found, or null if one is not found.
   * @throws RemoteInvocationException if Rest.li throws an unexpected exception (aside from 404 not found)
   */
  @Nullable
  public Ownership getLatestOwnership(final String urn) throws RemoteInvocationException {
    // Fetch the latest version of "ownership" aspect for the resource.
    try {
      Urn urnObj = Urn.createFromString(urn);
      final EntityResponse entityResponse = _entityClient.batchGetV2(
          urnObj.getEntityType(),
          Collections.singleton(urnObj),
          Collections.singleton(OWNERSHIP_ASPECT_NAME),
          null).get(urnObj);
      if (entityResponse != null && entityResponse.getAspects().containsKey(OWNERSHIP_ASPECT_NAME)) {
        return new Ownership(entityResponse.getAspects().get(OWNERSHIP_ASPECT_NAME).getValue().data());
      }
      return null;
    } catch (RestLiServiceException e) {
      if (HttpStatus.S_404_NOT_FOUND.equals(e.getStatus())) {
        // No aspect exists.
        return null;
      }
      throw e;
    } catch (URISyntaxException ue) {
      throw new RuntimeException(ue);
    }
  }
}
