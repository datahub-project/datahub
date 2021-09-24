package com.linkedin.entity.client;

import com.linkedin.common.Ownership;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.RestLiServiceException;
import javax.annotation.Nullable;

import static com.linkedin.metadata.Constants.*;


/**
 * Basic client that fetches {@link Ownership} aspects from the Metadata Service.
 */
public class OwnershipClient {

  private final AspectClient _aspectClient;

  public OwnershipClient(final AspectClient aspectClient) {
    _aspectClient = aspectClient;
  }

  /**
   * Retrieve the latest version of the standard {@link Ownership} aspect from the Metadata Service,
   * using a raw {@link AspectClient}.
   *
   * @param urn stringified urn associated with the entity to fetch Ownership for.
   * @return an instance of {@link Ownership} if one is found, or null if one is not found.
   * @throws RemoteInvocationException if Rest.li throws an unexpected exception (aside from 404 not found)
   */
  @Nullable
  public Ownership getLatestOwnership(final String urn) throws RemoteInvocationException {
    // Fetch the latest version of "ownership" aspect for the resource.
    try {
      final VersionedAspect aspect = _aspectClient.getAspect(
          urn,
          OWNERSHIP_ASPECT_NAME,
          ASPECT_LATEST_VERSION,
          SYSTEM_ACTOR);
      return aspect.getAspect().getOwnership();
    } catch (RestLiServiceException e) {
      if (HttpStatus.S_404_NOT_FOUND.equals(e.getStatus())) {
        // No aspect exists.
        return null;
      }
      throw e;
    }
  }
}
