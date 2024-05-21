package com.linkedin.datahub.graphql.resolvers.connection;

import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubConnection;
import com.linkedin.datahub.graphql.generated.DataHubConnectionDetails;
import com.linkedin.datahub.graphql.generated.DataHubJsonConnection;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.Constants;
import io.datahubproject.metadata.services.SecretService;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ConnectionMapper {
  /**
   * Maps a GMS encrypted connection details object into the decrypted form returned by the GraphQL
   * API.
   *
   * <p>Returns null if the Entity does not have the required aspects: dataHubConnectionDetails or
   * dataPlatformInstance.
   */
  @Nullable
  public static DataHubConnection map(
      @Nonnull final QueryContext context,
      @Nonnull final EntityResponse entityResponse,
      @Nonnull final SecretService secretService) {
    // If the connection does not exist, simply return null
    if (!hasAspects(entityResponse)) {
      return null;
    }

    final DataHubConnection result = new DataHubConnection();
    final Urn entityUrn = entityResponse.getUrn();
    final EnvelopedAspectMap aspects = entityResponse.getAspects();

    result.setUrn(entityUrn.toString());
    result.setType(EntityType.DATAHUB_CONNECTION);

    final EnvelopedAspect envelopedAssertionInfo =
        aspects.get(Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME);
    if (envelopedAssertionInfo != null) {
      result.setDetails(
          mapConnectionDetails(
              context,
              new com.linkedin.connection.DataHubConnectionDetails(
                  envelopedAssertionInfo.getValue().data()),
              secretService));
    }
    final EnvelopedAspect envelopedPlatformInstance =
        aspects.get(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME);
    if (envelopedPlatformInstance != null) {
      final DataMap data = envelopedPlatformInstance.getValue().data();
      result.setPlatform(mapPlatform(new DataPlatformInstance(data)));
    }
    return result;
  }

  private static DataHubConnectionDetails mapConnectionDetails(
      @Nonnull final QueryContext context,
      @Nonnull final com.linkedin.connection.DataHubConnectionDetails gmsDetails,
      @Nonnull final SecretService secretService) {
    final DataHubConnectionDetails result = new DataHubConnectionDetails();
    result.setType(
        com.linkedin.datahub.graphql.generated.DataHubConnectionDetailsType.valueOf(
            gmsDetails.getType().toString()));
    if (gmsDetails.hasJson() && ConnectionUtils.canManageConnections(context)) {
      result.setJson(mapJsonConnectionDetails(gmsDetails.getJson(), secretService));
    }
    if (gmsDetails.hasName()) {
      result.setName(gmsDetails.getName());
    }
    return result;
  }

  private static DataHubJsonConnection mapJsonConnectionDetails(
      @Nonnull final com.linkedin.connection.DataHubJsonConnection gmsJsonConnection,
      @Nonnull final SecretService secretService) {
    final DataHubJsonConnection result = new DataHubJsonConnection();
    // Decrypt the BLOB!
    result.setBlob(secretService.decrypt(gmsJsonConnection.getEncryptedBlob()));
    return result;
  }

  private static DataPlatform mapPlatform(final DataPlatformInstance platformInstance) {
    // Set dummy platform to be resolved.
    final DataPlatform partialPlatform = new DataPlatform();
    partialPlatform.setUrn(platformInstance.getPlatform().toString());
    return partialPlatform;
  }

  private static boolean hasAspects(@Nonnull final EntityResponse response) {
    return response.hasAspects()
        && response.getAspects().containsKey(Constants.DATAHUB_CONNECTION_DETAILS_ASPECT_NAME)
        && response.getAspects().containsKey(Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME);
  }

  private ConnectionMapper() {}
}
