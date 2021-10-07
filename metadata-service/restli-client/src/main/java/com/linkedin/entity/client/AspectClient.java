package com.linkedin.entity.client;

import com.linkedin.common.client.BaseClient;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.AspectsDoGetTimeseriesAspectValuesRequestBuilder;
import com.linkedin.entity.AspectsDoIngestProposalRequestBuilder;
import com.linkedin.entity.AspectsGetRequestBuilder;
import com.linkedin.entity.AspectsRequestBuilders;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.HttpStatus;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Slf4j
public class AspectClient extends BaseClient {

  private static final AspectsRequestBuilders ASPECTS_REQUEST_BUILDERS = new AspectsRequestBuilders();

  public AspectClient(@Nonnull final Client restliClient) {
    super(restliClient);
  }

  /**
   * Gets aspect at version for an entity
   *
   * @param urn urn for the entity
   * @return list of paths given urn
   * @throws RemoteInvocationException on remote request error.
   */
  @Nonnull
  public VersionedAspect getAspect(
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version,
      @Nonnull String actor)
      throws RemoteInvocationException {

    AspectsGetRequestBuilder requestBuilder =
        ASPECTS_REQUEST_BUILDERS.get().id(urn).aspectParam(aspect).versionParam(version);

    return sendClientRequest(requestBuilder, actor).getEntity();
  }

  /**
   * Gets aspect at version for an entity, or null if one doesn't exist.
   *
   * @param urn urn for the entity
   * @return list of paths given urn
   * @throws RemoteInvocationException on remote request error.
   */
  @Nullable
  public VersionedAspect getAspectOrNull(
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version,
      @Nonnull String actor)
      throws RemoteInvocationException {

    AspectsGetRequestBuilder requestBuilder =
        ASPECTS_REQUEST_BUILDERS.get().id(urn).aspectParam(aspect).versionParam(version);
    try {
      return sendClientRequest(requestBuilder, actor).getEntity();
    } catch (RestLiResponseException e) {
      if (e.getStatus() == HttpStatus.S_404_NOT_FOUND.getCode()) {
        // Then the aspect was not found. Return null.
        return null;
      }
      throw e;
    }
  }

  /**
   * Retrieve instances of a particular aspect.
   *
   * @param urn urn for the entity.
   * @param entity the name of the entity.
   * @param aspect the name of the aspect.
   * @param startTimeMillis the earliest desired event time of the aspect value in milliseconds.
   * @param endTimeMillis the latest desired event time of the aspect value in milliseconds.
   * @param limit the maximum number of desired aspect values.
   * @param actor the actor associated with the request [internal]
   * @return  the list of EnvelopedAspect values satisfying the input parameters.
   * @throws RemoteInvocationException on remote request error.
   */
  @Nonnull
  public List<EnvelopedAspect> getTimeseriesAspectValues(
      @Nonnull String urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis,
      @Nullable Integer limit,
      @Nullable String actor
  )
      throws RemoteInvocationException {

    AspectsDoGetTimeseriesAspectValuesRequestBuilder requestBuilder =
        ASPECTS_REQUEST_BUILDERS.actionGetTimeseriesAspectValues()
            .urnParam(urn)
            .entityParam(entity)
            .aspectParam(aspect);

    if (startTimeMillis != null) {
      requestBuilder.startTimeMillisParam(startTimeMillis);
    }

    if (endTimeMillis != null) {
      requestBuilder.endTimeMillisParam(endTimeMillis);
    }

    if (limit != null) {
      requestBuilder.limitParam(limit);
    }
    return sendClientRequest(requestBuilder, actor).getEntity().getValues();
  }

  /**
   * Ingest a MetadataChangeProposal event.
   */
  public Response<String> ingestProposal(@Nonnull final MetadataChangeProposal metadataChangeProposal, @Nonnull final String actor)
      throws RemoteInvocationException {
    final AspectsDoIngestProposalRequestBuilder requestBuilder = ASPECTS_REQUEST_BUILDERS.actionIngestProposal()
            .proposalParam(metadataChangeProposal);
    return sendClientRequest(requestBuilder, actor);
  }

  public <T extends RecordTemplate> Optional<T> getVersionedAspect(
          @Nonnull String urn,
          @Nonnull String aspect,
          @Nonnull Long version,
          @Nonnull String actor,
          @Nonnull Class<T> aspectClass)
          throws RemoteInvocationException {

    AspectsGetRequestBuilder requestBuilder =
            ASPECTS_REQUEST_BUILDERS.get().id(urn).aspectParam(aspect).versionParam(version);

    try {
      VersionedAspect entity = sendClientRequest(requestBuilder, actor).getEntity();
      if (entity.hasAspect()) {
        DataMap rawAspect = ((DataMap) entity.data().get("aspect"));
        if (rawAspect.containsKey(aspectClass.getCanonicalName())) {
          DataMap aspectDataMap = rawAspect.getDataMap(aspectClass.getCanonicalName());
          return Optional.of(RecordUtils.toRecordTemplate(aspectClass, aspectDataMap));
        }
      }
    } catch (RestLiResponseException e) {
      if (e.getStatus() == 404) {
        log.debug("Could not find aspect {} for entity {}", aspect, urn);
        return Optional.empty();
      } else {
        // re-throw other exceptions
        throw e;
      }
    }

    return Optional.empty();
  }

}
