package com.linkedin.metadata.service;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Share;
import com.linkedin.common.ShareResult;
import com.linkedin.common.ShareResultArray;
import com.linkedin.common.ShareResultState;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.service.util.ServiceUtils;
import io.datahubproject.openapi.client.OpenApiClient;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ShareService extends BaseService {

  public ShareService(
      @Nonnull EntityClient entityClient, @Nonnull Authentication systemAuthentication,
      @Nonnull final OpenApiClient openApiClient) {
    super(entityClient, systemAuthentication, openApiClient);
  }

  /**
   * Update the share aspect with either a failing or passing share result. If the result for the
   * given destination already exists, update it, otherwise create a new result.
   */
  public Share upsertShareResult(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn connectionUrn,
      @Nonnull final ShareResultState status,
      @Nonnull final Authentication authentication) {
    // Get share aspect or default create a new one with an empty list if empty
    final Share shareAspect = this.getShareOrDefault(entityUrn, authentication);

    // Create or update existing shareResult with new data
    ShareResult shareResult =
        shareAspect.getLastShareResults().stream()
            .filter(r -> r.getDestination().equals(connectionUrn))
            .findFirst()
            .orElse(null);
    shareResult =
        createOrUpdateShareResult(shareResult, connectionUrn, status, null, null, authentication);

    // Upsert the result into the results array
    final Share updatedShareAspect = upsertShareResult(shareResult, shareAspect);

    // Write the updated aspect to GMS, return the updated aspect
    try {
      this.entityClient.ingestProposal(
          AspectUtils.buildMetadataChangeProposal(
              entityUrn, Constants.SHARE_ASPECT_NAME, updatedShareAspect),
          authentication,
          false);
      return updatedShareAspect;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to ingest share aspect for urn %s", entityUrn), e);
    }
  }

  /** Retrieves the share aspect from GMS or returns null if it doesn't exist */
  @Nullable
  private Share getShareAspect(
      @Nonnull final Urn urn, @Nonnull final Authentication authentication) {
    try {
      EntityResponse response =
          this.entityClient.getV2(
              urn.getEntityType(),
              urn,
              ImmutableSet.of(Constants.SHARE_ASPECT_NAME),
              authentication);
      if (response != null && response.getAspects().containsKey(Constants.SHARE_ASPECT_NAME)) {
        return new Share(response.getAspects().get(Constants.SHARE_ASPECT_NAME).getValue().data());
      }
      return null;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to fetch share aspect for urn %s", urn), e);
    }
  }

  /** Retrieves the share aspect from GMS or defaults to an empty aspect with required fields */
  @Nonnull
  public Share getShareOrDefault(
      @Nonnull final Urn urn, @Nonnull final Authentication authentication) {
    Share shareAspect = getShareAspect(urn, authentication);
    if (shareAspect == null) {
      shareAspect = new Share();
      shareAspect.setLastShareResults(new ShareResultArray());
    }
    return shareAspect;
  }

  /**
   * Updates the currentShareResult with provided data if it exists, otherwise creates a new share
   * result
   */
  @Nonnull
  private ShareResult createOrUpdateShareResult(
      @Nullable final ShareResult currentShareResult,
      @Nonnull final Urn connectionUrn,
      @Nonnull final ShareResultState status,
      @Nullable final Urn implicitShareEntity,
      @Nullable final String message,
      @Nonnull final Authentication authentication) {
    ShareResult shareResult = currentShareResult != null ? currentShareResult : new ShareResult();
    shareResult.setDestination(connectionUrn);
    shareResult.setStatus(status);
    shareResult.setLastAttempt(ServiceUtils.createAuditStamp(authentication));
    if (!shareResult.hasCreated()) {
      shareResult.setCreated(ServiceUtils.createAuditStamp(authentication));
    }
    if (implicitShareEntity != null) {
      shareResult.setImplicitShareEntity(implicitShareEntity);
    }
    if (message != null) {
      shareResult.setMessage(message);
    }
    if (status == ShareResultState.SUCCESS) {
      shareResult.setLastSuccess(ServiceUtils.createAuditStamp(authentication));
    }
    return shareResult;
  }

  /**
   * Replace existing shareResult with same dest if it exists, otherwise append result at end of
   * array
   */
  @Nonnull
  private Share upsertShareResult(
      @Nonnull final ShareResult shareResult, @Nonnull final Share shareAspect) {
    ShareResultArray shareResults = shareAspect.getLastShareResults();
    Optional<ShareResult> existingResult =
        shareResults.stream()
            .filter(r -> r.getDestination().equals(shareResult.getDestination()))
            .findFirst();

    // if this result already exists, replace it in the array. Otherwise append to the end
    if (existingResult.isPresent()) {
      shareResults =
          shareResults.stream()
              .map(
                  result -> {
                    if (result.getDestination().equals(shareResult.getDestination())) {
                      return shareResult;
                    }
                    return result;
                  })
              .collect(Collectors.toCollection(ShareResultArray::new));
    } else {
      shareResults.add(shareResult);
    }

    shareAspect.setLastShareResults(shareResults);
    return shareAspect;
  }
}
