package com.linkedin.metadata.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionResult;
import com.linkedin.assertion.AssertionRunEvent;
import com.linkedin.assertion.AssertionRunStatus;
import com.linkedin.assertion.AssertionSource;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionType;
import com.linkedin.assertion.CustomAssertionInfo;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.key.AssertionKey;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AssertionService extends BaseService {

  private final GraphClient _graphClient;

  private static final String ASSERTS_RELATIONSHIP_NAME = "Asserts";

  public AssertionService(
      @Nonnull SystemEntityClient entityClient, @Nonnull GraphClient graphClient) {
    super(entityClient);
    _graphClient = graphClient;
  }

  @Nonnull
  public Urn generateAssertionUrn() {
    final AssertionKey key = new AssertionKey();
    final String id = UUID.randomUUID().toString();
    key.setAssertionId(id);
    return EntityKeyUtils.convertEntityKeyToUrn(key, Constants.ASSERTION_ENTITY_NAME);
  }

  /**
   * Retrieves the entity associated with the assertion
   *
   * @param opContext the operation context
   * @param assertionUrn the urn of the assertion to retrieve entity for
   * @return Entity urn associated with the assertion
   */
  public @Nullable Urn getEntityUrnForAssertion(
      @Nonnull OperationContext opContext, @Nonnull final Urn assertionUrn) {
    try {
      // Fetch the entity associated with the assertion from the Graph
      final EntityRelationships relationships =
          _graphClient.getRelatedEntities(
              assertionUrn.toString(),
              ImmutableList.of(ASSERTS_RELATIONSHIP_NAME),
              RelationshipDirection.OUTGOING,
              0,
              1,
              opContext.getActorContext().getActorUrn().toString());

      if (relationships.hasRelationships() && !relationships.getRelationships().isEmpty()) {
        return relationships.getRelationships().get(0).getEntity();
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve entity for assertion with urn %s", assertionUrn), e);
    }
    return null;
  }

  /**
   * Returns an instance of {@link EntityResponse} for the specified View urn, or null if one cannot
   * be found.
   *
   * @param assertionUrn the urn of the View
   * @return an instance of {@link EntityResponse} for the View, null if it does not exist.
   */
  @Nullable
  public EntityResponse getAssertionEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn assertionUrn) {
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");
    try {
      return this.entityClient.getV2(
          opContext,
          Constants.ASSERTION_ENTITY_NAME,
          assertionUrn,
          ImmutableSet.of(
              Constants.ASSERTION_INFO_ASPECT_NAME,
              Constants.ASSERTION_ACTIONS_ASPECT_NAME,
              Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME,
              Constants.GLOBAL_TAGS_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve Assertion with urn %s", assertionUrn), e);
    }
  }

  public Urn upsertCustomAssertion(
      @Nonnull OperationContext opContext,
      @Nonnull Urn assertionUrn,
      @Nonnull Urn entityUrn,
      @Nonnull String description,
      @Nullable String externalUrl,
      @Nonnull DataPlatformInstance dataPlatformInstance,
      @Nonnull CustomAssertionInfo customAssertionInfo) {

    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(description, "description must not be null");
    Objects.requireNonNull(customAssertionInfo, "info must not be null");
    Objects.requireNonNull(dataPlatformInstance, "opContext must not be null");

    AssertionInfo assertionInfo = new AssertionInfo();
    assertionInfo.setType(AssertionType.CUSTOM);
    assertionInfo.setDescription(description);
    if (externalUrl != null) {
      assertionInfo.setExternalUrl(new Url(externalUrl));
    }
    assertionInfo.setSource(new AssertionSource().setType(AssertionSourceType.EXTERNAL));
    assertionInfo.setCustomAssertion(customAssertionInfo);

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(
        AspectUtils.buildMetadataChangeProposal(
            assertionUrn, Constants.ASSERTION_INFO_ASPECT_NAME, assertionInfo));
    aspects.add(
        (AspectUtils.buildMetadataChangeProposal(
            assertionUrn, Constants.DATA_PLATFORM_INSTANCE_ASPECT_NAME, dataPlatformInstance)));

    try {
      this.entityClient.batchIngestProposals(opContext, aspects, false);
      return assertionUrn;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to upsert Custom Assertion with urn %s", assertionUrn), e);
    }
  }

  public void addAssertionRunEvent(
      @Nonnull OperationContext opContext,
      @Nonnull Urn assertionUrn,
      @Nonnull Urn asserteeUrn,
      @Nonnull Long timestampMillis,
      @Nonnull AssertionResult assertionResult) {
    AssertionRunEvent assertionRunEvent = new AssertionRunEvent();
    assertionRunEvent.setTimestampMillis(timestampMillis);
    assertionRunEvent.setRunId(timestampMillis.toString());
    assertionRunEvent.setAssertionUrn(assertionUrn);
    assertionRunEvent.setAsserteeUrn(asserteeUrn);
    assertionRunEvent.setStatus(AssertionRunStatus.COMPLETE);
    assertionRunEvent.setResult(assertionResult);

    try {
      this.entityClient.ingestProposal(
          opContext,
          AspectUtils.buildMetadataChangeProposal(
              assertionUrn, Constants.ASSERTION_RUN_EVENT_ASPECT_NAME, assertionRunEvent),
          false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to upsert Assertion Run Event for assertion with urn %s", assertionUrn),
          e);
    }
  }
}
