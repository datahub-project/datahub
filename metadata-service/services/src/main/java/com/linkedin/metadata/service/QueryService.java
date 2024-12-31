package com.linkedin.metadata.service;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.QueryKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.query.QueryProperties;
import com.linkedin.query.QuerySource;
import com.linkedin.query.QueryStatement;
import com.linkedin.query.QuerySubject;
import com.linkedin.query.QuerySubjectArray;
import com.linkedin.query.QuerySubjects;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is used to permit easy CRUD operations on a Query Currently it supports creating and
 * removing a Query.
 *
 * <p>Note that no Authorization is performed within the service. The expectation is that the caller
 * has already verified the permissions of the active Actor.
 */
@Slf4j
public class QueryService extends BaseService {

  public QueryService(@Nonnull SystemEntityClient entityClient) {
    super(entityClient);
  }

  /**
   * Creates a new Query.
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation.
   *
   * @param name optional name of the Query
   * @param description optional description of the Query
   * @param source the source of the query
   * @param statement the query statement
   * @param subjects the query subjects
   * @param authentication the current authentication
   * @param currentTimeMs the current time in millis
   * @return the urn of the newly created View
   */
  public Urn createQuery(
      @Nonnull OperationContext opContext,
      @Nullable String name,
      @Nullable String description,
      @Nonnull QuerySource source,
      @Nonnull QueryStatement statement,
      @Nonnull List<QuerySubject> subjects,
      long currentTimeMs) {
    Objects.requireNonNull(source, "source must not be null");
    Objects.requireNonNull(statement, "statement must not be null");
    Objects.requireNonNull(subjects, "subjects must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");

    // 1. Generate a unique id for the new Query.
    final QueryKey key = new QueryKey();
    key.setId(UUID.randomUUID().toString());

    // 2. Create a new instance of QueryProperties
    final QueryProperties queryProperties = new QueryProperties();
    queryProperties.setSource(source);
    queryProperties.setStatement(statement);
    queryProperties.setName(name, SetMode.IGNORE_NULL);
    queryProperties.setDescription(description, SetMode.IGNORE_NULL);
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(UrnUtils.getUrn(opContext.getSessionAuthentication().getActor().toUrnStr()))
            .setTime(currentTimeMs);
    queryProperties.setCreated(auditStamp);
    queryProperties.setLastModified(auditStamp);

    // 2. Create a new instance of QuerySubjects
    final QuerySubjects querySubjects = new QuerySubjects();
    querySubjects.setSubjects(new QuerySubjectArray(subjects));

    // 3. Write the new query to GMS, return the new URN.
    try {
      final Urn entityUrn = EntityKeyUtils.convertEntityKeyToUrn(key, Constants.QUERY_ENTITY_NAME);
      this.entityClient.ingestProposal(
          opContext,
          AspectUtils.buildMetadataChangeProposal(
              entityUrn, Constants.QUERY_PROPERTIES_ASPECT_NAME, queryProperties),
          false);
      return UrnUtils.getUrn(
          this.entityClient.ingestProposal(
              opContext,
              AspectUtils.buildMetadataChangeProposal(
                  entityUrn, Constants.QUERY_SUBJECTS_ASPECT_NAME, querySubjects),
              false));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Query", e);
    }
  }

  /**
   * Updates an existing Query. If a provided field is null, the previous value will be kept.
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation.
   *
   * @param urn the urn of the query
   * @param name optional name of the Query
   * @param description optional description of the Query
   * @param statement the query statement
   * @param subjects the query subjects
   * @param authentication the current authentication
   * @param currentTimeMs the current time in millis
   */
  public void updateQuery(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nullable String name,
      @Nullable String description,
      @Nullable QueryStatement statement,
      @Nullable List<QuerySubject> subjects,
      long currentTimeMs) {
    Objects.requireNonNull(urn, "urn must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");

    // 1. Check whether the Query exists
    QueryProperties properties = getQueryProperties(opContext, urn);

    if (properties == null) {
      throw new IllegalArgumentException(
          String.format("Failed to update Query. Query with urn %s does not exist.", urn));
    }

    // 2. Apply changes to existing Query
    if (name != null) {
      properties.setName(name);
    }
    if (description != null) {
      properties.setDescription(description);
    }
    if (statement != null) {
      properties.setStatement(statement);
    }

    properties.setLastModified(
        new AuditStamp()
            .setTime(currentTimeMs)
            .setActor(UrnUtils.getUrn(opContext.getSessionAuthentication().getActor().toUrnStr())));

    // 3. Write changes to GMS
    try {
      final List<MetadataChangeProposal> aspectsToIngest = new ArrayList<>();
      aspectsToIngest.add(
          AspectUtils.buildMetadataChangeProposal(
              urn, Constants.QUERY_PROPERTIES_ASPECT_NAME, properties));
      if (subjects != null) {
        aspectsToIngest.add(
            AspectUtils.buildMetadataChangeProposal(
                urn,
                Constants.QUERY_SUBJECTS_ASPECT_NAME,
                new QuerySubjects().setSubjects(new QuerySubjectArray(subjects))));
      }
      this.entityClient.batchIngestProposals(opContext, aspectsToIngest, false);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to update View with urn %s", urn), e);
    }
  }

  /**
   * Deletes an existing Query with a specific urn.
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation
   *
   * <p>If the Query does not exist, no exception will be thrown.
   *
   * @param queryUrn the urn of the Query
   * @param authentication the current authentication
   */
  public void deleteQuery(@Nonnull OperationContext opContext, @Nonnull Urn queryUrn) {
    try {
      this.entityClient.deleteEntity(
          opContext, Objects.requireNonNull(queryUrn, "queryUrn must not be null"));
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to delete Query with urn %s", queryUrn), e);
    }
  }

  /**
   * Returns an instance of {@link QueryProperties} for the specified Query urn, or null if one
   * cannot be found.
   *
   * @param queryUrn the urn of the Query
   * @param authentication the authentication to use
   * @return an instance of {@link QueryProperties} for the Query, null if it does not exist.
   */
  @Nullable
  public QueryProperties getQueryProperties(
      @Nonnull OperationContext opContext, @Nonnull final Urn queryUrn) {
    Objects.requireNonNull(queryUrn, "queryUrn must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    final EntityResponse response = getQueryEntityResponse(opContext, queryUrn);
    if (response != null
        && response.getAspects().containsKey(Constants.QUERY_PROPERTIES_ASPECT_NAME)) {
      return new QueryProperties(
          response.getAspects().get(Constants.QUERY_PROPERTIES_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Returns an instance of {@link QuerySubjects} for the specified Query urn, or null if one cannot
   * be found.
   *
   * @param queryUrn the urn of the Query
   * @param authentication the authentication to use
   * @return an instance of {@link QuerySubjects} for the Query, null if it does not exist.
   */
  @Nullable
  public QuerySubjects getQuerySubjects(
      @Nonnull OperationContext opContext, @Nonnull final Urn queryUrn) {
    Objects.requireNonNull(queryUrn, "queryUrn must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    final EntityResponse response = getQueryEntityResponse(opContext, queryUrn);
    if (response != null
        && response.getAspects().containsKey(Constants.QUERY_SUBJECTS_ASPECT_NAME)) {
      return new QuerySubjects(
          response.getAspects().get(Constants.QUERY_SUBJECTS_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Returns an instance of {@link EntityResponse} for the specified Query urn, or null if one
   * cannot be found.
   *
   * @param queryUrn the urn of the Query
   * @param authentication the authentication to use
   * @return an instance of {@link EntityResponse} for the Query, null if it does not exist.
   */
  @Nullable
  public EntityResponse getQueryEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn queryUrn) {
    Objects.requireNonNull(queryUrn, "queryUrn must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    try {
      return this.entityClient.getV2(
          opContext,
          Constants.QUERY_ENTITY_NAME,
          queryUrn,
          ImmutableSet.of(
              Constants.QUERY_PROPERTIES_ASPECT_NAME, Constants.QUERY_SUBJECTS_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve Query with urn %s", queryUrn), e);
    }
  }
}
