package com.linkedin.metadata.service;

import com.google.common.collect.ImmutableSet;
import com.linkedin.application.ApplicationKey;
import com.linkedin.application.ApplicationProperties;
import com.linkedin.application.Applications;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.SetMode;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is used to permit easy CRUD operations on an Application
 *
 * <p>Note that no Authorization is performed within the service. The expectation is that the caller
 * has already verified the permissions of the active Actor.
 */
@Slf4j
public class ApplicationService {
  private final EntityClient entityClient;

  public ApplicationService(@Nonnull EntityClient entityClient) {
    this.entityClient = entityClient;
  }

  /**
   * Creates a new Application.
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation.
   *
   * @param name optional name of the Application
   * @param description optional description of the Application
   * @return the urn of the newly created Application
   */
  public Urn createApplication(
      @Nonnull OperationContext opContext,
      @Nullable String id,
      @Nullable String name,
      @Nullable String description) {
    final ApplicationKey key = new ApplicationKey();
    if (id != null && !id.isBlank()) {
      key.setId(id);
    } else {
      key.setId(UUID.randomUUID().toString());
    }
    try {
      if (this.entityClient.exists(
          opContext,
          EntityKeyUtils.convertEntityKeyToUrn(key, Constants.APPLICATION_ENTITY_NAME))) {
        throw new IllegalArgumentException("This Application already exists!");
      }
    } catch (RemoteInvocationException e) {
      throw new RuntimeException("Unable to check for existence of Application!", e);
    }

    final ApplicationProperties properties = new ApplicationProperties();
    properties.setName(name, SetMode.IGNORE_NULL);
    properties.setDescription(description, SetMode.IGNORE_NULL);

    try {
      final Urn entityUrn =
          EntityKeyUtils.convertEntityKeyToUrn(key, Constants.APPLICATION_ENTITY_NAME);
      return UrnUtils.getUrn(
          this.entityClient.ingestProposal(
              opContext,
              AspectUtils.buildMetadataChangeProposal(
                  entityUrn, Constants.APPLICATION_PROPERTIES_ASPECT_NAME, properties),
              false));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Application", e);
    }
  }

  /**
   * @param applicationUrn the urn of the Application
   * @return an instance of {@link ApplicationProperties} for the Application, null if it does not
   *     exist.
   */
  @Nullable
  public ApplicationProperties getApplicationProperties(
      @Nonnull OperationContext opContext, @Nonnull final Urn applicationUrn) {
    Objects.requireNonNull(applicationUrn, "applicationUrn must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    final EntityResponse response;
    try {
      response =
          this.entityClient.getV2(
              opContext,
              Constants.APPLICATION_ENTITY_NAME,
              applicationUrn,
              ImmutableSet.of(Constants.APPLICATION_PROPERTIES_ASPECT_NAME));
    } catch (Exception e) {
      log.error(
          String.format("Failed to retrieve ApplicationProperties for urn %s", applicationUrn), e);
      return null;
    }
    if (response != null
        && response.getAspects().containsKey(Constants.APPLICATION_PROPERTIES_ASPECT_NAME)) {
      return new ApplicationProperties(
          response
              .getAspects()
              .get(Constants.APPLICATION_PROPERTIES_ASPECT_NAME)
              .getValue()
              .data());
    }
    return null;
  }

  @Nullable
  public EntityResponse getApplicationEntityResponse(
      @Nonnull OperationContext opContext, @Nonnull final Urn applicationUrn) {
    Objects.requireNonNull(applicationUrn, "applicationUrn must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    try {
      return this.entityClient.getV2(
          opContext,
          Constants.APPLICATION_ENTITY_NAME,
          applicationUrn,
          ImmutableSet.of(Constants.APPLICATION_PROPERTIES_ASPECT_NAME));
    } catch (Exception e) {
      log.error(String.format("Failed to fetch Application with urn %s", applicationUrn), e);
    }
    return null;
  }

  public void setDomain(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn applicationUrn,
      @Nonnull final Urn domainUrn) {
    Objects.requireNonNull(applicationUrn, "applicationUrn must not be null");
    Objects.requireNonNull(domainUrn, "domainUrn must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");

    Domains domains = new Domains(new DataMap());
    domains.setDomains(new UrnArray());
    domains.getDomains().add(domainUrn);

    try {
      this.entityClient.ingestProposal(
          opContext,
          AspectUtils.buildMetadataChangeProposal(
              applicationUrn, Constants.DOMAINS_ASPECT_NAME, domains),
          false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to set domain for Application with urn %s domain %s",
              applicationUrn, domainUrn),
          e);
    }
  }

  public void deleteApplication(@Nonnull OperationContext opContext, @Nonnull Urn applicationUrn) {
    Objects.requireNonNull(applicationUrn, "applicationUrn must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");
    try {
      this.entityClient.deleteEntity(opContext, applicationUrn);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to delete Application with urn %s", applicationUrn), e);
    }
  }

  public void batchSetApplicationAssets(
      @Nonnull OperationContext opContext,
      @Nonnull Urn applicationUrn,
      @Nonnull List<Urn> resourceUrns,
      @Nonnull Urn actorUrn) { // actorUrn for audit stamp if not in opContext
    Objects.requireNonNull(applicationUrn, "applicationUrn must not be null");
    Objects.requireNonNull(resourceUrns, "resourceUrns must not be null");

    final List<MetadataChangeProposal> proposals =
        resourceUrns.stream()
            .map(resourceUrn -> buildSetApplicationAssetsProposal(applicationUrn, resourceUrn))
            .collect(Collectors.toList());

    try {
      this.entityClient.batchIngestProposals(opContext, proposals, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch set application assets for application %s", applicationUrn),
          e);
    }
  }

  private MetadataChangeProposal buildSetApplicationAssetsProposal(
      @Nullable Urn applicationUrn, Urn resourceUrn) {
    Applications applications = new Applications(new DataMap());
    applications.setApplications(new UrnArray());
    if (applicationUrn != null) {
      applications.getApplications().add(applicationUrn);
    }

    return AspectUtils.buildMetadataChangeProposal(
        resourceUrn, Constants.APPLICATION_MEMBERSHIP_ASPECT_NAME, applications);
  }

  public void batchRemoveApplicationAssets(
      @Nonnull OperationContext opContext,
      @Nonnull Urn applicationUrn,
      @Nonnull List<Urn> resourceUrnsToRemove,
      @Nonnull Urn actorUrn) { // actorUrn for audit stamp
    Objects.requireNonNull(applicationUrn, "applicationUrn must not be null");
    Objects.requireNonNull(resourceUrnsToRemove, "resourceUrnsToRemove must not be null");

    final List<MetadataChangeProposal> proposals =
        resourceUrnsToRemove.stream()
            .map(resourceUrn -> buildSetApplicationAssetsProposal(null, resourceUrn))
            .collect(Collectors.toList());

    try {
      this.entityClient.batchIngestProposals(opContext, proposals, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch remove application from assets for application %s", applicationUrn),
          e);
    }
  }

  public void unsetApplication(
      @Nonnull OperationContext opContext, @Nonnull Urn resourceUrn, @Nonnull Urn actorUrn) {
    Objects.requireNonNull(resourceUrn, "resourceUrn must not be null");
    Objects.requireNonNull(actorUrn, "actorUrn must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");

    // To unset, we build a proposal that sets an empty list of applications.
    final MetadataChangeProposal proposal = buildSetApplicationAssetsProposal(null, resourceUrn);

    try {
      this.entityClient.ingestProposal(opContext, proposal, false);
      log.info("Successfully unset application for resource {}", resourceUrn);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to unset application for resource %s", resourceUrn), e);
    }
  }

  public boolean verifyEntityExists(@Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    try {
      return this.entityClient.exists(opContext, entityUrn);
    } catch (RemoteInvocationException e) {
      throw new RuntimeException("Failed to check entity existence: " + e.getMessage(), e);
    }
  }
}
