package com.linkedin.metadata.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.application.ApplicationAssociation;
import com.linkedin.application.ApplicationAssociationArray;
import com.linkedin.application.ApplicationKey;
import com.linkedin.application.ApplicationProperties;
import com.linkedin.common.EntityRelationships;
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
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
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
  private final EntityClient _entityClient;
  private final GraphClient
      _graphClient; // Keep for now, though direct relationship manipulation might change.

  public ApplicationService(@Nonnull EntityClient entityClient, @Nonnull GraphClient graphClient) {
    _entityClient = entityClient;
    _graphClient = graphClient;
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
      if (_entityClient.exists(
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
    // Initialize assets array as per PDL: assets: optional array[ApplicationAssociation]
    properties.setAssets(new ApplicationAssociationArray());

    try {
      final Urn entityUrn =
          EntityKeyUtils.convertEntityKeyToUrn(key, Constants.APPLICATION_ENTITY_NAME);
      return UrnUtils.getUrn(
          _entityClient.ingestProposal(
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
          _entityClient.getV2(
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
      return _entityClient.getV2(
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
      _entityClient.ingestProposal(
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

    // 1. Check properties-based assets
    ApplicationProperties properties = getApplicationProperties(opContext, applicationUrn);
    if (properties != null && properties.hasAssets() && !properties.getAssets().isEmpty()) {
      throw new RuntimeException(
          String.format(
              "Cannot delete application %s, it still contains assets (found in properties). Please remove assets before deleting.",
              applicationUrn));
    }

    // 2. Check graph-based relationships for assets
    // Based on PDL: @Relationship = { "/*/destinationUrn": { "name": "ApplicationContains", ... } }
    // This means an outgoing relationship from Application to Asset.
    try {
      EntityRelationships graphRelationships =
          _graphClient.getRelatedEntities(
              applicationUrn.toString(),
              ImmutableSet.of(
                  "ApplicationContains"), // Relationship type - corrected to ImmutableSet
              RelationshipDirection.OUTGOING,
              0, // start
              1, // count (we only need to know if at least one exists)
              opContext.getAuthentication().getActor().toUrnStr());
      if (graphRelationships != null
          && graphRelationships.getRelationships() != null
          && !graphRelationships.getRelationships().isEmpty()) {
        throw new RuntimeException(
            String.format(
                "Cannot delete application %s, it still contains assets (found via graph relationships). Please remove assets before deleting.",
                applicationUrn));
      }
    } catch (Exception e) {
      log.error(
          String.format(
              "Error checking graph relationships for application %s during delete: %s",
              applicationUrn, e.getMessage()),
          e);

      throw new RuntimeException(
          String.format(
              "Failed to verify graph relationships for application %s during delete.",
              applicationUrn),
          e);
    }

    // 3. Proceed with deletion if both checks pass
    try {
      _entityClient.deleteEntity(opContext, applicationUrn);
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

    ApplicationProperties properties = getApplicationProperties(opContext, applicationUrn);
    if (properties == null) {
      log.error(
          "Application properties not found for {}, cannot add assets. Create the application first.",
          applicationUrn);
      throw new IllegalArgumentException(
          "Application properties not found for "
              + applicationUrn
              + ". Create the application first.");
    }

    ApplicationAssociationArray currentAssets =
        properties.hasAssets() ? properties.getAssets() : new ApplicationAssociationArray();
    boolean changed = false;

    for (Urn resourceUrn : resourceUrns) {
      boolean exists = false;
      for (ApplicationAssociation existing : currentAssets) {
        if (existing.getDestinationUrn().equals(resourceUrn)) {
          exists = true;
          break;
        }
      }

      if (!exists) {
        ApplicationAssociation newAssociation = new ApplicationAssociation();
        newAssociation.setDestinationUrn(resourceUrn);
        currentAssets.add(newAssociation);
        changed = true;
      }
    }

    if (changed) {
      properties.setAssets(currentAssets);
      try {
        _entityClient.ingestProposal(
            opContext,
            AspectUtils.buildMetadataChangeProposal(
                applicationUrn, Constants.APPLICATION_PROPERTIES_ASPECT_NAME, properties),
            false);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format(
                "Failed to batch set application assets for %s with resources %s!",
                applicationUrn, resourceUrns),
            e);
      }
    }
  }

  public void batchRemoveApplicationAssets(
      @Nonnull OperationContext opContext,
      @Nonnull Urn applicationUrn,
      @Nonnull List<Urn> resourceUrnsToRemove,
      @Nonnull Urn actorUrn) { // actorUrn for audit stamp
    Objects.requireNonNull(applicationUrn, "applicationUrn must not be null");
    Objects.requireNonNull(resourceUrnsToRemove, "resourceUrnsToRemove must not be null");

    ApplicationProperties properties = getApplicationProperties(opContext, applicationUrn);
    if (properties == null || !properties.hasAssets() || properties.getAssets().isEmpty()) {
      log.info(
          "Application {} has no assets or properties not found, nothing to remove.",
          applicationUrn);
      return; // No assets to remove
    }

    ApplicationAssociationArray currentAssets = properties.getAssets();

    if (currentAssets == null) {
      log.info("Application {} has no assets, nothing to remove.", applicationUrn);
      return;
    }

    List<ApplicationAssociation> associationsToRemove = new ArrayList<>();
    for (ApplicationAssociation asset : currentAssets) {
      if (resourceUrnsToRemove.contains(asset.getDestinationUrn())) {
        associationsToRemove.add(asset);
      }
    }

    if (!associationsToRemove.isEmpty()) {
      for (ApplicationAssociation toRemove : associationsToRemove) {
        currentAssets.remove(toRemove);
      }
      properties.setAssets(currentAssets);

      try {
        _entityClient.ingestProposal(
            opContext,
            AspectUtils.buildMetadataChangeProposal(
                applicationUrn, Constants.APPLICATION_PROPERTIES_ASPECT_NAME, properties),
            false);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to batch remove assets from application %s", applicationUrn), e);
      }
    }
  }

  public void unsetApplication(
      @Nonnull OperationContext opContext, @Nonnull Urn resourceUrn, @Nonnull Urn actorUrn) {
    Objects.requireNonNull(resourceUrn, "resourceUrn must not be null");
    Objects.requireNonNull(actorUrn, "actorUrn must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");

    // Find the parent Application URN from the resourceUrn by querying the graph
    // We are looking for an INCOMING "ApplicationContains" relationship to the resourceUrn.
    // The source of this relationship will be the Application.
    Urn applicationUrn = null;
    try {
      final EntityRelationships relationships =
          _graphClient.getRelatedEntities(
              resourceUrn.toString(),
              ImmutableSet.of("ApplicationContains"), // Relationship type
              RelationshipDirection
                  .INCOMING, // Direction: From Application TO Resource, so INCOMING for Resource
              0, // start
              1, // count (expecting only one Application to contain a given resource)
              opContext.getAuthentication().getActor().toUrnStr());

      if (relationships != null
          && relationships.getRelationships() != null
          && !relationships.getRelationships().isEmpty()) {
        if (relationships.getRelationships().size() > 1) {
          log.warn(
              "Resource {} is part of multiple applications via 'ApplicationContains' relationship. Proceeding with the first one found: {}. This might indicate a data modeling issue.",
              resourceUrn,
              relationships.getRelationships().get(0).getEntity());
        }
        applicationUrn = relationships.getRelationships().get(0).getEntity();
      } else {
        log.info(
            "Resource {} is not part of any application via 'ApplicationContains' relationship. Nothing to unset.",
            resourceUrn);
        return; // Nothing to do if the resource is not part of any application
      }
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to find parent application for resource %s. Error: %s",
              resourceUrn, e.getMessage()),
          e);
      throw new RuntimeException(
          String.format("Failed to find parent application for resource %s", resourceUrn), e);
    }

    if (applicationUrn != null) {
      // Now that we have the application URN, call batchRemoveApplicationAssets
      batchRemoveApplicationAssets(
          opContext, applicationUrn, ImmutableList.of(resourceUrn), actorUrn);
      log.info(
          "Successfully unset application for resource {} from application {}",
          resourceUrn,
          applicationUrn);
    } else {
      // This case should ideally be caught by the graph query result, but as a safeguard:
      log.warn(
          "Could not determine application to unset for resource {}. No action taken.",
          resourceUrn);
    }
  }

  public boolean verifyEntityExists(@Nonnull OperationContext opContext, @Nonnull Urn entityUrn) {
    try {
      return _entityClient.exists(opContext, entityUrn);
    } catch (RemoteInvocationException e) {
      throw new RuntimeException("Failed to check entity existence: " + e.getMessage(), e);
    }
  }
}
