package com.linkedin.metadata.service;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.SetMode;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductKey;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.EntityKeyUtils;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is used to permit easy CRUD operations on a DataProduct
 *
 * <p>Note that no Authorization is performed within the service. The expectation is that the caller
 * has already verified the permissions of the active Actor.
 */
@Slf4j
public class DataProductService {
  private final EntityClient _entityClient;
  private final GraphClient _graphClient;

  public DataProductService(@Nonnull EntityClient entityClient, @Nonnull GraphClient graphClient) {
    _entityClient = entityClient;
    _graphClient = graphClient;
  }

  /**
   * Creates a new Data Product.
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation.
   *
   * @param name optional name of the DataProduct
   * @param description optional description of the DataProduct
   * @return the urn of the newly created DataProduct
   */
  public Urn createDataProduct(
      @Nullable String name, @Nullable String description, @Nonnull Authentication authentication) {

    // 1. Generate a unique id for the new DataProduct.
    final DataProductKey key = new DataProductKey();
    key.setId(UUID.randomUUID().toString());

    // 2. Create a new instance of DataProductProperties
    final DataProductProperties properties = new DataProductProperties();
    properties.setName(name, SetMode.IGNORE_NULL);
    properties.setDescription(description, SetMode.IGNORE_NULL);

    // 3. Write the new dataProduct to GMS, return the new URN.
    try {
      final Urn entityUrn =
          EntityKeyUtils.convertEntityKeyToUrn(key, Constants.DATA_PRODUCT_ENTITY_NAME);
      return UrnUtils.getUrn(
          _entityClient.ingestProposal(
              AspectUtils.buildMetadataChangeProposal(
                  entityUrn, Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME, properties),
              authentication,
              false));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create DataProduct", e);
    }
  }

  /**
   * Updates an existing DataProduct. If a provided field is null, the previous value will be kept.
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation.
   *
   * @param urn the urn of the DataProduct
   * @param name optional name of the DataProduct
   * @param description optional description of the DataProduct
   * @param authentication the current authentication
   */
  public Urn updateDataProduct(
      @Nonnull Urn urn,
      @Nullable String name,
      @Nullable String description,
      @Nonnull Authentication authentication) {
    Objects.requireNonNull(urn, "urn must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");

    // 1. Check whether the DataProduct exists
    DataProductProperties properties = getDataProductProperties(urn, authentication);

    if (properties == null) {
      throw new IllegalArgumentException(
          String.format(
              "Failed to update DataProduct. DataProduct with urn %s does not exist.", urn));
    }

    // 2. Apply changes to existing DataProduct
    if (name != null) {
      properties.setName(name);
    }
    if (description != null) {
      properties.setDescription(description);
    }

    // 3. Write changes to GMS
    try {
      return UrnUtils.getUrn(
          _entityClient.ingestProposal(
              AspectUtils.buildMetadataChangeProposal(
                  urn, Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME, properties),
              authentication,
              false));
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to update View with urn %s", urn), e);
    }
  }

  /**
   * @param dataProductUrn the urn of the DataProduct
   * @param authentication the authentication to use
   * @return an instance of {@link DataProductProperties} for the DataProduct, null if it does not
   *     exist.
   */
  @Nullable
  public DataProductProperties getDataProductProperties(
      @Nonnull final Urn dataProductUrn, @Nonnull final Authentication authentication) {
    Objects.requireNonNull(dataProductUrn, "dataProductUrn must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");
    final EntityResponse response = getDataProductEntityResponse(dataProductUrn, authentication);
    if (response != null
        && response.getAspects().containsKey(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME)) {
      return new DataProductProperties(
          response
              .getAspects()
              .get(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME)
              .getValue()
              .data());
    }
    // No aspect found
    return null;
  }

  /**
   * @param dataProductUrn the urn of the DataProduct
   * @param authentication the authentication to use
   * @return an instance of {@link DataProductProperties} for the DataProduct, null if it does not
   *     exist.
   */
  @Nullable
  public Domains getDataProductDomains(
      @Nonnull final Urn dataProductUrn, @Nonnull final Authentication authentication) {
    Objects.requireNonNull(dataProductUrn, "dataProductUrn must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");
    try {
      final EntityResponse response =
          _entityClient.getV2(
              Constants.DATA_PRODUCT_ENTITY_NAME,
              dataProductUrn,
              ImmutableSet.of(Constants.DOMAINS_ASPECT_NAME),
              authentication);
      if (response != null && response.getAspects().containsKey(Constants.DOMAINS_ASPECT_NAME)) {
        return new Domains(
            response.getAspects().get(Constants.DOMAINS_ASPECT_NAME).getValue().data());
      }
      // No aspect found
      return null;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve DataProduct with urn %s", dataProductUrn), e);
    }
  }

  /**
   * Returns an instance of {@link EntityResponse} for the specified DataProduct urn, or null if one
   * cannot be found.
   *
   * @param dataProductUrn the urn of the DataProduct
   * @param authentication the authentication to use
   * @return an instance of {@link EntityResponse} for the DataProduct, null if it does not exist.
   */
  @Nullable
  public EntityResponse getDataProductEntityResponse(
      @Nonnull final Urn dataProductUrn, @Nonnull final Authentication authentication) {
    Objects.requireNonNull(dataProductUrn, "dataProductUrn must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");
    try {
      return _entityClient.getV2(
          Constants.DATA_PRODUCT_ENTITY_NAME,
          dataProductUrn,
          ImmutableSet.of(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME),
          authentication);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve DataProduct with urn %s", dataProductUrn), e);
    }
  }

  /** Sets a given domain on a given Data Product. */
  public void setDomain(
      @Nonnull final Urn dataProductUrn,
      @Nonnull final Urn domainUrn,
      @Nonnull final Authentication authentication) {
    try {
      Domains domains = new Domains();

      EntityResponse entityResponse =
          _entityClient.getV2(
              Constants.DATA_PRODUCT_ENTITY_NAME,
              dataProductUrn,
              ImmutableSet.of(Constants.DOMAINS_ASPECT_NAME),
              authentication);

      if (entityResponse != null
          && entityResponse.getAspects().containsKey(Constants.DOMAINS_ASPECT_NAME)) {
        DataMap dataMap =
            entityResponse.getAspects().get(Constants.DOMAINS_ASPECT_NAME).getValue().data();
        domains = new Domains(dataMap);
      }

      final UrnArray newDomains = new UrnArray();
      newDomains.add(domainUrn);
      domains.setDomains(newDomains);
      _entityClient.ingestProposal(
          AspectUtils.buildMetadataChangeProposal(
              dataProductUrn, Constants.DOMAINS_ASPECT_NAME, domains),
          authentication,
          false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to set domain for DataProduct with urn %s", dataProductUrn), e);
    }
  }

  /**
   * Deletes an existing DataProduct with a specific urn.
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation
   *
   * <p>If the DataProduct does not exist, no exception will be thrown.
   *
   * @param dataProductUrn the urn of the DataProduct
   * @param authentication the current authentication
   */
  public void deleteDataProduct(
      @Nonnull Urn dataProductUrn, @Nonnull Authentication authentication) {
    try {
      _entityClient.deleteEntity(
          Objects.requireNonNull(dataProductUrn, "dataProductUrn must not be null"),
          Objects.requireNonNull(authentication, "authentication must not be null"));

      // Asynchronously Delete all references to the entity (to return quickly)
      CompletableFuture.runAsync(
          () -> {
            try {
              _entityClient.deleteEntityReferences(dataProductUrn, authentication);
            } catch (Exception e) {
              log.error(
                  String.format(
                      "Caught exception while attempting to clear all entity references for DataProduct with urn %s",
                      dataProductUrn),
                  e);
            }
          });

    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to delete DataProduct with urn %s", dataProductUrn), e);
    }
  }

  /**
   * Sets a Data Product for a given list of entities.
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation
   *
   * @param dataProductUrn the urn of the Data Product to set - null if removing Data Product
   * @param resourceUrns the urns of the entities to add the Data Product to
   * @param authentication the current authentication
   */
  public void batchSetDataProduct(
      @Nonnull Urn dataProductUrn,
      @Nonnull List<Urn> resourceUrns,
      @Nonnull Authentication authentication,
      @Nonnull Urn actorUrn) {
    try {
      DataProductProperties dataProductProperties =
          getDataProductProperties(dataProductUrn, authentication);
      if (dataProductProperties == null) {
        throw new RuntimeException(
            "Failed to batch set data product as data product does not exist");
      }

      DataProductAssociationArray dataProductAssociations = new DataProductAssociationArray();
      if (dataProductProperties.hasAssets()) {
        dataProductAssociations = dataProductProperties.getAssets();
      }

      List<Urn> existingResourceUrns =
          dataProductAssociations.stream()
              .map(DataProductAssociation::getDestinationUrn)
              .collect(Collectors.toList());
      List<Urn> newResourceUrns =
          resourceUrns.stream()
              .filter(urn -> !existingResourceUrns.contains(urn))
              .collect(Collectors.toList());

      // unset existing data product on resources first as we only allow one data product on an
      // entity at a time
      for (Urn resourceUrn : resourceUrns) {
        unsetDataProduct(resourceUrn, authentication, actorUrn);
      }

      AuditStamp nowAuditStamp =
          new AuditStamp().setTime(System.currentTimeMillis()).setActor(actorUrn);
      for (Urn resourceUrn : newResourceUrns) {
        DataProductAssociation association = new DataProductAssociation();
        association.setDestinationUrn(resourceUrn);
        association.setCreated(nowAuditStamp);
        association.setLastModified(nowAuditStamp);
        dataProductAssociations.add(association);
      }

      dataProductProperties.setAssets(dataProductAssociations);
      _entityClient.ingestProposal(
          AspectUtils.buildMetadataChangeProposal(
              dataProductUrn, Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME, dataProductProperties),
          authentication,
          false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update assets for %s", dataProductUrn), e);
    }
  }

  /**
   * Unsets a Data Product for a given entity. Remove this entity from its data product(s).
   *
   * <p>Note that this method does not do authorization validation. It is assumed that users of this
   * class have already authorized the operation
   *
   * @param resourceUrn the urn of the entity to remove the Data Product from
   * @param authentication the current authentication
   */
  public void unsetDataProduct(
      @Nonnull Urn resourceUrn, @Nonnull Authentication authentication, @Nonnull Urn actorUrn) {
    try {
      List<String> relationshipTypes = ImmutableList.of("DataProductContains");
      EntityRelationships relationships =
          _graphClient.getRelatedEntities(
              resourceUrn.toString(),
              relationshipTypes,
              RelationshipDirection.INCOMING,
              0,
              10, // should never be more than 1 as long as we only allow one
              actorUrn.toString());

      if (relationships.hasRelationships() && relationships.getRelationships().size() > 0) {
        relationships
            .getRelationships()
            .forEach(
                relationship -> {
                  Urn dataProductUrn = relationship.getEntity();
                  removeEntityFromDataProduct(dataProductUrn, resourceUrn, authentication);
                });
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to unset data product for %s", resourceUrn), e);
    }
  }

  private void removeEntityFromDataProduct(
      @Nonnull Urn dataProductUrn,
      @Nonnull Urn resourceUrn,
      @Nonnull Authentication authentication) {
    try {
      DataProductProperties dataProductProperties =
          getDataProductProperties(dataProductUrn, authentication);
      if (dataProductProperties == null) {
        throw new RuntimeException("Failed to unset data product as data product does not exist");
      }

      DataProductAssociationArray dataProductAssociations = new DataProductAssociationArray();
      if (dataProductProperties.hasAssets()) {
        dataProductAssociations = dataProductProperties.getAssets();
      }

      // get all associations except for the one for the given resourceUrn
      DataProductAssociationArray finalAssociations = new DataProductAssociationArray();
      for (DataProductAssociation association : dataProductAssociations) {
        if (!association.getDestinationUrn().equals(resourceUrn)) {
          finalAssociations.add(association);
        }
      }

      dataProductProperties.setAssets(finalAssociations);
      _entityClient.ingestProposal(
          AspectUtils.buildMetadataChangeProposal(
              dataProductUrn, Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME, dataProductProperties),
          authentication,
          false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to unset data product for %s", resourceUrn), e);
    }
  }

  public boolean verifyEntityExists(
      @Nonnull Urn entityUrn, @Nonnull Authentication authentication) {
    try {
      return _entityClient.exists(entityUrn, authentication);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to determine if entity with urn %s exists", entityUrn), e);
    }
  }
}
