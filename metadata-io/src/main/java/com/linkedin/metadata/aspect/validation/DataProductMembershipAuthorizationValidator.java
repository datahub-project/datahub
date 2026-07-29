package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;

import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.authorization.EntityAspectAuthorizationUtils;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Getter
@Accessors(chain = true)
public class DataProductMembershipAuthorizationValidator
    extends AbstractAspectAuthorizationValidator {

  @Nonnull private AspectPluginConfig config;

  @Override
  protected List<AspectValidationException> validateItems(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<? extends BatchItem> items,
      @Nonnull Collection<? extends BatchItem> batchItems,
      @Nonnull RetrieverContext retrieverContext,
      @Nonnull AuthorizationSession session) {

    AspectRetriever aspectRetriever = retrieverContext.getAspectRetriever();
    List<BatchItem> membershipChanges = new ArrayList<>();
    Map<Urn, Set<Urn>> changedAssetsByProduct = new HashMap<>();
    Set<Urn> nameChangeProducts = new HashSet<>();

    Set<Urn> dataProductUrns = items.stream().map(BatchItem::getUrn).collect(Collectors.toSet());
    Map<Urn, Map<String, Aspect>> currentPropertiesMap =
        aspectRetriever.getLatestAspectObjects(
            operationContext, dataProductUrns, Set.of(DATA_PRODUCT_PROPERTIES_ASPECT_NAME));

    for (BatchItem item : items) {
      Aspect currentAspect =
          currentPropertiesMap
              .getOrDefault(item.getUrn(), Map.of())
              .get(DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
      DataProductProperties proposed =
          resolveProposedProperties(item, aspectRetriever, currentAspect);
      DataProductProperties current = toProperties(currentAspect);
      Set<Urn> changedAssets = extractChangedAssetUrns(current, proposed);
      if (!changedAssets.isEmpty()) {
        membershipChanges.add(item);
        changedAssetsByProduct.put(item.getUrn(), changedAssets);
      }
      if (hasNameChange(current, proposed)) {
        nameChangeProducts.add(item.getUrn());
      }
    }

    if (membershipChanges.isEmpty() && nameChangeProducts.isEmpty()) {
      return List.of();
    }

    Map<Urn, Aspect> proposedProductDomainsAspects =
        extractProposedProductDomainsAspects(batchItems);
    List<AspectValidationException> failures = new ArrayList<>();

    if (!membershipChanges.isEmpty()) {
      Set<Urn> unauthorizedMembership =
          EntityAspectAuthorizationUtils.filterUnauthorizedToManageDataProductMembership(
              operationContext,
              session,
              aspectRetriever,
              changedAssetsByProduct,
              proposedProductDomainsAspects);

      for (BatchItem item : membershipChanges) {
        if (unauthorizedMembership.contains(item.getUrn())) {
          failures.add(
              authFailure(
                  item,
                  "Unauthorized to modify dataProductProperties.assets on data product: "
                      + item.getUrn()));
        }
      }
    }

    if (!nameChangeProducts.isEmpty()) {
      Set<Urn> unauthorizedRenames =
          EntityAspectAuthorizationUtils.filterUnauthorizedToRenameDataProduct(
              operationContext,
              session,
              aspectRetriever,
              nameChangeProducts,
              proposedProductDomainsAspects);

      for (BatchItem item : items) {
        if (nameChangeProducts.contains(item.getUrn())
            && unauthorizedRenames.contains(item.getUrn())) {
          failures.add(
              authFailure(
                  item,
                  "Unauthorized to modify dataProductProperties.name on data product: "
                      + item.getUrn()));
        }
      }
    }

    return failures;
  }

  @Nonnull
  private static Map<Urn, Aspect> extractProposedProductDomainsAspects(
      @Nonnull Collection<? extends BatchItem> batchItems) {
    Map<Urn, Aspect> proposedProductDomainsAspects = new HashMap<>();
    for (BatchItem item : batchItems) {
      if (DOMAINS_ASPECT_NAME.equals(item.getAspectName()) && item.getRecordTemplate() != null) {
        proposedProductDomainsAspects.put(
            item.getUrn(), new Aspect(item.getRecordTemplate().data()));
      }
    }
    return proposedProductDomainsAspects;
  }

  @Nonnull
  private static Set<Urn> extractChangedAssetUrns(
      @Nullable DataProductProperties current, @Nullable DataProductProperties proposed) {
    Set<String> currentUrns = extractAssetUrns(current);
    Set<String> proposedUrns = extractAssetUrns(proposed);
    Set<String> symmetricDiff = new HashSet<>(currentUrns);
    symmetricDiff.addAll(proposedUrns);
    Set<String> intersection = new HashSet<>(currentUrns);
    intersection.retainAll(proposedUrns);
    symmetricDiff.removeAll(intersection);
    return symmetricDiff.stream().map(UrnUtils::getUrn).collect(Collectors.toSet());
  }

  @Nonnull
  private static Set<String> extractAssetUrns(@Nullable DataProductProperties properties) {
    if (properties == null || !properties.hasAssets()) {
      return Set.of();
    }
    Set<String> urns = new HashSet<>();
    for (DataProductAssociation association : properties.getAssets()) {
      if (association.hasDestinationUrn()) {
        urns.add(association.getDestinationUrn().toString());
      }
    }
    return urns;
  }

  private static boolean hasNameChange(
      @Nullable DataProductProperties current, @Nullable DataProductProperties proposed) {
    if (proposed == null || !proposed.hasName()) {
      return false;
    }
    if (current == null || !current.hasName()) {
      return true;
    }
    return !proposed.getName().equals(current.getName());
  }

  @Nullable
  private static DataProductProperties toProperties(@Nullable Aspect aspect) {
    if (aspect == null) {
      return null;
    }
    return RecordUtils.toRecordTemplate(DataProductProperties.class, aspect.data());
  }

  @Nullable
  private static DataProductProperties resolveProposedProperties(
      @Nonnull BatchItem item,
      @Nonnull AspectRetriever aspectRetriever,
      @Nullable Aspect currentAspect) {
    DataProductProperties current = toProperties(currentAspect);
    if (ChangeType.PATCH.equals(item.getChangeType()) && item instanceof ProposedItem) {
      ProposedItem proposedItem = (ProposedItem) item;
      PatchItemImpl patchItem =
          PatchItemImpl.builder()
              .build(
                  proposedItem.getMetadataChangeProposal(),
                  proposedItem.getAuditStamp(),
                  aspectRetriever.getEntityRegistry());
      return patchItem.applyPatch(current, aspectRetriever).getAspect(DataProductProperties.class);
    }
    return item.getAspect(DataProductProperties.class);
  }
}
