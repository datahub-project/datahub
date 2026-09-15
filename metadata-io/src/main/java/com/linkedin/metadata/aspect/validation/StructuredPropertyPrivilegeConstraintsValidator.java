package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.AuthorizationSession;
import com.datahub.context.OperationFingerprint;
import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.entity.ebean.batch.PatchItemImpl;
import com.linkedin.metadata.entity.ebean.batch.ProposedItem;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
public class StructuredPropertyPrivilegeConstraintsValidator
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
    List<AspectValidationException> failures = new ArrayList<>();

    Map<Urn, Aspect> currentAspects = fetchCurrentAspects(operationContext, items, aspectRetriever);

    for (BatchItem item : items) {
      if (STRUCTURED_PROPERTIES_ASPECT_NAME.equals(item.getAspectName())) {
        failures.addAll(
            validateStructuredProperties(
                session, item, aspectRetriever, currentAspects.get(item.getUrn())));
      }
    }
    return failures;
  }

  @Nonnull
  private Map<Urn, Aspect> fetchCurrentAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull List<? extends BatchItem> items,
      @Nonnull AspectRetriever aspectRetriever) {
    Set<Urn> urns =
        items.stream()
            .filter(i -> STRUCTURED_PROPERTIES_ASPECT_NAME.equals(i.getAspectName()))
            .map(BatchItem::getUrn)
            .collect(Collectors.toSet());
    Map<Urn, Aspect> byUrn = new HashMap<>();
    if (urns.isEmpty()) {
      return byUrn;
    }
    AspectRetriever.getLatestAspectObjectsAcrossEntityTypes(
            aspectRetriever, operationContext, urns, Set.of(STRUCTURED_PROPERTIES_ASPECT_NAME))
        .forEach(
            (urn, aspectsByName) -> {
              Aspect aspect = aspectsByName.get(STRUCTURED_PROPERTIES_ASPECT_NAME);
              if (aspect != null) {
                byUrn.put(urn, aspect);
              }
            });
    return byUrn;
  }

  private List<AspectValidationException> validateStructuredProperties(
      AuthorizationSession session,
      BatchItem item,
      AspectRetriever aspectRetriever,
      @Nullable Aspect currentAspect) {
    StructuredProperties currentProps =
        currentAspect == null
            ? null
            : RecordUtils.toRecordTemplate(StructuredProperties.class, currentAspect.data());

    Set<Urn> difference;
    if (ChangeType.DELETE.equals(item.getChangeType())) {
      // The whole aspect is being removed, so every currently-assigned property is being
      // removed too - each one must be authorized, or a constrained user could bypass the
      // per-property removal check by deleting the aspect outright.
      difference =
          currentProps == null ? Collections.emptySet() : valuesByProperty(currentProps).keySet();
    } else {
      StructuredProperties newProps;
      if (ChangeType.PATCH.equals(item.getChangeType()) && item instanceof ProposedItem) {
        ProposedItem proposedItem = (ProposedItem) item;
        PatchItemImpl patchItem =
            PatchItemImpl.builder()
                .build(
                    proposedItem.getMetadataChangeProposal(),
                    proposedItem.getAuditStamp(),
                    aspectRetriever.getEntityRegistry());
        newProps =
            patchItem
                .applyPatch(currentProps, aspectRetriever)
                .getAspect(StructuredProperties.class);
      } else {
        newProps = item.getAspect(StructuredProperties.class);
      }

      if (newProps == null) {
        return Collections.emptyList();
      }
      difference = extractPropertyDifference(newProps, currentProps);
    }

    if (difference.isEmpty()) {
      return Collections.emptyList();
    }
    if (!AuthUtil.isAPIAuthorizedForStructuredPropertyModification(
        session, item.getUrn(), difference)) {
      return List.of(
          AspectValidationException.forItem(
              item, "Unauthorized to modify one or more structured property Urns: " + difference));
    }
    return Collections.emptyList();
  }

  /** A property URN is "changed" if it was added, removed, or its values differ. */
  private Set<Urn> extractPropertyDifference(
      @Nonnull StructuredProperties newProps, @Nullable StructuredProperties currentProps) {
    Map<Urn, PrimitivePropertyValueArray> newValues = valuesByProperty(newProps);
    Map<Urn, PrimitivePropertyValueArray> currentValues =
        currentProps == null ? Collections.emptyMap() : valuesByProperty(currentProps);

    Set<Urn> difference = new HashSet<>();
    newValues.forEach(
        (urn, values) -> {
          PrimitivePropertyValueArray existing = currentValues.get(urn);
          // PrimitivePropertyValueArray.equals() is order-sensitive, so re-sending the same
          // values in a different order is treated as a change. This only triggers an extra
          // authorization check (fail-restrictive), never a bypass.
          if (existing == null || !existing.equals(values)) {
            difference.add(urn);
          }
        });
    currentValues.keySet().stream()
        .filter(urn -> !newValues.containsKey(urn))
        .forEach(difference::add);
    return difference;
  }

  private static Map<Urn, PrimitivePropertyValueArray> valuesByProperty(
      @Nonnull StructuredProperties props) {
    return Optional.ofNullable(props.getProperties())
        .orElse(new StructuredPropertyValueAssignmentArray())
        .stream()
        .collect(
            Collectors.toMap(
                StructuredPropertyValueAssignment::getPropertyUrn,
                a -> Optional.ofNullable(a.getValues()).orElse(new PrimitivePropertyValueArray()),
                (a, b) -> b));
  }
}
