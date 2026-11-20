package com.linkedin.metadata.aspect.validation;

import static com.linkedin.metadata.Constants.DATA_PRODUCT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.validation.AspectPayloadValidator;
import com.linkedin.metadata.aspect.plugins.validation.AspectValidationException;
import com.linkedin.metadata.aspect.plugins.validation.ValidationExceptionCollection;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Requires a Data Product's parent to be an existing Data Product that is neither itself nor one of
 * its descendants, across all metadata write APIs. Only a changed parent is checked, so edits to a
 * product whose stored parent was later deleted still succeed.
 */
@Setter
@Getter
@Accessors(chain = true)
public class DataProductPropertiesParentValidator extends AspectPayloadValidator {

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<AspectValidationException> validateProposedAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<? extends BatchItem> mcpItems,
      @Nonnull RetrieverContext retrieverContext) {
    // Patches are resolved and the stored parent is known only at pre-commit.
    return Stream.empty();
  }

  @Override
  protected Stream<AspectValidationException> validatePreCommitAspects(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull RetrieverContext retrieverContext) {
    ValidationExceptionCollection exceptions = ValidationExceptionCollection.newCollection();
    Map<ChangeMCP, Urn> changedParents = new HashMap<>();

    for (ChangeMCP item : changeMCPs) {
      Urn parent = parentOf(item.getAspect(DataProductProperties.class));
      if (parent == null || parent.equals(parentOf(previousProperties(item)))) {
        continue;
      }
      if (!DATA_PRODUCT_ENTITY_NAME.equals(parent.getEntityType())) {
        exceptions.addException(
            AspectValidationException.forItem(item, "Parent must be a Data Product."));
      } else if (parent.equals(item.getUrn())) {
        exceptions.addException(
            AspectValidationException.forItem(item, "A Data Product cannot be its own parent."));
      } else {
        changedParents.put(item, parent);
      }
    }

    if (changedParents.isEmpty()) {
      return exceptions.streamAllExceptions();
    }

    // A parent written in the same batch counts as existing, so a parent and child can be
    // created together.
    Set<Urn> batchUrns = changeMCPs.stream().map(ChangeMCP::getUrn).collect(Collectors.toSet());
    Set<Urn> toCheck =
        changedParents.values().stream()
            .filter(parent -> !batchUrns.contains(parent))
            .collect(Collectors.toSet());
    Map<Urn, Boolean> exists =
        toCheck.isEmpty()
            ? Map.of()
            : retrieverContext.getAspectRetriever().entityExists(operationContext, toCheck);

    Set<Urn> rejected =
        exceptions
            .streamAllExceptions()
            .map(AspectValidationException::getEntityUrn)
            .collect(Collectors.toCollection(HashSet::new));
    changedParents.forEach(
        (item, parent) -> {
          if (toCheck.contains(parent) && !Boolean.TRUE.equals(exists.get(parent))) {
            exceptions.addException(
                AspectValidationException.forItem(
                    item, String.format("Parent Data Product %s does not exist.", parent)));
            rejected.add(item.getUrn());
          }
        });

    rejectCycles(
        operationContext, changeMCPs, changedParents, rejected, exceptions, retrieverContext);

    // The MCE consumer commits the rest of a batch when some items fail, so a child whose
    // same-batch parent is rejected here must be rejected too, down the whole chain.
    boolean changed = !rejected.isEmpty();
    while (changed) {
      changed = false;
      for (Map.Entry<ChangeMCP, Urn> entry : changedParents.entrySet()) {
        ChangeMCP item = entry.getKey();
        if (rejected.contains(entry.getValue()) && rejected.add(item.getUrn())) {
          exceptions.addException(
              AspectValidationException.forItem(
                  item, String.format("Parent Data Product %s was rejected.", entry.getValue())));
          changed = true;
        }
      }
    }

    return exceptions.streamAllExceptions();
  }

  /**
   * Rejects a new parent that has the product among its ancestors. The walk reads parents written
   * in the same batch before stored ones, so a batch can both create and break a cycle. Ancestors
   * are read one level at a time for all items together, and a walk stops at an ancestor it has
   * already visited, so a cycle already stored that does not include the product does not loop.
   */
  private static void rejectCycles(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPs,
      @Nonnull Map<ChangeMCP, Urn> changedParents,
      @Nonnull Set<Urn> rejected,
      @Nonnull ValidationExceptionCollection exceptions,
      @Nonnull RetrieverContext retrieverContext) {
    Map<Urn, Optional<Urn>> knownParents = new HashMap<>();
    changeMCPs.forEach(
        item ->
            knownParents.put(
                item.getUrn(),
                Optional.ofNullable(parentOf(item.getAspect(DataProductProperties.class)))));

    Map<ChangeMCP, Urn> cursors = new HashMap<>();
    Map<ChangeMCP, Set<Urn>> visited = new HashMap<>();
    changedParents.forEach(
        (item, parent) -> {
          if (!rejected.contains(item.getUrn())) {
            cursors.put(item, parent);
            visited.put(item, new HashSet<>());
          }
        });

    while (!cursors.isEmpty()) {
      Set<Urn> toFetch =
          cursors.values().stream()
              .filter(urn -> !knownParents.containsKey(urn))
              .collect(Collectors.toSet());
      knownParents.putAll(fetchStoredParents(operationContext, toFetch, retrieverContext));

      Iterator<Map.Entry<ChangeMCP, Urn>> it = cursors.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<ChangeMCP, Urn> entry = it.next();
        ChangeMCP item = entry.getKey();
        Urn ancestor = entry.getValue();
        if (ancestor.equals(item.getUrn())) {
          exceptions.addException(
              AspectValidationException.forItem(
                  item,
                  String.format(
                      "Parent Data Product %s is a descendant of this Data Product.",
                      changedParents.get(item))));
          rejected.add(item.getUrn());
          it.remove();
          continue;
        }
        Optional<Urn> next = knownParents.getOrDefault(ancestor, Optional.empty());
        if (!visited.get(item).add(ancestor) || next.isEmpty()) {
          it.remove();
        } else {
          entry.setValue(next.get());
        }
      }
    }
  }

  @Nonnull
  private static Map<Urn, Optional<Urn>> fetchStoredParents(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Set<Urn> urns,
      @Nonnull RetrieverContext retrieverContext) {
    if (urns.isEmpty()) {
      return Map.of();
    }
    Set<Urn> dataProducts =
        urns.stream()
            .filter(urn -> DATA_PRODUCT_ENTITY_NAME.equals(urn.getEntityType()))
            .collect(Collectors.toSet());
    Map<Urn, Map<String, Aspect>> stored =
        dataProducts.isEmpty()
            ? Map.of()
            : retrieverContext
                .getAspectRetriever()
                .getLatestAspectObjects(
                    operationContext, dataProducts, Set.of(DATA_PRODUCT_PROPERTIES_ASPECT_NAME));

    Map<Urn, Optional<Urn>> parents = new HashMap<>();
    for (Urn urn : urns) {
      Aspect aspect = stored.getOrDefault(urn, Map.of()).get(DATA_PRODUCT_PROPERTIES_ASPECT_NAME);
      parents.put(
          urn,
          Optional.ofNullable(
              aspect == null ? null : parentOf(new DataProductProperties(aspect.data()))));
    }
    return parents;
  }

  @Nullable
  private static DataProductProperties previousProperties(ChangeMCP item) {
    if (item.getPreviousRecordTemplate() == null) {
      return null;
    }
    return item.getPreviousAspect(DataProductProperties.class);
  }

  @Nullable
  private static Urn parentOf(@Nullable DataProductProperties properties) {
    return properties != null && properties.hasParentDataProduct()
        ? properties.getParentDataProduct()
        : null;
  }
}
