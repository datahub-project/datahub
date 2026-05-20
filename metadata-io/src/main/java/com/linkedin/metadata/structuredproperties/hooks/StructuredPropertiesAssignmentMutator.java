package com.linkedin.metadata.structuredproperties.hooks;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * Mutates {@code structuredProperties} assignments on read and write: hides soft-deleted
 * definitions on read, and optionally drops assignments for missing definitions on write.
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class StructuredPropertiesAssignmentMutator extends MutationHook {
  @Nonnull private AspectPluginConfig config;
  private boolean dropMissingPropertyValuesWithWarning = false;

  @Override
  protected Stream<Pair<ReadItem, Boolean>> readMutation(
      @Nonnull Collection<ReadItem> items, @Nonnull RetrieverContext retrieverContext) {
    Map<Urn, StructuredProperties> entityStructuredPropertiesMap =
        items.stream()
            .filter(i -> i.getRecordTemplate() != null)
            .map(i -> Pair.of(i.getUrn(), i.getAspect(StructuredProperties.class)))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    Map<Urn, Boolean> mutatedEntityStructuredPropertiesMap =
        StructuredPropertyUtils.filterSoftDelete(
            entityStructuredPropertiesMap, retrieverContext.getAspectRetriever());

    return items.stream()
        .map(i -> Pair.of(i, mutatedEntityStructuredPropertiesMap.getOrDefault(i.getUrn(), false)));
  }

  @Override
  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull Collection<ChangeMCP> changeMCPS, @Nonnull RetrieverContext retrieverContext) {
    if (!dropMissingPropertyValuesWithWarning) {
      return changeMCPS.stream().map(i -> Pair.of(i, false));
    }

    return changeMCPS.stream()
        .map(item -> Pair.of(item, dropMissingPropertyAssignments(item, retrieverContext)));
  }

  private boolean dropMissingPropertyAssignments(
      @Nonnull ChangeMCP item, @Nonnull RetrieverContext retrieverContext) {
    if (!STRUCTURED_PROPERTIES_ASPECT_NAME.equals(item.getAspectName())
        || item.getRecordTemplate() == null) {
      return false;
    }

    final StructuredProperties proposed = item.getAspect(StructuredProperties.class);
    final int assignmentCountBefore =
        proposed.hasProperties() ? proposed.getProperties().size() : 0;
    if (assignmentCountBefore == 0) {
      return false;
    }

    final Pair<StructuredProperties, Set<Urn>> filtered =
        StructuredPropertyUtils.filterMissingPropertyDefinitions(
            proposed, retrieverContext.getAspectRetriever());

    if (filtered.getSecond().isEmpty()) {
      return false;
    }

    filtered
        .getSecond()
        .forEach(
            propertyUrn ->
                log.warn(
                    "Dropping structured property assignment for non-existent definition {} on {}",
                    propertyUrn,
                    item.getUrn()));

    final StructuredPropertyValueAssignmentArray remaining = filtered.getFirst().getProperties();
    if (remaining == null || remaining.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Structured properties write rejected for %s: no valid property assignments remain"
                  + " after removing values for non-existent properties: %s",
              item.getUrn(), filtered.getSecond()));
    }

    item.getAspect(StructuredProperties.class).setProperties(filtered.getFirst().getProperties());
    return true;
  }
}
