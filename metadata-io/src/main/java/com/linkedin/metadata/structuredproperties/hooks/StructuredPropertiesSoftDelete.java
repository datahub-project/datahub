package com.linkedin.metadata.structuredproperties.hooks;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(chain = true)
public class StructuredPropertiesSoftDelete extends MutationHook {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<Pair<ReadItem, Boolean>> readMutation(
      @Nonnull Collection<ReadItem> items, @Nonnull RetrieverContext retrieverContext) {
    Map<Urn, StructuredProperties> entityStructuredPropertiesMap =
        items.stream()
            .filter(i -> i.getRecordTemplate() != null)
            .map(i -> Pair.of(i.getUrn(), i.getAspect(StructuredProperties.class)))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (a, b) -> a));

    // Apply filter
    Map<Urn, Boolean> mutatedEntityStructuredPropertiesMap =
        StructuredPropertyUtils.filterSoftDelete(
            entityStructuredPropertiesMap, retrieverContext.getAspectRetriever());

    return items.stream()
        .map(i -> Pair.of(i, mutatedEntityStructuredPropertiesMap.getOrDefault(i.getUrn(), false)));
  }
}
