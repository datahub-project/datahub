package com.linkedin.metadata.aspect.hooks;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.ReadItem;
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

public class StructuredPropertiesSoftDelete extends MutationHook {
  public StructuredPropertiesSoftDelete(AspectPluginConfig aspectPluginConfig) {
    super(aspectPluginConfig);
  }

  @Override
  protected Stream<Pair<ReadItem, Boolean>> readMutation(
      @Nonnull Collection<ReadItem> items, @Nonnull AspectRetriever aspectRetriever) {
    Map<Urn, StructuredProperties> entityStructuredPropertiesMap =
        items.stream()
            .filter(i -> i.getRecordTemplate() != null)
            .map(i -> Pair.of(i.getUrn(), i.getAspect(StructuredProperties.class)))
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    // Apply filter
    Map<Urn, Boolean> mutatedEntityStructuredPropertiesMap =
        StructuredPropertyUtils.filterSoftDelete(entityStructuredPropertiesMap, aspectRetriever);

    return items.stream()
        .map(i -> Pair.of(i, mutatedEntityStructuredPropertiesMap.getOrDefault(i.getUrn(), false)));
  }
}
