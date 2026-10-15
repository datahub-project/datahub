package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME;

import com.datahub.context.OperationFingerprint;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MutationHook;
import com.linkedin.search.template.SearchTemplateProperties;
import com.linkedin.util.Pair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Stamps {@code created} and {@code lastModified} on the aspect that will be written, including a
 * JSON patch once it has been applied to the stored aspect.
 */
@Setter
@Getter
@Accessors(chain = true)
public class SearchTemplateStampHook extends MutationHook {
  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<Pair<ChangeMCP, Boolean>> writeMutation(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPS,
      @Nonnull RetrieverContext retrieverContext) {
    // AspectsBatch drains this stream with count(), which does not evaluate map side effects
    // when the source size is known. Stamp before the stream is built.
    List<Pair<ChangeMCP, Boolean>> results = new ArrayList<>(changeMCPS.size());
    for (ChangeMCP item : changeMCPS) {
      if (!(item.getRecordTemplate() instanceof SearchTemplateProperties properties)
          || !SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME.equals(item.getAspectName())) {
        results.add(Pair.of(item, false));
        continue;
      }
      SearchTemplateStamps.apply(item, properties, read(operationContext, item, retrieverContext));
      results.add(Pair.of(item, true));
    }
    return results.stream();
  }

  @Nullable
  private static SearchTemplateProperties read(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull ChangeMCP item,
      @Nonnull RetrieverContext retrieverContext) {
    Aspect aspect =
        retrieverContext
            .getAspectRetriever()
            .getLatestAspectObject(
                operationContext, item.getUrn(), SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME);
    if (aspect == null || aspect.data() == null) {
      return null;
    }
    return new SearchTemplateProperties(aspect.data());
  }
}
