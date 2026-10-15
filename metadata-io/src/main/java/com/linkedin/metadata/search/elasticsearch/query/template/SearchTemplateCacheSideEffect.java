package com.linkedin.metadata.search.elasticsearch.query.template;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import java.util.Collection;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * Drops the template cache after the write commits. Doing it in the validator races the commit: a
 * search can reload the previous aspect and cache it again.
 */
@Setter
@Getter
@Accessors(chain = true)
public class SearchTemplateCacheSideEffect extends MCPSideEffect {
  @Nonnull private AspectPluginConfig config;
  @Nonnull private final SearchTemplateLoader loader;

  public SearchTemplateCacheSideEffect(@Nonnull SearchTemplateLoader loader) {
    this.loader = loader;
  }

  @Override
  protected Stream<ChangeMCP> applyMCPSideEffect(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<ChangeMCP> changeMCPS,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.empty();
  }

  @Override
  protected Stream<MCPItem> postMCPSideEffect(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull Collection<MCLItem> mclItems,
      @Nonnull RetrieverContext retrieverContext) {
    for (MCLItem item : mclItems) {
      loader.invalidate(item.getUrn().toString());
    }
    return Stream.empty();
  }
}
