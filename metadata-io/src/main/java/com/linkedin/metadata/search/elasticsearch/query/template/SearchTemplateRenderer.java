package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.query.v3.SearchV3QueryBuilder;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.QueryBuilder;

/** Delegates rendering to {@link SearchV3QueryBuilder} so a ladder rung can select its URN. */
public class SearchTemplateRenderer {
  private final SearchV3QueryBuilder queryBuilder;

  public SearchTemplateRenderer(@Nonnull SearchV3QueryBuilder queryBuilder) {
    this.queryBuilder = queryBuilder;
  }

  public SearchTemplateRenderer(
      @Nonnull SearchTemplateLoader loader,
      @Nonnull SearchConfiguration searchConfiguration,
      @Nullable CustomSearchConfiguration customSearchConfiguration) {
    this(new SearchV3QueryBuilder(loader, searchConfiguration, customSearchConfiguration));
  }

  @Nonnull
  public SearchV3QueryBuilder queryBuilder() {
    return queryBuilder;
  }

  @Nonnull
  public QueryBuilder render(
      @Nonnull OperationContext opContext,
      @Nonnull List<EntitySpec> entitySpecs,
      @Nonnull String query,
      boolean fulltext) {
    return queryBuilder.render(opContext, entitySpecs, query, fulltext, null, false).query();
  }
}
