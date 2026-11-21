package com.linkedin.metadata.search.elasticsearch.query.template;

import javax.annotation.Nonnull;

/** A component that owns the rung instead of reordering an engine page. */
public interface OwnRungComponent extends SearchTemplateComponent {
  @Nonnull
  SearchTemplateComponentPage execute(@Nonnull SearchTemplateComponentRequest request);
}
