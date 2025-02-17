package com.linkedin.metadata.search.elasticsearch.query.request;

import com.google.common.annotations.VisibleForTesting;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;

public abstract class BaseRequestHandler {

  /**
   * Provide the fields which are queried by default
   *
   * @return collection of field names
   */
  protected abstract Collection<String> getDefaultQueryFieldNames();

  protected abstract Collection<String> getValidQueryFieldNames();

  protected abstract Stream<String> highlightFieldExpansion(
      @Nonnull OperationContext opContext, @Nonnull String fieldName);

  @VisibleForTesting
  public HighlightBuilder getDefaultHighlights(@Nonnull OperationContext opContext) {
    return getHighlights(opContext, null);
  }

  @VisibleForTesting
  public HighlightBuilder getHighlights(
      @Nonnull OperationContext opContext, @Nullable Collection<String> fieldsToHighlight) {
    HighlightBuilder highlightBuilder =
        new HighlightBuilder()
            // Don't set tags to get the original field value
            .preTags("")
            .postTags("")
            .numOfFragments(1);

    final Stream<String> fieldStream;
    if (fieldsToHighlight == null || fieldsToHighlight.isEmpty()) {
      fieldStream = getDefaultQueryFieldNames().stream();
    } else {
      // filter for valid names
      fieldStream =
          fieldsToHighlight.stream()
              .filter(Objects::nonNull)
              .filter(fieldName -> !fieldName.isEmpty())
              .filter(getValidQueryFieldNames()::contains);
    }

    fieldStream
        .flatMap(fieldName -> highlightFieldExpansion(opContext, fieldName))
        .distinct()
        .map(HighlightBuilder.Field::new)
        .map(
            field -> {
              if (field.name().endsWith("ngram")) {
                return field.requireFieldMatch(false).noMatchSize(200);
              } else {
                return field;
              }
            })
        .forEach(highlightBuilder::field);

    return highlightBuilder;
  }
}
