package com.linkedin.metadata.search.elasticsearch.query.request;

import com.google.common.annotations.VisibleForTesting;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
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
    return buildHighlightsWithSelectiveExpansion(opContext, getDefaultQueryFieldNames(), Set.of());
  }

  /**
   * Build highlights with selective field expansion. Only applies expansion to fields that were not
   * explicitly configured.
   */
  @VisibleForTesting
  protected HighlightBuilder buildHighlightsWithSelectiveExpansion(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<String> fieldsToHighlight,
      @Nonnull Set<String> explicitlyConfiguredFields) {

    HighlightBuilder highlightBuilder =
        new HighlightBuilder().preTags("").postTags("").numOfFragments(1);

    Set<String> addedFields = new HashSet<>();

    for (String fieldName : fieldsToHighlight) {
      if (explicitlyConfiguredFields.contains(fieldName)) {
        // Field was explicitly configured - add as-is without expansion
        if (addedFields.add(fieldName)) {
          HighlightBuilder.Field field = new HighlightBuilder.Field(fieldName);
          if (fieldName.endsWith("ngram")) {
            field.requireFieldMatch(false).noMatchSize(200);
          }
          highlightBuilder.field(field);
        }
      } else {
        // Field was not explicitly configured - apply expansion
        highlightFieldExpansion(opContext, fieldName)
            .distinct()
            .filter(addedFields::add)
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
      }
    }

    return highlightBuilder;
  }
}
