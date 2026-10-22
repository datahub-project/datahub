package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields;
import com.linkedin.metadata.search.utils.ESUtils;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Indexed subfields shared by structured properties and ordinary text fields. */
public final class StructuredPropertySubfields {
  public static final String KEYWORD = ESUtils.KEYWORD;
  public static final String TEXT = V3SearchFields.TEXT;
  public static final String STEMMED = V3SearchFields.STEMMED;
  public static final String KEYWORD_SUFFIX = ESUtils.KEYWORD_SUFFIX;
  public static final String TEXT_SUFFIX = V3SearchFields.TEXT_SUFFIX;
  public static final String STEMMED_SUFFIX = V3SearchFields.STEMMED_SUFFIX;
  public static final List<String> SUFFIXES = List.of(KEYWORD_SUFFIX, TEXT_SUFFIX, STEMMED_SUFFIX);

  private StructuredPropertySubfields() {}

  @Nonnull
  public static String strip(@Nonnull String fieldName) {
    for (String suffix : SUFFIXES) {
      if (fieldName.endsWith(suffix)) {
        return fieldName.substring(0, fieldName.length() - suffix.length());
      }
    }
    return fieldName;
  }

  @Nullable
  public static String suffix(@Nullable String subfield) {
    if (subfield == null || subfield.isBlank()) {
      return null;
    }
    return subfield.startsWith(".") ? subfield : "." + subfield;
  }
}
