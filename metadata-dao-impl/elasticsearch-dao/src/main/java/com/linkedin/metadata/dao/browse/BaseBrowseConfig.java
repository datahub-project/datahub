package com.linkedin.metadata.dao.browse;

import com.linkedin.data.schema.RecordDataSchema;
import com.linkedin.data.template.RecordTemplate;
import javax.annotation.Nonnull;


public abstract class BaseBrowseConfig<DOCUMENT extends RecordTemplate> {
  private static RecordDataSchema searchDocumentSchema;

  @Nonnull
  public String getBrowseDepthFieldName() {
    return "browsePaths.length";
  }

  @Nonnull
  public String getBrowsePathFieldName() {
    return "browsePaths";
  }

  @Nonnull
  public String getUrnFieldName() {
    return "urn";
  }

  @Nonnull
  public String getSortingField() {
    return "urn";
  }

  @Nonnull
  public String getRemovedField() {
    return "removed";
  }

  public boolean hasFieldInSchema(@Nonnull String fieldName) {
    return getSearchDocumentSchema().contains(fieldName);
  }

  private RecordDataSchema getSearchDocumentSchema() {
    if (searchDocumentSchema == null) {
      try {
        searchDocumentSchema = getSearchDocument().newInstance().schema();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException("Couldn't create an instance of the search document");
      }
    }
    return searchDocumentSchema;
  }

  @Nonnull
  public String getIndexName() {
    return getSearchDocument().getSimpleName().toLowerCase();
  }

  public abstract Class<DOCUMENT> getSearchDocument();
}
