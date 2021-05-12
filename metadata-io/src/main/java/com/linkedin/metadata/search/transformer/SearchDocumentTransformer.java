package com.linkedin.metadata.search.transformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.extractor.FieldExtractor;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class SearchDocumentTransformer {
  private SearchDocumentTransformer() { }

  public static JsonNode transform(final RecordTemplate snapshot, final EntitySpec entitySpec) {
    Map<String, List<SearchableFieldSpec>> searchableFieldSpecsPerAspect = entitySpec.getAspectSpecMap()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getSearchableFieldSpecs()));
    Map<SearchableFieldSpec, Object> extractedFields =
        FieldExtractor.extractFields(snapshot, searchableFieldSpecsPerAspect);
    return null;
  }
}
