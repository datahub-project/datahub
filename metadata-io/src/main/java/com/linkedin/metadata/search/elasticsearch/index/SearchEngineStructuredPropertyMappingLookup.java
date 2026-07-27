package com.linkedin.metadata.search.elasticsearch.index;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.structuredproperties.validation.StructuredPropertyMappingLookup;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.cluster.metadata.MappingMetadata;

/**
 * Elasticsearch/OpenSearch implementation backed by the mapping API.
 *
 * <p>The mapping API reads cluster state and is not served by the search query cache, so each
 * definition-create validation observes the backend directly.
 */
@Slf4j
public class SearchEngineStructuredPropertyMappingLookup
    implements StructuredPropertyMappingLookup {

  private static final String PROPERTIES = "properties";

  @Nonnull private final SearchClientShim<?> searchClient;
  @Nonnull private final IndexConvention indexConvention;

  public SearchEngineStructuredPropertyMappingLookup(
      @Nonnull SearchClientShim<?> searchClient, @Nonnull IndexConvention indexConvention) {
    this.searchClient = searchClient;
    this.indexConvention = indexConvention;
  }

  @Override
  public boolean fieldExists(
      @Nonnull OperationFingerprint operationContext, @Nonnull String elasticsearchFieldName)
      throws IOException {
    List<String> indexPatterns = indexConvention.getAllEntityIndicesPatterns();
    if (indexPatterns.isEmpty()) {
      log.warn(
          "No entity index patterns are configured; structured property mapping collision "
              + "validation cannot inspect active mappings");
      return false;
    }

    GetMappingsRequest request =
        new GetMappingsRequest()
            .indices(indexPatterns.toArray(new String[0]))
            .indicesOptions(IndicesOptions.lenientExpandOpen());
    return searchClient
        .getIndexMapping(operationContext, request, RequestOptions.DEFAULT)
        .mappings()
        .values()
        .stream()
        .map(MappingMetadata::getSourceAsMap)
        .map(mapping -> childMap(mapping, PROPERTIES))
        .map(properties -> childMap(properties, STRUCTURED_PROPERTY_MAPPING_FIELD))
        .map(structuredProperties -> childMap(structuredProperties, PROPERTIES))
        .anyMatch(properties -> containsField(properties, elasticsearchFieldName));
  }

  private static boolean containsField(
      @Nonnull Map<String, Object> properties, @Nonnull String fieldName) {
    String[] path = fieldName.split("\\.");
    Map<String, Object> currentProperties = properties;
    for (int i = 0; i < path.length; i++) {
      String remainingPath = String.join(".", java.util.Arrays.copyOfRange(path, i, path.length));
      if (currentProperties.containsKey(remainingPath)) {
        return true;
      }

      Object fieldMapping = currentProperties.get(path[i]);
      if (!(fieldMapping instanceof Map)) {
        return false;
      }
      if (i == path.length - 1) {
        return true;
      }
      currentProperties = childMap(castMap(fieldMapping), PROPERTIES);
      if (currentProperties.isEmpty()) {
        return false;
      }
    }
    return false;
  }

  @Nonnull
  private static Map<String, Object> childMap(
      @Nonnull Map<String, Object> parent, @Nonnull String key) {
    Object value = parent.get(key);
    return value instanceof Map ? castMap(value) : Map.of();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> castMap(@Nonnull Object value) {
    return (Map<String, Object>) value;
  }
}
