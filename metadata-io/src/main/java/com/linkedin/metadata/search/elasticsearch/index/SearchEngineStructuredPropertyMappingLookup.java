package com.linkedin.metadata.search.elasticsearch.index;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;

import com.datahub.context.OperationFingerprint;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.structuredproperties.validation.StructuredPropertyMappingLookup;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
 * definition-create validation observes the backend directly. V2 and V3 patterns are queried on the
 * client that owns that family so a split-cluster deployment still sees both mappings.
 */
@Slf4j
public class SearchEngineStructuredPropertyMappingLookup
    implements StructuredPropertyMappingLookup {

  private static final String PROPERTIES = "properties";

  @Nonnull private final IndexConvention indexConvention;
  @Nonnull private final SearchClusterAccess searchClusterAccess;

  /**
   * Single-cluster constructor for tests. Production wiring uses {@link
   * #SearchEngineStructuredPropertyMappingLookup(IndexConvention, SearchClusterAccess)}.
   *
   * @deprecated use the registry-backed constructor
   */
  @Deprecated
  public SearchEngineStructuredPropertyMappingLookup(
      @Nonnull SearchClientShim<?> searchClient, @Nonnull IndexConvention indexConvention) {
    this(indexConvention, SearchClusterAccess.fixed(searchClient));
  }

  public SearchEngineStructuredPropertyMappingLookup(
      @Nonnull IndexConvention indexConvention, @Nonnull SearchClusterAccess searchClusterAccess) {
    this.indexConvention = indexConvention;
    this.searchClusterAccess = searchClusterAccess;
  }

  @Override
  public boolean fieldExists(
      @Nonnull OperationFingerprint operationContext, @Nonnull String elasticsearchFieldName)
      throws IOException {
    List<String> indexPatterns = indexConvention.getAllEntityIndicesPatterns(operationContext);
    if (indexPatterns.isEmpty()) {
      log.warn(
          "No entity index patterns are configured; structured property mapping collision "
              + "validation cannot inspect active mappings");
      return false;
    }

    Map<SearchClientShim<?>, List<String>> patternsByClient = new LinkedHashMap<>();
    for (String pattern : indexPatterns) {
      SearchClientShim<?> client = clientForPattern(pattern);
      patternsByClient.computeIfAbsent(client, ignored -> new ArrayList<>()).add(pattern);
    }

    for (Map.Entry<SearchClientShim<?>, List<String>> entry : patternsByClient.entrySet()) {
      if (mappingContainsField(
          operationContext, entry.getKey(), entry.getValue(), elasticsearchFieldName)) {
        return true;
      }
    }
    return false;
  }

  @Nonnull
  private SearchClientShim<?> clientForPattern(@Nonnull String pattern) {
    SearchComponent component =
        SearchClusterAccess.tryComponentForEntityIndex(indexConvention, pattern);
    if (component == null) {
      throw new IllegalArgumentException(
          "Entity index pattern '"
              + pattern
              + "' is not a Search V2, V3, or semantic family. Unrecognized patterns are a setup"
              + " error.");
    }
    return searchClusterAccess.clientFor(component);
  }

  private static boolean mappingContainsField(
      @Nonnull OperationFingerprint operationContext,
      @Nonnull SearchClientShim<?> client,
      @Nonnull List<String> indexPatterns,
      @Nonnull String elasticsearchFieldName)
      throws IOException {
    GetMappingsRequest request =
        new GetMappingsRequest()
            .indices(indexPatterns.toArray(new String[0]))
            .indicesOptions(IndicesOptions.lenientExpandOpen());
    return client
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
