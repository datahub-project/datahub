package io.datahubproject.test.fixtures.search;

import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.SEARCHABLE_ENTITY_TYPES;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.NonNull;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;

@Builder
public class EntityExporter {
  @NonNull private RestHighLevelClient client;
  @Builder.Default private int fetchSize = 3000;
  @NonNull private FixtureWriter writer;
  @NonNull private String fixtureName;
  @Builder.Default private String sourceIndexPrefix = "";
  @Builder.Default private String sourceIndexSuffix = "index_v2";

  @Builder.Default
  private Set<String> indexEntities =
      SEARCHABLE_ENTITY_TYPES.stream()
          .map(entityType -> entityType.toString().toLowerCase().replaceAll("_", ""))
          .collect(Collectors.toSet());

  public void export() throws IOException {
    Set<String> searchIndexSuffixes =
        indexEntities.stream()
            .map(entityName -> entityName + sourceIndexSuffix)
            .collect(Collectors.toSet());

    // Fetch indices
    GetMappingsResponse response =
        client.indices().getMapping(new GetMappingsRequest().indices("*"), RequestOptions.DEFAULT);

    response.mappings().keySet().stream()
        .filter(
            index ->
                searchIndexSuffixes.stream().anyMatch(index::contains)
                    && index.startsWith(sourceIndexPrefix))
        .map(index -> index.split(sourceIndexSuffix, 2)[0] + sourceIndexSuffix)
        .forEach(
            indexName -> {
              SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
              searchSourceBuilder.size(fetchSize);
              searchSourceBuilder.sort(SortBuilders.fieldSort("_id").order(SortOrder.ASC));

              SearchRequest searchRequest = new SearchRequest(indexName);
              searchRequest.source(searchSourceBuilder);

              String outputPath =
                  String.format(
                      "%s/%s.json", fixtureName, indexName.replaceFirst(sourceIndexPrefix, ""));
              writer.write(searchRequest, outputPath, false);
            });
  }
}
