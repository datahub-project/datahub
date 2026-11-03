package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME;

import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.search.elasticsearch.SearchClients;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchDocumentProjector;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateDocuments.Document;
import com.linkedin.search.template.SearchTemplateProperties;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Reads template bodies from entity-index {@code _source}. The request is a filter on URN, so it
 * does not select or execute a search template.
 */
public final class SearchTemplateDocumentSource implements SearchTemplateDocuments {
  private final EntityIndexConfiguration entityIndex;

  public SearchTemplateDocumentSource(@Nullable EntityIndexConfiguration entityIndex) {
    this.entityIndex = entityIndex;
  }

  @Nonnull
  @Override
  public Map<String, Document> read(
      @Nonnull OperationContext opContext, @Nonnull Collection<String> urns) {
    if (urns.isEmpty()) {
      return Map.of();
    }
    String index =
        EntitySearchIndexResolver.indexName(opContext, SEARCH_TEMPLATE_ENTITY_NAME, entityIndex);
    SearchSourceBuilder source =
        new SearchSourceBuilder()
            .query(
                QueryBuilders.boolQuery()
                    .filter(
                        QueryBuilders.termQuery(
                            MappingConstants.ENTITY_TYPE_FIELD, SEARCH_TEMPLATE_ENTITY_NAME))
                    .filter(QueryBuilders.termsQuery(MappingConstants.URN_FIELD, urns)))
            .size(urns.size())
            .fetchSource(
                new String[] {
                  MappingConstants.URN_FIELD,
                  MappingConstants.ASPECTS_FIELD_NAME
                      + "."
                      + SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME
                      + "."
                      + V3SearchDocumentProjector.ASPECT_JSON_FIELD,
                  MappingConstants.ASPECTS_FIELD_NAME
                      + "."
                      + SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME
                      + "."
                      + V3SearchDocumentProjector.SYSTEM_METADATA_FIELD
                },
                new String[] {});
    SearchRequest request = new SearchRequest(index).source(source);
    try {
      SearchHit[] hits =
          SearchClients.forEntityIndices(opContext, entityIndex, index)
              .search(opContext, request, RequestOptions.DEFAULT)
              .getHits()
              .getHits();
      Map<String, Document> found = new LinkedHashMap<>();
      for (SearchHit hit : hits) {
        JsonNode root = opContext.getObjectMapper().valueToTree(hit.getSourceAsMap());
        JsonNode urn = root.get(MappingConstants.URN_FIELD);
        if (urn == null || !urn.isTextual()) {
          continue;
        }
        JsonNode aspect =
            root.path(MappingConstants.ASPECTS_FIELD_NAME)
                .path(SEARCH_TEMPLATE_PROPERTIES_ASPECT_NAME);
        String aspectJson = V3SearchDocumentProjector.aspectJson(aspect);
        long modifiedAt =
            SearchTemplateModified.at(aspect.path(V3SearchDocumentProjector.SYSTEM_METADATA_FIELD));
        if (aspectJson == null) {
          found.put(urn.asText(), new Document(null, modifiedAt));
        } else {
          found.put(
              urn.asText(),
              new Document(
                  RecordUtils.toRecordTemplate(SearchTemplateProperties.class, aspectJson),
                  modifiedAt));
        }
      }
      return found;
    } catch (IOException e) {
      throw new IllegalStateException("Search template document read failed", e);
    }
  }
}
