package com.linkedin.metadata.search.elasticsearch.query;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.api.SearchDocFieldFetchConfig;
import com.linkedin.metadata.search.elasticsearch.SearchClients;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchRequestHandler;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateVectorRetrieval;
import com.linkedin.metadata.search.embedding.VectorStore;
import com.linkedin.metadata.search.hybrid.v3.V3HybridKnnRequestBuilder;
import com.linkedin.metadata.search.semantic.SemanticEntitySearchService;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * The Search V3 index as a vector store: one kNN request with the keyword leg's filter inside it,
 * so neighbors are retrieved inside the caller's filter and the system filters.
 */
@Slf4j
final class NativeVectorStore implements VectorStore {
  static final Set<String> CAPABILITIES = Set.of(PRE_FILTER);

  private final ESSearchDAO dao;

  NativeVectorStore(@Nonnull ESSearchDAO dao) {
    this.dao = dao;
  }

  @Nonnull
  @Override
  public List<Hit> search(
      @Nonnull OperationContext opContext, @Nonnull Request request, @Nonnull Duration timeout)
      throws IOException {
    final List<EntitySpec> entitySpecs =
        request.entityTypes().stream()
            .map(name -> opContext.getEntityRegistry().getEntitySpec(name))
            .toList();
    final SearchRequestHandler handler =
        SearchRequestHandler.getBuilder(
            opContext,
            entitySpecs,
            dao.searchConfiguration,
            dao.customSearchConfiguration,
            dao.queryFilterRewriteChain,
            dao.searchServiceConfig);
    final KnnSearchResponse response =
        SearchClients.forComponent(opContext, SearchComponent.SEARCH_V3)
            .searchKnn(
                opContext,
                KnnSearchRequest.builder()
                    .indexName(
                        String.join(
                            ",",
                            SemanticEntitySearchService.v3SemanticIndices(
                                    opContext,
                                    request.entityTypes(),
                                    dao.searchConfiguration.getEntityIndex().getSemanticSearch())
                                .values()))
                    .vectorField(V3HybridKnnRequestBuilder.vectorField(request.collection()))
                    .queryVector(request.vector())
                    .k(request.k())
                    .timeout(timeout)
                    .fieldsToFetch(
                        new ArrayList<>(
                            SearchDocFieldFetchConfig.resolve(
                                SearchDocFieldFetchConfig.DEFAULT_FIELDS_TO_FETCH_ON_SEARCH,
                                opContext.getSearchContext().getSearchFlags())))
                    .filter(ESUtils.filterMap(handler.getFilterQuery(opContext, request.filter())))
                    .build());
    if (response.partial()) {
      throw new IOException("The kNN response reported a timed-out or failed shard");
    }
    final List<Hit> hits = new ArrayList<>();
    for (KnnSearchResponse.Hit hit : response.hits()) {
      final Urn urn = urn(hit);
      if (urn != null) {
        hits.add(
            new Hit(
                urn, SearchTemplateVectorRetrieval.cosine(hit.score()), hit.score(), hit.source()));
      }
    }
    return hits;
  }

  @Nonnull
  @Override
  public List<float[]> findById(
      @Nonnull OperationContext opContext,
      @Nonnull String collection,
      @Nonnull Urn urn,
      @Nonnull Duration timeout) {
    throw new UnsupportedOperationException("The DataHub index store does not find entities by id");
  }

  @Nullable
  private static Urn urn(@Nonnull KnnSearchResponse.Hit hit) {
    if (!(hit.source().get("urn") instanceof String value) || value.isEmpty()) {
      return null;
    }
    try {
      return Urn.createFromString(value);
    } catch (URISyntaxException e) {
      log.warn("Invalid URN in a kNN hit: {}", value);
      return null;
    }
  }
}
