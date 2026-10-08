package com.linkedin.gms.factory.search;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.semantic.SemanticEntitySearchServiceFactory;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.ModelEmbeddingConfig;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.SemanticSearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntityDocumentIdHasher;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.search.embedding.NoOpEmbeddingProvider;
import com.linkedin.metadata.search.hybrid.HybridCandidateMerger;
import com.linkedin.metadata.search.hybrid.HybridLexicalScoreNormalizer;
import com.linkedin.metadata.search.hybrid.HybridQueryEmbeddingService;
import com.linkedin.metadata.search.hybrid.HybridScoreCombiner;
import com.linkedin.metadata.search.hybrid.HybridScoreMapBuilder;
import com.linkedin.metadata.search.hybrid.HybridSearchResultReranker;
import com.linkedin.metadata.search.hybrid.HybridVectorScoreNormalizer;
import com.linkedin.metadata.search.hybrid.V3HybridKnnRequestBuilder;
import com.linkedin.metadata.search.semantic.SemanticEntitySearchService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.ObjectMapperContext;
import java.io.IOException;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Slf4j
@Configuration
@Import(EntityDocumentIdHasherFactory.class)
public class ElasticSearchServiceFactory {

  @Autowired
  @Qualifier("baseElasticSearchComponents")
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents components;

  @Autowired
  @Qualifier("settingsBuilder")
  private SettingsBuilder settingsBuilder;

  @Autowired
  @Qualifier("entityRegistry")
  private EntityRegistry entityRegistry;

  @Bean
  protected ElasticSearchConfiguration elasticSearchConfiguration(
      final ConfigurationProvider configurationProvider) {
    log.info("Search configuration: {}", configurationProvider.getElasticSearch().getSearch());
    return configurationProvider.getElasticSearch();
  }

  @Bean
  @Nullable
  protected CustomSearchConfiguration customSearchConfiguration(
      final ElasticSearchConfiguration elasticSearchConfiguration) throws IOException {
    SearchConfiguration searchConfiguration = elasticSearchConfiguration.getSearch();
    return searchConfiguration.getCustom() == null
        ? null
        : searchConfiguration.getCustom().resolve(ObjectMapperContext.DEFAULT.getYamlMapper());
  }

  @Bean
  protected ESSearchDAO esSearchDAO(
      final ConfigurationProvider configurationProvider,
      final QueryFilterRewriteChain queryFilterRewriteChain,
      final ElasticSearchConfiguration elasticSearchConfiguration,
      @Nullable final CustomSearchConfiguration customSearchConfiguration,
      final EntityDocumentIdHasher entityDocumentIdHasher,
      @Qualifier("embeddingProvider")
          final ObjectProvider<EmbeddingProvider> embeddingProviderProvider,
      final SearchClusterRegistry searchClusterRegistry) {

    return new ESSearchDAO(
        elasticSearchConfiguration.getSearch().isPointInTimeCreationEnabled(),
        elasticSearchConfiguration,
        customSearchConfiguration,
        queryFilterRewriteChain,
        false,
        configurationProvider.getSearchService(),
        entityDocumentIdHasher,
        hybridSearchResultReranker(
            elasticSearchConfiguration.getEntityIndex(),
            embeddingProviderProvider::getIfAvailable,
            () -> searchClusterRegistry.clientFor(SearchComponent.SEARCH_V3)));
  }

  /**
   * The hybrid reranker when {@code elasticsearch.entityIndex.v3.hybridReadEnabled} is on with V3
   * enabled, else null. Hybrid read reranks V3 keyword results with the vectors V3 semantic reads
   * serve, so startup fails unless V3 keyword and semantic reads, semantic search, a real embedding
   * provider and the active model's mapping are all configured.
   */
  @Nullable
  static HybridSearchResultReranker hybridSearchResultReranker(
      @Nullable final EntityIndexConfiguration entityIndex,
      @Nonnull final Supplier<EmbeddingProvider> embeddingProvider,
      @Nonnull final Supplier<SearchClientShim<?>> searchV3Client) {
    if (entityIndex == null
        || entityIndex.getV3() == null
        || !entityIndex.getV3().isEnabled()
        || !entityIndex.getV3().isHybridReadEnabled()) {
      return null;
    }
    if (!EntitySearchIndexResolver.shouldReadV3(entityIndex)) {
      throw hybridConfigurationError(
          "keyword reads must use V3 (elasticsearch.entityIndex.v3.keywordReadEnabled)");
    }
    if (!entityIndex.getV3().isSemanticReadEnabled()) {
      throw hybridConfigurationError(
          "elasticsearch.entityIndex.v3.semanticReadEnabled must be true");
    }
    final SemanticSearchConfiguration semanticSearch = entityIndex.getSemanticSearch();
    if (semanticSearch == null || !semanticSearch.isEnabled()) {
      throw hybridConfigurationError(
          "elasticsearch.entityIndex.semanticSearch.enabled must be true");
    }
    final EmbeddingProvider provider = embeddingProvider.get();
    if (provider == null || provider instanceof NoOpEmbeddingProvider) {
      throw hybridConfigurationError("an embedding provider must be configured");
    }
    final String modelId =
        semanticSearch.getEmbeddingProvider() != null
            ? semanticSearch.getEmbeddingProvider().getModelId()
            : null;
    final String modelKey =
        SemanticEntitySearchServiceFactory.deriveModelEmbeddingKeyFromModelId(modelId);
    final ModelEmbeddingConfig model =
        semanticSearch.getModels() != null ? semanticSearch.getModels().get(modelKey) : null;
    if (model == null || model.getVectorDimension() <= 0) {
      throw hybridConfigurationError(
          "elasticsearch.entityIndex.semanticSearch.models must configure the active model key '"
              + modelKey
              + "' with a positive vectorDimension");
    }
    if (model.getSpaceType() == null
        || !HybridVectorScoreNormalizer.isSupportedMetric(model.getSpaceType())) {
      throw hybridConfigurationError(
          "elasticsearch.entityIndex.semanticSearch.models."
              + modelKey
              + ".spaceType must be cosine, L2 or inner product");
    }
    final SearchClientShim<?> client = searchV3Client.get();
    SemanticEntitySearchService.requireSupportedV3Engine(entityIndex, client);
    return new HybridSearchResultReranker(
        new HybridQueryEmbeddingService(provider, modelId, modelKey, model.getVectorDimension()),
        new V3HybridKnnRequestBuilder(semanticSearch),
        new HybridScoreMapBuilder(
            new HybridVectorScoreNormalizer(client.getEngineType(), model.getSpaceType())),
        new HybridCandidateMerger(new HybridLexicalScoreNormalizer(), new HybridScoreCombiner()));
  }

  private static IllegalStateException hybridConfigurationError(@Nonnull final String detail) {
    return new IllegalStateException(
        "elasticsearch.entityIndex.v3.hybridReadEnabled is on, but " + detail);
  }

  @Bean
  protected ESWriteDAO esWriteDAO(
      final ConfigurationProvider configurationProvider,
      final SearchClusterRegistry searchClusterRegistry) {
    ESWriteDAO esWriteDAO =
        new ESWriteDAO(
            components.getConfig(),
            components.getSearchClient(),
            components.getBulkProcessor(),
            searchClusterRegistry);
    if (configurationProvider.getDatahub().isReadOnly()) {
      esWriteDAO.setWritable(false);
    }
    return esWriteDAO;
  }

  @Bean(name = "elasticSearchService")
  @Nonnull
  protected ElasticSearchService getInstance(
      final ConfigurationProvider configurationProvider,
      final QueryFilterRewriteChain queryFilterRewriteChain,
      final ElasticSearchConfiguration elasticSearchConfiguration,
      @Nullable final CustomSearchConfiguration customSearchConfiguration,
      final ESSearchDAO esSearchDAO,
      final ESWriteDAO esWriteDAO,
      @Qualifier("mappingsBuilder") final MappingsBuilder mappingsBuilder,
      @Qualifier("settingsBuilder") final SettingsBuilder settingsBuilder,
      final SearchClusterRegistry searchClusterRegistry)
      throws IOException {

    warnOpenSearch3DocIdRisk(
        elasticSearchConfiguration, searchClusterRegistry.clientFor(SearchComponent.SEARCH_V2));

    return new ElasticSearchService(
        components.getIndexBuilder(),
        configurationProvider.getSearchService(),
        configurationProvider.getElasticSearch(),
        mappingsBuilder,
        settingsBuilder,
        // Null unless V2/V3/semantic are split across clusters, in which case each index family
        // must be built by the client that owns it.
        searchClusterRegistry.entityIndexBuilderResolver(components.getIndexConvention()),
        esSearchDAO,
        new ESBrowseDAO(
            elasticSearchConfiguration,
            customSearchConfiguration,
            queryFilterRewriteChain,
            configurationProvider.getSearchService()),
        esWriteDAO);
  }

  /**
   * OpenSearch 3.x enforces the 512-byte {@code _id} limit on bulk writes (2.x did not). Schema
   * field URNs are exempt from URN length validation and, with doc-ID hashing disabled, are used
   * URL-encoded as document ids, so long schema-field URNs that indexed fine on 2.x become rejected
   * bulk items on 3.x. Checked against the Search V2 cluster, because hashing is a V2-only setting.
   * Surfaced as a startup warning rather than a failure so existing 2.x-migrated deployments can
   * start; the rejected items themselves are logged at ERROR by the bulk listener.
   */
  static void warnOpenSearch3DocIdRisk(
      @Nonnull final ElasticSearchConfiguration configuration,
      @Nullable final SearchClientShim<?> searchClient) {
    // Null in Spring tests that mock the components holder; nothing to warn about.
    if (searchClient == null
        || searchClient.getEngineType() != SearchClientShim.SearchEngineType.OPENSEARCH_3) {
      return;
    }
    final boolean hashIdEnabled =
        configuration.getEntityIndex() != null
            && configuration.getEntityIndex().getV2() != null
            && configuration.getEntityIndex().getV2().isSchemaFieldDocIdHashEnabled();
    if (!hashIdEnabled) {
      log.warn(
          "OpenSearch 3.x enforces a 512-byte _id limit on bulk writes, and schema-field document"
              + " ids use the URL-encoded URN while"
              + " elasticsearch.entityIndex.v2.docIds.schemaField.hashIdEnabled is false. Long"
              + " schema-field URNs will be rejected at index time (logged as bulk failures). Enable"
              + " ELASTICSEARCH_INDEX_DOC_IDS_SCHEMA_FIELD_HASH_ID_ENABLED for new OpenSearch 3.x"
              + " deployments.");
    }
  }
}
