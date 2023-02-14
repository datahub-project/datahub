package com.linkedin.metadata;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.config.ElasticSearchConfiguration;
import com.linkedin.metadata.config.EntityDocCountCacheConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.elastic.ESGraphQueryDAO;
import com.linkedin.metadata.graph.elastic.ESGraphWriteDAO;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.LineageRegistry;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.aggregator.AllEntitiesSearchAggregator;
import com.linkedin.metadata.search.cache.CachingAllEntitiesSearchAggregator;
import com.linkedin.metadata.search.cache.EntityDocCountCache;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.SettingsBuilder;
import com.linkedin.metadata.search.elasticsearch.query.ESBrowseDAO;
import com.linkedin.metadata.search.elasticsearch.query.ESSearchDAO;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.elasticsearch.update.ESWriteDAO;
import com.linkedin.metadata.search.ranker.SearchRanker;
import com.linkedin.metadata.search.ranker.SimpleRanker;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.version.GitVersion;
import io.datahub.test.fixtures.elasticsearch.FixtureReader;
import java.util.Optional;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;


@TestConfiguration
@Import(ESTestConfiguration.class)
public class ESSearchLineageFixture {

    @Autowired
    private ESBulkProcessor _bulkProcessor;

    @Autowired
    private RestHighLevelClient _searchClient;

    @Bean(name = "searchLineagePrefix")
    protected String indexPrefix() {
        return "srchlin";
    }

    @Bean(name = "searchLineageIndexConvention")
    protected IndexConvention indexConvention(@Qualifier("searchLineagePrefix") String prefix) {
        return new IndexConventionImpl(prefix);
    }

    @Bean(name = "searchLineageFixtureName")
    protected String fixtureName() {
        return "search_lineage";
    }

    @Bean(name = "searchLineageEntityIndexBuilders")
    protected EntityIndexBuilders entityIndexBuilders(
            @Qualifier("entityRegistry") EntityRegistry entityRegistry,
            @Qualifier("searchLineageIndexConvention") IndexConvention indexConvention
    ) {
        GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
        ESIndexBuilder indexBuilder = new ESIndexBuilder(_searchClient, 1, 0, 1,
                1, Map.of(), true, false,
                new ElasticSearchConfiguration(), gitVersion);
        SettingsBuilder settingsBuilder = new SettingsBuilder(null);
        return new EntityIndexBuilders(indexBuilder, entityRegistry, indexConvention, settingsBuilder);
    }

    @Bean(name = "searchLineageEntitySearchService")
    protected ElasticSearchService entitySearchService(
            @Qualifier("entityRegistry") EntityRegistry entityRegistry,
            @Qualifier("searchLineageEntityIndexBuilders") EntityIndexBuilders indexBuilders,
            @Qualifier("searchLineageIndexConvention") IndexConvention indexConvention
    ) {
        ESSearchDAO searchDAO = new ESSearchDAO(entityRegistry, _searchClient, indexConvention);
        ESBrowseDAO browseDAO = new ESBrowseDAO(entityRegistry, _searchClient, indexConvention);
        ESWriteDAO writeDAO = new ESWriteDAO(entityRegistry, _searchClient, indexConvention, _bulkProcessor, 1);
        return new ElasticSearchService(indexBuilders, searchDAO, browseDAO, writeDAO);
    }

    @Bean(name = "searchLineageESIndexBuilder")
    @Nonnull
    protected ESIndexBuilder esIndexBuilder() {
        GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
        return new ESIndexBuilder(_searchClient, 1, 1, 1, 1, Map.of(),
                true, true,
                new ElasticSearchConfiguration(), gitVersion);
    }

    @Bean(name = "searchLineageGraphService")
    @Nonnull
    protected ElasticSearchGraphService graphService(
            @Qualifier("entityRegistry") EntityRegistry entityRegistry,
            @Qualifier("searchLineageESIndexBuilder") ESIndexBuilder indexBuilder,
            @Qualifier("searchLineageIndexConvention") IndexConvention indexConvention
    ) {
        LineageRegistry lineageRegistry = new LineageRegistry(entityRegistry);
        ElasticSearchGraphService graphService = new ElasticSearchGraphService(lineageRegistry, _bulkProcessor, indexConvention,
                new ESGraphWriteDAO(indexConvention, _bulkProcessor, 1),
                new ESGraphQueryDAO(_searchClient, lineageRegistry, indexConvention), indexBuilder);
        graphService.configure();
        return graphService;
    }

    @Bean(name = "searchLineageLineageSearchService")
    @Nonnull
    protected LineageSearchService lineageSearchService(
            @Qualifier("searchLineageSearchService") SearchService searchService,
            @Qualifier("searchLineageGraphService") ElasticSearchGraphService graphService,
            @Qualifier("searchLineagePrefix") String prefix,
            @Qualifier("searchLineageFixtureName") String fixtureName
    ) throws IOException {

        // Load fixture data (after graphService mappings applied)
        FixtureReader.builder()
                .bulkProcessor(_bulkProcessor)
                .fixtureName(fixtureName)
                .targetIndexPrefix(prefix)
                .build()
                .read();

        return new LineageSearchService(searchService, graphService, null, false);
    }

    @Bean(name = "searchLineageSearchService")
    @Nonnull
    protected SearchService searchService(
            @Qualifier("entityRegistry") EntityRegistry entityRegistry,
            @Qualifier("searchLineageEntitySearchService") ElasticSearchService entitySearchService,
            @Qualifier("searchLineageEntityIndexBuilders") EntityIndexBuilders indexBuilders
    ) throws IOException {

        int batchSize = 100;
        SearchRanker<Double> ranker = new SimpleRanker();
        CacheManager cacheManager = new ConcurrentMapCacheManager();
        EntityDocCountCacheConfiguration entityDocCountCacheConfiguration = new EntityDocCountCacheConfiguration();
        entityDocCountCacheConfiguration.setTtlSeconds(600L);

        SearchService service = new SearchService(
                new EntityDocCountCache(entityRegistry, entitySearchService, entityDocCountCacheConfiguration),
                new CachingEntitySearchService(
                        cacheManager,
                        entitySearchService,
                        batchSize,
                        false
                ),
                new CachingAllEntitiesSearchAggregator(
                        cacheManager,
                        new AllEntitiesSearchAggregator(
                                entityRegistry,
                                entitySearchService,
                                new CachingEntitySearchService(
                                        cacheManager,
                                        entitySearchService,
                                        batchSize,
                                        false
                                ),
                                ranker,
                                entityDocCountCacheConfiguration
                        ),
                        batchSize,
                        false
                ),
                ranker
        );

        // Build indices
        indexBuilders.reindexAll();

        return service;
    }

    @Bean(name = "searchLineageEntityClient")
    @Nonnull
    protected EntityClient entityClient(
            @Qualifier("searchLineageSearchService") SearchService searchService,
            @Qualifier("searchLineageEntitySearchService") ElasticSearchService entitySearchService,
            @Qualifier("entityRegistry") EntityRegistry entityRegistry
    ) {
        CachingEntitySearchService cachingEntitySearchService = new CachingEntitySearchService(
                new ConcurrentMapCacheManager(),
                entitySearchService,
                1,
                false);

        return new JavaEntityClient(
                new EntityService(null, null, entityRegistry),
                null,
                entitySearchService,
                cachingEntitySearchService,
                searchService,
                null,
                null,
                null,
                null);
    }
}
