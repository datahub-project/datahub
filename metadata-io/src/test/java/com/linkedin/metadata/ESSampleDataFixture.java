package com.linkedin.metadata;

import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.config.ElasticSearchConfiguration;
import com.linkedin.metadata.config.EntityDocCountCacheConfiguration;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
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
public class ESSampleDataFixture {

    @Autowired
    private ESBulkProcessor _bulkProcessor;

    @Autowired
    private RestHighLevelClient _searchClient;

    @Bean(name = "sampleDataPrefix")
    protected String indexPrefix() {
        return "smpldat";
    }

    @Bean(name = "sampleDataIndexConvention")
    protected IndexConvention indexConvention(@Qualifier("sampleDataPrefix") String prefix) {
        return new IndexConventionImpl(prefix);
    }

    @Bean(name = "sampleDataFixtureName")
    protected String fixtureName() {
        return "sample_data";
    }

    @Bean(name = "sampleDataEntityIndexBuilders")
    protected EntityIndexBuilders entityIndexBuilders(
            @Qualifier("entityRegistry") EntityRegistry entityRegistry,
            @Qualifier("sampleDataIndexConvention") IndexConvention indexConvention
    ) {
        GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
        ESIndexBuilder indexBuilder = new ESIndexBuilder(_searchClient, 1, 0, 1,
                1, Map.of(), true, false,
                new ElasticSearchConfiguration(), gitVersion);
        SettingsBuilder settingsBuilder = new SettingsBuilder(null);
        return new EntityIndexBuilders(indexBuilder, entityRegistry, indexConvention, settingsBuilder);
    }

    @Bean(name = "sampleDataEntitySearchService")
    protected ElasticSearchService entitySearchService(
            @Qualifier("entityRegistry") EntityRegistry entityRegistry,
            @Qualifier("sampleDataEntityIndexBuilders") EntityIndexBuilders indexBuilders,
            @Qualifier("sampleDataIndexConvention") IndexConvention indexConvention
    ) {
        ESSearchDAO searchDAO = new ESSearchDAO(entityRegistry, _searchClient, indexConvention);
        ESBrowseDAO browseDAO = new ESBrowseDAO(entityRegistry, _searchClient, indexConvention);
        ESWriteDAO writeDAO = new ESWriteDAO(entityRegistry, _searchClient, indexConvention, _bulkProcessor, 1);
        return new ElasticSearchService(indexBuilders, searchDAO, browseDAO, writeDAO);
    }

    @Bean(name = "sampleDataSearchService")
    @Nonnull
    protected SearchService searchService(
            @Qualifier("entityRegistry") EntityRegistry entityRegistry,
            @Qualifier("sampleDataEntitySearchService") ElasticSearchService entitySearchService,
            @Qualifier("sampleDataEntityIndexBuilders") EntityIndexBuilders indexBuilders,
            @Qualifier("sampleDataPrefix") String prefix,
            @Qualifier("sampleDataFixtureName") String fixtureName
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

        // Build indices & write fixture data
        indexBuilders.reindexAll();

        FixtureReader.builder()
                .bulkProcessor(_bulkProcessor)
                .fixtureName(fixtureName)
                .targetIndexPrefix(prefix)
                .build()
                .read();

        return service;
    }

    @Bean(name = "sampleDataEntityClient")
    @Nonnull
    protected EntityClient entityClient(
            @Qualifier("sampleDataSearchService") SearchService searchService,
            @Qualifier("sampleDataEntitySearchService") ElasticSearchService entitySearchService,
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
