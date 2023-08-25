package com.linkedin.metadata;

import com.linkedin.entity.Entity;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.config.cache.EntityDocCountCacheConfiguration;
import com.linkedin.metadata.config.search.CustomConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SearchConfiguration;
import com.linkedin.metadata.config.search.custom.CustomSearchConfiguration;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityAspect;
import com.linkedin.metadata.entity.EntityAspectIdentifier;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchService;
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

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@TestConfiguration
@Import(ESTestConfiguration.class)
public class ESLongTailFixture {
    @Autowired
    @Qualifier("longTailBulkProcessor")
    private ESBulkProcessor _bulkProcessor;

    @Autowired
    private RestHighLevelClient _searchClient;

    @Autowired
    private SearchConfiguration _searchConfiguration;

    @Autowired
    private CustomSearchConfiguration _customSearchConfiguration;

    @Bean(name = "longTailPrefix")
    protected String longTailIndexPrefix() {
        return "lngtl";
    }

    @Bean(name = "longTailIndexConvention")
    protected IndexConvention longTailIndexConvention(@Qualifier("longTailPrefix") String prefix) {
        return new IndexConventionImpl(prefix);
    }

    @Bean(name = "longTailFixtureName")
    protected String longTailFixtureName() {
        return "long_tail";
    }

    @Bean(name = "longTailEntityIndexBuilders")
    protected EntityIndexBuilders longTailEntityIndexBuilders(
            @Qualifier("entityRegistry") EntityRegistry entityRegistry,
            @Qualifier("longTailIndexConvention") IndexConvention indexConvention
    ) {
        GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
        ESIndexBuilder indexBuilder = new ESIndexBuilder(_searchClient, 1, 0, 1,
                1, Map.of(), true, false,
                new ElasticSearchConfiguration(), gitVersion);
        SettingsBuilder settingsBuilder = new SettingsBuilder(null);
        return new EntityIndexBuilders(indexBuilder, entityRegistry, indexConvention, settingsBuilder);
    }

    @Bean(name = "longTailEntitySearchService")
    protected EntitySearchService longTailEntitySearchService(
            @Qualifier("entityRegistry") EntityRegistry entityRegistry,
            @Qualifier("longTailEntityIndexBuilders") EntityIndexBuilders longTailIndexBuilders,
            @Qualifier("longTailIndexConvention") IndexConvention longTailIndexConvention
    ) throws IOException {
        CustomConfiguration customConfiguration = new CustomConfiguration();
        customConfiguration.setEnabled(true);
        customConfiguration.setFile("search_config_fixture_test.yml");
        CustomSearchConfiguration customSearchConfiguration = customConfiguration.resolve(new YAMLMapper());

        ESSearchDAO searchDAO = new ESSearchDAO(entityRegistry, _searchClient, longTailIndexConvention, false,
                ELASTICSEARCH_IMPLEMENTATION_ELASTICSEARCH, _searchConfiguration, customSearchConfiguration);
        ESBrowseDAO browseDAO = new ESBrowseDAO(entityRegistry, _searchClient, longTailIndexConvention, _searchConfiguration, _customSearchConfiguration);
        ESWriteDAO writeDAO = new ESWriteDAO(entityRegistry, _searchClient, longTailIndexConvention, _bulkProcessor, 1);
        return new ElasticSearchService(longTailIndexBuilders, searchDAO, browseDAO, writeDAO);
    }

    @Bean(name = "longTailSearchService")
    @Nonnull
    protected SearchService longTailSearchService(
            @Qualifier("entityRegistry") EntityRegistry entityRegistry,
            @Qualifier("longTailEntitySearchService") ElasticSearchService longTailEntitySearchService,
            @Qualifier("longTailEntityIndexBuilders") EntityIndexBuilders longTailIndexBuilders,
            @Qualifier("longTailPrefix") String longTailPrefix,
            @Qualifier("longTailFixtureName") String longTailFixtureName
    ) throws IOException {
        int batchSize = 100;
        SearchRanker<Double> ranker = new SimpleRanker();
        CacheManager cacheManager = new ConcurrentMapCacheManager();
        EntityDocCountCacheConfiguration entityDocCountCacheConfiguration = new EntityDocCountCacheConfiguration();
        entityDocCountCacheConfiguration.setTtlSeconds(600L);

        SearchService service = new SearchService(
                new EntityDocCountCache(entityRegistry, longTailEntitySearchService, entityDocCountCacheConfiguration),
                new CachingEntitySearchService(
                        cacheManager,
                        longTailEntitySearchService,
                        batchSize,
                        false
                ),
                ranker
        );

        // Build indices & write fixture data
        longTailIndexBuilders.reindexAll();

        FixtureReader.builder()
                .bulkProcessor(_bulkProcessor)
                .fixtureName(longTailFixtureName)
                .targetIndexPrefix(longTailPrefix)
                .build()
                .read();

        return service;
    }

    @Bean(name = "longTailEntityClient")
    @Nonnull
    protected EntityClient longTailEntityClient(
            @Qualifier("longTailSearchService") SearchService searchService,
            @Qualifier("longTailEntitySearchService") ElasticSearchService entitySearchService,
            @Qualifier("entityRegistry") EntityRegistry entityRegistry
    ) {
        CachingEntitySearchService cachingEntitySearchService = new CachingEntitySearchService(
                new ConcurrentMapCacheManager(),
                entitySearchService,
                1,
                false);

        AspectDao mockAspectDao = mock(AspectDao.class);
        when(mockAspectDao.batchGet(anySet())).thenReturn(Map.of(mock(EntityAspectIdentifier.class), mock(EntityAspect.class)));

        PreProcessHooks preProcessHooks = new PreProcessHooks();
        preProcessHooks.setUiEnabled(true);
        return new JavaEntityClient(
                new EntityServiceImpl(mockAspectDao, null, entityRegistry, true, null,
                        preProcessHooks),
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
