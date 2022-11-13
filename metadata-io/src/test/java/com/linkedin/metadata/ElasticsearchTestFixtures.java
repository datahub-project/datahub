package com.linkedin.metadata;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.Authorizer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.client.JavaEntityClient;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.SearchResult;
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
import org.apache.commons.io.FilenameUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.SEARCHABLE_ENTITY_TYPES;
import static com.linkedin.datahub.graphql.resolvers.search.SearchUtils.AUTO_COMPLETE_ENTITY_TYPES;

@TestConfiguration
@Import(ElasticSearchTestConfiguration.class)
public class ElasticsearchTestFixtures {
    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final static String FIXTURE_BASE = "src/test/resources/elasticsearch";

    public final static List<String> SEARCHABLE_ENTITIES;
    static {
        SEARCHABLE_ENTITIES = Stream.concat(
                SEARCHABLE_ENTITY_TYPES.stream(), AUTO_COMPLETE_ENTITY_TYPES.stream())
                .map(EntityTypeMapper::getName)
                .distinct()
                .collect(Collectors.toList());
    }

    public static SearchResult sampleDataSearch(SearchService searchService, String query) {
        return searchService.searchAcrossEntities(SEARCHABLE_ENTITIES, query, null, null, 0,
                100, null);
    }

    public static AutoCompleteResults sampleDataAutoComplete(SearchableEntityType<?, String> searchableEntityType, String query) throws Exception {
        return searchableEntityType.autoComplete(query, null, null, 100, new QueryContext() {
            @Override
            public boolean isAuthenticated() {
                return true;
            }

            @Override
            public Authentication getAuthentication() {
                return null;
            }

            @Override
            public Authorizer getAuthorizer() {
                return null;
            }
        });
    }

    @Autowired
    private ESBulkProcessor _bulkProcessor;

    @Autowired
    private RestHighLevelClient _searchClient;

    @Bean(name = "sampleDataPrefix")
    protected String sampleDataPrefix() {
        return "smpldat";
    }

    @Bean(name = "sampleDataIndexConvention")
    protected IndexConvention indexConvention(@Qualifier("sampleDataPrefix") String prefix) {
        return new IndexConventionImpl(prefix);
    }

    @Bean(name = "sampleDataFixtureName")
    protected String sampleDataFixtureName() {
        return "sample_data";
    }

    @Bean(name = "entityRegistry")
    protected EntityRegistry entityRegistry() {
        return new ConfigEntityRegistry(
                ElasticsearchTestFixtures.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
    }

    @Bean(name = "sampleDataEntityIndexBuilders")
    protected EntityIndexBuilders entityIndexBuilders(
            @Qualifier("entityRegistry") EntityRegistry entityRegistry,
            @Qualifier("sampleDataIndexConvention") IndexConvention indexConvention
    ) {
        ESIndexBuilder indexBuilder = new ESIndexBuilder(_searchClient, 1, 0, 1,
                1, Map.of(), true, false);
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

    @Primary
    @Bean(name = "sampleDataSearchService")
    @Nonnull
    protected SearchService sampleDataSearchService(
            @Qualifier("entityRegistry") EntityRegistry entityRegistry,
            @Qualifier("sampleDataEntitySearchService") ElasticSearchService entitySearchService,
            @Qualifier("sampleDataEntityIndexBuilders") EntityIndexBuilders indexBuilders,
            @Qualifier("sampleDataPrefix") String prefix,
            @Qualifier("sampleDataFixtureName") String fixtureName
    ) throws IOException {

        int batchSize = 100;
        SearchRanker<Double> ranker = new SimpleRanker();
        CacheManager cacheManager = new ConcurrentMapCacheManager();

        SearchService service = new SearchService(
                new EntityDocCountCache(entityRegistry, entitySearchService),
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
                                ranker
                        ),
                        batchSize,
                        false
                ),
                ranker
        );

        // Build indices & write fixture data
        indexBuilders.buildAll();
        indexFixture(fixtureName, prefix);

        return service;
    }

    @Primary
    @Bean(name = "sampleDataEntityClient")
    @Nonnull
    protected EntityClient sampleDataEntityClient(
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

    private Set<String> indexFixture(String fixture, String prefix) throws IOException {
        try (Stream<Path> files = Files.list(Paths.get(String.format("%s/%s", FIXTURE_BASE, fixture)))) {
            return files.map(file -> {
                String indexName = String.format("%s_%s", prefix, FilenameUtils.getBaseName(file.toAbsolutePath().toString()));

                try (Stream<String> lines = Files.lines(Paths.get(file.toAbsolutePath().toString()))) {
                    lines.forEach(line -> {
                        try {
                            UrnDocument doc = OBJECT_MAPPER.readValue(line, UrnDocument.class);
                            IndexRequest request = new IndexRequest(indexName)
                                    .id(doc.urn)
                                    .source(line.getBytes(), XContentType.JSON);

                            _bulkProcessor.add(request);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException(e);
                        }
                    });
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                return indexName;
            }).collect(Collectors.toSet());
        } finally {
            _bulkProcessor.flush();
        }
    }

    @Test
    @Ignore("Fixture capture logic")
    /*
     * Run this to capture test fixtures
     * 1. Update fixture name
     * 2. Comment @Ignore
     * 3. Run extraction
     */
    private static void extractTestFixtureData() throws IOException {

        String fixtureName = "sample_data";
        String host = "localhost";
        int port = 9200;

        String commonSuffix = "index_v2";
        int fetchSize = 1000;

        Set<String> searchIndexSuffixes = SEARCHABLE_ENTITY_TYPES.stream()
                .map(entityType -> entityType.toString().toLowerCase() + commonSuffix)
                .collect(Collectors.toSet());

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(host, port, "http"));

        try (RestHighLevelClient client = new RestHighLevelClient(builder)) {
            GetMappingsResponse response = client.indices().getMapping(new GetMappingsRequest().indices("*"),
                    RequestOptions.DEFAULT);
            response.mappings().keySet().stream()
                    .filter(index -> searchIndexSuffixes.stream().anyMatch(index::contains))
                    .map(index -> index.split(commonSuffix, 2)[0] + commonSuffix)
                    .forEach(indexName -> {

                        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                        searchSourceBuilder.size(fetchSize);
                        searchSourceBuilder.sort(SortBuilders.fieldSort("_id").order(SortOrder.ASC));

                        SearchRequest searchRequest = new SearchRequest(indexName);
                        searchRequest.source(searchSourceBuilder);

                        try {
                            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
                            SearchHits hits = searchResponse.getHits();
                            long remainingHits = hits.getTotalHits().value;

                            if (remainingHits > 0) {
                                try (FileWriter writer = new FileWriter(String.format("%s/%s/%s.json", FIXTURE_BASE, fixtureName, indexName));
                                     BufferedWriter bw = new BufferedWriter(writer)) {

                                    while (remainingHits > 0) {
                                        SearchHit lastHit = null;
                                        for (SearchHit hit : hits.getHits()) {
                                            lastHit = hit;
                                            remainingHits -= 1;
                                            bw.write(hit.getSourceAsString());
                                            bw.newLine();
                                        }
                                        if (lastHit != null) {
                                            searchSourceBuilder.searchAfter(lastHit.getSortValues());
                                            hits = client.search(searchRequest, RequestOptions.DEFAULT).getHits();
                                        }
                                    }
                                }
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    public static class UrnDocument {
        public String urn;
    }
}
