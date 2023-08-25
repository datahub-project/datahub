package com.linkedin.metadata.search.elasticsearch.fixtures;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.ESLongTailFixture;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.*;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import org.apache.lucene.search.DoubleValuesSource;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.linkedin.metadata.ESTestConfiguration.syncAfterWrite;
import static com.linkedin.metadata.ESTestUtils.*;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.*;

@Import(ESLongTailFixture.class)
public class ElasticSearchGoldenTests extends AbstractTestNGSpringContextTests {

    private static final List<String> SEARCHABLE_LONGTAIL_ENTITIES = Stream.of(EntityType.CHART, EntityType.CONTAINER,
                    EntityType.DASHBOARD, EntityType.DATASET, EntityType.DOMAIN, EntityType.TAG
            ).map(EntityTypeMapper::getName)
            .collect(Collectors.toList());

    private static final Map<String, Long> entityDocCount = Map.of(
            EntityTypeMapper.getName(EntityType.CHART), 223L,
            EntityTypeMapper.getName(EntityType.CONTAINER), 70L,
            EntityTypeMapper.getName(EntityType.DASHBOARD), 43L,
            EntityTypeMapper.getName(EntityType.DATASET), 1898L,
            EntityTypeMapper.getName(EntityType.DOMAIN), 4L,
            EntityTypeMapper.getName(EntityType.TAG), 23L
    );

    @Autowired
    private RestHighLevelClient _searchClient;

    @Autowired
    @Qualifier("longTailSearchService")
    protected SearchService searchService;

    @Autowired
    @Qualifier("longTailEntityClient")
    protected EntityClient entityClient;

    @Autowired
    private EntityRegistry entityRegistry;

    @Autowired
    private ESBulkProcessor _bulkProcessor;

    @Autowired
    @Qualifier("longTailEntitySearchService")
    private EntitySearchService _entitySearchService;

//    @BeforeMethod
//    public void setupTest() {
//        for (String entity : SEARCHABLE_LONGTAIL_ENTITIES) {
//            Long docCount = _entitySearchService.docCount(entity);
//            entityDocCount.putIfAbsent(entity, docCount);
//        }
//    }

    @BeforeMethod
    public void setupMethod() throws Exception {
        syncAfterWrite(_bulkProcessor);

        System.out.println("entityDocCount: " + entityDocCount);
        for (String entity : SEARCHABLE_LONGTAIL_ENTITIES) {
            Long docCount = _entitySearchService.docCount(entity);
            assertEquals(entityDocCount.get(entity), docCount);
        }
    }

    @BeforeMethod
    public void sync() throws Exception {
        syncAfterWrite(_bulkProcessor);
    }

    @Test
    public void testNameMatchPetProfiles() {
        /*
          Searching for "pet profiles" should return "pet_profiles" as the first 2 search results
         */
        assertNotNull(searchService);
        assertNotNull(entityRegistry);

        SearchResult searchResult = searchAcrossCustomEntities(searchService, "pet profiles", SEARCHABLE_LONGTAIL_ENTITIES);
        assertTrue(searchResult.getEntities().size() >= 2);
        Urn firstResultUrn = searchResult.getEntities().get(0).getEntity();
        System.out.println("firstResultUrn: " + firstResultUrn);
        Urn secondResultUrn = searchResult.getEntities().get(1).getEntity();
        System.out.println("secondResultUrn: " + secondResultUrn);

        assertTrue(firstResultUrn.toString().contains("pet_profiles"));
        assertTrue(secondResultUrn.toString().contains("pet_profiles"));
    }

    @Test
    public void testNameMatchPetProfile() {
        /*
          Searching for "pet profile" should return "pet_profiles" as the first 2 search results
         */
        assertNotNull(searchService);
        SearchResult searchResult = searchAcrossEntities(searchService, "pet profile", SEARCHABLE_LONGTAIL_ENTITIES);
        assertTrue(searchResult.getEntities().size() >= 2);
        Urn firstResultUrn = searchResult.getEntities().get(0).getEntity();
        System.out.println("firstResultUrn: " + firstResultUrn);
        Urn secondResultUrn = searchResult.getEntities().get(1).getEntity();
        System.out.println("secondResultUrn: " + secondResultUrn);

        assertTrue(firstResultUrn.toString().contains("pet_profiles"));
        assertTrue(secondResultUrn.toString().contains("pet_profiles"));
    }

    @Test
    public void testGlossaryTerms() {
        /*
          Searching for "ReturnRate" should return all tables that have the glossary term applied before
          anything else
         */
        assertNotNull(searchService);
        SearchResult searchResult = searchAcrossEntities(searchService, "ReturnRate", SEARCHABLE_LONGTAIL_ENTITIES);
        System.out.println("GLOSSARY TERMS RESULTS: " + searchResult);
        SearchEntityArray entities = searchResult.getEntities();
        assertTrue(searchResult.getEntities().size() >= 2);
        MatchedFieldArray firstResultMatchedFields = entities.get(0).getMatchedFields();
        System.out.println("firstResultMatchedFields: " + firstResultMatchedFields);
        MatchedFieldArray secondResultMatchedFields = entities.get(1).getMatchedFields();
        System.out.println("secondResultMatchedFields: " + secondResultMatchedFields);
        MatchedFieldArray thirdResultMatchedFields = entities.get(2).getMatchedFields();
        System.out.println("thirdResultMatchedFields: " + thirdResultMatchedFields);
        MatchedFieldArray fourthResultMatchedFields = entities.get(3).getMatchedFields();
        System.out.println("fourthResultMatchedFields: " + fourthResultMatchedFields);

        assertTrue(firstResultMatchedFields.toString().contains("ReturnRate"));
        assertTrue(secondResultMatchedFields.toString().contains("ReturnRate"));
        assertTrue(thirdResultMatchedFields.toString().contains("ReturnRate"));
        assertTrue(fourthResultMatchedFields.toString().contains("ReturnRate"));
    }

    @Test
    public void testNameMatchPartiallyQualified() {
        /*
          Searching for "analytics.pet_details" (partially qualified) should return the fully qualified table
          name as the first search results before any others
         */
        assertNotNull(searchService);
        SearchResult searchResult = searchAcrossEntities(searchService, "analytics.pet_details", SEARCHABLE_LONGTAIL_ENTITIES);
        assertTrue(searchResult.getEntities().size() >= 2);
        Urn firstResultUrn = searchResult.getEntities().get(0).getEntity();
        System.out.println("firstResultUrn: " + firstResultUrn);
        Urn secondResultUrn = searchResult.getEntities().get(1).getEntity();
        System.out.println("secondResultUrn: " + secondResultUrn);

        assertTrue(firstResultUrn.toString().contains("long_tail_companions.analytics.pet_details"));
        assertTrue(secondResultUrn.toString().contains("long_tail_companions.analytics.pet_details"));
    }

    @Test
    public void testNameMatchCollaborativeActionitems() {
        /*
          Searching for "collaborative actionitems" should return "collaborative_actionitems" as the first search
          result, followed by anything else that includes "collaborative_actionitems"
         */
        assertNotNull(searchService);
        SearchResult searchResult = searchAcrossEntities(searchService, "collaborative actionitems", SEARCHABLE_LONGTAIL_ENTITIES);
        assertTrue(searchResult.getEntities().size() >= 2);
        Urn firstResultUrn = searchResult.getEntities().get(0).getEntity();
        System.out.println("firstResultUrn: " + firstResultUrn);
        Urn secondResultUrn = searchResult.getEntities().get(1).getEntity();
        System.out.println("secondResultUrn: " + secondResultUrn);

        // Checks that the table name is not suffixed with anything
        assertTrue(firstResultUrn.toString().contains("collaborative_actionitems,"));
        assertTrue(secondResultUrn.toString().contains("collaborative_actionitems"));

        Double firstResultScore = searchResult.getEntities().get(0).getScore();
        System.out.println("firstResultScore: " + firstResultScore);
        Double secondResultScore = searchResult.getEntities().get(1).getScore();
        System.out.println("secondResultScore: " + secondResultScore);

        // Checks that the scores aren't tied so that we are matching on table name more than column name
        assertTrue(firstResultScore > secondResultScore);
    }

    @Test
    public void testNameMatchCustomerOrders() {
        /*
          Searching for "customer orders" should return "customer_orders" as the first search
          result, not suffixed by anything
         */
        assertNotNull(searchService);
        SearchResult searchResult = searchAcrossEntities(searchService, "customer orders", SEARCHABLE_LONGTAIL_ENTITIES);
        assertTrue(searchResult.getEntities().size() >= 2);
        Urn firstResultUrn = searchResult.getEntities().get(0).getEntity();
        System.out.println("firstResultUrn: " + firstResultUrn);

        // firstResultUrn: urn:li:dataset:(urn:li:dataPlatform:dbt,calm-pagoda-323403.jaffle_shop.orders,PROD)

        // Checks that the table name is not suffixed with anything
        assertTrue(firstResultUrn.toString().contains("customer_orders,"));

        Double firstResultScore = searchResult.getEntities().get(0).getScore();
        System.out.println("firstResultScore: " + firstResultScore);
        Double secondResultScore = searchResult.getEntities().get(1).getScore();
        System.out.println("secondResultScore: " + secondResultScore);

        // Checks that the scores aren't tied so that we are matching on table name more than column name
        assertTrue(firstResultScore > secondResultScore);
    }

    /*
      Tests that should pass but do not yet can be added below here, with the following annotation:
      @Test(enabled = false)
     */

}
