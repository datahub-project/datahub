package com.linkedin.metadata.search.elasticsearch.fixtures;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.resolvers.EntityTypeMapper;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.ESSampleDataFixture;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.linkedin.metadata.ESTestUtils.searchAcrossEntities;
import static com.linkedin.metadata.ESTestUtils.searchAcrossLongtailEntities;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.*;

@Import(ESSampleDataFixture.class)
public class ElasticSearchGoldenTest extends AbstractTestNGSpringContextTests {

    private static final List<String> SEARCHABLE_LONGTAIL_ENTITIES = Stream.of(EntityType.CHART, EntityType.CONTAINER, EntityType.DASHBOARD, EntityType.DATASET, EntityType.DOMAIN, EntityType.TAG)
            .map(EntityTypeMapper::getName)
            .collect(Collectors.toList());
    @Autowired
    private RestHighLevelClient _searchClient;

    @Autowired
    @Qualifier("longTailSampleDataSearchService")
    protected SearchService searchService;

    @Autowired
    @Qualifier("longTailSampleDataEntityClient")
    protected EntityClient entityClient;

    @Autowired
    @Qualifier("longTailEntityRegistry")
    private EntityRegistry entityRegistry;

//    @BeforeClass
//    public void setup() {
//        SEARCHABLE_LONGTAIL_ENTITIES =
//    }

    @Test
    public void testNameMatchPetProfiles() {
        /*
          Searching for "pet profiles" should return "pet_profiles" as the first 2 search results
         */
        assertNotNull(searchService);
        assertNotNull(entityRegistry);
        SearchResult searchResult = searchAcrossLongtailEntities(searchService, "pet profiles");
        assertTrue(searchResult.getEntities().size() >= 2);
        Urn firstResultUrn = searchResult.getEntities().get(0).getEntity();
        Urn secondResultUrn = searchResult.getEntities().get(1).getEntity();

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
        Urn secondResultUrn = searchResult.getEntities().get(1).getEntity();

        assertTrue(firstResultUrn.toString().contains("pet_profiles"));
        assertTrue(secondResultUrn.toString().contains("pet_profiles"));
    }

    @Test
    public void testNameMatchMemberInWorkspace() {
        /*
          Searching for "collaborative actionitems" should return "collaborative_actionitems" as the first search
          result, followed by "collaborative_actionitems_old"
         */
        assertNotNull(searchService);
        SearchResult searchResult = searchAcrossEntities(searchService, "collaborative actionitems", SEARCHABLE_LONGTAIL_ENTITIES);
        assertTrue(searchResult.getEntities().size() >= 2);
        Urn firstResultUrn = searchResult.getEntities().get(0).getEntity();
        Urn secondResultUrn = searchResult.getEntities().get(1).getEntity();

        // Checks that the table name is not suffixed with anything
        assertTrue(firstResultUrn.toString().contains("collaborative_actionitems,"));
        assertTrue(secondResultUrn.toString().contains("collaborative_actionitems_old"));
    }

    @Test
    public void testGlossaryTerms() {
        /*
          Searching for "ReturnRate" should return all tables that have the glossary term applied before
          anything else
         */
        assertNotNull(searchService);
        SearchResult searchResult = searchAcrossEntities(searchService, "ReturnRate", SEARCHABLE_LONGTAIL_ENTITIES);
        SearchEntityArray entities = searchResult.getEntities();
        assertTrue(searchResult.getEntities().size() >= 4);
        MatchedFieldArray firstResultMatchedFields = entities.get(0).getMatchedFields();
        MatchedFieldArray secondResultMatchedFields = entities.get(1).getMatchedFields();
        MatchedFieldArray thirdResultMatchedFields = entities.get(2).getMatchedFields();
        MatchedFieldArray fourthResultMatchedFields = entities.get(3).getMatchedFields();

        assertTrue(firstResultMatchedFields.toString().contains("ReturnRate"));
        assertTrue(secondResultMatchedFields.toString().contains("ReturnRate"));
        assertTrue(thirdResultMatchedFields.toString().contains("ReturnRate"));
        assertTrue(fourthResultMatchedFields.toString().contains("ReturnRate"));
    }

//    @Test
//    public void testTrailingUnderscore() {
//
//    }

    /**
     *
     * The test below should be added back in as improvements are made to search,
     * via the linked tickets.
     *
     **/

    // TODO: enable once PFP-481 is complete
    @Test()
    public void testNameMatchPartiallyQualified() {
        /*
          Searching for "analytics.pet_details" (partially qualified) should return the fully qualified table
          name as the first search results before any others

          Current results:
          {score=131.7781982421875, features={SEARCH_BACKEND_SCORE=131.7781982421875}, matchedFields=[{name=urn, value=urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD)}, {name=fieldPaths, value=pet_fk}, {name=qualifiedName, value=long_tail_companions.analytics.pet_details}, {name=name, value=PET_DETAILS}, {name=id, value=long_tail_companions.analytics.pet_details}], entity=urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_details,PROD)}
          {score=101.43421936035156, features={SEARCH_BACKEND_SCORE=101.43421936035156}, matchedFields=[{name=urn, value=urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_status_history,PROD)}, {name=qualifiedName, value=long_tail_companions.analytics.pet_status_history}, {name=name, value=PET_STATUS_HISTORY}, {name=id, value=long_tail_companions.analytics.pet_status_history}], entity=urn:li:dataset:(urn:li:dataPlatform:snowflake,long_tail_companions.analytics.pet_status_history,PROD)}
          {score=92.98851013183594, features={SEARCH_BACKEND_SCORE=92.98851013183594}, matchedFields=[{name=urn, value=urn:li:dataset:(urn:li:dataPlatform:dbt,long_tail_companions.analytics.pet_details,PROD)}, {name=fieldPaths, value=pet_fk}, {name=name, value=pet_details}, {name=description, value=Table with all pet-related details}, {name=id, value=long_tail_companions.analytics.pet_details}], entity=urn:li:dataset:(urn:li:dataPlatform:dbt,long_tail_companions.analytics.pet_details,PROD)}
          {score=71.41470336914062, features={SEARCH_BACKEND_SCORE=71.41470336914062}, matchedFields=[{name=urn, value=urn:li:dataset:(urn:li:dataPlatform:dbt,long_tail_companions.analytics.pet_status_history,PROD)}, {name=name, value=pet_status_history}, {name=description, value=Incremental table containing all historical statuses of a pet}, {name=id, value=long_tail_companions.analytics.pet_status_history}], entity=urn:li:dataset:(urn:li:dataPlatform:dbt,long_tail_companions.analytics.pet_status_history,PROD)}
         */
        assertNotNull(searchService);
        SearchResult searchResult = searchAcrossEntities(searchService, "analytics.pet_details", SEARCHABLE_LONGTAIL_ENTITIES);
        assertTrue(searchResult.getEntities().size() >= 2);
        Urn firstResultUrn = searchResult.getEntities().get(0).getEntity();
        Urn secondResultUrn = searchResult.getEntities().get(1).getEntity();

        assertTrue(firstResultUrn.toString().contains("snowflake,long_tail_companions.analytics.pet_details"));
        assertTrue(secondResultUrn.toString().contains("dbt,long_tail_companions.analytics.pet_details"));
    }

}
