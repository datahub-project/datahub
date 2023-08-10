package com.linkedin.metadata.search.elasticsearch.fixtures;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.ESSampleDataFixture;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.MatchedFieldArray;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.EntityIndexBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import javax.annotation.Nonnull;
import java.io.IOException;

import static com.linkedin.metadata.ESTestUtils.searchAcrossEntities;
import static org.testng.AssertJUnit.*;

@Import(ESSampleDataFixture.class)
public class ElasticSearchGoldenTest extends AbstractTestNGSpringContextTests {

    @Autowired
    @Qualifier("longTailSampleDataSearchService")
    protected SearchService searchService;

    @Test
    public void testNameMatchPetProfiles() {
        /*
          Searching for "pet profiles" should return "pet_profiles" as the first 2 search results
         */
        assertNotNull(searchService);
        SearchResult searchResult = searchAcrossEntities(searchService, "pet profiles");
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
        SearchResult searchResult = searchAcrossEntities(searchService, "pet profile");
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
        SearchResult searchResult = searchAcrossEntities(searchService, "collaborative actionitems");
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
        SearchResult searchResult = searchAcrossEntities(searchService, "ReturnRate");
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

    /**
     *
     * The test below should be added back in as improvements are made to search,
     * via the linked tickets.
     *
     **/

    // TODO: enable once PFP-481 is complete
    @Test(enabled = false)
    public void testNameMatchPartiallyQualified() {
        /*
          Searching for "analytics.pet_details" (partially qualified) should return the fully qualified table
          name as the first search results before any others
         */
        assertNotNull(searchService);
        SearchResult searchResult = searchAcrossEntities(searchService, "analytics.pet_details");
        assertTrue(searchResult.getEntities().size() >= 2);
        Urn firstResultUrn = searchResult.getEntities().get(0).getEntity();
        Urn secondResultUrn = searchResult.getEntities().get(1).getEntity();

        assertTrue(firstResultUrn.toString().contains("snowflake,long_tail_companions.analytics.pet_details"));
        assertTrue(secondResultUrn.toString().contains("dbt,long_tail_companions.analytics.pet_details"));
    }

}
