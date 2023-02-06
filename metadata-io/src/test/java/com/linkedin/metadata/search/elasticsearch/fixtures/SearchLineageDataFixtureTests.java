package com.linkedin.metadata.search.elasticsearch.fixtures;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.ESSearchLineageFixture;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import java.net.URISyntaxException;

import static com.linkedin.metadata.ESTestUtils.search;
import static com.linkedin.metadata.ESTestUtils.lineage;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


@Import(ESSearchLineageFixture.class)
public class SearchLineageDataFixtureTests extends AbstractTestNGSpringContextTests {

    @Autowired
    @Qualifier("searchLineageSearchService")
    protected SearchService searchService;

    @Autowired
    @Qualifier("searchLineageLineageSearchService")
    protected LineageSearchService lineageService;


    @Test
    public void testFixtureInitialization() {
        assertNotNull(searchService);
        SearchResult noResult = search(searchService, "no results");
        assertEquals(noResult.getEntities().size(), 0);

        SearchResult result = search(searchService, "e3859789eed1cef55288b44f016ee08290d9fd08973e565c112d8");
        assertEquals(result.getEntities().size(), 1);

        assertEquals(result.getEntities().get(0).getEntity().toString(),
                "urn:li:dataset:(urn:li:dataPlatform:9cf8c96,e3859789eed1cef55288b44f016ee08290d9fd08973e565c112d8,PROD)");

        LineageSearchResult lineageResult = lineage(lineageService, result.getEntities().get(0).getEntity(), 1);
        assertEquals(lineageResult.getEntities().size(), 10);
    }

    @Test
    public void testDatasetLineage() throws URISyntaxException {
        Urn testUrn = Urn.createFromString(
                "urn:li:dataset:(urn:li:dataPlatform:9cf8c96,e3859789eed1cef55288b44f016ee08290d9fd08973e565c112d8,PROD)");

        // 1 hops
        LineageSearchResult lineageResult = lineage(lineageService, testUrn, 1);
        assertEquals(lineageResult.getEntities().size(), 10);

        // 2 hops
        lineageResult = lineage(lineageService, testUrn, 2);
        assertEquals(lineageResult.getEntities().size(), 5);

        // 3 hops
        lineageResult = lineage(lineageService, testUrn, 3);
        assertEquals(lineageResult.getEntities().size(), 12);
    }
}
