package com.linkedin.metadata.search.fixtures;

import static io.datahubproject.test.search.SearchTestUtils.lineage;
import static io.datahubproject.test.search.SearchTestUtils.searchAcrossEntities;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchService;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public abstract class LineageDataFixtureTestBase extends AbstractTestNGSpringContextTests {

  @Nonnull
  protected abstract LineageSearchService getLineageService();

  @Nonnull
  protected abstract SearchService getSearchService();

  @Test
  public void testFixtureInitialization() {
    assertNotNull(getSearchService());
    SearchResult noResult = searchAcrossEntities(getSearchService(), "no results");
    assertEquals(noResult.getEntities().size(), 0);

    SearchResult result =
        searchAcrossEntities(
            getSearchService(), "e3859789eed1cef55288b44f016ee08290d9fd08973e565c112d8");
    assertEquals(result.getEntities().size(), 1);

    assertEquals(
        result.getEntities().get(0).getEntity().toString(),
        "urn:li:dataset:(urn:li:dataPlatform:9cf8c96,e3859789eed1cef55288b44f016ee08290d9fd08973e565c112d8,PROD)");

    LineageSearchResult lineageResult =
        lineage(getLineageService(), result.getEntities().get(0).getEntity(), 1);
    assertEquals(lineageResult.getEntities().size(), 10);
  }

  @Test
  public void testDatasetLineage() throws URISyntaxException {
    Urn testUrn =
        Urn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:9cf8c96,e3859789eed1cef55288b44f016ee08290d9fd08973e565c112d8,PROD)");

    // 1 hops
    LineageSearchResult lineageResult = lineage(getLineageService(), testUrn, 1);
    assertEquals(lineageResult.getEntities().size(), 10);

    // 2 hops
    lineageResult = lineage(getLineageService(), testUrn, 2);
    assertEquals(lineageResult.getEntities().size(), 5);

    // 3 hops
    lineageResult = lineage(getLineageService(), testUrn, 3);
    assertEquals(lineageResult.getEntities().size(), 12);
  }
}
