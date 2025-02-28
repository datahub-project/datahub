package io.datahubproject.metadata.context;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import org.testng.annotations.Test;

public class SearchContextTest {

  @Test
  public void searchContextId() {
    SearchContext testNoFlags =
        SearchContext.builder().indexConvention(IndexConventionImpl.noPrefix("MD5")).build();

    assertEquals(
        testNoFlags.getCacheKeyComponent(),
        SearchContext.builder()
            .indexConvention(IndexConventionImpl.noPrefix("MD5"))
            .build()
            .getCacheKeyComponent(),
        "Expected consistent context ids across instances");

    SearchContext testWithFlags =
        SearchContext.builder()
            .indexConvention(IndexConventionImpl.noPrefix("MD5"))
            .searchFlags(new SearchFlags().setFulltext(true))
            .build();

    assertEquals(
        testWithFlags.getCacheKeyComponent(),
        SearchContext.builder()
            .indexConvention(IndexConventionImpl.noPrefix("MD5"))
            .searchFlags(new SearchFlags().setFulltext(true))
            .build()
            .getCacheKeyComponent(),
        "Expected consistent context ids across instances");

    assertNotEquals(
        testNoFlags.getCacheKeyComponent(),
        testWithFlags.getCacheKeyComponent(),
        "Expected differences in search flags to result in different caches");
    assertNotEquals(
        testWithFlags.getCacheKeyComponent(),
        SearchContext.builder()
            .indexConvention(IndexConventionImpl.noPrefix("MD5"))
            .searchFlags(new SearchFlags().setFulltext(true).setIncludeRestricted(true))
            .build()
            .getCacheKeyComponent(),
        "Expected differences in search flags to result in different caches");

    assertNotEquals(
        testNoFlags.getCacheKeyComponent(),
        SearchContext.builder()
            .indexConvention(
                new IndexConventionImpl(
                    IndexConventionImpl.IndexConventionConfig.builder()
                        .prefix("Some Prefix")
                        .hashIdAlgo("MD5")
                        .build()))
            .searchFlags(null)
            .build()
            .getCacheKeyComponent(),
        "Expected differences in index convention to result in different caches");

    assertNotEquals(
        SearchContext.builder()
            .indexConvention(IndexConventionImpl.noPrefix("MD5"))
            .searchFlags(
                new SearchFlags()
                    .setFulltext(false)
                    .setIncludeRestricted(true)
                    .setSkipAggregates(true))
            .build()
            .getCacheKeyComponent(),
        SearchContext.builder()
            .indexConvention(IndexConventionImpl.noPrefix("MD5"))
            .searchFlags(new SearchFlags().setFulltext(true).setIncludeRestricted(true))
            .build()
            .getCacheKeyComponent(),
        "Expected differences in search flags to result in different caches");
  }

  @Test
  public void testImmutableSearchFlags() {
    SearchContext initial =
        SearchContext.builder().indexConvention(IndexConventionImpl.noPrefix("MD5")).build();
    assertEquals(initial.getSearchFlags(), new SearchFlags().setSkipCache(false));

    SearchContext mutated = initial.withFlagDefaults(flags -> flags.setSkipCache(true));
    assertEquals(mutated.getSearchFlags(), new SearchFlags().setSkipCache(true));

    // ensure original is not changed
    assertEquals(initial.getSearchFlags(), new SearchFlags().setSkipCache(false));
  }
}
