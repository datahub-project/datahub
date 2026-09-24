package io.datahubproject.metadata.context;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.utils.elasticsearch.ConfiguredIndexPrefixResolver;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SearchContextTest {

  private static EntityIndexConfiguration createDefaultEntityIndexConfiguration() {
    EntityIndexConfiguration config = new EntityIndexConfiguration();
    // Enable V2 by default for backward compatibility
    EntityIndexVersionConfiguration v2Config = new EntityIndexVersionConfiguration();
    v2Config.setEnabled(true);
    v2Config.setCleanup(true);
    config.setV2(v2Config);

    // Disable V3 by default for backward compatibility
    EntityIndexVersionConfiguration v3Config = new EntityIndexVersionConfiguration();
    v3Config.setEnabled(false);
    v3Config.setCleanup(false);
    config.setV3(v3Config);

    return config;
  }

  @Test
  public void searchContextId() {
    EntityIndexConfiguration entityIndexConfig = createDefaultEntityIndexConfiguration();

    SearchContext testNoFlags =
        SearchContext.builder()
            .indexConvention(IndexConventionImpl.noPrefix("MD5", entityIndexConfig))
            .build();

    assertEquals(
        testNoFlags.getCacheKeyComponent(),
        SearchContext.builder()
            .indexConvention(IndexConventionImpl.noPrefix("MD5", entityIndexConfig))
            .build()
            .getCacheKeyComponent(),
        "Expected consistent context ids across instances");

    SearchContext testWithFlags =
        SearchContext.builder()
            .indexConvention(IndexConventionImpl.noPrefix("MD5", entityIndexConfig))
            .searchFlags(new SearchFlags().setFulltext(true))
            .build();

    assertEquals(
        testWithFlags.getCacheKeyComponent(),
        SearchContext.builder()
            .indexConvention(IndexConventionImpl.noPrefix("MD5", entityIndexConfig))
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
            .indexConvention(IndexConventionImpl.noPrefix("MD5", entityIndexConfig))
            .searchFlags(new SearchFlags().setFulltext(true).setIncludeRestricted(true))
            .build()
            .getCacheKeyComponent(),
        "Expected differences in search flags to result in different caches");

    // The index-name prefix is intentionally NOT part of the SearchContext cache key: it is
    // resolved per operation (see SearchContext#getCacheKeyComponent), so two contexts differing
    // only by their index-prefix resolver now share a cache key. A deployment that scopes the
    // prefix per operation folds that discriminator into the OperationContext-level key instead.
    assertEquals(
        testNoFlags.getCacheKeyComponent(),
        SearchContext.builder()
            .indexConvention(
                new IndexConventionImpl(
                    IndexConventionImpl.IndexConventionConfig.builder().hashIdAlgo("MD5").build(),
                    new ConfiguredIndexPrefixResolver("Some Prefix"),
                    entityIndexConfig))
            .searchFlags(null)
            .build()
            .getCacheKeyComponent(),
        "Index-name prefix is intentionally excluded from the SearchContext cache key");

    assertNotEquals(
        SearchContext.builder()
            .indexConvention(IndexConventionImpl.noPrefix("MD5", entityIndexConfig))
            .searchFlags(
                new SearchFlags()
                    .setFulltext(false)
                    .setIncludeRestricted(true)
                    .setSkipAggregates(true))
            .build()
            .getCacheKeyComponent(),
        SearchContext.builder()
            .indexConvention(IndexConventionImpl.noPrefix("MD5", entityIndexConfig))
            .searchFlags(new SearchFlags().setFulltext(true).setIncludeRestricted(true))
            .build()
            .getCacheKeyComponent(),
        "Expected differences in search flags to result in different caches");
  }

  @Test
  public void testImmutableSearchFlags() {
    EntityIndexConfiguration entityIndexConfig = createDefaultEntityIndexConfiguration();

    SearchContext initial =
        SearchContext.builder()
            .indexConvention(IndexConventionImpl.noPrefix("MD5", entityIndexConfig))
            .build();
    assertEquals(initial.getSearchFlags(), new SearchFlags().setSkipCache(false));

    SearchContext mutated = initial.withFlagDefaults(flags -> flags.setSkipCache(true));
    assertEquals(mutated.getSearchFlags(), new SearchFlags().setSkipCache(true));

    // ensure original is not changed
    assertEquals(initial.getSearchFlags(), new SearchFlags().setSkipCache(false));
  }

  @Test
  public void testSearchClusterAccessIsExcludedFromCacheKeyAndEquals() {
    EntityIndexConfiguration entityIndexConfig = createDefaultEntityIndexConfiguration();
    IndexConvention convention = IndexConventionImpl.noPrefix("MD5", entityIndexConfig);
    SearchClusterAccess accessA = SearchClusterAccess.fixed(Mockito.mock(SearchClientShim.class));
    SearchClusterAccess accessB = SearchClusterAccess.fixed(Mockito.mock(SearchClientShim.class));

    SearchContext withoutAccess = SearchContext.builder().indexConvention(convention).build();
    SearchContext withAccess =
        SearchContext.builder().indexConvention(convention).searchClusterAccess(accessA).build();
    SearchContext otherAccess =
        SearchContext.builder().indexConvention(convention).searchClusterAccess(accessB).build();

    assertEquals(withoutAccess.getCacheKeyComponent(), withAccess.getCacheKeyComponent());
    assertEquals(withAccess.getCacheKeyComponent(), otherAccess.getCacheKeyComponent());
    assertEquals(withoutAccess, withAccess);
    assertEquals(withAccess, otherAccess);

    SearchContext copied = withAccess.withFlagDefaults(flags -> flags.setSkipCache(true));
    assertEquals(copied.getSearchClusterAccess(), accessA);
  }
}
