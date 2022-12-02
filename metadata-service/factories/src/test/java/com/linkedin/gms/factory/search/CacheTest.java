package com.linkedin.gms.factory.search;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.spring.cache.HazelcastCacheManager;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.cache.CacheableSearcher;
import java.util.List;
import org.javatuples.Quintet;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CacheTest extends JetTestSupport {

    @Test
    public void hazelcastTest() {
        Config config = new Config();

        HazelcastInstance instance1 = createHazelcastInstance(config);
        HazelcastInstance instance2 = createHazelcastInstance(config);

        HazelcastCacheManager cacheManager1 = new HazelcastCacheManager(instance1);
        HazelcastCacheManager cacheManager2 = new HazelcastCacheManager(instance2);

        CorpuserUrn corpuserUrn = new CorpuserUrn("user");
        SearchEntity searchEntity = new SearchEntity().setEntity(corpuserUrn);
        SearchResult searchResult = new SearchResult()
            .setEntities(new SearchEntityArray(List.of(searchEntity)))
            .setNumEntities(1)
            .setFrom(0)
            .setPageSize(1)
            .setMetadata(new SearchResultMetadata());

        Quintet<List<String>, String, Filter, SortCriterion, CacheableSearcher.QueryPagination>
            quintet = Quintet.with(List.of(corpuserUrn.toString()), "*", null, null,
            new CacheableSearcher.QueryPagination(0, 1));

        CacheableSearcher<Quintet<List<String>, String, Filter, SortCriterion, CacheableSearcher.QueryPagination>> cacheableSearcher1 =
            new CacheableSearcher<>(cacheManager1.getCache("test"), 10,
            querySize -> searchResult,
            querySize -> quintet, null, true);

        CacheableSearcher<Quintet<List<String>, String, Filter, SortCriterion, CacheableSearcher.QueryPagination>> cacheableSearcher2 =
            new CacheableSearcher<>(cacheManager2.getCache("test"), 10,
                querySize -> searchResult,
                querySize -> quintet, null, true);

        // Cache result
        SearchResult result = cacheableSearcher1.getSearchResults(0, 1);
        Assert.assertNotEquals(result, null);

        Assert.assertEquals(instance1.getMap("test").get(quintet), instance2.getMap("test").get(quintet));
        Assert.assertEquals(cacheableSearcher1.getSearchResults(0, 1), searchResult);
        Assert.assertEquals(cacheableSearcher1.getSearchResults(0, 1), cacheableSearcher2.getSearchResults(0, 1));
    }
}
