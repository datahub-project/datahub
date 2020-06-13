package com.linkedin.metadata.restli;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.BrowseResult;
import com.linkedin.metadata.query.BrowseResultEntity;
import com.linkedin.metadata.query.BrowseResultEntityArray;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.testng.annotations.Test;

import static com.linkedin.testing.TestUtils.*;
import static org.testng.Assert.*;

public class BrowsableClientTest {

  public static class TestBrowsableClient implements BrowsableClient<Urn> {

    static final StringArray BROWSE_PATHS = new StringArray(Collections.singletonList("/a/b/foo"));

    static final BrowseResultEntityArray BROWSE_RESULT_ENTITIES = new BrowseResultEntityArray(
        ImmutableList.of(makeBrowseResultEntity("/foo/1", makeUrn(1)), makeBrowseResultEntity("/foo/2", makeUrn(2))));

    @Override
    @Nonnull
    public BrowseResult browse(@Nonnull String inputPath, @Nullable Map<String, String> requestFilters, int from, int size) {
      return new BrowseResult().setFrom(from).setPageSize(size).setNumEntities(2).setEntities(BROWSE_RESULT_ENTITIES);
    }

    @Override
    @Nonnull
    public StringArray getBrowsePaths(@Nonnull Urn urn) {
      return BROWSE_PATHS;
    }

    @Nonnull
    private static BrowseResultEntity makeBrowseResultEntity(String name, Urn urn) {
      return new BrowseResultEntity().setName(name).setUrn(urn);
    }
  }

  @Test
  public void testClient() {
    TestBrowsableClient testBrowsableClient = new TestBrowsableClient();

    BrowseResult result = testBrowsableClient.browse("/foo", new HashMap<>(), 0, 2);
    assertEquals(result.getEntities(), TestBrowsableClient.BROWSE_RESULT_ENTITIES);

    List<String> browsePaths = testBrowsableClient.getBrowsePaths(makeUrn(1));
    assertEquals(browsePaths, TestBrowsableClient.BROWSE_PATHS);
  }
}